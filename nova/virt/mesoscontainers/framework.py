#!/usr/bin/env python
# coding=utf-8
#
# Copyright 2016 Mirantis Inc.
# All Rights Reserved.
# Author: Georgy Okrokvertskhov
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.


from collections import deque
import json
import os
import signal
import sys
import socket
import time
import task_state
import datetime

from threading import Thread
from zk_store import getZKStore
from oslo_log import log as logging
from oslo_config import cfg

from pymesos import  MesosExecutorDriver, MesosSchedulerDriver
from mesos.interface import Scheduler, Executor
from mesos.interface import mesos_pb2

from nova.virt.mesoscontainers import resources
from nova.virt.mesoscontainers.persist import get_persistent_storage

mesos_opts = [
    cfg.IntOpt('offers_limit',
                default=2,
                help='Limits the number of accepted offers '),
    cfg.StrOpt('zk_hosts',
               default="127.0.0.1:2881",
               help='Comma separated list of zk hosts'),
    cfg.BoolOpt('persist_in_zk',
                default=False,
                help='Persist instance info in ZK'),
]

CONF = cfg.CONF
CONF.register_opts(mesos_opts, 'mesos')



LOG = logging.getLogger(__name__)

TASK_CPUS = 0.1
TASK_MEM = 32
SHUTDOWN_TIMEOUT = 30  # in seconds
TASK_ATTEMPTS = 5  # how many times a task is attempted
TASK_PREFIX = "nova-"
OFFERS_MAX = CONF.mesos.offers_limit

class VolumeInfo:
    target = None
    mount_point = None
    def __init__(self, target, mountpoint, type):
        self.mount_point = mountpoint
        self.target = target
        self.type = type

# See the Mesos Framework Development Guide:
# http://mesos.apache.org/documentation/latest/app-framework-development-guide
class OfferInfo:
    offer_id = None
    offer = None
    cpus = None
    mem = None
    cpu_used = None
    mem_used = None

    def __init__(self, cpus, mem, offer):
        self.cpus = cpus
        self.mem = mem
        self.offer = offer
        self.offer_id = offer.id.value
        self.cpu_used = 0
        self.mem_used = 0

class NovaFramework(Scheduler):
    def __init__(self, queue_size, maxRenderTasks):
        self.taskQueue = deque([])
        self.tasksCreated  = 0
        self.tasksRunning = 0
        self.tasksFailed = 0
        self.tasksRetrying = {}
        self.resources = resources.get_resources()
        self.offers = []
        self.tasks = {}
        self.shuttingDown = False
        self.persistent_store = get_persistent_storage()

    def registered(self, driver, frameworkId, masterInfo):
        #TODO(gokrokve) Framework ID should be saved as Mesos uses it for re-regestering
        print "Registered with framework ID [%s]" % frameworkId.value
        zk = getZKStore(ZK_HOSTS)
        host = socket.gethostname()
        id = frameworkId.value
        if zk.isConnected():
            zk.write("/nova-mesos/" + host, '/id', bytes(id))

    def makeTaskPrototype(self, offer, instance):


        task = mesos_pb2.TaskInfo()
        tid = self.tasksCreated
        self.tasksCreated += 1
        task.task_id.value = str(tid).zfill(5)
        task.slave_id.value = offer.slave_id.value
        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = instance.req_vcpus
        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = instance.req_mem

        disk = task.resources.add()
        disk.name = "disk"
        disk.type = mesos_pb2.Value.SCALAR
        disk.scalar.value = instance.req_disk * 1024 #Nova pass disk in Gb
        return task



    def new_docker_task(self, offer, instance_object):

        instance = instance_object.instance_info
        image_meta = instance_object.image_meta
        network_info = instance_object.network_info
        volume_info_list = instance_object.volume_info_list

        task = self.makeTaskPrototype(offer, instance_object)
        # We want of container of type Docker
        container = mesos_pb2.ContainerInfo()
        container.type = 1  # mesos_pb2.ContainerInfo.Type.DOCKER


        # Let's create a volume
        # container.volumes, in mesos.proto, is a repeated element
        # For repeated elements, we use the method "add()" that returns an object that can be updated
        if volume_info_list:
            for volume_info in volume_info_list:
                if volume_info.type == "rbd":
                  volume = container.volumes.add()
                  #TODO(gokrokve) fix volume mountpoints. Here we hardcode
                  # a volume path to /mnt/volume
                  # originally it was /mnt/<volume name>
                  volume.container_path = "/mnt/volume" #+ volume_info.target['name'] # Path in container
                  volume.host_path = volume_info.target['name']  # Path on host
                  volume.mode = 1  # mesos_pb2.Volume.Mode.RW
            # volume.mode = 2 # mesos_pb2.Volume.Mode.RO

        # Define the command line to execute in the Docker container
        # Command value is empry to run container's default
        # Shell is disabled as well
        #executorInfo = mesos_pb2.ExecutorInfo()
        #executorInfo.executor_id.value = task.task_id.value
        command = mesos_pb2.CommandInfo()
        command.shell = False

        task.command.MergeFrom(command)
        task.name = "nova-mesos-01"

        # Let's use Docker containers
        docker = mesos_pb2.ContainerInfo.DockerInfo()
        docker.image = instance_object.image_name
        docker.force_pull_image = False
        #TODO(gokrokve) Fix networking type to support new network model in docker
        docker.network = 2  # mesos_pb2.ContainerInfo.DockerInfo.Network.BRIDGE
        docker.volume_driver = "rbd"
        # Set docker info in container.docker

        container.docker.MergeFrom(docker)
        network = container.network_infos.add()
        #TODO(gokrokve) collect ip addresses from network_info[][subnets]

        network.ip_address=instance_object.ip
        network.protocol = 1
        labels = mesos_pb2.Labels()
        label = labels.labels.add()
        label.key = "network_id"
        #TODO(gokrokve) add proper network id
        label.value = instance_object.network_id
        #label.value = "none"
        network.labels.MergeFrom(labels)
        # Set docker container in task.container
        task.container.MergeFrom(container)
        #task.executor.container.MergeFrom(container)
        # Return the object
        return task

    def get_network_ip(self, network_info):
        vif = network_info[0] #Lets take first interface
        for subnet in vif['network']['subnets']:
            if subnet['version'] == 4:  # Let's take only IPv4
                ip = subnet['ips'][0]['address']  #Let's take first IP we have
                return ip
        return None

    def get_network_id(self, network_info):
        vif = network_info[0]  # Lets take first interface
        return vif['network']['id']

    def retryTask(self, task_id, url):
        instance = self.tasks.get(task_id, None)
        if instance is not None:
            LOG.info("Task %s has been rejected. Reschedule task" % task_id)
            del self.tasks[task_id]
            self.taskQueue.append(instance)
            self.submit_tasks()

    def printStatistics(self):
        print "Queue length: %d  Tasks: %d running, %d failed" % (
          len(self.taskQueue), self.tasksRunning, self.tasksFailed
        )

    def maxTasksForOffer(self, offer):
        count = 0
        cpus = next(rsc.scalar.value for rsc in offer.resources if rsc.name == "cpus")
        mem = next(rsc.scalar.value for rsc in offer.resources if rsc.name == "mem")
        while cpus >= TASK_CPUS and mem >= TASK_MEM:
            count += 1
            cpus -= TASK_CPUS
            mem -= TASK_MEM
        return count

    def resourceOffers(self, driver, offers):
        self.printStatistics()
        #TODO(gokrokve) In Kolla-Mesos approach slaves are tagged with special tag (compute, or controller)
        #Here we need to filter out offers from computes and never touch controller offers

        for offer in offers:
            if len(self.offers) > OFFERS_MAX:
                driver.declineOffer(offer.id, mesos_pb2.Filters())
            else:
                print "Got resource offer [%s]" % offer.id.value
                for rsc in offer.resources:
                    LOG.info("Resource %s value: %s" % (rsc.name, rsc.scalar.value))
                cpus = next(rsc.scalar.value for rsc in offer.resources if rsc.name == "cpus")
                mem = next(rsc.scalar.value for rsc in offer.resources if rsc.name == "mem")
                disk = next(rsc.scalar.value for rsc in offer.resources if rsc.name == "disk") / 1024
                print "Offer info: %d cpu %d mem %d disk" % (cpus, mem, disk)
                self.resources.add_offer(cpus,mem, disk)
                offerInfo = OfferInfo(cpus, mem, offer)
                self.offers.append(offerInfo)

                if self.shuttingDown:
                    print "Shutting down: declining offer on [%s]" % offer.hostname
                    driver.declineOffer(offer.id)
                    continue


        self.submit_tasks()


    def scheduleTask(self, instance):
        self.taskQueue.append(instance)
        self.submit_tasks()

    def deleteTask(self, instance):
        task_id = mesos_pb2.TaskID()
        task_id.value = instance.task_id
        driver.killTask(task_id)

    def submit_tasks(self):
        prev_q_len=0
        prev_task_len=0
        if (len(self.offers) >0) and (len(self.taskQueue)>0):
            list = self.taskQueue.__copy__()
            for instance in list:
                offer = self.select_offer(instance)
                if offer:
                    instance.offer = offer.offer
                    task = self.new_docker_task(offer.offer, instance)
                    # TODO(gokrokve) this is not thread safe. Should be properly changed
                    instance.taskInfo = task
                    instance.task_id = task.task_id.value
                    tasks = []
                    tasks.append(task)
                    LOG.info("Removing offer for a task")
                    self.offers.remove(offer)
                    self.resources.delete_offer(offer.offer)
                    self.taskQueue.remove(instance)
                    driver.launchTasks(offer.offer.id, tasks, mesos_pb2.Filters())
                    self.tasks[task.task_id.value] = instance
                    self.persistent_store.storeByTask(task.task_id.value, instance)
                if len(self.offers) == 0:
                    #No more offers available. Lets wait for new offers
                    break



    def select_offer(self, instance):
        #flavor = instance.instance_info.flavor
        req_vcpus=instance.req_vcpus
        req_mem=instance.req_mem
        req_disk=instance.req_disk

        for offer in self.offers:
            if ((offer.cpus - offer.cpu_used) > req_vcpus) and ((offer.mem - offer.mem_used) > req_mem):
                offer.cpu_used += req_vcpus
                offer.mem_used += req_mem
                return offer
        return None


    def statusUpdate(self, driver, update):
        stateName = task_state.nameFor[update.state]
        print "Task [%s] is in state [%s]" % (update.task_id.value, stateName)
        task = self.persistent_store.getByTaskId(update.task_id.value)

        if update.state == 1:  # Running
            self.tasksRunning += 1
            if task:
                task.status = 1
                task.state = 0x01

        elif update.state == 3:  # Failed, retry
            print "Task [%s] failed with message \"%s\"" \
              % (update.task_id.value, update.message)
            self.tasksRunning -= 1
            self.retryTask(update.task_id.value, update.data)
            task.state = 0x0
            return

        elif update.state > 1: # Terminal state
            if self.tasksRunning > 0:
                self.tasksRunning -= 1
            if task is not None:
               task.status = 2
               task.state = 0x04 #PowerOFF

    def frameworkMessage(self, driver, executorId, slaveId, message):
        o = json.loads(message)
        print "Message: %s" % o
        # if executorId.value == crawlExecutor.executor_id.value:

    def reconcile(self):
        driver.reconcileTasks()



def hard_shutdown():
    driver.stop()

def graceful_shutdown(signal, frame):
    print "Nova is shutting down"
    nova.shuttingDown = True

    wait_started = datetime.datetime.now()
    while (nova.tasksRunning > 0) and \
      (SHUTDOWN_TIMEOUT > (datetime.datetime.now() - wait_started).total_seconds()):
        time.sleep(1)

    if (nova.tasksRunning > 0):
        print "Shutdown by timeout, %d task(s) have not completed" % rendler.tasksRunning

    hard_shutdown()

#
# Execution entry point:
#
# "172.20.10.7:2181,172.20.10.8:2181,172.20.10.9:2181"

ZK_HOSTS = CONF.mesos.zk_hosts

zk = getZKStore(ZK_HOSTS)
host = socket.gethostname()
if not zk.isConnected():
    time.sleep(1)

if not zk.isConnected():
   LOG.error("Can't connect to ZK. Continue with ZK less mode")
else:
    framework_id = zk.read("/nova-mesos/"+host+'/id')


framework = mesos_pb2.FrameworkInfo()
framework.user = "root"  # Have Mesos fill in the current user.
framework.name = "Nova"
framework.failover_timeout = 604800

if framework_id is not None:
    framework.id.value = framework_id



try:
    maxRenderTasks = int(3)
except:
    maxRenderTasks = 0
nova = NovaFramework(5, maxRenderTasks)

#TODO (gokrokve) fix this
driver = MesosSchedulerDriver(nova, framework, "zk://"+ZK_HOSTS+"/mesos")
#driver = MesosSchedulerDriver(nova, framework, "192.168.86.197:5050")

def init():

    # baseURI = "/home/vagrant/hostfiles"
    # suffixURI = "python"
    # uris = [ "crawl_executor.py",
    #          "export_dot.py",
    #          "render_executor.py",
    #          "results.py",
    #          "task_state.py" ]
    # uris = [os.path.join(baseURI, suffixURI, uri) for uri in uris]
    # uris.append(os.path.join(baseURI, "render.js"))
    #
    # crawlExecutor = mesos_pb2.ExecutorInfo()
    # crawlExecutor.executor_id.value = "crawl-executor"
    # crawlExecutor.command.value = "python crawl_executor.py"


    # crawlExecutor.name = "Crawler"
    #
    # renderExecutor = mesos_pb2.ExecutorInfo()
    # renderExecutor.executor_id.value = "render-executor"
    # renderExecutor.command.value = "python render_executor.py --local"


    # renderExecutor.name = "Renderer"
    #


    # driver.run() blocks; we run it in a separate thread
    def run_driver_async():
        status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1
        driver.stop()
        sys.exit(status)
    framework_thread = Thread(target = run_driver_async, args = ())
    framework_thread.start()
    signal.signal(signal.SIGINT, graceful_shutdown)
