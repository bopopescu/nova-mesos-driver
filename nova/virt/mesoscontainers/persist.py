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
import socket
import json


from oslo_config import cfg
from nova.virt.mesoscontainers.instance import Instance
from nova.virt.mesoscontainers.zk_store import getZKStore

CONF = cfg.CONF

class PersistentStorage(object):
    zk = None
    host = None
    def __init__(self):
        self.store = {}
        self.store_task_id={}
        self.persist_zk = CONF.mesos.persist_in_zk
        if self.persist_zk:
           self.zk = getZKStore(CONF.mesos.zk_hosts)
           self.host = socket.gethostname()
           self.load_from_zk()

    def load_from_zk(self):
        inst_list_str = self.zk.read("/nova-mesos/" + self.host + "/instances")
        if inst_list_str:
            inst_list = json.loads(inst_list_str)
            for instance in inst_list:
                instance_str = self.zk.read("/nova-mesos/"+self.host+"/"+instance)
                inst = Instance(None, None, None, instance_str)
                self.store[inst.uuid] = inst
                self.store_task_id[inst.task_id] = inst



    def persist(self, item):
        id = item.uuid # We will require item to have an id
        self.store[id] = item
        self.store_task_id[item.task_id] = item
        if self.persist_zk:
            self.zk.write("/nova-mesos/"+self.host,id,bytes(item.serialize()))
            inst_list_str = self.zk.read("/nova-mesos/"+self.host+"/instances")
            if inst_list_str is None:
                inst_list = []
            else:
                inst_list = json.loads(inst_list_str)
            inst_list.append(id)
            upd_list_str = json.dumps(inst_list)
            self.zk.write("/nova-mesos/"+self.host, "instances", bytes(upd_list_str))

    def getItem(self, id):
        return self.store.get(id, None)

    def hasItem(self, id):
        return self.store.has_key(id)

    def listIDs(self):
        return self.store.keys()

    def deleteItem(self, id):
        item = self.store.get(id, None)
        del self.store[id]
        if item is not None:
            del self.store_task_id[item.task_id]
        if self.persist_zk:
            inst_list_str = self.zk.read("/nova-mesos/" + self.host + "/instances")
            if inst_list_str is not None:
                inst_list = json.loads(inst_list_str)
                inst_list.remove(id)
                upd_list_str = json.dumps(inst_list)
                self.zk.write("/nova-mesos/" + self.host, "instances", bytes(upd_list_str))
                self.zk.zk.delete("/nova-mesos/" + self.host +"/" + id, recursive=True)

    def getByTaskId(self, task_id):
        return self.store_task_id.get(task_id, None)

    def storeByTask(self, task_id, item):
        self.store_task_id[task_id] = item




__persistent_storage = None
def get_persistent_storage():
    global __persistent_storage
    if __persistent_storage is None:
        __persistent_storage = PersistentStorage()

    return __persistent_storage

