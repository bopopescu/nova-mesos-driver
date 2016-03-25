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

import json

class Instance(object):
    offer = None
    #OPenStack part
    instance_info = None
    image_meta = None
    network_info = None
    volume_info_list = []
    #We need to store task Id as taskInfo will not be serialized
    task_id = None
    #Mesos part (notice different style)
    taskInfo = None
    status = 0
    state = 0
    name = ""

    def __init__(self, instanceInfo=None, networkInfo=None, imageMeta=None, string_rep=None):
        if string_rep is not None:
            self.deserialize(string_rep)
        else:
            self.instance_info = instanceInfo
            self.network_info = networkInfo
            self.image_meta = imageMeta
            flavor = instanceInfo.flavor
            self.req_vcpus=flavor.vcpus
            self.req_mem=flavor.memory_mb
            self.req_disk=flavor.root_gb
            self.uuid = instanceInfo.uuid
            self.ip = self.get_network_ip(networkInfo)
            self.network_id = self.get_network_id(networkInfo)
            self.image_name = imageMeta['name']

    def get_network_ip(self, network_info):
        vif = network_info[0]  # Lets take first interface
        for subnet in vif['network']['subnets']:
            if subnet['version'] == 4:  # Let's take only IPv4
                ip = subnet['ips'][0]['address']  # Let's take first IP we have
                return ip
        return None

    def get_network_id(self, network_info):
        vif = network_info[0]  # Lets take first interface
        return vif['network']['id']


    def serialize(self):
        instance = {
            "req_vcpus": self.req_vcpus,
            "req_mem": self.req_mem,
            "req_disk": self.req_disk,
            "uuid": self.uuid,
            "ip": self.ip,
            "network_id": self.network_id ,
            "image_name": self.image_name,
            "task_id": self.task_id,
            "status": self.status,
            "state": self.state,
            "name": self.name
        }
        return json.dumps(instance)

    def deserialize(self, string):
        instance=json.loads(string)
        self.req_vcpus=instance["req_vcpus"]
        self.req_mem =instance["req_mem"]
        self.req_disk = instance["req_disk"]
        self.uuid =instance["uuid"]
        self.ip =instance["ip"]
        self.network_id=instance["network_id"]
        self.image_name=instance["image_name"]
        self.task_id=instance["task_id"]
        self.status=instance["status"]
        self.state = instance.get("state",1)
        self.name=instance.get("name","")

