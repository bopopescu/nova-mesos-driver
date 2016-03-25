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
from kazoo.client import KazooClient
from kazoo.client import KazooState

__state__ = 0
__store__ = None

def listener(state):
    global __state__
    if state == KazooState.LOST:
        # Register somewhere that the session was lost
        __state__ = 0
    elif state == KazooState.SUSPENDED:
        # Handle being disconnected from Zookeeper
        __state__ = 2
    else:
        # Handle being connected/reconnected to Zookeeper
        __state__ = 1

class ZKStore:
    def __init__(self, hosts):
        self.zk = KazooClient(hosts=hosts)
        self.zk.add_listener(listener)
        self.zk.start()


    def isConnected(self):
        if __state__ == 1:
            return True
        return False


    def write(self, path, node, value):
        self.zk.ensure_path(path)
        if self.zk.exists(path+"/"+node):
           self.zk.set(path+"/"+node, value)
        else:
           self.zk.create(path + "/" + node, value)


    def read(self, path):
        if self.zk.exists(path):
            data, stat = self.zk.get(path)
            return data
        return None



def getZKStore(hosts):
    global __store__
    if __store__ is not None:
        return __store__
    __store__ = ZKStore(hosts)
    return __store__