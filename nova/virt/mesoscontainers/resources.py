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

import threading

class Resources(object):
    vcpus = 0
    memory_mb = 0
    local_gb = 0
    vcpus_used = 0
    memory_mb_used = 0
    local_gb_used = 0
    free_disk_gb = 0
    free_ram_mb = 0
    lock = threading.Lock()

    def __init__(self, vcpus=8, memory_mb=8000, local_gb=500):
        self.vcpus = vcpus
        self.memory_mb = memory_mb
        self.local_gb = local_gb

    def add_offer(self, vcpu, memory, disk):
        self.vcpus = self.vcpus_used +  vcpu
        self.memory_mb = self.memory_mb_used + memory
        self.local_gb = self.local_gb_used + disk
        self.free_disk_gb += disk
        self.free_ram_mb += memory

    def delete_offer(self, offer):
        cpus = next(rsc.scalar.value for rsc in offer.resources if rsc.name == "cpus")
        mem = next(rsc.scalar.value for rsc in offer.resources if rsc.name == "mem")
        disk = next(rsc.scalar.value for rsc in offer.resources if rsc.name == "disk") / 1024
        self.vcpus -= cpus
        self.memory_mb -= mem
        #TODO(gokrokve) Fix this
        self.local_gb -= disk
        self.free_disk_gb -= disk

    def claim(self, vcpus=0, mem=0, disk=0):
        self.lock.acquire()
        self.vcpus_used += vcpus
        self.memory_mb_used += mem
        self.local_gb_used += disk
        self.lock.release()

    def release(self, vcpus=0, mem=0, disk=0):
        self.lock.acquire()
        self.vcpus_used -= vcpus
        self.memory_mb_used -= mem
        self.local_gb_used -= disk
        self.free_disk_gb += disk
        self.lock.release()

    def dump(self):
        try:
            self.lock.acquire()
            return {
                'vcpus': self.vcpus,
                'memory_mb': self.memory_mb,
                'local_gb': self.local_gb,
                'vcpus_used': self.vcpus_used,
                'memory_mb_used': self.memory_mb_used,
                'local_gb_used': self.local_gb_used,
                'free_disk_gb': self.free_disk_gb,
                'free_ram_mb' : self.free_ram_mb
            }
        finally:
            self.lock.release()

resources = None

def get_resources():
    global resources
    if not resources:
        resources = Resources(0, 0, 0)
    return resources