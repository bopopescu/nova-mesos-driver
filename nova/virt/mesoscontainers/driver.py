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
# This file is based on nova/virt/fake.py from http://github.com/openstack/nova

import collections
import contextlib
import eventlet

from oslo_config import cfg
from oslo_log import log as logging
from oslo_serialization import jsonutils
from oslo_utils import versionutils

from nova.compute import arch
from nova.compute import hv_type
from nova.compute import power_state
from nova.compute import task_states
from nova.compute import vm_mode
from nova.console import type as ctype
from nova import exception
from nova.i18n import _LW
from nova.virt import diagnostics
from nova.virt import driver
from nova.virt import hardware
from nova.virt import virtapi
from nova.virt.mesoscontainers import resources
from nova.virt.mesoscontainers.framework import nova
from nova.virt.mesoscontainers.framework import VolumeInfo
from nova.virt.mesoscontainers.persist import get_persistent_storage
from nova.virt.mesoscontainers.instance import Instance


CONF = cfg.CONF

LOG = logging.getLogger(__name__)


_FAKE_NODES = None


def set_nodes(nodes):
    """Sets FakeDriver's node.list.

    It has effect on the following methods:
        get_available_nodes()
        get_available_resource

    To restore the change, call restore_nodes()
    """
    global _FAKE_NODES
    _FAKE_NODES = nodes


def restore_nodes():
    """Resets FakeDriver's node list modified by set_nodes().

    Usually called from tearDown().
    """
    global _FAKE_NODES
    _FAKE_NODES = [CONF.host]


class FakeInstance(Instance):

    def __init__(self, instanceInfo, networkInfo, imageMeta,name, state):
        super(FakeInstance, self).__init__(instanceInfo, networkInfo, imageMeta)
        self.name = name
        self.state = state


    def __getitem__(self, key):
        return getattr(self, key)


class MesosContainerDriver(driver.ComputeDriver):
    capabilities = {
        "has_imagecache": True,
        "supports_recreate": True,
        "supports_migrate_to_same_host": True
        }

    #TODO: (gokrokve) Add Mesos call to retrieve this information
    vcpus = 1000
    memory_mb = 800000
    local_gb = 600000

    """Fake hypervisor driver."""

    def __init__(self, virtapi, read_only=False):
        super(MesosContainerDriver, self).__init__(virtapi)
        self.instances = {}
        self.resources = resources.get_resources()
        self.host_status_base = {
          'hypervisor_type': 'docker',
          'hypervisor_version': versionutils.convert_version_to_int('1.0'),
          'hypervisor_hostname': CONF.host,
          'cpu_info': {},
          'disk_available_least': 400,
          'supported_instances': [(arch.X86_64, hv_type.DOCKER, vm_mode.EXE)],
          'numa_topology': None,
          }
        self._mounts = {}
        self._interfaces = {}
        self._storage = get_persistent_storage()
        if len(self._storage.listIDs()) >0:
            nova.reconcile()
        if not _FAKE_NODES:
            set_nodes([CONF.host])

    def init_host(self, host):
        return

    def list_instances(self):
        return [self._storage.getItem(uuid).name for uuid in self._storage.listIDs()]

    def list_instance_uuids(self):
        return self._storage.listIDs()

    def plug_vifs(self, instance, network_info):
        """Plug VIFs into networks."""
        pass

    def unplug_vifs(self, instance, network_info):
        """Unplug VIFs from networks."""
        pass

    def spawn(self, context, instance, image_meta, injected_files,
              admin_password, network_info=None, block_device_info=None):
        uuid = instance.uuid
        state = power_state.RUNNING
        LOG.info("Network_Info:%s" % network_info)
        LOG.info("Instance_Info:%s" % instance)
        LOG.info("Image_Meta:%s" % image_meta)
        flavor = instance.flavor
        self.resources.claim(
            vcpus=flavor.vcpus,
            mem=flavor.memory_mb,
            disk=flavor.root_gb)
        new_instance = FakeInstance(instance, network_info, image_meta, instance.name, state)
        new_instance.instance_info = instance
        new_instance.image_meta = image_meta
        new_instance.network_info = network_info
        nova.scheduleTask(new_instance)
        self.wait_for_instance(new_instance)
        self._storage.persist(new_instance)
        LOG.info("Scheduled instance: %s" % new_instance.serialize())

    def wait_for_instance(self, instance):
        while True:
            if (instance.status == 0) or (instance.status == 6):
                eventlet.greenthread.sleep(1)
            LOG.info("Got confirmation for instance. Returning Running state :-) %s" % instance.status)
            if instance.status == 1:
                break
            if (instance.status > 1) and (instance.status != 3):
                LOG.error("Instance scheduled returned failed state")
                raise exception.InstanceNotRunning(instance_id=instance.uuid)


    def snapshot(self, context, instance, image_id, update_task_state):
        if instance.uuid not in self.instances:
            raise exception.InstanceNotRunning(instance_id=instance.uuid)
        update_task_state(task_state=task_states.IMAGE_UPLOADING)

    def reboot(self, context, instance, network_info, reboot_type,
               block_device_info=None, bad_volumes_callback=None):
        pass

    def get_host_ip_addr(self):
        return '192.168.0.1'

    def set_admin_password(self, instance, new_pass):
        pass

    def inject_file(self, instance, b64_path, b64_contents):
        pass

    def resume_state_on_host_boot(self, context, instance, network_info,
                                  block_device_info=None):
        pass

    def rescue(self, context, instance, network_info, image_meta,
               rescue_password):
        pass

    def unrescue(self, instance, network_info):
        pass

    def poll_rebooting_instances(self, timeout, instances):
        pass

    def migrate_disk_and_power_off(self, context, instance, dest,
                                   flavor, network_info,
                                   block_device_info=None,
                                   timeout=0, retry_interval=0):
        pass

    def finish_revert_migration(self, context, instance, network_info,
                                block_device_info=None, power_on=True):
        pass

    def post_live_migration_at_destination(self, context, instance,
                                           network_info,
                                           block_migration=False,
                                           block_device_info=None):
        pass

    def power_off(self, instance, timeout=0, retry_interval=0):
        pass

    def power_on(self, context, instance, network_info,
                 block_device_info=None):
        pass

    def inject_nmi(self, instance):
        pass

    def soft_delete(self, instance):
        pass

    def restore(self, instance):
        pass

    def pause(self, instance):
        pass

    def unpause(self, instance):
        pass

    def suspend(self, context, instance):
        pass

    def resume(self, context, instance, network_info, block_device_info=None):
        pass

    def destroy(self, context, instance, network_info, block_device_info=None,
                destroy_disks=True, migrate_data=None):
        key = instance.uuid
        if self._storage.hasItem(key):
            flavor = instance.flavor
            self.resources.release(
                vcpus=flavor.vcpus,
                mem=flavor.memory_mb,
                disk=flavor.root_gb)
            instanceInfo = self._storage.getItem(key)
            nova.deleteTask(instanceInfo)
            self._storage.deleteItem(key)
        else:
            LOG.warning(_LW("Key '%(key)s' not in instances '%(inst)s'"),
                        {'key': key,
                         'inst': self.instances}, instance=instance)

    def cleanup(self, context, instance, network_info, block_device_info=None,
                destroy_disks=True, migrate_data=None, destroy_vifs=True):
        pass

    def attach_volume(self, context, connection_info, instance, mountpoint,
                      disk_bus=None, device_type=None, encryption=None):
        """Attach the disk to the instance at mountpoint using info."""
        LOG.info("(GOK) Volume attach: Connection Info: %s" % connection_info)
        LOG.info("(GOK) Volume attach: mountpoint Info: %s" % mountpoint)
        instance_name = instance.name
        if instance_name not in self._mounts:
            self._mounts[instance_name] = {}
        self._mounts[instance_name][mountpoint] = connection_info
        #TODO(gokrokve) Docker does not support on-line attachment
        # Here we will kill previous task and spawn a new one with updated volume info
        key = instance.uuid
        if self._storage.hasItem(key):
            instanceInfo = self._storage.getItem(key)
            volume = VolumeInfo(connection_info['data'], mountpoint, connection_info['driver_volume_type'])
            instanceInfo.volume_info_list.append(volume)
            nova.deleteTask(instanceInfo)
            nova.scheduleTask(instanceInfo)
            self.wait_for_instance(instanceInfo)
        else:
            LOG.warning(_LW("Key '%(key)s' not in instances '%(inst)s'"),
                        {'key': key,
                         'inst': self.instances}, instance=instance)




    def detach_volume(self, connection_info, instance, mountpoint,
                      encryption=None):
        """Detach the disk attached to the instance."""
        try:
            del self._mounts[instance.name][mountpoint]
        except KeyError:
            pass

    def swap_volume(self, old_connection_info, new_connection_info,
                    instance, mountpoint, resize_to):
        """Replace the disk attached to the instance."""
        instance_name = instance.name
        if instance_name not in self._mounts:
            self._mounts[instance_name] = {}
        self._mounts[instance_name][mountpoint] = new_connection_info

    def attach_interface(self, instance, image_meta, vif):
        if vif['id'] in self._interfaces:
            raise exception.InterfaceAttachFailed(
                    instance_uuid=instance.uuid)
        self._interfaces[vif['id']] = vif

    def detach_interface(self, instance, vif):
        try:
            del self._interfaces[vif['id']]
        except KeyError:
            raise exception.InterfaceDetachFailed(
                    instance_uuid=instance.uuid)

    def get_info(self, instance):
        if not self._storage.hasItem(instance.uuid ):
            raise exception.InstanceNotFound(instance_id=instance.uuid)
        i = self._storage.getItem(instance.uuid)
        LOG.info("Periodic instance check: %s state: %s" %(instance.uuid, i.serialize()))
        return hardware.InstanceInfo(state=i.state,
                                     max_mem_kb=0,
                                     mem_kb=0,
                                     num_cpu=2,
                                     cpu_time_ns=0)

    def get_diagnostics(self, instance):
        return {'cpu0_time': 17300000000,
                'memory': 524288,
                'vda_errors': -1,
                'vda_read': 262144,
                'vda_read_req': 112,
                'vda_write': 5778432,
                'vda_write_req': 488,
                'vnet1_rx': 2070139,
                'vnet1_rx_drop': 0,
                'vnet1_rx_errors': 0,
                'vnet1_rx_packets': 26701,
                'vnet1_tx': 140208,
                'vnet1_tx_drop': 0,
                'vnet1_tx_errors': 0,
                'vnet1_tx_packets': 662,
        }

    def get_instance_diagnostics(self, instance):
        diags = diagnostics.Diagnostics(state='running', driver='fake',
                hypervisor_os='fake-os', uptime=46664, config_drive=True)
        diags.add_cpu(time=17300000000)
        diags.add_nic(mac_address='01:23:45:67:89:ab',
                      rx_packets=26701,
                      rx_octets=2070139,
                      tx_octets=140208,
                      tx_packets = 662)
        diags.add_disk(id='fake-disk-id',
                       read_bytes=262144,
                       read_requests=112,
                       write_bytes=5778432,
                       write_requests=488)
        diags.memory_details.maximum = 524288
        return diags

    def get_all_bw_counters(self, instances):
        """Return bandwidth usage counters for each interface on each
           running VM.
        """
        bw = []
        return bw

    def get_all_volume_usage(self, context, compute_host_bdms):
        """Return usage info for volumes attached to vms on
           a given host.
        """
        volusage = []
        return volusage

    def get_host_cpu_stats(self):
        stats = {'kernel': 5664160000000,
                'idle': 1592705190000000,
                'user': 26728850000000,
                'iowait': 6121490000000}
        stats['frequency'] = 800
        return stats

    def block_stats(self, instance, disk_id):
        return [0, 0, 0, 0, None]

    def get_console_output(self, context, instance):
        return 'FAKE CONSOLE OUTPUT\nANOTHER\nLAST LINE'

    def get_vnc_console(self, context, instance):
        return ctype.ConsoleVNC(internal_access_path='FAKE',
                                host='fakevncconsole.com',
                                port=6969)

    def get_spice_console(self, context, instance):
        return ctype.ConsoleSpice(internal_access_path='FAKE',
                                  host='fakespiceconsole.com',
                                  port=6969,
                                  tlsPort=6970)

    def get_rdp_console(self, context, instance):
        return ctype.ConsoleRDP(internal_access_path='FAKE',
                                host='fakerdpconsole.com',
                                port=6969)

    def get_serial_console(self, context, instance):
        return ctype.ConsoleSerial(internal_access_path='FAKE',
                                   host='fakerdpconsole.com',
                                   port=6969)

    def get_mks_console(self, context, instance):
        return ctype.ConsoleMKS(internal_access_path='FAKE',
                                host='fakemksconsole.com',
                                port=6969)

    def get_console_pool_info(self, console_type):
        return {'address': '127.0.0.1',
                'username': 'fakeuser',
                'password': 'fakepassword'}

    def refresh_security_group_rules(self, security_group_id):
        return True

    def refresh_instance_security_rules(self, instance):
        return True

    def get_available_resource(self, nodename):
        """Updates compute manager resource info on ComputeNode table.

           Since we don't have a real hypervisor, pretend we have lots of
           disk and ram.
        """
        cpu_info = collections.OrderedDict([
            ('arch', 'x86_64'),
            ('model', 'Nehalem'),
            ('vendor', 'Intel'),
            ('features', ['pge', 'clflush']),
            ('topology', {
                'cores': 1,
                'threads': 1,
                'sockets': 4,
                }),
            ])
        if nodename not in _FAKE_NODES:
            return {}

        host_status = self.host_status_base.copy()
        host_status.update(self.resources.dump())
        host_status['disk_available_least'] = self.resources.local_gb
        host_status['hypervisor_hostname'] = nodename
        host_status['host_hostname'] = nodename
        host_status['host_name_label'] = nodename
        host_status['cpu_info'] = jsonutils.dumps(cpu_info)
        return host_status

    def ensure_filtering_rules_for_instance(self, instance, network_info):
        return

    def get_instance_disk_info(self, instance, block_device_info=None):
        return

    def live_migration(self, context, instance, dest,
                       post_method, recover_method, block_migration=False,
                       migrate_data=None):
        post_method(context, instance, dest, block_migration,
                            migrate_data)
        return

    def check_can_live_migrate_destination_cleanup(self, context,
                                                   dest_check_data):
        return

    def check_can_live_migrate_destination(self, context, instance,
                                           src_compute_info, dst_compute_info,
                                           block_migration=False,
                                           disk_over_commit=False):
        return {}

    def check_can_live_migrate_source(self, context, instance,
                                      dest_check_data, block_device_info=None):
        return

    def finish_migration(self, context, migration, instance, disk_info,
                         network_info, image_meta, resize_instance,
                         block_device_info=None, power_on=True):
        return

    def confirm_migration(self, migration, instance, network_info):
        return

    def pre_live_migration(self, context, instance, block_device_info,
                           network_info, disk_info, migrate_data=None):
        return

    def unfilter_instance(self, instance, network_info):
        return

    def _test_remove_vm(self, instance_uuid):
        """Removes the named VM, as if it crashed. For testing."""
        self.instances.pop(instance_uuid)

    def host_power_action(self, action):
        """Reboots, shuts down or powers up the host."""
        return action

    def host_maintenance_mode(self, host, mode):
        """Start/Stop host maintenance window. On start, it triggers
        guest VMs evacuation.
        """
        if not mode:
            return 'off_maintenance'
        return 'on_maintenance'

    def set_host_enabled(self, enabled):
        """Sets the specified host's ability to accept new instances."""
        if enabled:
            return 'enabled'
        return 'disabled'

    def get_volume_connector(self, instance):
        return {'ip': CONF.my_block_storage_ip,
                'initiator': 'fake',
                'host': 'fakehost'}

    def get_available_nodes(self, refresh=False):
        return _FAKE_NODES

    def instance_on_disk(self, instance):
        return False

    def quiesce(self, context, instance, image_meta):
        pass

    def unquiesce(self, context, instance, image_meta):
        pass


class FakeVirtAPI(virtapi.VirtAPI):
    @contextlib.contextmanager
    def wait_for_instance_event(self, instance, event_names, deadline=300,
                                error_callback=None):
        # NOTE(danms): Don't actually wait for any events, just
        # fall through
        yield


