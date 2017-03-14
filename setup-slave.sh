#!/bin/bash -xue

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

exec 1>&2

# Install system updates
sudo apt-get update && sudo apt-get -y upgrade

sudo apt-get install -y curl vim realpath lxc lvm2 xfsprogs tree htop

mkdir -p $HOME/var/yarn-ec2

pushd $HOME/var/yarn-ec2 > /dev/null

for vm in `sudo lxc-ls` ; do
    sudo lxc-stop -k -n $vm && sudo lxc-destroy -f -n $vm
    sleep 0.1
done
sudo service lxc stop
sudo service lxc-net stop
sudo rm -f /var/lib/misc/dnsmasq.lxcbr0.leases

PRIMARY_IP=`curl http://169.254.169.254/latest/meta-data/local-ipv4`
echo "$PRIMARY_IP" > my_primary_ip
MAC=`curl http://169.254.169.254/latest/meta-data/mac`
CIDR=`curl http://169.254.169.254/latest/meta-data/network/interfaces/macs/$MAC/subnet-ipv4-cidr-block`
PRIVATE_IPS=`curl http://169.254.169.254/latest/meta-data/network/interfaces/macs/$MAC/local-ipv4s`
echo "$PRIVATE_IPS" > my_ips
OFFSET=`cat all-nodes | fgrep -n $PRIMARY_IP | cut -d: -f1`
ID=$(( OFFSET - 1 ))
echo "$ID" > my_id

MASK=`echo $CIDR | cut -d/ -f2`
DEV=`ls -1 /sys/class/net/ | fgrep -v lxc | fgrep -v lo | head -1`

sudo ip addr show dev $DEV
sudo ip addr flush secondary dev $DEV
for ipv4 in `cat my_ips` ; do
    if [ x"$ipv4" != x"$PRIMARY_IP" ] ; then
        sudo ip addr add $ipv4/$MASK brd + dev $DEV
    fi
done
sudo ip addr show dev $DEV

cat <<EOF | sudo tee /etc/hosts
127.0.0.1   localhost

4.4.4.4 mashiro
8.8.8.8 ibuki

# The following lines are desirable for IPv6 capable hosts
::1     ip6-localhost ip6-loopback
fe00::0 ip6-localnet
ff00::0 ip6-mcastprefix
ff02::1 ip6-allnodes
ff02::2 ip6-allrouters


EOF

cat hosts | sudo tee -a /etc/hosts

XFS_MOUNT_OPTS="defaults,noatime,nodiratime,allocsize=8m"
DISKS=`lsblk -ln | fgrep -v part | fgrep -v lvm | fgrep -v da | cut -d' ' -f1`
echo "$DISKS" | awk '{print "/dev/" $0}' > my_disks
NUM_DISKS=`cat my_disks | wc -l`
LV_NAME="lxclv0"
VG_NAME="lxcvg0"
LV="/dev/$VG_NAME/$LV_NAME"
VG="/dev/$VG_NAME"

function maybe_umount_path() { ### @param fspath ###
    sudo umount -f $1 || echo "OK"
}

sudo lsblk
maybe_umount_path /mnt &>/dev/null
if [ -e $LV ] ; then
    maybe_umount_path $LV &>/dev/null
    sudo lvremove -f $LV
fi
if [ -e $VG ] ; then
    sudo vgremove -f $VG
fi
if [ $NUM_DISKS -gt 0 ] ; then
    for dev in `cat my_disks` ; do
        sudo pvcreate -ff -y $dev
    done
    sudo vgcreate -y $VG_NAME `cat my_disks | paste -sd ' ' -`
    sudo lvcreate -y -Wy -Zy -l 100%FREE \
        -n $LV_NAME $VG_NAME
    sleep 0.1
    if [ -e $LV ] ; then
        sudo mkfs.xfs -f $LV
        sudo mount -o $XFS_MOUNT_OPTS $LV /mnt
    fi
fi
sudo rm -rf /mnt/*
sudo lsblk

sudo df -h

NUM_CPUS=`cat /proc/cpuinfo | fgrep proc | wc -l`
echo "$NUM_CPUS" > my_ncpus

sudo cp -f $HOME/share/yarn-ec2/lxc/share/lxc/templates/* /usr/share/lxc/templates/
sudo cp -f $HOME/share/yarn-ec2/lxc/etc/default/* /etc/default/
sudo cp -f $HOME/share/yarn-ec2/lxc/etc/lxc/* /etc/lxc/
sudo service lxc-net start
sudo service lxc start

function create_vm() {
### @param rack_id, host_id, ip, mem, ncpus ###
    VM_NAME=`echo r"$1"h"$2"`
    sudo lxc-create -n $VM_NAME -t ubuntu -- --packages "sysbench"
    sudo sed -i "/lxc.network.ipv4 =/c lxc.network.ipv4 = $3" \
        /mnt/$VM_NAME/config
    sudo sed -i "/lxc.cgroup.memory.max_usage_in_bytes =/c lxc.cgroup.memory.max_usage_in_bytes = $4" \
        /mnt/$VM_NAME/config
    sudo sed -i "/lxc.cgroup.memory.limit_in_bytes =/c lxc.cgroup.memory.limit_in_bytes = $4" \
        /mnt/$VM_NAME/config
    core_begin=$(( "$2" * "$5" ))
    core_end=$(( core_begin + "$5" - 1 ))
    VM_CPUS=`echo "$core_begin"-"$core_end"`
    sudo sed -i "/lxc.cgroup.cpuset.cpus =/c lxc.cgroup.cpuset.cpus = $VM_CPUS" \
        /mnt/$VM_NAME/config
}

RACK_ID="$ID"
HOST_ID=0
for ip in `cat rack-$ID/vmips` ; do
    NODE_ID=$(( HOST_ID + 10 ))
    create_vm $RACK_ID $HOST_ID "192.168.1.$NODE_ID/24 192.168.1.255" \
        "`cat rack-$ID/vmmem`" "`cat rack-$ID/vmncpus`"
    HOST_ID=$(( HOST_ID + 1 ))
done

popd > /dev/null

exit 0
