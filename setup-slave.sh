#!/bin/bash

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

set -euxo pipefail

exec 1>&2

# Install system updates
sudo apt-get update && sudo apt-get -y upgrade

sudo apt-get install -y wget curl vim git realpath tree htop sysbench build-essential \
    default-jdk lxc lvm2 xfsprogs pssh openmpi-bin

mkdir -p $HOME/var/yarn-ec2

pushd $HOME/var/yarn-ec2 > /dev/null

function maybe_umount_path() { ### @param path to umount ###
    sudo umount -f $1 || :
}

for vm in `sudo lxc-ls` ; do
    sudo lxc-stop -k -n $vm || :
    sudo lxc-destroy -f -n $vm
    sleep 0.1
done

sudo service lxc stop
sudo service lxc-net stop
sudo rm -f /var/lib/misc/dnsmasq.lxcbr0.leases
sudo killall -9 java || :

sudo rm -rf /tmp/Jetty*
sudo rm -rf /tmp/hadoop*
sudo rm -rf /tmp/yarn*
sudo rm -rf /tmp/hd*

sudo rm -rf /opt/hadoop-*
HADOOP_URL=https://archive.apache.org/dist/hadoop/common/hadoop-2.2.0/hadoop-2.2.0.tar.gz
wget --no-check-certificate $HADOOP_URL -O /tmp/hadoop-2.2.0.tar.gz
sudo tar xzf /tmp/hadoop-2.2.0.tar.gz -C /opt
sudo chown -R root:root /opt/hadoop-2.2.0
sudo umount -f /usr/local/hd || :
sudo mkdir -p /usr/local/hd
sudo mount --bind -o ro /opt/hadoop-2.2.0 /usr/local/hd

mkdir /tmp/hd
mkdir /tmp/hd/logs

ln -s /usr/local/hd/bin /tmp/hd/
ln -s /usr/local/hd/lib /tmp/hd/
ln -s /usr/local/hd/libexec /tmp/hd/
ln -s /usr/local/hd/sbin /tmp/hd/
ln -s /usr/local/hd/share /tmp/hd/

mkdir /tmp/hd/conf

ln -s /usr/local/hd/etc/hadoop/* /tmp/hd/conf/

rm -f /tmp/hd/conf/core-site.xml
rm -f /tmp/hd/conf/hdfs-site.xml
rm -f /tmp/hd/conf/container*
rm -f /tmp/hd/conf/httpfs*
rm -f /tmp/hd/conf/mapred*
rm -f /tmp/hd/conf/yarn*
rm -f /tmp/hd/conf/*-scheduler.xml
rm -f /tmp/hd/conf/*example
rm -f /tmp/hd/conf/*cmd

rm -f /tmp/hd/conf/slaves
cat hosts | fgrep r | fgrep -v h | cut -d' ' -f2 > /tmp/hd/conf/slaves
echo "r0" > /tmp/hd/conf/boss
cp $HOME/share/yarn-ec2/hd/conf/core-site.xml /tmp/hd/conf/
cp $HOME/share/yarn-ec2/hd/conf/hdfs-site.xml /tmp/hd/conf/

mkdir /tmp/yarn
mkdir /tmp/yarn/logs

ln -s /usr/local/hd/bin /tmp/yarn/
ln -s /usr/local/hd/lib /tmp/yarn/
ln -s /usr/local/hd/libexec /tmp/yarn/
ln -s /usr/local/hd/sbin /tmp/yarn/
ln -s /usr/local/hd/share /tmp/yarn/

mkdir /tmp/yarn/conf

ln -s /usr/local/hd/etc/hadoop/* /tmp/yarn/conf/

rm -f /tmp/yarn/conf/core-site.xml
rm -r /tmp/yarn/conf/yarn-site.xml
rm -f /tmp/yarn/conf/hdfs*
rm -f /tmp/yarn/conf/httpfs*
rm -f /tmp/yarn/conf/mapred*
rm -f /tmp/yarn/conf/*example
rm -f /tmp/yarn/conf/*cmd

rm -f /tmp/yarn/conf/slaves
cat hosts | fgrep r | fgrep h | cut -d' ' -f2 > /tmp/yarn/conf/slaves
echo "r0" > /tmp/yarn/conf/boss
cp $HOME/share/yarn-ec2/hd/conf/core-site.xml /tmp/yarn/conf/
cp $HOME/share/yarn-ec2/resource-mngr/conf/yarn-site.xml /tmp/yarn/conf/

cat <<EOF | sudo tee /etc/environment
PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games"
JAVA_HOME="/usr/lib/jvm/default-java"

HADOOP_HOME="/usr/local/hd"

HADOOP_HEAPSIZE="2000"

EOF

PRIMARY_IP=`curl http://169.254.169.254/latest/meta-data/local-ipv4`
echo "$PRIMARY_IP" > my_primary_ip
MAC=`curl http://169.254.169.254/latest/meta-data/mac`
CIDR=`curl http://169.254.169.254/latest/meta-data/network/interfaces/macs/$MAC/subnet-ipv4-cidr-block`
echo "$CIDR" > my_cidr
PRIVATE_IPS=`curl http://169.254.169.254/latest/meta-data/network/interfaces/macs/$MAC/local-ipv4s`
echo "$PRIVATE_IPS" > my_ips
OFFSET=`cat all-nodes | fgrep -n $PRIMARY_IP | cut -d: -f1`
ID=$(( OFFSET - 1 ))
echo "$ID" > my_id

MASK=`echo $CIDR | cut -d/ -f2`
DEV=`ls -1 /sys/class/net/ | fgrep -v lxc | fgrep -v lo | head -1`

sudo ip link set dev $DEV mtu 1500

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
HOSTNAME=`echo r"$ID"`
echo $HOSTNAME | sudo tee /etc/hostname
sudo hostname $HOSTNAME

cat <<EOF | sudo tee /etc/ssh/ssh_config
Host *
    PasswordAuthentication no
    HashKnownHosts no
    UserKnownHostsFile /dev/null
    StrictHostKeyChecking no
    GSSAPIAuthentication yes
    GSSAPIDelegateCredentials no
    SendEnv LANG LC_*


EOF

function try_fgrep() {
    fgrep $@ || :
}

XFS_MOUNT_OPTS="defaults,noatime,nodiratime,allocsize=8m"
DISKS=`lsblk -ln | fgrep disk | cut -d' ' -f1 | try_fgrep -v da`
echo -n "$DISKS" | awk '{print "/dev/" $0}' > my_disks
NUM_DISKS=`cat my_disks | wc -l`
LV_NAME="lxclv0"
VG_NAME="lxcvg0"
LV="/dev/$VG_NAME/$LV_NAME"
VG="/dev/$VG_NAME"

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
sudo mkdir /mnt/hdscratch
sudo chmod 777 /mnt/hdscratch
sudo lsblk

sudo df -h

NUM_CPUS=`cat /proc/cpuinfo | fgrep proc | wc -l`
echo "$NUM_CPUS" > my_ncpus

sudo cp -f $HOME/share/yarn-ec2/lxc/share/lxc/templates/* /usr/share/lxc/templates/
sudo cp -f $HOME/share/yarn-ec2/lxc/etc/default/* /etc/default/
sudo cp -f $HOME/share/yarn-ec2/lxc/etc/lxc/* /etc/lxc/

function create_vm() {
### @param rack_id, host_id, ip, mem, ncpus ###
    VM_NAME=`echo r"$1"h"$2"`
    sudo lxc-create -n $VM_NAME -t ubuntu -- \
        --auth-key ~/.ssh/id_rsa.pub ### --packages "sysbench" ###
    sudo cp -f ~/.ssh/id_rsa ~/.ssh/id_rsa.pub /mnt/$VM_NAME/rootfs/home/ubuntu/.ssh/
    sudo chown ubuntu:ubuntu /mnt/$VM_NAME/rootfs/home/ubuntu/.ssh/id_rsa.pub
    sudo chown ubuntu:ubuntu /mnt/$VM_NAME/rootfs/home/ubuntu/.ssh/id_rsa
    sudo cp -f /etc/ssh/ssh_config /mnt/$VM_NAME/rootfs/etc/ssh/
    sudo cp -r $HOME/share /mnt/$VM_NAME/rootfs/home/ubuntu/
    sudo chown -R ubuntu:ubuntu /mnt/$VM_NAME/rootfs/home/ubuntu/share
    sudo sed -i "/lxc.network.ipv4 =/c lxc.network.ipv4 = $3" \
        /mnt/$VM_NAME/config
    sudo sed -i "/lxc.cgroup.memory.max_usage_in_bytes =/c lxc.cgroup.memory.max_usage_in_bytes = $4" \
        /mnt/$VM_NAME/config
    sudo sed -i "/lxc.cgroup.memory.limit_in_bytes =/c lxc.cgroup.memory.limit_in_bytes = $4" \
        /mnt/$VM_NAME/config
    core_begin=$(( $2 * $5 ))
    core_end=$(( core_begin + $5 - 1 ))
    VM_CPUS=`echo "$core_begin"-"$core_end"`
    sudo sed -i "/lxc.cgroup.cpuset.cpus =/c lxc.cgroup.cpuset.cpus = $VM_CPUS" \
        /mnt/$VM_NAME/config
}

RACK_ID="$ID"
HOST_ID=0
for ip in `cat rack-$ID/vmips` ; do
    NODE_ID=$(( HOST_ID + RACK_ID * 10 + 100))
    sudo sed -i "s/$ip/192.168.1.$NODE_ID/" /etc/hosts
    create_vm $RACK_ID $HOST_ID "192.168.1.$NODE_ID/24 192.168.1.255" \
        "`cat rack-$ID/vmmem`" "`cat rack-$ID/vmncpus`"
    HOST_ID=$(( HOST_ID + 1 ))
done

sudo service lxc-net start
sudo iptables -t nat -F  ### will use our own rules ###
sudo iptables -t nat -L -n
sudo service lxc start
sudo lxc-ls -f

rm -f $HOME/bin/hdup
rm -f $HOME/bin/hddown

mkdir -p $HOME/bin

ln -s $HOME/share/yarn-ec2/exec/hdup $HOME/bin/
ln -s $HOME/share/yarn-ec2/exec/hddown $HOME/bin/

popd > /dev/null

exit 0
