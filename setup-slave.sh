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

sudo apt-get update && sudo apt-get -y upgrade

sudo apt-get install -y csh wget curl vim git realpath tree htop libsnappy1v5 \
    lxc lvm2 xfsprogs pssh

pushd ~/var/yarn-ec2 > /dev/null

for vm in `sudo lxc-ls` ; do
    sudo lxc-stop -k -n $vm || :
    sudo lxc-destroy -f -n $vm
    sleep 0.1
done

sudo service lxc stop
sudo service lxc-net stop
sudo rm -f /var/lib/misc/dnsmasq.lxcbr0.leases
sudo killall -9 java || :
sleep 0.1

sudo rm -rf /tmp/Jetty*
sudo rm -rf /tmp/hadoop*
sudo rm -rf /tmp/yarn*

sudo mkdir -p /opt/tarfiles

sudo rm -rf /opt/yarn*
sudo rm -rf /opt/hadoop-*
sudo rm -rf /opt/jdk*

HADOOP_TGZ=hadoop-2.2.0.tar.gz
HADOOP_URL=https://s3.amazonaws.com/ubuntu-ursus-packages/$HADOOP_TGZ
[ ! -e /opt/tarfiles/$HADOOP_TGZ -o ! -s /opt/tarfiles/$HADOOP_TGZ ] && \
sudo wget --no-check-certificate $HADOOP_URL -O /opt/tarfiles/$HADOOP_TGZ
sudo tar xzf /opt/tarfiles/$HADOOP_TGZ -C /opt
sudo chown -R root:root /opt/hadoop-2.2.0
sudo umount -l /usr/local/hd || :
sudo mkdir -p /usr/local/hd
sudo mount --bind -o ro /opt/hadoop-2.2.0 /usr/local/hd
TAPACK_TGZ=yarn-2.2.0-ta-pack-v1.tar.gz
TAPACK_URL=https://s3.amazonaws.com/ubuntu-ursus-packages/$TAPACK_TGZ
[ ! -e /opt/tarfiles/$TAPACK_TGZ -o ! -s /opt/tarfiles/$TAPACK_TGZ ] && \
    sudo wget --no-check-certificate $TAPACK_URL -O /opt/tarfiles/$TAPACK_TGZ
sudo tar xzf /opt/tarfiles/$TAPACK_TGZ -C /opt
sudo chown -R root:root /opt/yarn-2.2.0-ta-pack-v1
sudo cp /opt/yarn-2.2.0-ta-pack-v1/jobexe/* /opt/hadoop-2.2.0/
sudo mv /opt/hadoop-2.2.0/share/hadoop/yarn/hadoop-yarn-server-resourcemanager-2.2.0.jar \
    /opt/hadoop-2.2.0/share/hadoop/yarn/hadoop-yarn-server-resourcemanager-2.2.0.jar.origin
sudo cp /opt/yarn-2.2.0-ta-pack-v1/hadoop-yarn-server-resourcemanager-2.2.0.jar /opt/hadoop-2.2.0/share/hadoop/yarn/
sudo cp /opt/yarn-2.2.0-ta-pack-v1/hadoop-yarn-applications-mpirunner-2.2.0.jar /opt/hadoop-2.2.0/
sudo cp /opt/yarn-2.2.0-ta-pack-v1/hadoop-yarn-applications-gpu-2.2.0.jar /opt/hadoop-2.2.0/

SUNJDK_TGZ=jdk-8u121-linux-x64.tar.gz
SUNJDK_URL=https://s3.amazonaws.com/ubuntu-ursus-packages/$SUNJDK_TGZ
[ ! -e /opt/tarfiles/$SUNJDK_TGZ -o ! -s /opt/tarfiles/$SUNJDK_TGZ ] && \
    sudo wget --no-check-certificate $SUNJDK_URL -O /opt/tarfiles/$SUNJDK_TGZ
sudo tar xzf /opt/tarfiles/$SUNJDK_TGZ -C /opt
sudo chown -R root:root /opt/jdk1.8.0_121
sudo umount -l /usr/lib/jvm/sunjdk || :
sudo mkdir -p /usr/lib/jvm/sunjdk
sudo mount --bind -o ro /opt/jdk1.8.0_121 /usr/lib/jvm/sunjdk

THRIFT_TGZ=thrift-0.9.1.tar.gz
THRIFT_URL=https://s3.amazonaws.com/ubuntu-ursus-packages/$THRIFT_TGZ
[ ! -e /opt/tarfiles/$THRIFT_TGZ -o ! -s /opt/tarfiles/$THRIFT_TGZ ] && \
    sudo wget --no-check-certificate $THRIFT_URL -O /opt/tarfiles/$THRIFT_TGZ
sudo tar xzf /opt/tarfiles/$THRIFT_TGZ -C /opt
sudo chown -R root:root /opt/thrift-0.9.1

sudo rm -rf /srv/hdfs*
sudo rm -rf /srv/yarn*

sudo mkdir /srv/hdfs

sudo ln -s /usr/local/hd/bin /srv/hdfs/
sudo ln -s /usr/local/hd/lib /srv/hdfs/
sudo ln -s /usr/local/hd/libexec /srv/hdfs/
sudo ln -s /usr/local/hd/sbin /srv/hdfs/
sudo ln -s /usr/local/hd/share /srv/hdfs/

sudo mkdir /srv/hdfs/logs
sudo mkdir /srv/hdfs/conf

sudo ln -s /usr/local/hd/etc/hadoop/* /srv/hdfs/conf/

sudo rm -f /srv/hdfs/conf/core-site.xml
sudo rm -f /srv/hdfs/conf/hdfs-site.xml
sudo rm -f /srv/hdfs/conf/container*
sudo rm -f /srv/hdfs/conf/httpfs*
sudo rm -f /srv/hdfs/conf/mapred*
sudo rm -f /srv/hdfs/conf/yarn*
sudo rm -f /srv/hdfs/conf/*-scheduler.xml
sudo rm -f /srv/hdfs/conf/*example
sudo rm -f /srv/hdfs/conf/*cmd

sudo rm -f /srv/hdfs/conf/slaves
cat hosts | fgrep r | fgrep -v h | cut -d' ' -f2 | sudo tee /srv/hdfs/conf/slaves
echo "r0" | sudo tee /srv/hdfs/conf/boss
sudo cp ~/share/yarn-ec2/hd/conf/core-site.xml /srv/hdfs/conf/
sudo cp ~/share/yarn-ec2/hd/conf/hdfs-site.xml /srv/hdfs/conf/

sudo mkdir /srv/yarn

sudo ln -s /usr/local/hd/bin /srv/yarn/
sudo ln -s /usr/local/hd/lib /srv/yarn/
sudo ln -s /usr/local/hd/libexec /srv/yarn/
sudo ln -s /usr/local/hd/sbin /srv/yarn/
sudo ln -s /usr/local/hd/share /srv/yarn/
sudo ln -s /usr/local/hd/hadoop-yarn-applications-* /srv/yarn/
sudo ln -s /usr/local/hd/bt* /srv/yarn/
sudo ln -s /usr/local/hd/cg* /srv/yarn/
sudo ln -s /usr/local/hd/ft* /srv/yarn/
sudo ln -s /usr/local/hd/sp* /srv/yarn/

sudo mkdir /srv/yarn/logs
sudo mkdir /srv/yarn/conf

sudo ln -s /usr/local/hd/etc/hadoop/* /srv/yarn/conf/

sudo rm -f /srv/yarn/conf/core-site.xml
sudo rm -r /srv/yarn/conf/yarn-site.xml
sudo rm -f /srv/yarn/conf/hdfs*
sudo rm -f /srv/yarn/conf/httpfs*
sudo rm -f /srv/yarn/conf/mapred*
sudo rm -f /srv/yarn/conf/*example
sudo rm -f /srv/yarn/conf/*cmd

sudo rm -f /srv/yarn/conf/slaves
cat hosts | fgrep r | fgrep h | cut -d' ' -f2 | tee /srv/yarn/conf/slaves
echo "r0" | tee /srv/yarn/conf/boss
sudo cp ~/share/yarn-ec2/hd/conf/core-site.xml /srv/yarn/conf/
cat ~/share/yarn-ec2/resource-mngr/conf/yarn-site.xml | \
    sed "s/yarn.resourcemanager.scheduler.class.value/`cat ~/etc/yarn-scheduler.txt`" \
        > /srv/yarn/conf/yarn-site.xml

cat <<EOF | sudo tee /etc/environment
PATH="/usr/local/sbin:/usr/local/bin:/usr/lib/jvm/sunjdk/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games"

JAVA_HOME="/usr/lib/jvm/sunjdk"


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

sudo lsof | grep /mnt || :
sudo fuser -k /mnt/*log || :

sudo lsblk

sudo umount -f /mnt || :
if [ -e $LV ] ; then
    sudo umount -f $LV || :
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

sudo lsblk

sudo df -h

NUM_CPUS=`cat /proc/cpuinfo | fgrep proc | wc -l`
echo "$NUM_CPUS" > my_ncpus

sudo cp -f ~/share/yarn-ec2/lxc/share/lxc/templates/* /usr/share/lxc/templates/
sudo cp -f ~/share/yarn-ec2/lxc/etc/default/* /etc/default/
sudo cp -f ~/share/yarn-ec2/lxc/etc/lxc/* /etc/lxc/

function create_vm() {
### @param rack_id, host_id, ip, mem, ncpus ###
    VM_NAME=`echo r"$1"h"$2"`
    sudo lxc-create -n $VM_NAME -t debian -- \
        --release wheezy  ### --packages ??? ###
    sudo cp -r ~/.ssh /mnt/$VM_NAME/rootfs/root/
    sudo chown -R root:root /mnt/$VM_NAME/rootfs/root/.ssh
    sudo cp -f /etc/ssh/ssh_config /mnt/$VM_NAME/rootfs/etc/ssh/
    sudo cp -f /etc/profile /mnt/$VM_NAME/rootfs/etc/
    sudo cp -r /srv/yarn /srv/yarn-$VM_NAME
    sudo rm -f /srv/yarn-$VM_NAME/conf/yarn-site.xml
    sudo cp ~/share/yarn-ec2/node-mngr/conf/yarn-site.xml /srv/yarn-$VM_NAME/conf/
    echo "lxc.mount.entry = /srv/yarn-$VM_NAME srv/yarn none rw,bind,create=dir" | \
         sudo tee -a /mnt/$VM_NAME/config
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

sudo cp -f ~/share/yarn-ec2/exec/* /usr/local/sbin/
sudo cp -f ~/share/yarn-ec2/hd/exec/* /usr/local/sbin/
sudo cp -f ~/share/yarn-ec2/resource-mngr/exec/* /usr/local/sbin/
sudo cp -f ~/share/yarn-ec2/node-mngr/exec/* /usr/local/sbin/

sudo mkdir -p ~/lib
sudo mkdir -p ~/bin
sudo mkdir -p ~/src

popd > /dev/null

exit 0
