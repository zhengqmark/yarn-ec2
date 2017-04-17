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

pushd ~/var/yarn-ec2 > /dev/null

CIDR=`cat my_cidr`
ID=`cat my_id`
DEV=`cat my_nic`

for vm in `sudo lxc-ls` ; do
    sudo lxc-stop -k -n $vm || :
    sleep 0.1
done

sudo tc qdisc del dev $DEV root || :  ### purge old network queues ###
sudo iptables -t nat -F  ### will use our own rules ###

sudo tc qdisc add dev $DEV root handle 1: htb

RACK_ID="$ID"
HOST_ID=0
for ip in `cat rack-$ID/vmips` ; do
    NODE_ID=$(( HOST_ID + RACK_ID * 10 + 100))
    cat /etc/hosts | fgrep "192.168.1.$NODE_ID "
    sudo iptables -t nat -A PREROUTING -s $CIDR -d $ip -j DNAT --to 192.168.1.$NODE_ID
    sudo iptables -t nat -A POSTROUTING -s 192.168.1.$NODE_ID -d $CIDR -j SNAT --to $ip
    sudo tc class add dev $DEV parent 1: classid 1:$NODE_ID htb rate 625mbit ceil 625mbit
    sudo tc filter add dev $DEV protocol ip parent 1: prio 1 u32 match ip src $ip flowid 1:$NODE_ID
    VM_NAME=`echo r"$RACK_ID"h"$HOST_ID"`
    sudo lxc-start -n $VM_NAME
    HOST_ID=$(( HOST_ID + 1 ))
done

sudo iptables -t nat -A POSTROUTING -s 192.168.1.0/24 ! -d 192.168.1.0/24 \
    -j SNAT --to `cat my_primary_ip`
sudo iptables -t nat -L -n
sudo tc filter show dev $DEV
sudo lxc-ls -f

popd > /dev/null

exit 0
