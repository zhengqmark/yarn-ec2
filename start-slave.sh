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

pushd $HOME/var/yarn-ec2 > /dev/null

CIDR=`cat my_cidr`
ID=`cat my_id`

sudo iptables -t nat -F  ### will use our own rules ###

function maybe_stop_vm() { ### @param vm_name ###
    sudo lxc-stop -k -n $1 || echo "OK"
}

for vm in `sudo lxc-ls` ; do
    maybe_stop_vm $vm &>/dev/null
    sleep 0.1
done

RACK_ID="$ID"
HOST_ID=0
for ip in `cat rack-$ID/vmips` ; do
    NODE_ID=$(( HOST_ID + RACK_ID * 10 + 100))
    cat /etc/hosts | fgrep 192.168.1.$NODE_ID
    sudo iptables -t nat -A PREROUTING -s $CIDR -d $ip -j DNAT --to 192.168.1.$NODE_ID
    sudo iptables -t nat -A POSTROUTING -s 192.168.1.$NODE_ID -d $CIDR -j SNAT --to $ip
    VM_NAME=`echo r"$RACK_ID"h"$HOST_ID"`
    sudo lxc-start -n $VM_NAME
    HOST_ID=$(( HOST_ID + 1 ))
done

sudo iptables -t nat -A POSTROUTING -s 192.168.1.0/24 ! -d 192.168.1.0/24 \
    -j SNAT --to `cat my_primary_ip`
sudo iptables -t nat -L -n
sudo lxc-ls -f

popd > /dev/null

exit 0
