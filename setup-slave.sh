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

sudo apt-get install -y curl vim realpath

pushd $HOME > /dev/null

PRIMARY_IP=`curl http://169.254.169.254/latest/meta-data/local-ipv4`
MAC=`curl http://169.254.169.254/latest/meta-data/mac`
CIDR=`curl http://169.254.169.254/latest/meta-data/network/interfaces/macs/$MAC/subnet-ipv4-cidr-block`
PRIVATE_IPS=`curl http://169.254.169.254/latest/meta-data/network/interfaces/macs/$MAC/local-ipv4s`

MASK=`echo $CIDR | cut -d/ -f2`

DEV=`ls -1 /sys/class/net/ | fgrep -v lxc | fgrep -v lo | head -1`

sudo ip addr show dev $DEV

sudo ip addr flush secondary $DEV

for ip in `echo $PRIVATE_IPS | grep -v $PRIMARY_IP` ; do
    sudo ip addr add ${ip}/$MASK brd + dev $DEV
done

sudo ip addr show dev $DEV

popd > /dev/null

exit 0
