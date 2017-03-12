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

# Install system updates
sudo apt-get update && sudo apt-get -y upgrade

sudo apt-get install -y lxc default-jre pssh

# Load the cluster variables set by the deploy script
if [ -f $HOME/etc/yarn-ec2.rc ] ; then
    source $HOME/etc/yarn-ec2.rc
fi

pushd $HOME > /dev/null

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=5"

echo "Setting up YARN on `hostname`..." > /dev/null

# Set up the masters, slaves, etc files based on cluster env variables
echo "$MASTERS" > masters
echo "$SLAVES" > slaves


echo "RSYNC'ing $HOME/share/yarn-ec2 to other cluster nodes..." > /dev/null
for node in `cat slaves` ; do
  echo $node > /dev/null
  rsync -e "ssh $SSH_OPTS" -az "$HOME/share/yarn-ec2" \
      "$node:$HOME" &
  sleep 0.1
done
wait

echo "Running setup-slave on all cluster nodes..." > /dev/null
pssh --inline \
    --host "`cat slaves`" \
    --user `whoami` \
    --extra-args "-t -t $SSH_OPTS" \
    --timeout 0 \
    "$HOME/share/yarn-ec2/setup-slave.sh"

popd > /dev/null

exit 0
