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

mkdir -p ~/tmp

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=5"
export PDSH_SSH_ARGS_APPEND="$SSH_OPTS"
PDSH="pdsh -S -R ssh -b"

echo "starting lxc on all cluster nodes..." > /dev/null
$PDSH -w ^all-nodes ~/share/yarn-ec2/start-slave.sh \
    2>&1 | tee ~/tmp/start-slaves.log

popd > /dev/null

exit 0
