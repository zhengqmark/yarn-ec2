#!/usr/bin/env python
# -*- coding: utf-8 -*-

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

from __future__ import division, print_function, with_statement

import codecs
import hashlib
import itertools
import logging
import os
import os.path
import pipes
import random
import shutil
import string
import subprocess
import sys
import tarfile
import tempfile
import textwrap
import time
import warnings
from datetime import datetime
from optparse import OptionParser
from stat import S_IRUSR
from sys import stderr

if sys.version < "3":
    from urllib2 import urlopen, Request, HTTPError
else:
    from urllib.request import urlopen, Request
    from urllib.error import HTTPError

    raw_input = input
    xrange = range

YARN_EC2_VERSION = "master"
YARN_EC2_DIR = os.path.dirname(os.path.realpath(__file__))

VALID_YARN_VERSIONS = set([
    "master"
])

DEFAULT_YARN_VERSION = YARN_EC2_VERSION
DEFAULT_YARN_GITHUB_REPO = "https://github.com/zhengqmark/yarn"

# Default location to get the yarn-ec2 scripts (and ami-list) from
DEFAULT_YARN_EC2_GITHUB_REPO = "https://github.com/zhengqmark/yarn-ec2"
DEFAULT_YARN_EC2_BRANCH = "master"


def setup_external_libs(libs):
    """
    Download external libraries from PyPI to YARN_EC2_DIR/lib and prepend them to our PATH.
    """
    PYPI_URL_PREFIX = "https://pypi.python.org/packages"
    YARN_EC2_LIB_DIR = os.path.join(YARN_EC2_DIR, "lib")

    if not os.path.exists(YARN_EC2_LIB_DIR):
        print("Downloading external libraries that yarn-ec2 needs from PyPI to {path}...".format(
            path=YARN_EC2_LIB_DIR
        ))
        print("This should be a one-time operation.")
        os.mkdir(YARN_EC2_LIB_DIR)

    for lib in libs:
        versioned_lib_name = "{n}-{v}".format(n=lib["name"], v=lib["version"])
        lib_dir = os.path.join(YARN_EC2_LIB_DIR, versioned_lib_name)

        if not os.path.isdir(lib_dir):
            tgz_file_path = os.path.join(YARN_EC2_LIB_DIR, versioned_lib_name + ".tar.gz")
            print(" - Downloading {lib}-{ver}...".format(lib=lib["name"], ver=lib["version"]))
            download_stream = urlopen(
                "{prefix}/{h0}/{h1}/{h2}/{lib_name}-{lib_version}.tar.gz".format(
                    prefix=PYPI_URL_PREFIX,
                    h0=lib["hash"][:2],
                    h1=lib["hash"][2:4],
                    h2=lib["hash"][4:],
                    lib_name=lib["name"],
                    lib_version=lib["version"]
                )
            )
            with open(tgz_file_path, "wb") as tgz_file:
                tgz_file.write(download_stream.read())
            with open(tgz_file_path, "rb") as tar:
                if hashlib.md5(tar.read()).hexdigest() != lib["md5"]:
                    print("ERROR: Got wrong md5sum for {lib}.".format(lib=lib["name"]), file=stderr)
                    sys.exit(1)
            tar = tarfile.open(tgz_file_path)
            tar.extractall(path=YARN_EC2_LIB_DIR)
            tar.close()
            os.remove(tgz_file_path)
            print(" - Finished downloading {lib}.".format(lib=lib["name"]))
        sys.path.insert(1, lib_dir)


# Only PyPI libraries are supported.
external_libs = [
    {
        "name": "boto",
        "version": "2.46.1",
        "hash": "b1f9cf8fa9a4a48e651294fc88446edee96f8b965f1d3ca044befc5dd7c9449b",
        "md5": "0f952cefb7631d7847da07febb2b15cd"
    }
]

setup_external_libs(external_libs)

from boto.ec2.blockdevicemapping import BlockDeviceMapping, BlockDeviceType, EBSBlockDeviceType
from boto import ec2

import boto


class UsageError(Exception):
    pass


# Configure and parse our command-line arguments
def parse_args():
    parser = OptionParser(
        prog="yarn-ec2",
        version="%prog {v}".format(v=YARN_EC2_VERSION),
        usage="%prog [options] <action> <cluster_name>\n\n"
              + "<action> can be: launch, destroy, login, get-master, stop, start")

    parser.add_option(
        "-s", "--slaves", type="int", default=1,
        help="Number of slaves to launch (default: %default)")
    parser.add_option(
        "-k", "--key-pair",
        help="Key pair to use on instances")
    parser.add_option(
        "-i", "--identity-file",
        help="SSH private key file to use for logging into instances")
    parser.add_option(
        "-p", "--profile", default=None,
        help="If you have multiple profiles (AWS or boto config), you can configure " +
             "additional, named profiles by using this option (default: %default)")
    parser.add_option(
        "-t", "--instance-type", default="c3.4xlarge",
        help="Type of instance to launch (default: %default). " +
             "WARNING: must be 64-bit; small instances won't work")
    parser.add_option(
        "-m", "--master-instance-type", default="",
        help="Master instance type (leave empty for same as instance-type)")
    parser.add_option(
        "-r", "--region", default="us-east-1",
        help="EC2 region used to launch instances in, or to find them in (default: %default)")
    parser.add_option(
        "-z", "--zone", default="",
        help="Availability zone to launch instances in, or 'all' to spread " +
             "slaves across multiple (an additional $0.01/Gb for bandwidth" +
             "between zones applies) (default: a single zone chosen at random)")
    parser.add_option(
        "-a", "--ami",
        help="Amazon Machine Image ID to use")
    parser.add_option(
        "-v", "--yarn-version", default=DEFAULT_YARN_VERSION,
        help="Version of YARN to use: 'X.Y.Z' or a specific git hash (default: %default)")
    parser.add_option(
        "--yarn-git-repo",
        default=DEFAULT_YARN_GITHUB_REPO,
        help="Github repo from which to checkout supplied commit hash (default: %default)")
    parser.add_option(
        "--yarn-ec2-git-repo",
        default=DEFAULT_YARN_EC2_GITHUB_REPO,
        help="Github repo from which to checkout yarn-ec2 (default: %default)")
    parser.add_option(
        "--yarn-ec2-git-branch",
        default=DEFAULT_YARN_EC2_BRANCH,
        help="Github repo branch of yarn-ec2 to use (default: %default)")
    parser.add_option(
        "-D", metavar="[ADDRESS:]PORT", dest="proxy_port",
        help="Use SSH dynamic port forwarding to create a SOCKS proxy at " +
             "the given local address (for use with login)")
    parser.add_option(
        "--resume", action="store_true", default=False,
        help="Resume installation on a previously launched cluster " +
             "(for debugging)")
    parser.add_option(
        "--ebs-vol-size", metavar="SIZE", type="int", default=0,
        help="Size (in GB) of each EBS volume.")
    parser.add_option(
        "--ebs-vol-type", default="standard",
        help="EBS volume type (e.g. 'gp2', 'standard').")
    parser.add_option(
        "--ebs-vol-num", type="int", default=1,
        help="Number of EBS volumes to attach to each node as /vol[x]. " +
             "The volumes will be deleted when the instances terminate. " +
             "Only possible on EBS-backed AMIs. " +
             "EBS volumes are only attached if --ebs-vol-size > 0. " +
             "Only support up to 8 EBS volumes.")
    parser.add_option(
        "--placement-group", type="string", default=None,
        help="Which placement group to try and launch " +
             "instances into. Assumes placement group is already " +
             "created.")
    parser.add_option(
        "--spot-price", metavar="PRICE", type="float",
        help="If specified, launch slaves as spot instances with the given " +
             "maximum price (in dollars)")
    parser.add_option(
        "-u", "--user", default="ubuntu",
        help="The SSH user you want to connect as (default: %default)")
    parser.add_option(
        "--delete-groups", action="store_true", default=False,
        help="When destroying a cluster, delete the security groups that were created")
    parser.add_option(
        "--use-existing-master", action="store_true", default=False,
        help="Launch fresh slaves, but use an existing stopped master if possible")
    parser.add_option(
        "--user-data", type="string", default="",
        help="Path to a user-data file (most AMIs interpret this as an initialization script)")
    parser.add_option(
        "--authorized-address", type="string", default="0.0.0.0/0",
        help="Address to authorize on created security groups (default: %default)")
    parser.add_option(
        "--additional-security-group", type="string", default="",
        help="Additional security group to place the machines in")
    parser.add_option(
        "--additional-tags", type="string", default="",
        help="Additional tags to set on the machines; tags are comma-separated, while name and " +
             "value are colon separated; ex: \"Course:advcc,Project:yarn\"")
    parser.add_option(
        "--subnet-id", default=None,
        help="VPC subnet to launch instances in")
    parser.add_option(
        "--vpc-id", default=None,
        help="VPC to launch instances in")
    parser.add_option(
        "--secondary-ips", type="int", default=6,
        help="Num of secondary private ip addresses to assign for each cluster node")
    parser.add_option(
        "--private-ips", action="store_true", default=False,
        help="Use private IPs for instances rather than public if VPC/subnet " +
             "requires that.")
    parser.add_option(
        "--instance-initiated-shutdown-behavior", default="stop",
        choices=["stop", "terminate"],
        help="Whether instances should terminate when shut down or just stop")
    parser.add_option(
        "--instance-profile-name", default=None,
        help="IAM profile name to launch instances under")

    (opts, args) = parser.parse_args()
    if len(args) != 2:
        parser.print_help()
        sys.exit(1)
    (action, cluster_name) = args

    # Boto config check
    # http://boto.cloudhackers.com/en/latest/boto_config_tut.html
    home_dir = os.getenv('HOME')
    if home_dir is None or not os.path.isfile(home_dir + '/.boto'):
        if not os.path.isfile('/etc/boto.cfg'):
            # If there is no boto config, check aws credentials
            if not os.path.isfile(home_dir + '/.aws/credentials'):
                if os.getenv('AWS_ACCESS_KEY_ID') is None:
                    print("ERROR: The environment variable AWS_ACCESS_KEY_ID must be set",
                          file=stderr)
                    sys.exit(1)
                if os.getenv('AWS_SECRET_ACCESS_KEY') is None:
                    print("ERROR: The environment variable AWS_SECRET_ACCESS_KEY must be set",
                          file=stderr)
                    sys.exit(1)
    return (opts, action, cluster_name)


# Get the EC2 security group of the given name, creating it if it doesn't exist
def get_or_make_group(conn, name, vpc_id):
    groups = conn.get_all_security_groups()
    group = [g for g in groups if g.name == name]
    if len(group) > 0:
        return group[0]
    else:
        print("Creating security group " + name)
        return conn.create_security_group(name, "yarn-ec2 group", vpc_id)


def get_validate_yarn_version(version, repo):
    if "." in version:
        version = version.replace("v", "")
        if version not in VALID_YARN_VERSIONS:
            print("Don't know about YARN version: {v}".format(v=version), file=stderr)
            sys.exit(1)
        return version
    else:
        github_commit_url = "{repo}/commit/{commit_hash}".format(repo=repo, commit_hash=version)
        request = Request(github_commit_url)
        request.get_method = lambda: 'HEAD'
        try:
            response = urlopen(request)
        except HTTPError as e:
            print("Couldn't validate YARN commit: {url}".format(url=github_commit_url),
                  file=stderr)
            print("Received HTTP response code of {code}.".format(code=e.code), file=stderr)
            sys.exit(1)
        return version


# Source: http://aws.amazon.com/amazon-linux-ami/instance-type-matrix/
# Last Updated: 2017-03-11
# For easy maintainability, please keep this manually-inputted dictionary sorted by key.
EC2_INSTANCE_TYPES = {
    "c3.large": "hvm",
    "c3.xlarge": "hvm",
    "c3.2xlarge": "hvm",
    "c3.4xlarge": "hvm",
    "c3.8xlarge": "hvm",
    "c4.large": "hvm",
    "c4.xlarge": "hvm",
    "c4.2xlarge": "hvm",
    "c4.4xlarge": "hvm",
    "c4.8xlarge": "hvm",
    "m3.medium": "hvm",
    "m3.large": "hvm",
    "m3.xlarge": "hvm",
    "m3.2xlarge": "hvm",
    "m4.large": "hvm",
    "m4.xlarge": "hvm",
    "m4.2xlarge": "hvm",
    "m4.4xlarge": "hvm",
    "m4.10xlarge": "hvm",
    "m4.16xlarge": "hvm",
    "r3.large": "hvm",
    "r3.xlarge": "hvm",
    "r3.2xlarge": "hvm",
    "r3.4xlarge": "hvm",
    "r3.8xlarge": "hvm",
    "r4.large": "hvm",
    "r4.xlarge": "hvm",
    "r4.2xlarge": "hvm",
    "r4.4xlarge": "hvm",
    "r4.8xlarge": "hvm",
    "r4.16xlarge": "hvm",
    "t2.nano": "hvm",
    "t2.micro": "hvm",
    "t2.small": "hvm",
    "t2.medium": "hvm",
    "t2.large": "hvm",
    "t2.xlarge": "hvm",
    "t2.2xlarge": "hvm",
}


# Attempt to resolve an appropriate AMI given the architecture and region of the request.
def get_yarn_ami(opts):
    ami = 'ami-f4cc1de2'  # Ubuntu 16.04
    print("AMI: " + ami)
    return ami


# Init a new security group
def init_security_group(sg, cidr):
    sg.authorize(ip_protocol='icmp', from_port=-1, to_port=-1, cidr_ip=cidr)

    sg.authorize('tcp', 0, 65535, cidr)
    sg.authorize('udp', 0, 65535, cidr)


# Launch a cluster of the given name, by setting up its security groups,
# and then starting new instances in them.
# Returns a tuple of EC2 reservation objects for the master and slaves
# Fails if there already instances running in the cluster's groups.
def launch_cluster(conn, opts, cluster_name):
    if opts.identity_file is None:
        print("ERROR: Must provide an identity file (-i) for ssh connections.", file=stderr)
        sys.exit(1)

    if opts.key_pair is None:
        print("ERROR: Must provide a key pair name (-k) to use on instances.", file=stderr)
        sys.exit(1)

    user_data_content = None
    if opts.user_data:
        with open(opts.user_data) as user_data_file:
            user_data_content = user_data_file.read()

    print("Setting up security groups...")
    authorized_address = opts.authorized_address
    master_group = get_or_make_group(conn, cluster_name + "-master", opts.vpc_id)
    if master_group.rules == []:  # Group was just now created
        init_security_group(master_group, authorized_address)
    slave_group = get_or_make_group(conn, cluster_name + "-slaves", opts.vpc_id)
    if slave_group.rules == []:  # Group was just now created
        init_security_group(slave_group, authorized_address)

    # Check if instances are already running in our groups
    existing_masters, existing_slaves = get_existing_cluster(
        conn, opts, cluster_name, die_on_error=False)
    if existing_slaves or (existing_masters and not opts.use_existing_master):
        print("ERROR: There are already instances running in group %s or %s" %
              (master_group.name, slave_group.name), file=stderr)
        sys.exit(1)

    # Figure out AMI
    if opts.ami is None:
        opts.ami = get_yarn_ami(opts)

    # Use group ids to work around https://github.com/boto/boto/issues/350
    additional_group_ids = []
    if opts.additional_security_group:
        additional_group_ids = [sg.id
                                for sg in conn.get_all_security_groups()
                                if opts.additional_security_group in (sg.name, sg.id)]
    print("Launching instances...")

    try:
        image = conn.get_all_images(image_ids=[opts.ami])[0]
    except:
        print("Could not find AMI " + opts.ami, file=stderr)
        sys.exit(1)

    # Create block device mapping so that we can add EBS volumes if asked to.
    # The first drive is attached as /dev/sds, 2nd as /dev/sdt, ... /dev/sdz
    block_map = BlockDeviceMapping()
    if opts.ebs_vol_size > 0:
        for i in range(opts.ebs_vol_num):
            device = EBSBlockDeviceType()
            device.size = opts.ebs_vol_size
            device.volume_type = opts.ebs_vol_type
            device.delete_on_termination = True
            block_map["/dev/sd" + chr(ord('s') + i)] = device

    # AMI-specified block device mapping for C3 instances
    if opts.instance_type.startswith('c3.'):
        for i in range(get_num_disks(opts.instance_type)):
            dev = BlockDeviceType()
            dev.ephemeral_name = 'ephemeral%d' % i
            # The first ephemeral drive is /dev/sdb.
            name = '/dev/sd' + string.ascii_letters[i + 1]
            block_map[name] = dev

    # Launch slaves
    if opts.slaves != 0 and opts.spot_price is not None:
        # Launch spot instances with the requested price
        print("Requesting %d slaves at $%.3f per hour" %
              (opts.slaves, opts.spot_price))
        zones = get_zones(conn, opts)
        num_zones = len(zones)
        i = 0
        slave_req_ids = []
        for zone in zones:
            num_slaves_this_zone = get_partition(opts.slaves, num_zones, i)
            slave_reqs = conn.request_spot_instances(
                price=opts.spot_price,
                image_id=opts.ami,
                launch_group="yarn-launch-group-%s" % cluster_name,
                placement=zone,
                count=num_slaves_this_zone,
                key_name=opts.key_pair,
                security_group_ids=[slave_group.id] + additional_group_ids,
                instance_type=opts.instance_type,
                block_device_map=block_map,
                subnet_id=opts.subnet_id,
                placement_group=opts.placement_group,
                user_data=user_data_content,
                instance_profile_name=opts.instance_profile_name)
            slave_req_ids += [req.id for req in slave_reqs]
            i += 1

        print("Waiting...")
        try:
            while True:
                time.sleep(10)
                reqs = conn.get_all_spot_instance_requests()
                id_to_req = {}
                for r in reqs:
                    id_to_req[r.id] = r
                slave_instance_ids = []
                for i in slave_req_ids:
                    if i in id_to_req and id_to_req[i].state == "active":
                        slave_instance_ids.append(id_to_req[i].instance_id)
                if len(slave_instance_ids) == opts.slaves:
                    print("%d slaves granted" % opts.slaves)
                    reservations = conn.get_all_reservations(slave_instance_ids)
                    slave_nodes = []
                    for r in reservations:
                        slave_nodes += r.instances
                    break
                else:
                    print("%d of %d slaves granted, waiting longer" % (
                        len(slave_instance_ids), opts.slaves))
        except:
            print("Canceling spot instance requests")
            conn.cancel_spot_instance_requests(slave_req_ids)
            # Log a warning if any of these requests actually launched instances:
            (master_nodes, slave_nodes) = get_existing_cluster(
                conn, opts, cluster_name, die_on_error=False)
            running = len(master_nodes) + len(slave_nodes)
            if running:
                print(("WARNING: %d instances are still running" % running), file=stderr)
            sys.exit(0)
    else:
        # Launch non-spot instances
        zones = get_zones(conn, opts)
        num_zones = len(zones)
        i = 0
        slave_nodes = []
        for zone in zones:
            num_slaves_this_zone = get_partition(opts.slaves, num_zones, i)
            if num_slaves_this_zone > 0:
                slave_res = image.run(
                    key_name=opts.key_pair,
                    security_group_ids=[slave_group.id] + additional_group_ids,
                    instance_type=opts.instance_type,
                    placement=zone,
                    min_count=num_slaves_this_zone,
                    max_count=num_slaves_this_zone,
                    block_device_map=block_map,
                    subnet_id=opts.subnet_id,
                    placement_group=opts.placement_group,
                    user_data=user_data_content,
                    instance_initiated_shutdown_behavior=opts.instance_initiated_shutdown_behavior,
                    instance_profile_name=opts.instance_profile_name)
                slave_nodes += slave_res.instances
                print("Launched {s} slave{plural_s} in {z}".format(
                    s=num_slaves_this_zone,
                    plural_s=('' if num_slaves_this_zone == 1 else 's'),
                    z=zone))
            i += 1

    # Launch or resume masters
    if existing_masters:
        print("Starting master...")
        for inst in existing_masters:
            if inst.state not in ["shutting-down", "terminated"]:
                inst.start()
        master_nodes = existing_masters
    else:
        if opts.spot_price is not None:
            # Launch spot instances with the requested price
            print("Requesting 1 master at $%.3f per hour" % opts.spot_price)
            master_type = opts.master_instance_type
            if master_type == "":
                master_type = opts.instance_type
            master_zone = opts.zone
            if master_zone == 'all':
                master_zone = random.choice(conn.get_all_zones()).name
            master_req_ids = []
            master_req = conn.request_spot_instances(
                price=opts.spot_price,
                image_id=opts.ami,
                launch_group="yarn-launch-group-%s" % cluster_name,
                placement=master_zone,
                count=1,
                key_name=opts.key_pair,
                security_group_ids=[master_group.id] + additional_group_ids,
                instance_type=master_type,
                block_device_map=block_map,
                subnet_id=opts.subnet_id,
                placement_group=opts.placement_group,
                user_data=user_data_content,
                instance_profile_name=opts.instance_profile_name)
            master_req_ids += [req.id for req in master_req]

            print("Waiting...")
            try:
                while True:
                    time.sleep(10)
                    reqs = conn.get_all_spot_instance_requests()
                    id_to_req = {}
                    for r in reqs:
                        id_to_req[r.id] = r
                    master_instance_ids = []
                    for i in master_req_ids:
                        if i in id_to_req and id_to_req[i].state == "active":
                            master_instance_ids.append(id_to_req[i].instance_id)
                    if len(master_instance_ids) == 1:
                        print("1 master granted")
                        reservations = conn.get_all_reservations(master_instance_ids)
                        master_nodes = []
                        for r in reservations:
                            master_nodes += r.instances
                        break
                    else:
                        print("%d of %d master granted, waiting longer" % (
                            len(master_instance_ids), 1))
            except:
                print("Canceling spot instance requests")
                conn.cancel_spot_instance_requests(master_req_ids)
                # Log a warning if any of these requests actually launched instances:
                (master_nodes, slave_nodes) = get_existing_cluster(
                    conn, opts, cluster_name, die_on_error=False)
                running = len(master_nodes) + len(slave_nodes)
                if running:
                    print(("WARNING: %d instances are still running" % running), file=stderr)
                sys.exit(0)
        else:
            # Launch non-spot instances
            master_type = opts.master_instance_type
            if master_type == "":
                master_type = opts.instance_type
            master_zone = opts.zone
            if master_zone == 'all':
                master_zone = random.choice(conn.get_all_zones()).name
            master_nodes = []
            master_res = image.run(
                key_name=opts.key_pair,
                security_group_ids=[master_group.id] + additional_group_ids,
                instance_type=master_type,
                placement=master_zone,
                min_count=1,
                max_count=1,
                block_device_map=block_map,
                subnet_id=opts.subnet_id,
                placement_group=opts.placement_group,
                user_data=user_data_content,
                instance_initiated_shutdown_behavior=opts.instance_initiated_shutdown_behavior,
                instance_profile_name=opts.instance_profile_name)
            master_nodes += master_res.instances
            print("Launched 1 master in {z}".format(z=master_zone))

    # Timed wait
    print("Waiting for aws to propagate instance metadata...")
    time.sleep(15)
    print("OK")

    # Give the instances descriptive names and set additional tags
    additional_tags = {}
    if opts.additional_tags.strip():
        additional_tags = dict(
            map(str.strip, tag.split(':', 1)) for tag in opts.additional_tags.split(',')
        )

    for master in master_nodes:
        master.add_tags(
            dict(additional_tags, Name='{cn}-master-{iid}'.format(cn=cluster_name, iid=master.id))
        )

    for slave in slave_nodes:
        slave.add_tags(
            dict(additional_tags, Name='{cn}-slave-{iid}'.format(cn=cluster_name, iid=slave.id))
        )

    # Return all the instances
    return (master_nodes, slave_nodes)


# Reset 2nd ip addresses
def reassign_cluster_ips(conn, master_nodes, slave_nodes, opts, cluster_name):
    ''' reset cluster ip addresses '''
    print("Reassigning secondary ip addresses...")

    for inst in master_nodes + slave_nodes:
        if inst.state != "terminated" and len(inst.interfaces) != 0:
            nif = inst.interfaces[0]
            if len(nif.private_ip_addresses) != opts.secondary_ips + 1:
                succ = True
                for addr in nif.private_ip_addresses:
                    if not addr.primary:
                        succ = conn.unassign_private_ip_addresses(nif.id, addr.private_ip_address)
                        if not succ:
                            break
                succ = conn.assign_private_ip_addresses(
                    nif.id, secondary_private_ip_address_count=opts.secondary_ips,
                    allow_reassignment=False) if succ else False
                if not succ:
                    print("Could not reassign secondary ip addresses", file=stderr)
                    sys.exit(1)
                else:
                    nif.update(conn)

    print("OK")


# Retrieve an outstanding cluster
def get_existing_cluster(conn, opts, cluster_name, die_on_error=True):
    """
    Get the EC2 instances in an existing cluster if available.
    Returns a tuple of lists of EC2 instance objects for the masters and slaves.
    """
    print("Searching for existing cluster {c} in region {r}...".format(
        c=cluster_name, r=opts.region))

    def get_instances(group_names):
        """
        Get all non-terminated instances that belong to any of the provided security groups.

        EC2 reservation filters and instance states are documented here:
            http://docs.aws.amazon.com/cli/latest/reference/ec2/describe-instances.html#options
        """
        reservations = conn.get_all_reservations(
            filters={"instance.group-name": group_names})
        instances = itertools.chain.from_iterable(r.instances for r in reservations)
        return [i for i in instances if i.state not in ["shutting-down", "terminated"]]

    master_instances = get_instances([cluster_name + "-master"])
    slave_instances = get_instances([cluster_name + "-slaves"])

    if any((master_instances, slave_instances)):
        print("Found {m} master{plural_m}, {s} slave{plural_s}.".format(
            m=len(master_instances),
            plural_m=('' if len(master_instances) == 1 else 's'),
            s=len(slave_instances),
            plural_s=('' if len(slave_instances) == 1 else 's')))

    if not master_instances and die_on_error:
        print("ERROR: Could not find a master for cluster {c} in region {r}.".format(
            c=cluster_name, r=opts.region), file=sys.stderr)
        sys.exit(1)

    return (master_instances, slave_instances)


# Deploy configuration files and run setup scripts on a newly launched or started cluster.
def setup_cluster(conn, master_nodes, slave_nodes, opts, deploy_ssh_key):
    master = get_dns_name(master_nodes[0], opts.private_ips)
    if deploy_ssh_key:
        print("Generating cluster's SSH key on master...")
        key_setup = """
          [ -f ~/.ssh/id_rsa ] ||
            (ssh-keygen -q -t rsa -N '' -f ~/.ssh/id_rsa -C ibuki &&
             cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys)
        """
        ssh(master, opts, key_setup)
        dot_ssh_tar = ssh_read(master, opts, ['tar', 'c', '.ssh'])
        print("Transferring cluster's SSH key to slaves...")
        for slave in slave_nodes:
            slave_address = get_dns_name(slave, opts.private_ips)
            print(slave_address)
            ssh_write(
                host=slave_address,
                opts=opts,
                command=['tar', 'x'],
                arguments=dot_ssh_tar
            )
        print("Passing SSH keys to root...")
        for node in master_nodes + slave_nodes:
            ssh(get_dns_name(node, opts.private_ips), opts, "sudo cp -r ~/.ssh /root/")

    print("Cloning yarn-ec2 scripts from {r}/tree/{b} on master...".format(
        r=opts.yarn_ec2_git_repo, b=opts.yarn_ec2_git_branch))
    ssh(
        host=master,
        opts=opts,
        command="sudo rm -rf /root/share/yarn-ec2"
                + " && "
                + "sudo git clone {r} -b {b} /root/share/yarn-ec2".format(
            r=opts.yarn_ec2_git_repo,
            b=opts.yarn_ec2_git_branch
        )
    )

    print("Deploying files to master...")
    deploy_files(
        conn=conn,
        root_dir=YARN_EC2_DIR + "/" + "deploy.generic",
        opts=opts,
        master_nodes=master_nodes,
        slave_nodes=slave_nodes
    )

    print("Running setup on master...")
    setup_spark_cluster(master, opts)
    print("Done!")


def setup_spark_cluster(master, opts):
    ssh(master, opts, "chmod u+x /root/share/yarn-ec2/setup.sh")
    ssh(master, opts, "/root/share/yarn-ec2/setup.sh")


def is_ssh_available(host, opts, print_ssh_output=True):
    """
    Check if SSH is available on a host.
    """
    s = subprocess.Popen(
        ssh_command(opts) + ['-q', '-t', '-t', '-o', 'ConnectTimeout=3',
                             '%s@%s' % (opts.user, host), stringify_command('true')],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT  # we pipe stderr through stdout to preserve output order
    )
    cmd_output = s.communicate()[0]  # [1] is stderr, which we redirected to stdout

    if s.returncode != 0 and print_ssh_output:
        # extra leading newline is for spacing in wait_for_cluster_state()
        print(textwrap.dedent("""\n
            Warning: SSH connection error. (This could be temporary.)
            Host: {h}
            SSH return code: {r}
            SSH output: {o}
        """).format(
            h=host,
            r=s.returncode,
            o=cmd_output.strip()
        ))

    return s.returncode == 0


def is_cluster_ssh_available(cluster_instances, opts):
    """
    Check if SSH is available on all the instances in a cluster.
    """
    for i in cluster_instances:
        dns_name = get_dns_name(i, opts.private_ips)
        if not is_ssh_available(host=dns_name, opts=opts):
            return False
    else:
        return True


def wait_for_cluster_state(conn, opts, cluster_instances, cluster_state):
    """
    Wait for all the instances in the cluster to reach a designated state.

    cluster_instances: a list of boto.ec2.instance.Instance
    cluster_state: a string representing the desired state of all the instances in the cluster
           value can be 'ssh-ready' or a valid value from boto.ec2.instance.InstanceState such as
           'running', 'terminated', etc.
           (would be nice to replace this with a proper enum: http://stackoverflow.com/a/1695250)
    """
    sys.stdout.write(
        "Waiting for cluster to enter '{s}' state...".format(s=cluster_state)
    )
    sys.stdout.flush()

    start_time = datetime.now()
    num_attempts = 0

    while True:
        time.sleep(5 * num_attempts)  # seconds

        for i in cluster_instances:
            i.update()

        max_batch = 100
        statuses = []
        for j in xrange(0, len(cluster_instances), max_batch):
            batch = [i.id for i in cluster_instances[j:j + max_batch]]
            statuses.extend(conn.get_all_instance_status(instance_ids=batch))

        if cluster_state == 'ssh-ready':
            if all(i.state == 'running' for i in cluster_instances) and \
                    all(s.system_status.status == 'ok' for s in statuses) and \
                    all(s.instance_status.status == 'ok' for s in statuses) and \
                    is_cluster_ssh_available(cluster_instances, opts):
                break
        else:
            if all(i.state == cluster_state for i in cluster_instances):
                break

        num_attempts += 1

        sys.stdout.write(".")
        sys.stdout.flush()

    sys.stdout.write("\n")

    end_time = datetime.now()
    print("Cluster is now in '{s}' state\nWaited {t} seconds".format(
        s=cluster_state,
        t=(end_time - start_time).seconds
    ))


# Get number of ip addresses available per nic for a given EC2 instance type.
def get_nic_width(instance_type):
    # Source: http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-eni.html
    # Last Updated: 2017-03-11
    # For easy maintainability, please keep this manually-inputted dictionary sorted by key.
    nic_ips_by_instance = {
        "c3.large": "10",
        "c3.xlarge": "15",
        "c3.2xlarge": "15",
        "c3.4xlarge": "30",
        "c3.8xlarge": "30",
        "c4.large": "10",
        "c4.xlarge": "15",
        "c4.2xlarge": "15",
        "c4.4xlarge": "30",
        "c4.8xlarge": "30",
        "m3.medium": "6",
        "m3.large": "10",
        "m3.xlarge": "15",
        "m3.2xlarge": "30",
        "m4.large": "10",
        "m4.xlarge": "15",
        "m4.2xlarge": "15",
        "m4.4xlarge": "30",
        "m4.10xlarge": "30",
        "m4.16xlarge": "30",
        "r3.large": "10",
        "r3.xlarge": "15",
        "r3.2xlarge": "15",
        "r3.4xlarge": "30",
        "r3.8xlarge": "30",
        "r4.large": "10",
        "r4.xlarge": "15",
        "r4.2xlarge": "15",
        "r4.4xlarge": "30",
        "r4.8xlarge": "30",
        "r4.16xlarge": "50",
        "t2.nano": "2",
        "t2.micro": "2",
        "t2.small": "4",
        "t2.medium": "6",
        "t2.large": "12",
        "t2.xlarge": "15",
        "t2.2xlarge": "15",
    }
    if instance_type in nic_ips_by_instance:
        return int(nic_ips_by_instance[instance_type])
    else:
        print("WARNING: Don't know the max number of ips per nic on instance type %s; assuming 2"
              % instance_type, file=stderr)
        return 2


# Get number of local disks available for a given EC2 instance type.
def get_num_disks(instance_type):
    # Source: http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/InstanceStorage.html
    # Last Updated: 2017-03-11
    # For easy maintainability, please keep this manually-inputted dictionary sorted by key.
    disks_by_instance = {
        "c3.large": "2",
        "c3.xlarge": "2",
        "c3.2xlarge": "2",
        "c3.4xlarge": "2",
        "c3.8xlarge": "2",
        "c4.large": "0",
        "c4.xlarge": "0",
        "c4.2xlarge": "0",
        "c4.4xlarge": "0",
        "c4.8xlarge": "0",
        "m3.medium": "1",
        "m3.large": "1",
        "m3.xlarge": "2",
        "m3.2xlarge": "2",
        "m4.large": "0",
        "m4.xlarge": "0",
        "m4.2xlarge": "0",
        "m4.4xlarge": "0",
        "m4.10xlarge": "0",
        "m4.16xlarge": "0",
        "r3.large": "1",
        "r3.xlarge": "1",
        "r3.2xlarge": "1",
        "r3.4xlarge": "1",
        "r3.8xlarge": "2",
        "r4.large": "0",
        "r4.xlarge": "0",
        "r4.2xlarge": "0",
        "r4.4xlarge": "0",
        "r4.8xlarge": "0",
        "r4.16xlarge": "0",
        "t2.nano": "0",
        "t2.micro": "0",
        "t2.small": "0",
        "t2.medium": "0",
        "t2.large": "0",
        "t2.xlarge": "0",
        "t2.2xlarge": "0",
    }
    if instance_type in disks_by_instance:
        return int(disks_by_instance[instance_type])
    else:
        print("WARNING: Don't know the number of disks on instance type %s; assuming 0"
              % instance_type, file=stderr)
        return 0


# Deploy the configuration file templates in a given local directory to
# a cluster, filling in any template parameters with information about the
# cluster (e.g. lists of masters and slaves). Files are only deployed to
# the first master instance in the cluster, and we expect the setup
# script to be run on that instance to copy them to other nodes.
#
# root_dir should be an absolute path to the directory with the files we want to deploy.
def deploy_files(conn, root_dir, opts, master_nodes, slave_nodes):
    active_master = get_dns_name(master_nodes[0], opts.private_ips)

    master_addresses = [get_dns_name(i, True) for i in master_nodes]
    slave_addresses = [get_dns_name(i, True) for i in slave_nodes]

    # Instantiate templates
    template_vars = {
        "master_list": '\n'.join(master_addresses),
        "slave_list": '\n'.join(slave_addresses),
        "rack0": '\n'.join(get_secondary_ip_addresses(master_nodes[0])),
        "rack1": '',
        "rack2": '',
        "rack3": '',
        "rack4": '',
    }

    for i in xrange(0, len(slave_nodes)):
        template_vars['rack' + str(i + 1)] = '\n'.join(get_secondary_ip_addresses(slave_nodes[i]))

    # Create a temp directory in which we will place all the files to be
    # deployed after we substitute template parameters in them
    tmp_dir = tempfile.mkdtemp()
    for path, dirs, files in os.walk(root_dir):
        if path.find(".svn") == -1:
            dest_dir = os.path.join('/', path[len(root_dir):])
            local_dir = tmp_dir + dest_dir
            if not os.path.exists(local_dir):
                os.makedirs(local_dir)
            for filename in files:
                if filename[0] not in '#.~' and filename[-1] != '~':
                    dest_file = os.path.join(dest_dir, filename)
                    local_file = tmp_dir + dest_file
                    with open(os.path.join(path, filename)) as src:
                        with open(local_file, "w") as dest:
                            text = src.read()
                            for key in template_vars:
                                text = text.replace("{{" + key + "}}", template_vars[key])
                            dest.write(text)
                            dest.close()
    # rsync the whole directory over to the master machine
    command = [
        'rsync', '-rv',
        '-e', stringify_command(ssh_command(opts)),
        "%s/" % tmp_dir,
        "%s@%s:/root" % ("root", active_master)
    ]
    subprocess.check_call(command)
    # Remove the temp directory we created above
    shutil.rmtree(tmp_dir)


def stringify_command(parts):
    if isinstance(parts, str):
        return parts
    else:
        return ' '.join(map(pipes.quote, parts))


def ssh_args(opts):
    parts = []

    parts += ['-o', 'StrictHostKeyChecking=no']
    parts += ['-o', 'UserKnownHostsFile=/dev/null']
    if opts.identity_file is not None:
        parts += ['-i', opts.identity_file]
    return parts


def ssh_command(opts):
    return ['ssh'] + ssh_args(opts)


# Run a command on a host through ssh, retrying up to five times
# and then throwing an exception if ssh continues to fail.
def ssh(host, opts, command):
    tries = 0
    while True:
        try:
            return subprocess.check_call(
                ssh_command(opts) + ['-t', '-t', '%s@%s' % (opts.user, host),
                                     stringify_command(command)])
        except subprocess.CalledProcessError as e:
            if tries > 5:
                # If this was an ssh failure, provide the user with hints.
                if e.returncode == 255:
                    raise UsageError(
                        "Failed to SSH to remote host {0}.\n"
                        "Please check that you have provided the correct --identity-file and "
                        "--key-pair parameters and try again.".format(host))
                else:
                    raise e
            print("Error executing remote command, retrying after 30 seconds: {0}".format(e),
                  file=stderr)
            time.sleep(30)
            tries = tries + 1


# Backported from Python 2.7 for compatiblity with 2.6 (See SPARK-1990)
def _check_output(*popenargs, **kwargs):
    if 'stdout' in kwargs:
        raise ValueError('stdout argument not allowed, it will be overridden.')
    process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
    output, unused_err = process.communicate()
    retcode = process.poll()
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        raise subprocess.CalledProcessError(retcode, cmd, output=output)
    return output


def ssh_read(host, opts, command):
    return _check_output(
        ssh_command(opts) + ['%s@%s' % (opts.user, host), stringify_command(command)])


def ssh_write(host, opts, command, arguments):
    tries = 0
    while True:
        proc = subprocess.Popen(
            ssh_command(opts) + ['%s@%s' % (opts.user, host), stringify_command(command)],
            stdin=subprocess.PIPE)
        proc.stdin.write(arguments)
        proc.stdin.close()
        status = proc.wait()
        if status == 0:
            break
        elif tries > 5:
            raise RuntimeError("ssh_write failed with error %s" % proc.returncode)
        else:
            print("Error {0} while executing remote command, retrying after 30 seconds".
                  format(status), file=stderr)
            time.sleep(30)
            tries = tries + 1


# Gets a list of zones to launch instances in
def get_zones(conn, opts):
    if opts.zone == 'all':
        zones = [z.name for z in conn.get_all_zones()]
    else:
        zones = [opts.zone]
    return zones


# Gets the number of items in a partition
def get_partition(total, num_partitions, current_partitions):
    num_slaves_this_zone = total // num_partitions
    if (total % num_partitions) - current_partitions > 0:
        num_slaves_this_zone += 1
    return num_slaves_this_zone


# Gets a list of secondary ip addresses
def get_secondary_ip_addresses(instance):
    if len(instance.interfaces) != 0:
        return [addr.private_ip_address for addr in instance.interfaces[0].private_ip_addresses if not addr.primary]
    else:
        return []


# Gets the IP address, taking into account the --private-ips flag
def get_ip_address(instance, private_ips=False):
    ip = instance.ip_address if not private_ips else \
        instance.private_ip_address
    return ip


# Gets the DNS name, taking into account the --private-ips flag
def get_dns_name(instance, private_ips=False):
    dns = instance.public_dns_name if not private_ips else \
        instance.private_ip_address
    if not dns:
        raise UsageError("Failed to determine hostname of {0}.\n"
                         "Please check that you provided --private-ips if "
                         "necessary".format(instance))
    return dns


def real_main():
    (opts, action, cluster_name) = parse_args()

    # Input parameter validation
    get_validate_yarn_version(opts.yarn_version, opts.yarn_git_repo)

    # Ensure identity file
    if opts.identity_file is not None:
        if not os.path.exists(opts.identity_file):
            print("ERROR: The identity file '{f}' doesn't exist.".format(f=opts.identity_file),
                  file=stderr)
            sys.exit(1)

        file_mode = os.stat(opts.identity_file).st_mode
        if not (file_mode & S_IRUSR) or not oct(file_mode)[-2:] == '00':
            print("ERROR: The identity file must be accessible only by you.", file=stderr)
            print('You can fix this with: chmod 400 "{f}"'.format(f=opts.identity_file),
                  file=stderr)
            sys.exit(1)

    if opts.instance_type not in EC2_INSTANCE_TYPES:
        print("Warning: Unrecognized EC2 instance type for instance-type: {t}".format(
            t=opts.instance_type), file=stderr)

        if opts.master_instance_type != "":
            if opts.master_instance_type not in EC2_INSTANCE_TYPES:
                print("Warning: Unrecognized EC2 instance type for master-instance-type: {t}".format(
                    t=opts.master_instance_type), file=stderr)
        # Since we try instance types even if we can't resolve them, we check if they resolve first
        # and, if they do, see if they resolve to the same VM type.
        if opts.instance_type in EC2_INSTANCE_TYPES and \
                        opts.master_instance_type in EC2_INSTANCE_TYPES:
            if EC2_INSTANCE_TYPES[opts.instance_type] != \
                    EC2_INSTANCE_TYPES[opts.master_instance_type]:
                print("Error: yarn-ec2 currently does not support having a master and slaves "
                      "with different AMI virtualization types.", file=stderr)
                print("master instance virtualization type: {t}".format(
                    t=EC2_INSTANCE_TYPES[opts.master_instance_type]), file=stderr)
                print("slave instance virtualization type: {t}".format(
                    t=EC2_INSTANCE_TYPES[opts.instance_type]), file=stderr)
                sys.exit(1)

    if opts.secondary_ips + 1 > get_nic_width(opts.instance_type):
        print("Error: unable to allocate {c} secondary ip addresses for instance-type: {t}".format(
            c=opts.secondary_ips, t=opts.instance_type))
        sys.exit(1)

    if opts.master_instance_type != "":
        if opts.secondary_ips + 1 > get_nic_width(opts.master_instance_type):
            print("Error: unable to allocate {c} secondary ip addresses for master-instance-type: {t}".format(
                c=opts.secondary_ips, t=opts.master_instance_type))
            sys.exit(1)

    if opts.ebs_vol_num > 8:
        print("ebs-vol-num cannot be greater than 8", file=stderr)
        sys.exit(1)

    # Prevent breaking ami_prefix (/, .git and startswith checks)
    # Prevent forks with non yarn-ec2 names for now.
    if opts.yarn_ec2_git_repo.endswith("/") or \
            opts.yarn_ec2_git_repo.endswith(".git") or \
            not opts.yarn_ec2_git_repo.startswith("https://github.com") or \
            not opts.yarn_ec2_git_repo.endswith("yarn-ec2"):
        print("yarn-ec2-git-repo must be a github repo and it must not have a trailing / or .git. "
              "Furthermore, we currently only support forks named yarn-ec2.", file=stderr)
        sys.exit(1)

    try:
        if opts.profile is None:
            conn = ec2.connect_to_region(opts.region)
        else:
            conn = ec2.connect_to_region(opts.region, profile_name=opts.profile)
    except Exception as e:
        print((e), file=stderr)
        sys.exit(1)

    # Select an AZ at random if it was not specified.
    if opts.zone == "":
        opts.zone = random.choice(conn.get_all_zones()).name

    if action == "launch":
        if opts.slaves <= 0:
            opts.slaves = 0
        if opts.resume:
            (master_nodes, slave_nodes) = get_existing_cluster(conn, opts, cluster_name)
        else:
            (master_nodes, slave_nodes) = launch_cluster(conn, opts, cluster_name)
        wait_for_cluster_state(
            conn=conn,
            opts=opts,
            cluster_instances=(master_nodes + slave_nodes),
            cluster_state='ssh-ready'
        )
        reassign_cluster_ips(
            conn=conn,
            master_nodes=master_nodes,
            slave_nodes=slave_nodes,
            opts=opts,
            cluster_name=cluster_name
        )
        setup_cluster(
            conn=conn,
            master_nodes=master_nodes,
            slave_nodes=slave_nodes,
            opts=opts,
            deploy_ssh_key=True
        )

    elif action == "get-master":
        (master_nodes, slave_nodes) = get_existing_cluster(conn, opts, cluster_name)
        if not master_nodes[0].public_dns_name and not opts.private_ips:
            print("Master has no public DNS name.  Maybe you meant to specify --private-ips?")
        else:
            print(get_dns_name(master_nodes[0], opts.private_ips))

    elif action == "login":
        (master_nodes, slave_nodes) = get_existing_cluster(conn, opts, cluster_name)
        if not master_nodes[0].public_dns_name and not opts.private_ips:
            print("Master has no public DNS name.  Maybe you meant to specify --private-ips?")
        else:
            master = get_dns_name(master_nodes[0], opts.private_ips)
            print("Logging into master " + master + "...")
            proxy_opt = []
            if opts.proxy_port is not None:
                proxy_opt = ['-D', opts.proxy_port]
            subprocess.check_call(
                ssh_command(opts) + proxy_opt + ['-t', '-t', "%s@%s" % (opts.user, master)])

    elif action == "stop":
        response = raw_input(
            "Are you sure you want to stop the cluster " +
            cluster_name + "?\nDATA ON EPHEMERAL DISKS WILL BE LOST, " +
            "BUT THE CLUSTER WILL KEEP USING SPACE ON\n" +
            "AMAZON EBS IF IT IS EBS-BACKED!!\n" +
            "ALL DATA ON SPOT-INSTANCES WILL ALSO BE LOST!!\n" +
            "Stop cluster " + cluster_name + " (y/N): ")
        if response == "y":
            (master_nodes, slave_nodes) = get_existing_cluster(
                conn, opts, cluster_name, die_on_error=False)
            print("Stopping master...")
            for inst in master_nodes:
                if inst.state not in ["shutting-down", "terminated"]:
                    if inst.spot_instance_request_id:
                        inst.terminate()
                    else:
                        inst.stop()
            print("Stopping slaves...")
            for inst in slave_nodes:
                if inst.state not in ["shutting-down", "terminated"]:
                    if inst.spot_instance_request_id:
                        inst.terminate()
                    else:
                        inst.stop()

    elif action == "start":
        (master_nodes, slave_nodes) = get_existing_cluster(conn, opts, cluster_name)
        print("Starting slaves...")
        for inst in slave_nodes:
            if inst.state not in ["shutting-down", "terminated"]:
                inst.start()
        print("Starting master...")
        for inst in master_nodes:
            if inst.state not in ["shutting-down", "terminated"]:
                inst.start()
        wait_for_cluster_state(
            conn=conn,
            opts=opts,
            cluster_instances=(master_nodes + slave_nodes),
            cluster_state='ssh-ready'
        )
        reassign_cluster_ips(
            conn=conn,
            master_nodes=master_nodes,
            slave_nodes=slave_nodes,
            opts=opts,
            cluster_name=cluster_name
        )
        setup_cluster(
            conn=conn,
            master_nodes=master_nodes,
            slave_nodes=slave_nodes,
            opts=opts,
            deploy_ssh_key=True
        )

        # Determine types of running instances
        existing_master_type = master_nodes[0].instance_type
        existing_slave_type = slave_nodes[0].instance_type
        # Setting opts.master_instance_type to the empty string indicates we
        # have the same instance type for the master and the slaves
        if existing_master_type == existing_slave_type:
            existing_master_type = ""
        opts.master_instance_type = existing_master_type
        opts.instance_type = existing_slave_type

    elif action == "destroy":
        (master_nodes, slave_nodes) = get_existing_cluster(
            conn, opts, cluster_name, die_on_error=False)

        if any(master_nodes + slave_nodes):
            print("The following instances will be terminated:")
            for inst in master_nodes + slave_nodes:
                print("> %s" % get_dns_name(inst, opts.private_ips))
            print("ALL DATA ON ALL INSTANCES WILL BE LOST!!")

        msg = "Are you sure you want to destroy the cluster {c}? (y/N) ".format(c=cluster_name)
        response = raw_input(msg)
        if response == "y":
            print("Terminating master...")
            for inst in master_nodes:
                inst.terminate()
            print("Terminating slaves...")
            for inst in slave_nodes:
                inst.terminate()

            # Delete security groups as well
            if opts.delete_groups:
                group_names = [cluster_name + "-master", cluster_name + "-slaves"]
                wait_for_cluster_state(
                    conn=conn,
                    opts=opts,
                    cluster_instances=(master_nodes + slave_nodes),
                    cluster_state='terminated'
                )
                print("Deleting security groups (this may take some time)...")
                attempt = 1
                while attempt <= 3:
                    print("Attempt %d" % attempt)
                    groups = [g for g in conn.get_all_security_groups() if g.name in group_names]
                    success = True
                    # Delete individual rules in all groups before deleting groups to
                    # remove dependencies between them
                    for group in groups:
                        print("Deleting rules in security group " + group.name)
                        for rule in group.rules:
                            for grant in rule.grants:
                                success &= group.revoke(ip_protocol=rule.ip_protocol,
                                                        from_port=rule.from_port,
                                                        to_port=rule.to_port,
                                                        src_group=grant)

                    # Sleep for AWS eventual-consistency to catch up, and for instances
                    # to terminate
                    time.sleep(30)  # Yes, it does have to be this long :-(
                    for group in groups:
                        try:
                            # It is needed to use group_id to make it work with VPC
                            conn.delete_security_group(group_id=group.id)
                            print("Deleted security group %s" % group.name)
                        except boto.exception.EC2ResponseError:
                            success = False
                            print("Failed to delete security group %s" % group.name)

                    # Unfortunately, group.revoke() returns True even if a rule was not
                    # deleted, so this needs to be rerun if something fails
                    if success:
                        break

                    attempt += 1

                if not success:
                    print("Failed to delete all security groups after 3 tries.")
                    print("Try re-running in a few minutes.")


    else:
        print("Invalid action: %s" % action, file=stderr)
        sys.exit(1)


def main():
    try:
        real_main()
    except UsageError as e:
        print("\nError:\n", e, file=stderr)
        sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig()
    main()
