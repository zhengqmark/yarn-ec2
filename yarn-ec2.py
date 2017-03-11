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
from stat import S_IRUSR
import subprocess
import sys
import tarfile
import tempfile
import textwrap
import time
import warnings
from datetime import datetime
from optparse import OptionParser
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

import boto
from boto.ec2.blockdevicemapping import BlockDeviceMapping, BlockDeviceType, EBSBlockDeviceType
from boto import ec2


class UsageError(Exception):
    pass


# Configure and parse our command-line arguments
def parse_args():
    parser = OptionParser(
        prog="yarn-ec2",
        version="%prog {v}".format(v=YARN_EC2_VERSION),
        usage="%prog [options] <action> <cluster_name>\n\n"
              + "<action> can be: launch, destroy, login, stop, start")

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
        "-t", "--instance-type", default="c3.large",
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
        "--deploy-root-dir",
        default=None,
        help="A directory to copy into / on the first master. " +
             "Must be absolute. Note that a trailing slash is handled as per rsync: " +
             "If you omit it, the last directory of the --deploy-root-dir path will be created " +
             "in / before copying its contents. If you append the trailing slash, " +
             "the directory is not created and its contents are copied directly into /. " +
             "(default: %default).")
    parser.add_option(
        "--hadoop-major-version", default="1",
        help="Major version of Hadoop. Valid options are 1 (Hadoop 1.0.4), 2 (CDH 4.2.0), yarn " +
             "(Hadoop 2.4.0) (default: %default)")
    parser.add_option(
        "-D", metavar="[ADDRESS:]PORT", dest="proxy_port",
        help="Use SSH dynamic port forwarding to create a SOCKS proxy at " +
             "the given local address (for use with login)")
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
        "-u", "--user", default="root",
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
    master_group = get_or_make_group(conn, cluster_name + "-master", opts.vpc_id)
    slave_group = get_or_make_group(conn, cluster_name + "-slaves", opts.vpc_id)
    authorized_address = opts.authorized_address
    if master_group.rules == []:  # Group was just now created
        master_group.authorize('tcp', 0, 65535, authorized_address)
        master_group.authorize('udp', 0, 65535, authorized_address)
        master_group.authorize('icmp', -1, -1, authorized_address)
    if slave_group.rules == []:  # Group was just now created
        slave_group.authorize('tcp', 0, 65535, authorized_address)
        slave_group.authorize('udp', 0, 65535, authorized_address)
        slave_group.authorize('icmp', -1, -1, authorized_address)

    # Check if instances are already running in our groups
    existing_masters, existing_slaves = get_existing_cluster(conn, opts, cluster_name,
                                                             die_on_error=False)
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
    if opts.spot_price is not None:
        # Launch spot instances with the requested price
        print("Requesting %d slaves as spot instances with price $%.3f" %
              (opts.slaves, opts.spot_price))
        zones = get_zones(conn, opts)
        num_zones = len(zones)
        if num_zones != 1:
            print("WARNING: creating instances across multiple zones", file=stderr)
        i = 0
        my_req_ids = []
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
            my_req_ids += [req.id for req in slave_reqs]
            i += 1

        print("Waiting for spot instances to be granted...")
        try:
            while True:
                time.sleep(10)
                reqs = conn.get_all_spot_instance_requests()
                id_to_req = {}
                for r in reqs:
                    id_to_req[r.id] = r
                master_instance_ids = []
                for i in my_req_ids:
                    if i in id_to_req and id_to_req[i].state == "active":
                        master_instance_ids.append(id_to_req[i].instance_id)
                if len(master_instance_ids) == opts.slaves:
                    print("All %d slaves granted" % opts.slaves)
                    reservations = conn.get_all_reservations(master_instance_ids)
                    slave_nodes = []
                    for r in reservations:
                        slave_nodes += r.instances
                    break
                else:
                    print("%d of %d slaves granted, waiting longer" % (
                        len(master_instance_ids), opts.slaves))
        except:
            print("Canceling spot instance requests")
            conn.cancel_spot_instance_requests(my_req_ids)
            # Log a warning if any of these requests actually launched instances:
            (master_nodes, slave_nodes) = get_existing_cluster(
                conn, opts, cluster_name, die_on_error=False)
            running = len(master_nodes) + len(slave_nodes)
            if running:
                print(("WARNING: %d instances are still running" % running), file=stderr)
            sys.exit(0)
    else:
        # Launch non-spot instances
        print("WARNING: not using spot-instances", file=stderr)
        zones = get_zones(conn, opts)
        num_zones = len(zones)
        if num_zones != 1:
            print("WARNING: creating instances across multiple zones", file=stderr)
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
                print("Launched {s} slave{plural_s} in {z}, regid = {r}".format(
                    s=num_slaves_this_zone,
                    plural_s=('' if num_slaves_this_zone == 1 else 's'),
                    z=zone,
                    r=slave_res.id))
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
            print("Requesting the master as a spot instance with price $%.3f" % opts.spot_price)
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

            print("Waiting for the master spot instance to be granted...")
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
            print("Launched 1 master in {z}, regid = {r}".format(z=master_zone, r=master_res.id))


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
        print("WARNING: Don't know number of disks on instance type %s; assuming 0"
              % instance_type, file=stderr)
        return 0


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

    if not (opts.deploy_root_dir is None or
                (os.path.isabs(opts.deploy_root_dir) and
                     os.path.isdir(opts.deploy_root_dir) and
                     os.path.exists(opts.deploy_root_dir))):
        print("--deploy-root-dir must be an absolute path to a directory that exists "
              "on the local file system", file=stderr)
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
            print("ERROR: You have to start at least 1 slave", file=sys.stderr)
            sys.exit(1)

        launch_cluster(conn, opts, cluster_name)


def main():
    try:
        real_main()
    except UsageError as e:
        print("\nError:\n", e, file=stderr)
        sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig()
    main()
