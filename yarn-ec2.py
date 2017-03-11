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

YARN_EC2_VERSION = "0.17"
YARN_EC2_DIR = os.path.dirname(os.path.realpath(__file__))

VALID_YARN_VERSIONS = set([
    "0.17"
])

DEFAULT_YARN_VERSION = YARN_EC2_VERSION
DEFAULT_YARN_GITHUB_REPO = "https://github.com/apache/spark"

# Default location to get the yarn-ec2 scripts (and ami-list) from
DEFAULT_YARN_EC2_GITHUB_REPO = "https://github.com/amplab/spark-ec2"
DEFAULT_YARN_EC2_BRANCH = "branch-1.6"


def setup_external_libs(libs):
    """
    Download external libraries from PyPI to YARN_EC2_DIR/lib and prepend them to our PATH.
    """
    PYPI_URL_PREFIX = "https://pypi.python.org/packages/source"
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
            print(" - Downloading {lib}...".format(lib=lib["name"]))
            download_stream = urlopen(
                "{prefix}/{first_letter}/{lib_name}/{lib_name}-{lib_version}.tar.gz".format(
                    prefix=PYPI_URL_PREFIX,
                    first_letter=lib["name"][:1],
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
        "md5": "0f952cefb7631d7847da07febb2b15cd"
    }
]

setup_external_libs(external_libs)

import boto
from boto.ec2.blockdevicemapping import BlockDeviceMapping, BlockDeviceType, EBSBlockDeviceType
from boto import ec2


class UsageError(Exception):
    pass

def real_main():
    pass

def main():
    try:
        real_main()
    except UsageError as e:
        print("\nError:\n", e, file=stderr)
        sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig()
    main()
