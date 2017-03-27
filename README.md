**IT WORKS**

YARN-ec2
========

Setup YARN on a set of ec2 instances in a single highly-automated step ^_^

http://www.cs.cmu.edu/~15719/

[![License](https://img.shields.io/badge/license-Apache%202-blue.svg)](LICENSE)

## USAGE

```
Usage: yarn-ec2 [options] <action> <cluster_name>

<action> can be: launch, destroy, login, get-master, stop, start

Options:
  --version             show program's version number and exit
  -h, --help            show this help message and exit
  -s SLAVES, --slaves=SLAVES
                        Number of slaves to launch (default: 1)
  -k KEY_PAIR, --key-pair=KEY_PAIR
                        Key pair to use on instances
  -i IDENTITY_FILE, --identity-file=IDENTITY_FILE
                        SSH private key file to use for logging into instances
  -p PROFILE, --profile=PROFILE
                        If you have multiple profiles (AWS or boto config),
                        you can configure additional, named profiles by using
                        this option (default: none)
  -t INSTANCE_TYPE, --instance-type=INSTANCE_TYPE
                        Type of instance to launch (default: c3.4xlarge).
                        WARNING: must be 64-bit; small instances won't work
  -m MASTER_INSTANCE_TYPE, --master-instance-type=MASTER_INSTANCE_TYPE
                        Master instance type (leave empty for same as
                        instance-type)
  -r REGION, --region=REGION
                        EC2 region used to launch instances in, or to find
                        them in (default: us-east-1)
  -z ZONE, --zone=ZONE  Availability zone to launch instances in, or 'all' to
                        spread slaves across multiple (an additional $0.01/Gb
                        for bandwidthbetween zones applies) (default: a single
                        zone chosen at random)
  -a AMI, --ami=AMI     Amazon Machine Image ID to use
  -v YARN_VERSION, --yarn-version=YARN_VERSION
                        Version of YARN to use: 'X.Y.Z' or a specific git hash
                        (default: master)
  --yarn-git-repo=YARN_GIT_REPO
                        Github repo from which to checkout supplied commit
                        hash (default: https://github.com/zhengqmark/yarn)
  --yarn-ec2-git-repo=YARN_EC2_GIT_REPO
                        Github repo from which to checkout yarn-ec2 (default:
                        https://github.com/zhengqmark/yarn-ec2)
  --yarn-ec2-git-branch=YARN_EC2_GIT_BRANCH
                        Github repo branch of yarn-ec2 to use (default:
                        master)
  -D [ADDRESS:]PORT     Use SSH dynamic port forwarding to create a SOCKS
                        proxy at the given local address (for use with login)
  --resume              Resume installation on a previously launched cluster
                        (for debugging)
  --ebs-vol-size=SIZE   Size (in GB) of each EBS volume.
  --ebs-vol-type=EBS_VOL_TYPE
                        EBS volume type (e.g. 'gp2', 'standard').
  --ebs-vol-num=EBS_VOL_NUM
                        Number of EBS volumes to attach to each node as
                        /vol[x]. The volumes will be deleted when the
                        instances terminate. Only possible on EBS-backed AMIs.
                        EBS volumes are only attached if --ebs-vol-size > 0.
                        Only support up to 8 EBS volumes.
  --placement-group=PLACEMENT_GROUP
                        Which placement group to try and launch instances
                        into. Assumes placement group is already created.
  --spot-price=PRICE    If specified, launch slaves as spot instances with the
                        given maximum price (in dollars)
  -u USER, --user=USER  The SSH user you want to connect as (default: ubuntu)
  --delete-groups       When destroying a cluster, delete the security groups
                        that were created
  --use-existing-master
                        Launch fresh slaves, but use an existing stopped
                        master if possible
  --user-data=USER_DATA
                        Path to a user-data file (most AMIs interpret this as
                        an initialization script)
  --authorized-address=AUTHORIZED_ADDRESS
                        Address to authorize on created security groups
                        (default: 0.0.0.0/0)
  --additional-security-group=ADDITIONAL_SECURITY_GROUP
                        Additional security group to place the machines in
  --additional-tags=ADDITIONAL_TAGS
                        Additional tags to set on the machines; tags are
                        comma-separated, while name and value are colon
                        separated; ex: "Course:advcc,Project:yarn"
  --subnet-id=SUBNET_ID
                        VPC subnet to launch instances in
  --vpc-id=VPC_ID       VPC to launch instances in
  --secondary-ips=SECONDARY_IPS
                        Num of secondary private ip addresses to assign for
                        each cluster node
  --private-ips         Use private IPs for instances rather than public if
                        VPC/subnet requires that.
  --instance-initiated-shutdown-behavior=INSTANCE_INITIATED_SHUTDOWN_BEHAVIOR
                        Whether instances should terminate when shut down or
                        just stop
  --instance-profile-name=INSTANCE_PROFILE_NAME
                        IAM profile name to launch instances under
```
