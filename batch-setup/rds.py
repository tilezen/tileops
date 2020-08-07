import boto3
from botocore.exceptions import ClientError
import re


# TODO: this is copy-pasta. should we merge all the python stuff together into
# a single module which gets re-used?
def does_snapshot_exist(rds, instance_id):
    try:
        response = rds.describe_db_snapshots(DBSnapshotIdentifier=instance_id)
        return len(response['DBSnapshots']) > 0

    except ClientError as e:
        if e.response['Error']['Code'] == 'DBSnapshotNotFound':
            return False
        else:
            raise


# TODO: sigh - more copy-pasta
def ensure_vpc_security_group(security_group_name):
    ec2 = boto3.client('ec2')

    try:
        response = ec2.describe_security_groups(
            GroupNames=[security_group_name])
        if len(response['SecurityGroups']) == 1:
            sg_id = response['SecurityGroups'][0]['GroupId']

    except ClientError as e:
        if e.response['Error']['Code'] != 'InvalidGroup.NotFound':
            raise

    # if security group doesn't exist - create it.
    if not sg_id:
        response = ec2.create_security_group(
            Description='Security group for Tilezen database',
            GroupName=security_group_name,
        )
        sg_id = response['GroupId']

    # allow the security group to connect to the database
    ec2.authorize_security_group_ingress(
        GroupId=sg_id,
        SourceSecurityGroupName=security_group_name,
    )

    return sg_id


def databases_running_snapshot(rds, snapshot_id):
    databases = []
    prefix = '%s-rr' % snapshot_id

    paginator = rds.get_paginator('describe_db_instances')
    for page in paginator.paginate():
        for instance in page['DBInstances']:
            instance_id = instance['DBInstanceIdentifier']
            if instance_id.startswith(prefix):
                databases.append(instance_id)

    return databases


REPLICA_ID_PATTERN = re.compile('.*-rr([0-9]{3})$')


def parse_replica_id(replica_id):
    m = REPLICA_ID_PATTERN.match(replica_id)
    if not m:
        raise RuntimeError("Unable to parse replica ID from %r (expected it "
                           "to end -rrNNN)" % replica_id)

    return int(m.groups(1)[0])


def parse_next_replica_id(databases):
    if databases:
        ids = map(parse_replica_id, databases)
        return max(ids) + 1
    else:
        return 1


def start_database_from_snapshot(rds, snapshot_id, replica_num):
    instance_id = '%s-rr%03d' % (snapshot_id, replica_num)

    print("Starting instance %r" % instance_id)
    result = rds.restore_db_instance_from_db_snapshot(
        DBInstanceIdentifier=instance_id,
        DBSnapshotIdentifier=snapshot_id,
        PubliclyAccessible=False,
    )

    return result['DBInstance']['DBInstanceIdentifier']


def set_databases_security_group(rds, security_group_id, databases):
    for instance_id in databases:
        rds.modify_db_instance(
            DBInstanceIdentifier=instance_id,
            VpcSecurityGroupIds=[security_group_id],
        )


def ensure_dbs(run_id, num_instances):
    snapshot_id = 'postgis-prod-' + run_id
    rds = boto3.client('rds')

    if not does_snapshot_exist(rds, snapshot_id):
        raise RuntimeError("Snapshot %r does not exist, was the database "
                           "import run successfully?" % snapshot_id)

    security_group = snapshot_id + '-security-group'
    security_group_id = ensure_vpc_security_group(security_group)

    databases = databases_running_snapshot(rds, snapshot_id)
    print("%d running instances, want %d" % (len(databases), num_instances))

    if len(databases) < num_instances:
        # start new instances
        num_new_instances = num_instances - len(databases)
        print("Starting %d new instances from snapshot" % num_new_instances)

        next_replica_id = parse_next_replica_id(databases)
        new_databases = []

        for i in range(0, num_new_instances):
            replica_id = start_database_from_snapshot(
                rds, snapshot_id, next_replica_id + i)
            new_databases.append(replica_id)

        waiter = rds.get_waiter('db_instance_available')
        for replica_id in new_databases:
            print("Waiting for %r to come up" % replica_id)
            waiter.wait(DBInstanceIdentifier=replica_id)

        # make sure new database is in the right security group. not sure why
        # this isn't available as a parameter when restoring from snapshot,
        # though?
        set_databases_security_group(rds, security_group_id, new_databases)

        # wait _again_ to make sure that the SG modifications have taken hold
        for replica_id in new_databases:
            print("Waiting for %r to move to new SG" % replica_id)
            waiter.wait(DBInstanceIdentifier=replica_id)

        print("All database snapshots up")
        databases.extend(new_databases)

    elif len(databases) > num_instances:
        # need to stop some databases
        num = len(databases) - num_instances
        print("Stopping %d databases" % num)
        to_stop = sorted(databases)[-num:]
        for replica_id in to_stop:
            rds.delete_db_instance(
                DBInstanceIdentifier=replica_id,
                SkipFinalSnapshot=True,
            )
        waiter = rds.get_waiter('db_instance_deleted')
        for replica_id in to_stop:
            print("Waiting for %r to stop" % replica_id)
            waiter.wait(DBInstanceIdentifier=replica_id)
        print("Deleted %d databases" % num)
        databases = sorted(databases)[:-num]

    assert len(databases) == num_instances
    return security_group_id, databases
