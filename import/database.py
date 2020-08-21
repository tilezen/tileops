import boto3
from botocore.exceptions import ClientError
from collections import namedtuple


Database = namedtuple(
    'Database',
    'instance_id security_group_id password host port dbname user')


def does_snapshot_exist(rds, instance_id):
    try:
        response = rds.describe_db_snapshots(DBSnapshotIdentifier=instance_id)
        return len(response['DBSnapshots']) > 0

    except ClientError as e:
        if e.response['Error']['Code'] == 'DBSnapshotNotFound':
            return False
        else:
            raise


def does_instance_exist(rds, instance_id):
    try:
        response = rds.describe_db_instances(DBInstanceIdentifier=instance_id)

        return len(response['DBInstances']) == 1

    except ClientError as e:
        if e.response['Error']['Code'] == 'DBInstanceNotFound':
            return False
        else:
            raise


def ensure_vpc_security_group(security_group_name):
    ec2 = boto3.client('ec2')

    sg_id = None
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
    try:
        ec2.authorize_security_group_ingress(
            GroupId=sg_id,
            SourceSecurityGroupName=security_group_name,
        )
    except ClientError as e:
        if e.response['Error']['Code'] != 'InvalidPermission.Duplicate':
            raise

    return sg_id


def ensure_database(run_id, master_user_password):
    instance_id = 'postgis-prod-' + run_id

    rds = boto3.client('rds')

    if does_snapshot_exist(rds, instance_id):
        raise RuntimeError(
            "A snapshot with ID %r already exists, suggesting that this "
            "import has already completed. If you are sure that you want "
            "to re-run this import in its entirety, please delete that "
            "snapshot first." % instance_id)

    security_group = instance_id + '-security-group'
    security_group_id = ensure_vpc_security_group(security_group)
    print("Database is using security group %s" % security_group_id)

    if not does_instance_exist(rds, instance_id):
        print("Creating DB instance")
        rds.create_db_instance(
            DBName='gis',
            DBInstanceIdentifier=instance_id,
            AllocatedStorage=1500,
            DBInstanceClass='db.r3.2xlarge',
            Engine='postgres',
            MasterUsername='gisuser',
            MasterUserPassword=master_user_password,
            VpcSecurityGroupIds=[security_group_id],
            BackupRetentionPeriod=7,
            MultiAZ=False,
            AutoMinorVersionUpgrade=False,
            LicenseModel='postgresql-license',
            PubliclyAccessible=False,
            StorageType='gp2',
            StorageEncrypted=False,
        )

    print("Waiting for database to come up")
    waiter = rds.get_waiter('db_instance_available')
    waiter.wait(DBInstanceIdentifier=instance_id)

    response = rds.describe_db_instances(DBInstanceIdentifier=instance_id)
    assert len(response['DBInstances']) == 1

    db = response['DBInstances'][0]
    assert len(db['VpcSecurityGroups']) == 1

    print("Database instance up!")

    host = db['Endpoint']['Address']
    port = db['Endpoint']['Port']
    dbname = db['DBName']
    user = db['MasterUsername']

    return Database(
        instance_id=instance_id,
        security_group_id=security_group_id,
        password=master_user_password,
        host=host,
        port=port,
        dbname=dbname,
        user=user,
    )


def take_snapshot_and_shutdown(db, run_id):
    instance_id = 'postgis-prod-' + run_id

    rds = boto3.client('rds')

    print("Creating database snapshot")
    rds.create_db_snapshot(
        DBSnapshotIdentifier=instance_id,
        DBInstanceIdentifier=instance_id,
    )
    waiter = rds.get_waiter('db_snapshot_completed')
    waiter.wait(
        DBSnapshotIdentifier=instance_id,
        WaiterConfig=dict(
            Delay=60,
            MaxAttempts=240,
        ),
    )

    print("Created snapshot, shutting down database.")
    rds.delete_db_instance(
        DBInstanceIdentifier=instance_id,
        SkipFinalSnapshot=True,
    )
    waiter = rds.get_waiter('db_instance_deleted')
    waiter.wait(DBInstanceIdentifier=instance_id)

    print("Database shut down and deleted.")
