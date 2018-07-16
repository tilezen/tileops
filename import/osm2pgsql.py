import boto3
from StringIO import StringIO
from paramiko.rsakey import RSAKey
from botocore.exceptions import ClientError
from contextlib import contextmanager


def flat_nodes_key(planet_date):
    return planet_date.strftime('flat-nodes-%y%m%d/flat.nodes')


def does_flat_nodes_file_exist(bucket, planet_date):
    s3 = boto3.client('s3')
    key = flat_nodes_key(planet_date)

    try:
        response = s3.head_object(Bucket=bucket, Key=key)
        return 'DeleteMarker' not in response

    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise


def reset_flat_nodes_file(bucket, planet_date):
    s3 = boto3.client('s3')
    key = flat_nodes_key(planet_date)
    s3.delete_object(Bucket=bucket, Key=key)


def does_database_have_data(instance, db):
    """
    Run a script on the host to see if the database import finished.
    """

    return instance.query(
        'database_looks_finished.sh',
        db_pass=db.password,
        db_host=db.host,
        db_port=db.port,
        db_name=db.dbname,
        db_user=db.user,
    )


def reset_database(instance, db):
    """
    Run a script on the host to reset the database back to empty.
    """

    return instance.query(
        'reset_database.sh',
        db_pass=db.password,
        db_host=db.host,
        db_port=db.port,
        db_name=db.dbname,
        db_user=db.user,
    )


def login_key(planet_date):
    try:
        filename = planet_date.strftime("import-private-key-%Y%m%d.pem")
        with open(filename) as fh:
            return RSAKey.from_private_key(fh)

    except StandardError:
        return None


def create_login_key(ec2, planet_date, key_pair_name):
    try:
        ec2.delete_key_pair(KeyName=key_pair_name)
    except ClientError as e:
        if e.response['Error']['Code'] != 'NotFound':
            raise

    response = ec2.create_key_pair(KeyName=key_pair_name)
    pem = response['KeyMaterial']
    key = RSAKey.from_private_key(StringIO(pem))

    filename = planet_date.strftime("import-private-key-%Y%m%d.pem")
    with open(filename, 'w') as fh:
        key.write_private_key(fh)

    return key


def find_import_instance(ec2, planet_date):
    date_tag = planet_date.strftime('%Y-%m-%d')
    result = ec2.describe_instances(
        Filters=[
            dict(Name='tag:osm2pgsql-import', Values=[date_tag]),
            dict(Name='instance-state-name', Values=['pending', 'running']),
        ],
    )
    reservations = result['Reservations']
    if len(reservations) == 0:
        return None

    assert len(reservations) == 1
    instances = reservations[0]['Instances']
    assert len(instances) < 2

    if len(instances) == 0:
        return None

    else:
        return instances[0]['InstanceId']


def find_security_group(ec2, sg_name):
    response = ec2.describe_security_groups(Filters=[
        dict(
            Name='group-name',
            Values=[sg_name],
        ),
    ])
    groups = response['SecurityGroups']
    if groups:
        assert len(groups) == 1
        return groups[0]['GroupId']

    else:
        return None


def create_security_group_allowing(ec2, sg_name, ip):
    response = ec2.create_security_group(
        Description='Allows access from the machine controlling the import '
        'to the one running osm2pgsql.',
        GroupName=sg_name,
    )
    sg_id = response['GroupId']

    ec2.authorize_security_group_ingress(
        CidrIp='%s/32' % ip,
        GroupId=sg_id,
        IpProtocol='tcp',
        FromPort=22,
        ToPort=22,
    )

    return sg_id


def create_security_group_allowing_this_ip(ec2):
    from ipaddress import ip_address
    import requests

    # TODO: if this is happening from inside EC2, we'll need a different way
    # of setting up access.
    ip = ip_address(requests.get('https://api.ipify.org').text)
    sg_name = 'osm2pgsql-import-allow-' + str(ip).replace('.', '-')

    # try to find existing SG called this
    sg_id = find_security_group(ec2, sg_name)

    # else create one
    if sg_id is None:
        sg_id = create_security_group_allowing(ec2, sg_name, ip)

    # return security group ID.
    return sg_id


def start_osm2pgsql_instance(ec2, planet_date, key_pair_name,
                             security_group_id, iam_instance_profile):
    # find latest official ubuntu server image
    response = ec2.describe_images(
        Filters=[
            dict(Name='name', Values=[
                'ubuntu/images/hvm-ssd/ubuntu-xenial-16.04-amd64-server-*',
            ]),
            dict(Name='owner-id', Values=['099720109477']),
        ],
    )
    latest_image = max(response['Images'], key=(lambda i: i['CreationDate']))
    assert latest_image

    ami_id = latest_image['ImageId']

    allow_this_ip = create_security_group_allowing_this_ip(ec2)

    date_tag = planet_date.strftime('%Y-%m-%d')
    response = ec2.run_instances(
        BlockDeviceMappings=[dict(
            DeviceName='/dev/sda1',
            Ebs=dict(
                DeleteOnTermination=True,
                Iops=5000,
                VolumeSize=100,
                VolumeType='io1',
            ),
        )],
        ImageId=ami_id,
        InstanceType='r4.2xlarge',
        KeyName=key_pair_name,
        SecurityGroupIds=[security_group_id, allow_this_ip],
        EbsOptimized=True,
        MinCount=1,
        MaxCount=1,
        # add tags to the instance so that we can find any running import
        # instances later.
        TagSpecifications=[
            dict(
                ResourceType='instance',
                Tags=[
                    dict(Key='osm2pgsql-import', Value=date_tag),
                ],
            ),
        ],
        # need to specify a role which (unlike the default) is able to write
        # the flat nodes file into an S3 bucket.
        IamInstanceProfile=dict(
            Arn=iam_instance_profile,
        ),
    )
    instances = response['Instances']
    assert len(instances) == 1
    instance_id = instances[0]['InstanceId']

    # wait for it to come up
    waiter = ec2.get_waiter('instance_status_ok')
    waiter.wait(InstanceIds=[instance_id])

    return instance_id


class Instance(object):
    """
    Represents an EC2 instance that we can run scripts on.
    """

    def __init__(self, ec2, instance_id, key):
        self.ec2 = ec2
        self.instance_id = instance_id
        self.key = key

    @contextmanager
    def _ssh(self, script_file, parameters):
        """
        Copies a templated script to the EC2 instance and yields the SSH
        session.
        """

        import os.path
        from paramiko import SSHClient
        from paramiko.client import AutoAddPolicy

        this_file_dir = os.path.dirname(__file__)
        with open(os.path.join(this_file_dir, script_file)) as fh:
            script = fh.read() % parameters

        response = self.ec2.describe_instances(InstanceIds=[self.instance_id])
        instance = response['Reservations'][0]['Instances'][0]
        ip = instance['PublicIpAddress']

        ssh = SSHClient()
        # TODO: is there any way to get the host key via AWS APIs?
        ssh.set_missing_host_key_policy(AutoAddPolicy)
        ssh.connect(ip, username='ubuntu', pkey=self.key, look_for_keys=False)

        try:
            with ssh.open_sftp() as ftp:
                with ftp.file(script_file, 'w') as fh:
                    fh.write(script)

                yield ssh

        except StandardError as e:
            raise RuntimeError("ERROR: %s (on ubuntu@%s)" % (e, ip))

        finally:
            ssh.close()

    def query(self, script_file, **kwargs):
        """
        Run a script, returning its exit status.
        """

        with self._ssh(script_file, kwargs) as ssh:
            stdin, stdout, stderr = ssh.exec_command(
                "/bin/bash %s </dev/null >/dev/null 2>&1 "
                "&& echo PASS || echo FAIL" % script_file)

            # we don't provide any input
            stdin.close()

            # there shouldn't be any stderr, as we redirect it all to
            # /dev/null, but we should drain it all anyway.
            stderr.read()

            # status should be either PASS or FAIL
            status = stdout.read().strip()

            if status == 'PASS':
                return True
            elif status == 'FAIL':
                return False
            else:
                raise ValueError("Running remote script %r, expecting "
                                 "either pass or fail response but got %r."
                                 % (script_file, status))

    def repeatedly(self, script_file, **kwargs):
        """
        Run the given script file repeatedly until the status file contains
        'finished' or 'failed'.

        Starts an SSH session to the EC2 instance using the given SSH private
        key. Substitutes the parameters into the script file and copies that
        to the host, running it in a `nohup` environment repeatedly (i.e:
        handle locking in the script!) until a status file named like
        `script_file.status` contains either 'finished' for success or 'failed'
        to indicate that something went wrong. Any other value is treated as
        meaning the script is still running and will just be echoed back to
        the user.
        """

        import time
        import errno

        with self._ssh(script_file, kwargs) as ssh:
            while True:
                # can't mark it executable? so have to run it explicitly
                # through bash
                stdin, stdout, stderr = ssh.exec_command(
                    "sh -c '/usr/bin/nohup /bin/bash -x %s </dev/null "
                    ">>nohup.log 2>&1 &'" % script_file)

                # we don't provide any input
                stdin.close()

                # there shouldn't be any output, as we redirect it all to a
                # file, but we should drain it all anyway.
                stdout.read()
                stderr.read()

                time_now = time.strftime("%Y-%m-%dT%H:%M:%S%z")
                # TODO: less hackish way of doing this?
                # check the status by copying back the status file.
                try:
                    stdin, stdout, stderr = ssh.exec_command(
                        'cat %s.status' % script_file)
                    stdin.close()
                    stderr.read()
                    status = stdout.read().rstrip()

                    print "[%s] Import status: %r" % (time_now, status)
                    if status == "finished":
                        break
                    elif status == "failed":
                        raise RuntimeError(
                            "Something went wrong with the import!")

                except OSError as e:
                    if e.errno == errno.ENOENT:
                        print "[%s] Script still starting up..." % (time_now)
                    else:
                        raise

                # otherwise, wait a while to check again.
                time.sleep(15)


def shutdown_and_cleanup(ec2, import_instance_id, planet_date):
    import os

    # shut down the instance and delete the key-pair
    print "Terminating instance (id=%r)." % import_instance_id
    ec2.terminate_instances(InstanceIds=[import_instance_id])
    waiter = ec2.get_waiter('instance_terminated')
    waiter.wait(InstanceIds=[import_instance_id])
    print "Instance terminated."

    filename = planet_date.strftime("import-private-key-%Y%m%d.pem")
    os.remove(filename)
    key_pair_name = planet_date.strftime('osm2pgsql-import-%Y%m%d')
    ec2.delete_key_pair(KeyName=key_pair_name)

    # note: this won't actually create - this security group is already in use
    # on the instance.
    sg_id = create_security_group_allowing_this_ip(ec2)
    try:
        ec2.delete_security_group(GroupId=sg_id)

    except ClientError as e:
        # if the security group is in use, then we'll just leave it. whatever's
        # using it will have to clean up after itself.
        if e.response['Error']['Code'] != 'DependencyViolation':
            raise


def ensure_import(planet_date, db, iam_instance_profile, bucket, aws_region,
                  vector_datasource_version='master'):
    ec2 = boto3.client('ec2')

    # is there already an import instance running?
    import_instance_id = find_import_instance(ec2, planet_date)

    if import_instance_id:
        key = login_key(planet_date)
        if not key:
            raise RuntimeError(
                "Import instance is running, but we don't have the key to "
                "log in. Terminate the instance and re-run if you want to "
                "start again from scratch.")

        # instance running!
        print "Using running instance (id=%r)." % import_instance_id
        instance = Instance(ec2, import_instance_id, key)

    # keep a flag to tell if we have finished or not. this will be set if,
    # after starting the instance, it looks like the database and flat nodes
    # files are both present. (i.e: we crashed after the import finished, but
    # before the database snapshot was taken).
    finished = False

    if not import_instance_id:
        print "No instance running - starting one."

        # no import instance running - either it was never started, or we just
        # stopped it and cleaned up. so we'll need to start one!
        key_pair_name = planet_date.strftime('osm2pgsql-import-%Y%m%d')
        key = login_key(planet_date)
        if not key:
            key = create_login_key(ec2, planet_date, key_pair_name)

        import_instance_id = start_osm2pgsql_instance(
            ec2, planet_date, key_pair_name, db.security_group_id,
            iam_instance_profile)

        # instance started!
        print "Instance started (id=%r)." % import_instance_id
        instance = Instance(ec2, import_instance_id, key)

        # before running the script, check if everything is already finished.
        # if the instance was already started, then we assume that the import
        # might be part-way through, and we just want to skip to running the
        # script on the remote host.
        flat_nodes_exists = does_flat_nodes_file_exist(bucket, planet_date)
        database_full = does_database_have_data(instance, db)
        finished = flat_nodes_exists and database_full

        # don't run cleanup if both flat nodes exists _and_ database is full,
        # that means we've already finished the import, but must have crashed
        # before the database snapshot could be created.
        if not finished:
            if flat_nodes_exists:
                # flat nodes, but no database data => reset by deleting the
                # flat nodes file
                reset_flat_nodes_file(bucket, planet_date)

            if database_full:
                # database, but no flat nodes => reset by clearing the database
                reset_database(instance, db)

    if not finished:
        # run the script on the host
        instance.repeatedly(
            'import_planet.sh',
            planet_year=planet_date.year,
            planet_date=planet_date.strftime('%y%m%d'),
            db_pass=db.password,
            db_host=db.host,
            db_port=db.port,
            db_name=db.dbname,
            db_user=db.user,
            flat_nodes_bucket=bucket,
            flat_nodes_key=flat_nodes_key(planet_date),
            aws_region=aws_region,
            vector_datasource_version=vector_datasource_version,
        )

    shutdown_and_cleanup(ec2, import_instance_id, planet_date)