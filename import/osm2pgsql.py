import boto3
import os
from io import BytesIO
from paramiko.rsakey import RSAKey
from botocore.exceptions import ClientError
from contextlib import contextmanager


def flat_nodes_key(run_id):
    return 'flat-nodes-%s/flat.nodes' % (run_id,)


def does_flat_nodes_file_exist(bucket, run_id):
    s3 = boto3.client('s3')
    key = flat_nodes_key(run_id)

    try:
        response = s3.head_object(Bucket=bucket, Key=key)
        return 'DeleteMarker' not in response

    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise


def reset_flat_nodes_file(bucket, run_id):
    s3 = boto3.client('s3')
    key = flat_nodes_key(run_id)
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


def login_key(run_id):
    filename = "import-private-key-%s.pem" % (run_id,)

    if not os.path.exists(filename):
        return None

    with open(filename) as fh:
        return RSAKey.from_private_key(fh)


def create_login_key(ec2, run_id, key_pair_name):
    try:
        ec2.delete_key_pair(KeyName=key_pair_name)
    except ClientError as e:
        if e.response['Error']['Code'] != 'NotFound':
            raise

    response = ec2.create_key_pair(KeyName=key_pair_name)
    pem = response['KeyMaterial']
    key = RSAKey.from_private_key(BytesIO(pem))

    filename = "import-private-key-%s.pem" % (run_id,)
    with open(filename, 'w') as fh:
        key.write_private_key(fh)

    return key


def find_import_instance(ec2, run_id):
    result = ec2.describe_instances(
        Filters=[
            dict(Name='tag:osm2pgsql-import', Values=[run_id]),
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


def _estimate_planet_size_gb():
    """
    Return an integer estimate of the planet size in GB.

    Note: this is in "disk" GB, i.e: 1,000,000,000 bytes, not "memory" GB,
    i.e: 2 ** 30 bytes. This is because we're intending to use this to set
    the capacity of an EC2 disk.
    """

    import requests

    # just use latest planet - unless we're loading a super-old planet, then
    # it won't be too different from the latest. and the latest is likely to
    # be the largest and therefore most conservative estimate.
    url = 'https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf'

    r = requests.head(url)
    if r.status_code == 200:
        content_length = r.headers.get('Content-Length')
        if content_length:
            return int(content_length) // (1000 * 1000 * 1000)

    # if there wasn't a content-length, or we couldn't fetch the planet, then
    # take a guess. this will become out of date at some point. the planet is
    # currently growing a little over 110MB per week, or 5.7GB per year. an
    # estimate of 63GB should see us okay until about 2022.
    return 63


def create_security_group_allowing_this_ip(ec2, ip_addr):
    from ipaddress import ip_address

    ip = ip_address(ip_addr)
    sg_name = 'osm2pgsql-import-allow-' + str(ip).replace('.', '-')

    # try to find existing SG called this
    sg_id = find_security_group(ec2, sg_name)

    # else create one
    if sg_id is None:
        sg_id = create_security_group_allowing(ec2, sg_name, ip)

    # return security group ID.
    return sg_id


def start_osm2pgsql_instance(
        ec2, run_id, key_pair_name, security_group_id,
        iam_instance_profile, ip_addr):
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

    allow_this_ip = create_security_group_allowing_this_ip(ec2, ip_addr)

    # get an approximate planet size, for figuring out how much disk space we
    # need for the planet, flat nodes, etc...
    planet_size = _estimate_planet_size_gb()

    # we need roughly: storage for 1 planet, storage for flat nodes (which is
    # hand-wavingly about the same as the planet), and a bit extra for working
    # space and shapefiles. we can approximate that as 3x the planet size and
    # see how that works out.
    disk_size = 3 * planet_size

    response = ec2.run_instances(
        BlockDeviceMappings=[dict(
            DeviceName='/dev/sda1',
            Ebs=dict(
                DeleteOnTermination=True,
                Iops=5000,
                VolumeSize=disk_size,
                VolumeType='io1',
            ),
        )],
        ImageId=ami_id,
        InstanceType='r5.4xlarge',
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
                    dict(Key='osm2pgsql-import', Value=run_id),
                    dict(Key='Name', Value='osm2pgsql Runner'),
                    dict(Key='cost_sub_feature', Value="Tile Build"),
                    dict(Key='cost_resource_group', Value=run_id),
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

        except Exception as e:
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
                    status = stdout.read().rstrip().decode('utf8')

                    print("[%s] Import status: %r" % (time_now, status))
                    if status == "finished":
                        break
                    elif status == "failed":
                        raise RuntimeError(
                            "Something went wrong with the import!")

                except OSError as e:
                    if e.errno == errno.ENOENT:
                        print("[%s] Script still starting up..." % time_now)
                    else:
                        raise

                # otherwise, wait a while to check again.
                time.sleep(15)


def shutdown_and_cleanup(ec2, import_instance_id, run_id, ip_addr):
    import os

    # shut down the instance and delete the key-pair
    print("Terminating instance (id=%r)." % import_instance_id)
    ec2.terminate_instances(InstanceIds=[import_instance_id])
    waiter = ec2.get_waiter('instance_terminated')
    waiter.wait(InstanceIds=[import_instance_id])
    print("Instance terminated.")

    filename = "import-private-key-%s.pem" % (run_id,)
    os.remove(filename)
    key_pair_name = 'osm2pgsql-import-' + run_id
    ec2.delete_key_pair(KeyName=key_pair_name)

    # note: this won't actually create - this security group is already in use
    # on the instance.
    sg_id = create_security_group_allowing_this_ip(ec2, ip_addr)
    try:
        ec2.delete_security_group(GroupId=sg_id)

    except ClientError as e:
        # if the security group is in use, then we'll just leave it. whatever's
        # using it will have to clean up after itself.
        if e.response['Error']['Code'] != 'DependencyViolation':
            raise


def ensure_import(
        run_id, planet_url, planet_md5_url, planet_file,
        db, iam_instance_profile, bucket, aws_region, ip_addr,
        vector_datasource_version='master'):
    ec2 = boto3.client('ec2')

    # is there already an import instance running?
    import_instance_id = find_import_instance(ec2, run_id)

    if import_instance_id:
        key = login_key(run_id)
        if not key:
            raise RuntimeError(
                "Import instance is running, but we don't have the key to "
                "log in. Terminate the instance and re-run if you want to "
                "start again from scratch.")

        # instance running!
        print("Using running instance (id=%r)." % import_instance_id)
        instance = Instance(ec2, import_instance_id, key)

    # keep a flag to tell if we have finished or not. this will be set if,
    # after starting the instance, it looks like the database and flat nodes
    # files are both present. (i.e: we crashed after the import finished, but
    # before the database snapshot was taken).
    finished = False

    if not import_instance_id:
        print("No instance running - starting one.")

        # no import instance running - either it was never started, or we just
        # stopped it and cleaned up. so we'll need to start one!
        key_pair_name = 'osm2pgsql-import-' + run_id
        key = login_key(run_id)
        if not key:
            key = create_login_key(ec2, run_id, key_pair_name)

        import_instance_id = start_osm2pgsql_instance(
            ec2, run_id, key_pair_name, db.security_group_id,
            iam_instance_profile, ip_addr)

        # instance started!
        print("Instance started (id=%r)." % import_instance_id)
        instance = Instance(ec2, import_instance_id, key)

        # before running the script, check if everything is already finished.
        # if the instance was already started, then we assume that the import
        # might be part-way through, and we just want to skip to running the
        # script on the remote host.
        flat_nodes_exists = does_flat_nodes_file_exist(bucket, run_id)
        database_full = does_database_have_data(instance, db)
        finished = flat_nodes_exists and database_full

        # don't run cleanup if both flat nodes exists _and_ database is full,
        # that means we've already finished the import, but must have crashed
        # before the database snapshot could be created.
        if not finished:
            if flat_nodes_exists:
                # flat nodes, but no database data => reset by deleting the
                # flat nodes file
                reset_flat_nodes_file(bucket, run_id)

            if database_full:
                # database, but no flat nodes => reset by clearing the database
                reset_database(instance, db)

    if not finished:
        # run the script on the host
        instance.repeatedly(
            'import_planet.sh',
            planet_url=planet_url,
            planet_md5_url=planet_md5_url,
            planet_file=planet_file,
            db_pass=db.password,
            db_host=db.host,
            db_port=db.port,
            db_name=db.dbname,
            db_user=db.user,
            flat_nodes_bucket=bucket,
            flat_nodes_key=flat_nodes_key(run_id),
            aws_region=aws_region,
            vector_datasource_version=vector_datasource_version,
        )

    shutdown_and_cleanup(ec2, import_instance_id, run_id, ip_addr)
