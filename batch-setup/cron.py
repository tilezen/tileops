import json
from collections import namedtuple

import boto3
from botocore.exceptions import ClientError
from run_id import assert_run_id_format


# locations stores the S3 locations to use for tile assets, RAWR tiles, meta
# tiles and missing tile logs. each location is a Bucket, which combines an
# S3 bucket name with a date prefix within the bucket (although there might
# be a hash in front of that prefix now).
Locations = namedtuple('Locations', 'assets rawr meta missing')
Bucket = namedtuple('Bucket', 'name prefix')


def assume_role_policy_document(service):
    return dict(
        Version='2012-10-17',
        Statement=[dict(
            Effect='Allow',
            Action='sts:AssumeRole',
            Principal=dict(
                Service=service,
            ),
        )],
    )


def find_profile(iam, profile_name):
    """
    Tries to find the named profile, returning either the instance profile
    object with the matching name, or None if the instance profile could not
    be found.
    """

    profile = None
    try:
        response = iam.get_instance_profile(
            InstanceProfileName=profile_name,
        )
        profile = response['InstanceProfile']

    except ClientError as e:
        if e.response['Error']['Code'] != 'NoSuchEntity':
            raise

    return profile


def wait_for_profile(iam, profile_name):
    """
    Waits for the named profile to be created.
    """

    import time

    waiter = iam.get_waiter('instance_profile_exists')
    waiter.wait(InstanceProfileName=profile_name)
    # not sure why, but even after waiting, the ec2 instance creation
    # fails with an invalid profile arn error
    # sleeping a little bit of time fixes it :(
    time.sleep(10)


def create_tps_profile(iam, profile_name, locations):
    """
    Creates a profile suitable for use as the tps instance.
    """

    instance_profile = iam.create_instance_profile(
        InstanceProfileName=profile_name,
        Path='/',
    )

    iam.create_role(
        RoleName=profile_name,
        Path='/',
        AssumeRolePolicyDocument=json.dumps(
            assume_role_policy_document('ec2.amazonaws.com')),
    )

    iam.add_role_to_instance_profile(
        InstanceProfileName=profile_name,
        RoleName=profile_name,
    )

    for policy in (
            'AmazonRDSFullAccess',
            'AmazonEC2ContainerRegistryFullAccess',
            'AWSBatchFullAccess',
    ):
        arn = 'arn:aws:iam::aws:policy/' + policy
        iam.attach_role_policy(
            RoleName=profile_name,
            PolicyArn=arn,
        )

    ec2_policy = dict(
        Version='2012-10-17',
        Statement=dict(
            Effect='Allow',
            Action=[
                'ec2:AuthorizeSecurityGroupIngress',
                'ec2:DescribeInstances',
                'ec2:TerminateInstances',
                'ec2:CreateKeyPair',
                'ec2:CreateTags',
                'ec2:RunInstances',
                'ec2:DescribeSecurityGroups',
                'ec2:DescribeImages',
                'ec2:CreateSecurityGroup',
                'ec2:DeleteSecurityGroup',
                'ec2:DescribeSubnets',
                'ec2:DeleteKeyPair',
                'ec2:DescribeInstanceStatus',
            ],
            Resource='*',
        ),
    )

    iam_policy = dict(
        Version='2012-10-17',
        Statement=dict(
            Effect='Allow',
            Action=[
                'iam:ListPolicies',
                'iam:CreatePolicy',
                'iam:GetRole',
                'iam:CreateRole',
                'iam:AttachRolePolicy',
                'iam:PassRole',
                'iam:GetInstanceProfile',
                'iam:AddRoleToInstanceProfile',
            ],
            Resource='*',
        ),
    )

    assets_path = '/' + locations.assets.prefix + '/*'
    s3_policy = dict(
        Version='2012-10-17',
        Statement=[
            dict(
                Effect='Allow',
                Action=[
                    's3:DeleteObject',
                ],
                Resource=[
                    'arn:aws:s3:::' + locations.missing.name + '/*',
                ],
            ),
            dict(
                Effect='Allow',
                Action=[
                    's3:ListBucket',
                ],
                Resource=[
                    'arn:aws:s3:::' + locations.missing.name,
                    'arn:aws:s3:::' + locations.assets.name,
                    'arn:aws:s3:::' + locations.rawr.name,
                    'arn:aws:s3:::' + locations.meta.name,
                ],
            ),
            dict(
                Effect='Allow',
                Action=[
                    's3:GetObject',
                ],
                Resource=[
                    'arn:aws:s3:::' + locations.assets.name + '/tileops/*',
                    'arn:aws:s3:::' + locations.assets.name + assets_path,
                    'arn:aws:s3:::' + locations.missing.name + '/*',
                    'arn:aws:s3:::' + locations.rawr.name + '/*',
                    'arn:aws:s3:::' + locations.meta.name + '/*',
                ],
            ),
        ],
    )

    cw_policy = dict(
        Version='2012-10-17',
        Statement=[
            dict(
                Effect='Allow',
                Action=[
                    'logs:CreateLogGroup',
                    'logs:CreateLogStream',
                    'logs:PutLogEvents',
                    'logs:DescribeLogStreams',
                ],
                Resource=[
                    'arn:aws:logs:*:*:*',
                ]
            ),
        ],
    )

    for name, policy in [
            ('AllowEC2', ec2_policy),
            ('AllowIAM', iam_policy),
            ('AllowS3', s3_policy),
            ('WriteLogs', cw_policy),
    ]:
        iam.put_role_policy(
            RoleName=profile_name,
            PolicyName=name,
            PolicyDocument=json.dumps(policy),
        )

    wait_for_profile(iam, profile_name)
    return instance_profile['InstanceProfile']


def find_latest_ami(ec2):
    """
    Find the AMI ID of the latest HVM/EBS image published by Amazon.
    """

    # find latest official image with the given name
    response = ec2.describe_images(
        Filters=[
            dict(Name='name', Values=[
                'amzn-ami-hvm-*',
            ]),
            dict(Name='owner-id', Values=['137112412989']),
        ],
    )
    ebs_images = filter(lambda i: i['Name'].endswith('-ebs'),
                        response['Images'])
    latest_image = max(ebs_images, key=(lambda i: i['CreationDate']))
    assert latest_image

    ami_id = latest_image['ImageId']
    return ami_id


def find_role(iam, role_name):
    """
    Finds the named role, returning None if it does not exist.
    """

    role = None
    try:
        response = iam.get_role(RoleName=role_name)
        role = response['Role']

    except ClientError as e:
        if e.response['Error']['Code'] != 'NoSuchEntity':
            raise

    return role


def create_tile_assets_profile(iam, profile_name, locations):
    """
    Creates a profile (and corresponding role) with read and write access to
    the tile assets bucket.
    """

    profile = iam.create_instance_profile(
        InstanceProfileName=profile_name,
        Path='/',
    )

    iam.create_role(
        RoleName=profile_name,
        AssumeRolePolicyDocument=json.dumps(
            assume_role_policy_document('ec2.amazonaws.com')),
    )

    iam.add_role_to_instance_profile(
        InstanceProfileName=profile_name,
        RoleName=profile_name,
    )

    assets_path = locations.assets.name + '/' + locations.assets.prefix + '/*'
    policy = {
        'Version': '2012-10-17',
        'Statement': [
            {
                'Sid': 'VisualEditor0',
                'Effect': 'Allow',
                'Action': [
                    's3:PutObject',
                    's3:GetObject',
                    's3:DeleteObject'
                ],
                'Resource': 'arn:aws:s3:::' + assets_path,
            },
            {
                'Sid': 'VisualEditor1',
                'Effect': 'Allow',
                'Action': 's3:ListBucket',
                'Resource': 'arn:aws:s3:::' + locations.assets.name,
            }
        ]
    }

    iam.put_role_policy(
        RoleName=profile_name,
        PolicyName='AllowReadWriteAccessToTilesAssetsBucket',
        PolicyDocument=json.dumps(policy),
    )

    return profile['InstanceProfile']


def generate_password(length):
    """
    Generates a random password of the given length using alpha-numeric
    characters.
    """

    # see:
    # https://stackoverflow.com/questions/3854692/generate-password-in-python
    #
    # ideally, we'd use Python 3.6's "secrets" module, but since we're still
    # in Python 2.7 land, we have to basically reimplement it.
    import string
    from os import urandom
    from struct import unpack

    chars = string.ascii_letters + string.digits
    password = ''
    for i in range(0, length):
        # get two bytes of random and turn into an integer. given the small
        # size of the chars alphabet (=62 values), two bytes (=65,536 values)
        # should be enough to avoid too big a bias towards the lower range of
        # the alphabet.
        value, = unpack('=H', urandom(2))
        idx = value % len(chars)
        password += chars[idx]

    return password


def generate_or_update_password(smgr, password, name, description):
    secret = None
    try:
        secret = smgr.describe_secret(SecretId=name)

    except smgr.exceptions.ResourceNotFoundException:
        pass

    if secret is None:
        # no existing password - use passed-in password or generate one.
        if not password:
            # generate new password. it would be better to not generate a
            # password at all, and instead use a client certificate to access
            # postgres, but that seems less than straightforward at the moment.
            # the database is secured in its own security group, so the
            # password could be a blank string, but that feels very wrong.
            password_length = 64
            password = generate_password(password_length)

        secret = smgr.create_secret(
            Name=name,
            Description=description,
            SecretString=password,
        )

    else:
        # existing password - means we either use it, or update it.
        if not password:
            # fetch existing secret
            response = smgr.get_secret_value(SecretId=secret['ARN'])
            password = response['SecretString']

        else:
            # update existing secret
            smgr.update_secret(
                SecretId=secret['ARN'],
                Description=description,
                SecretString=password,
            )

    return password


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

    ip_addr = requests.get('https://api.ipify.org').text
    ip = ip_address(ip_addr)
    sg_name = 'tps-allow-' + str(ip).replace('.', '-')

    # try to find existing SG called this
    sg_id = find_security_group(ec2, sg_name)

    # else create one
    if sg_id is None:
        sg_id = create_security_group_allowing(ec2, sg_name, ip)

    # return security group ID.
    return sg_id


def is_power_of_two(x):
    """
    Returns true if x is a power of two, false otherwise.
    """

    from math import log
    if x <= 0:
        return False
    log2 = int(log(x, 2))
    return x == 2 ** log2


if __name__ == '__main__':
    import argparse
    import os
    from base64 import b64encode

    parser = argparse.ArgumentParser(
        'Script to automate the tile production system.')
    parser.add_argument('planet_url', help='URL to an OSM Planet file')
    parser.add_argument('run_id', help='Distinctive run ID to give to '
                        'this build.')
    parser.add_argument('--region', help='AWS region to use. This must be '
                        'provided if the environment variable '
                        'AWS_DEFAULT_REGION is not set.')
    parser.add_argument('--bucket-prefix', help='Bucket prefix. If set, then '
                        'buckets will be named like ${prefix}-${function}-'
                        '${region}, for example if the prefix is "tilezen" '
                        'and the region is "us-east-1", then the tile assets '
                        'bucket will be named '
                        '"tilezen-tile-assets-us-east-1". If you want a '
                        'different naming scheme, override with '
                        '--assets-bucket and similar options.')
    parser.add_argument('--assets-bucket', help='Override default name for '
                        'tile assets bucket (e.g: where flat nodes is '
                        'stored).')
    parser.add_argument('--rawr-bucket', help='Override default name for '
                        'RAWR tiles bucket.')
    parser.add_argument('--missing-bucket', help='Override default name for '
                        'missing tile logs bucket.')
    parser.add_argument('--meta-bucket', help='Override default name for '
                        'meta tiles bucket.')
    parser.add_argument('--profile-name', help='Profile name to use. Default '
                        'is "tps-runid" with given run ID.')
    parser.add_argument('--ec2-key-name', help='Provide this to set an EC2 '
                        'SSH key name. If you do not want to log into the '
                        'instance, you do not need to provide one.')
    parser.add_argument('--ec2-instance-type', help='EC2 instance type to use '
                        'for tps instance.', default='t2.micro')
    parser.add_argument('--ec2-ami-image', help='EC2 AMI image ID for the '
                        'tps instance. Default is to use the latest '
                        'Amazon Linux HVM/EBS image.')
    parser.add_argument('--ec2-security-group', help='EC2 security group ID '
                        'to start tps instance in, default is to use the '
                        'default security group.')
    parser.add_argument('--tile-assets-profile-name', help='Name of the '
                        'profile with read and write access to the tile '
                        'assets bucket. If one does not exist, it will be '
                        'created.', default='ec2TilesAssetsRole')
    parser.add_argument('--db-password', help='Override the default database '
                        'password.')
    parser.add_argument('--db-password-secret-name', help='The AWS '
                        'SecretsManager name for the database password. '
                        'Defaults to a name with the run ID like '
                        '"TilesDatabasePasswordRUNID".')
    parser.add_argument('--sg-allow-this-ip', help='If creating a security '
                        'group, then allow this IP address.',
                        action='store_true')
    parser.add_argument('--raw-tiles-version', default='master',
                        help='Version (git hash, tag or branch) of the '
                        'raw_tiles software to use.')
    parser.add_argument('--tilequeue-version', default='master',
                        help='Version (git hash, tag or branch) of the '
                        'tilequeue software to use.')
    parser.add_argument('--vector-datasource-version', default='master',
                        help='Version (git hash, tag or branch) of the '
                        'vector-datasource software to use.')
    parser.add_argument('--tileops-version', default='master',
                        help='Version (git hash, tag or branch) of the '
                        'tileops software to use on the TPS instance.')
    parser.add_argument('--metatile-size', default=8, type=int,
                        help='Metatile size (in 256px tiles).')
    parser.add_argument('--meta-date-prefix', help='Meta tile bucket '
                        'date prefix, defaults to run ID. You can '
                        'also set the environment variable '
                        'META_DATE_PREFIX.')
    parser.add_argument('--job-env-overrides', default=[], nargs='*',
                        help='Overrides for the Batch job environment.')
    parser.add_argument('--num-db-replicas', default=10, type=int,
                        help='Number of database replicas to use.')
    parser.add_argument('--max-vcpus', default=32768, type=int,
                        help='Number of VCPUs to request in the Batch '
                        'environment')
    parser.add_argument('--planet-md5-url', help='Override the default '
                        '<planet-url>.md5 convention to specify an md5 '
                        'file for the OSM planet file specified by the '
                        'planet-url argument.')
    parser.add_argument('--skip-post-import-steps',
                        dest='run_post_import_steps',
                        action='store_false',
                        help='Whether to skip the snapshot creation and '
                             'instance deletion')
    parser.add_argument('--skip-osm2pgsql-instance-shutdown', default=False,
                        action='store_true',
                        help='Whether to skip terminating the osm2pgsql EC2 '
                             'instance after the osm2pgsql import')

    args = parser.parse_args()
    assert_run_id_format(args.run_id)

    planet_md5_url = args.planet_md5_url or args.planet_url + '.md5'

    region = args.region or os.environ.get('AWS_DEFAULT_REGION')
    if region is None:
        import sys
        print('ERROR: Need environment variable AWS_DEFAULT_REGION to be set.')
        sys.exit(1)

    # checking that metatile size has a valid value, and it's in a sensible
    # range
    assert is_power_of_two(args.metatile_size)
    assert args.metatile_size > 0
    assert args.metatile_size < 100

    profile_name = args.profile_name or ('tps-' + args.run_id)

    def bucket_name(arg_name, bucket_function):
        # command line argument is most important
        prop_name = arg_name.lstrip('-').replace('-', '_')
        value = getattr(args, prop_name)
        if value:
            return value

        # if that doesn't exist, look for an environment variable
        env_name = prop_name.upper()
        if env_name in os.environ:
            return os.environ[env_name]

        # otherwise, default to a value constructed from the bucket prefix.
        if args.bucket_prefix:
            return '%s-%s-%s' % (args.bucket_prefix, bucket_function, region)

        # finally, error out if we can't figure out a value.
        raise RuntimeError('Must provide either --bucket-prefix or %s.'
                           % (arg_name,))

    assets_bucket = bucket_name('--assets-bucket', 'tile-assets')
    rawr_bucket = bucket_name('--rawr-bucket', 'rawr-tiles')
    missing_bucket = bucket_name('--missing-bucket', 'missing-tiles')
    meta_bucket = bucket_name('--meta-bucket', 'meta-tiles')
    locations = Locations(
        Bucket(assets_bucket, 'flat-nodes-' + args.run_id),
        Bucket(rawr_bucket, args.run_id),
        Bucket(meta_bucket, args.run_id),
        Bucket(missing_bucket, args.run_id),
    )
    meta_date_prefix = (args.meta_date_prefix or
                        os.environ.get('META_DATE_PREFIX') or
                        args.run_id)

    iam = boto3.client('iam')

    profile = find_profile(iam, profile_name)
    if profile is None:
        profile = create_tps_profile(iam, profile_name, locations)
    assert profile is not None

    tile_assets_profile = find_profile(iam, args.tile_assets_profile_name)
    if tile_assets_profile is None:
        tile_assets_profile = create_tile_assets_profile(
            iam, args.tile_assets_profile_name, locations)
    assert tile_assets_profile is not None

    smgr = boto3.client('secretsmanager')
    smgr_name = (args.db_password_secret_name or
                 ('TilesDatabasePassword' + args.run_id))
    smgr_description = 'Tiles database password for %s import' % (args.run_id,)
    db_password = generate_or_update_password(
        smgr, args.db_password, smgr_name, smgr_description)

    # this script is run at startup by the EC2 instance.
    provision_params = dict(
        region=region,
        assets_bucket=locations.assets.name,
        assets_profile_arn=tile_assets_profile['Arn'],
        db_password=db_password,
        rawr_bucket=locations.rawr.name,
        meta_bucket=locations.meta.name,
        missing_bucket=locations.missing.name,
        planet_url=args.planet_url,
        planet_md5_url=planet_md5_url,
        run_id=args.run_id,
        raw_tiles_version=args.raw_tiles_version,
        tilequeue_version=args.tilequeue_version,
        vector_datasource_version=args.vector_datasource_version,
        tileops_version=args.tileops_version,
        metatile_size=args.metatile_size,
        meta_date_prefix=meta_date_prefix,
        job_env_overrides=' '.join(args.job_env_overrides),
        num_db_replicas=args.num_db_replicas,
        max_vcpus=args.max_vcpus,
        run_post_import_steps=args.run_post_import_steps,
        skip_osm2pgsql_instance_shutdown=args.skip_osm2pgsql_instance_shutdown,
    )

    script_dir = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(script_dir, 'provision.sh'), 'r') as fh:
        provision = fh.read() % provision_params
        provision_base64 = b64encode(provision.encode('utf8'))

    ec2 = boto3.client('ec2')

    ec2_ami_image = args.ec2_ami_image
    if ec2_ami_image is None:
        ec2_ami_image = find_latest_ami(ec2)

    run_instances_params = dict(
        MaxCount=1,
        MinCount=1,
        InstanceType=args.ec2_instance_type,
        ImageId=ec2_ami_image,
        IamInstanceProfile={'Arn': profile['Arn']},
        # NOTE: the following parameter is just AWS's way of saying "run this
        # script at startup".
        UserData=provision_base64,
        TagSpecifications=[dict(
            ResourceType='instance',
            Tags=[
                dict(Key='tps-instance', Value=args.run_id),
                dict(Key='Name', Value='Tileops Runner'),
                dict(Key='cost_sub_feature', Value='Tile Build'),
                dict(Key='cost_resource_group', Value=args.run_id),
            ],
        )],
    )
    if args.ec2_key_name:
        run_instances_params['KeyName'] = args.ec2_key_name
    if args.ec2_security_group:
        run_instances_params['SecurityGroupIds'] = [args.ec2_security_group]
    elif args.sg_allow_this_ip:
        sg_id = create_security_group_allowing_this_ip(ec2)
        run_instances_params['SecurityGroupIds'] = [sg_id]

    response = ec2.run_instances(**run_instances_params)

    reservation_id = response['ReservationId']
    assert len(response['Instances']) == 1
    instance = response['Instances'][0]
    instance_id = instance['InstanceId']

    print('reservation ID: %s' % (reservation_id,))
    print('instance ID:    %s' % (instance_id,))

    print('Waiting for instance to come up...')
    waiter = ec2.get_waiter('instance_status_ok')
    waiter.wait(InstanceIds=[instance_id])

    response = ec2.describe_instances(InstanceIds=[instance_id])
    instance = response['Reservations'][0]['Instances'][0]
    print('public IP:      %r' % (instance.get('PublicIpAddress'),))
