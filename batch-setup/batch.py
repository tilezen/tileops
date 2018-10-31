import boto3
import tempfile
import shutil
import os.path
import yaml
from subprocess import Popen
from batch_setup import find_policy
from botocore.exceptions import ClientError
import json
from collections import namedtuple


def db_lookup(databases):
    rds = boto3.client('rds')

    db_hosts = []
    db_name = None
    db_user = None

    for instance_id in databases:
        result = rds.describe_db_instances(DBInstanceIdentifier=instance_id)
        assert len(result['DBInstances']) == 1
        db = result['DBInstances'][0]
        # TODO: support for ports other than 5432?
        assert db['Endpoint']['Port'] == 5432
        db_hosts.append(db['Endpoint']['Address'])
        if db_name and db_name != db['DBName']:
            raise RuntimeError("Different database names (%r & %r) found, but "
                               "all databases must have the same name to be "
                               "used as replicas for tilequeue/batch."
                               % (db_name, db['DBName']))
        db_name = db['DBName']
        if db_user and db_user != db['MasterUsername']:
            raise RuntimeError("Different user names (%r & %r) found, but all "
                               "databases must have the same username to be "
                               "used as replicas for tilequeue/batch."
                               % (db_user, db['MasterUsername']))
        db_user = db['MasterUsername']

    assert db_hosts
    assert db_name
    assert db_user

    return db_hosts, db_name, db_user


def env_for_image(name, db_hosts, db_name, db_user, db_password, buckets,
                  region, date_prefixes, check_metatile_exists, overrides):
    if name == 'rawr-batch':
        env = {
            'TILEQUEUE__RAWR__POSTGRESQL__HOST': db_hosts,
            'TILEQUEUE__RAWR__POSTGRESQL__DBNAME': db_name,
            'TILEQUEUE__RAWR__POSTGRESQL__USER': db_user,
            'TILEQUEUE__RAWR__POSTGRESQL__PASSWORD': db_password,
            'TILEQUEUE__RAWR__SINK__S3__BUCKET': buckets.rawr,
            'TILEQUEUE__RAWR__SINK__S3__REGION': region,
            'TILEQUEUE__RAWR__SINK__S3__PREFIX': date_prefixes.rawr,
            'TILEQUEUE__RAWR__SINK__S3__TAGS': dict(
                prefix=date_prefixes.rawr,
                run_id=date_prefixes.rawr,
            ),
        }

    elif name == 'meta-batch':
        env = {
            'TILEQUEUE__STORE__NAME': buckets.meta,
            'TILEQUEUE__STORE__DATE-PREFIX': date_prefixes.meta,
            'TILEQUEUE__STORE__TAGS': dict(
                prefix=date_prefixes.meta,
                run_id=date_prefixes.meta,
            ),
            'TILEQUEUE__RAWR__SOURCE__S3__BUCKET': buckets.rawr,
            'TILEQUEUE__RAWR__SOURCE__S3__REGION': region,
            'TILEQUEUE__RAWR__SOURCE__S3__PREFIX': date_prefixes.rawr,
            'TILEQUEUE__BATCH__CHECK-METATILE-EXISTS': check_metatile_exists,
        }

    elif name == 'meta-low-zoom-batch':
        env = {
            'TILEQUEUE__POSTGRESQL__HOST': db_hosts,
            'TILEQUEUE__POSTGRESQL__DBNAMES': [db_name],
            'TILEQUEUE__POSTGRESQL__USER': db_user,
            'TILEQUEUE__POSTGRESQL__PASSWORD': db_password,
            'TILEQUEUE__STORE__NAME': buckets.meta,
            'TILEQUEUE__STORE__DATE-PREFIX': date_prefixes.meta,
            'TILEQUEUE__STORE__TAGS': dict(
                prefix=date_prefixes.meta,
                run_id=date_prefixes.meta,
            ),
            'TILEQUEUE__BATCH__CHECK-METATILE-EXISTS': check_metatile_exists,
        }

    elif name == 'missing-meta-tiles-write':
        env = {}

    else:
        raise RuntimeError("Unknown image name %r while building environment."
                           % (name))

    # add extra overrides passed in as options. note the double underscore
    # separator at the end.
    name_as_env_prefix = name.upper().replace('-', '_') + '__'
    for k, v in overrides.iteritems():
        if k.startswith(name_as_env_prefix):
            new_k = k[len(name_as_env_prefix):]
            env[new_k] = v

    # serialise the values in key-value dicts as JSON strings. this is because
    # we will serialise the env to a YAML file, but the env var set should be
    # an encoded YAML string. YAML is a bit weird about one-liners, and JSON is
    # a strict subset of YAML, so we get a more readable encoding for most
    # (simple) objects by using JSON instead.
    env = {k: json.dumps(v) for k, v in env.items()}

    return env


def cmd_for_image(name, region):
    if name == 'rawr-batch':
        cmd = ['tilequeue', 'rawr-tile',
               '--config', '/etc/tilequeue/config.yaml',
               '--tile', 'Ref::tile',
               '--run_id', 'Ref::run_id']

    elif name == 'meta-batch':
        cmd = ['tilequeue', 'meta-tile',
               '--config', '/etc/tilequeue/config.yaml',
               '--tile', 'Ref::tile',
               '--run_id', 'Ref::run_id']

    elif name == 'meta-low-zoom-batch':
        cmd = ['tilequeue', 'meta-tile-low-zoom',
               '--config', '/etc/tilequeue/config.yaml',
               '--tile', 'Ref::tile',
               '--run_id', 'Ref::run_id']

    elif name == 'missing-meta-tiles-write':
        cmd = [
            '/tz-missing-meta-tiles-write',
            '-dest-bucket',
            'Ref::dest_bucket',
            '-dest-date-prefix',
            'Ref::dest_date_prefix',
            '-src-bucket',
            'Ref::src_bucket',
            '-src-date-prefix',
            'Ref::src_date_prefix',
            '-hex-prefix',
            'Ref::hex_prefix',
            '-region',
            region,
            '-key-format-type',
            'Ref::key_format_type',
        ]

    else:
        raise RuntimeError("Unknown image name %r while building command."
                           % (name))
    return cmd


def s3_policy(bucket, date_prefix, allow_write=False):
    actions = ['s3:GetObject', 's3:GetObjectTagging']
    if allow_write:
        actions.extend([
            's3:PutObject',
            's3:PutObjectTagging',
            's3:DeleteObject',
        ])

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "ObjectOps",
                "Effect": "Allow",
                "Action": actions,
                "Resource": [
                    # allow access to objects under the date prefix
                    "arn:aws:s3:::%s/%s/*" % (bucket, date_prefix),
                    # and also objects under a hash + date prefix
                    "arn:aws:s3:::%s/*/%s/*" % (bucket, date_prefix),
                ],
            },
            {
                "Sid": "BucketOps",
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": "arn:aws:s3:::%s" % (bucket),
            }
        ]
    }

    return policy


def find_or_create_s3_policy(iam, bucket_name, bucket, date_prefix,
                             allow_write=False):
    """
    Finds a policy in the run's environment to access the given bucket,
    or creates one.

    Returns the policy's ARN.
    """

    access_descriptor = "Write" if allow_write else "Read"
    policy_name = "Allow%sAccessTo%sBucket%s" \
                  % (access_descriptor, bucket_name, date_prefix)
    policy = find_policy(iam, policy_name)

    if policy is None:
        policy_json = s3_policy(bucket, date_prefix, allow_write)
        response = iam.create_policy(
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy_json),
            Description='Allows the %s batch environment to %s the %s '
            'S3 bucket.' % (date_prefix, access_descriptor, bucket_name))
        policy = response['Policy']

    return policy['Arn']


def kebab_to_camel(name):
    return "".join(map(lambda s: s.capitalize(), name.split('-')))


def ensure_job_role_arn(iam, run_id, name, buckets, date_prefixes):
    role_name = kebab_to_camel(
        "batch-%s-%s" % (name, run_id))

    arn = None
    try:
        result = iam.get_role(RoleName=role_name)
        arn = result['Role']['Arn']

    except ClientError as e:
        if e.response['Error']['Code'] != 'NoSuchEntity':
            raise

    if arn is None:
        arn = create_role(iam, name, role_name, buckets, date_prefixes)

    return arn


# NOTE: i don't really understand what this is for, but copied it from the
# open-terrain environment, so hopefully it works.
ASSUME_ROLE_POLICY = dict(
    Version="2012-10-17",
    Statement=[
        dict(
            Effect="Allow",
            Principal=dict(Service="ecs-tasks.amazonaws.com"),
            Action="sts:AssumeRole",
        ),
    ]
)


# keeps track of the different buckets that are needed for different parts of
# the process. the RAWR bucket stores RAWR tiles, meta stores meta tiles, and
# missing stores lists of missing tiles (which weren't found in the meta
# bucket).
Buckets = namedtuple('Buckets', 'rawr meta missing')


def create_role(iam, image_name, role_name, buckets, date_prefixes):

    """
    Create a role with the given role_name for the image in the run's
    environment.
    """

    role = iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(ASSUME_ROLE_POLICY),
        Description='Role to perform %s batch jobs in %s/%s environment.' %
        (image_name, date_prefixes.rawr, date_prefixes.meta))

    class RolePolicies(object):
        def __init__(self, iam, role_name):
            self.iam = iam
            self.role_name = role_name

        def allow_s3_read(self, name, bucket, date_prefix):
            policy_arn = find_or_create_s3_policy(
                self.iam, name, bucket, date_prefix, allow_write=False)
            self.iam.attach_role_policy(
                RoleName=self.role_name,
                PolicyArn=policy_arn,
            )

        def allow_s3_write(self, name, bucket, date_prefix):
            policy_arn = find_or_create_s3_policy(
                self.iam, name, bucket, date_prefix, allow_write=True)
            self.iam.attach_role_policy(
                RoleName=self.role_name,
                PolicyArn=policy_arn,
            )

    rp = RolePolicies(iam, role_name)

    if image_name == 'rawr-batch':
        rp.allow_s3_write('RAWR', buckets.rawr, date_prefixes.rawr)
        # note: needs DB access, but that's handled through a security group

    elif image_name == 'meta-batch':
        rp.allow_s3_read('RAWR', buckets.rawr, date_prefixes.rawr)
        rp.allow_s3_write('meta', buckets.meta, date_prefixes.meta)

    elif image_name == 'meta-low-zoom-batch':
        rp.allow_s3_write('meta', buckets.meta, date_prefixes.meta)
        # note: needs DB access, but that's handled through a security group

    elif image_name == 'missing-meta-tiles-write':
        rp.allow_s3_write('missing', buckets.missing, date_prefixes.rawr)
        rp.allow_s3_read('meta', buckets.meta, date_prefixes.meta)
        rp.allow_s3_read('RAWR', buckets.rawr, date_prefixes.rawr)

    else:
        raise RuntimeError("Unknown image name %r while building job role."
                           % (image_name))

    return role['Role']['Arn']


def make_job_definitions(
        iam, run_id, region, repo_urls, databases, buckets,
        db_password, memory, vcpus, retry_attempts, date_prefixes,
        check_metatile_exists, job_env_overrides):

    db_hosts, db_name, db_user = db_lookup(databases)

    definitions = []
    definition_names = {}
    for name, image in repo_urls.items():
        job_role_arn = ensure_job_role_arn(
            iam, run_id, name, buckets, date_prefixes)
        memory_value = memory[name] if isinstance(memory, dict) else memory
        vcpus_value = vcpus[name] if isinstance(vcpus, dict) else vcpus
        retry_value = retry_attempts[name] \
            if isinstance(retry_attempts, dict) else retry_attempts

        job_name = "%s-%s" % (name, run_id)
        definition = {
            'name': job_name,
            'job-role-arn': job_role_arn,
            'image': image,
            'command': cmd_for_image(name, region),
            'environment': env_for_image(
                name, db_hosts, db_name, db_user, db_password, buckets, region,
                date_prefixes, check_metatile_exists, job_env_overrides),
            'memory': memory_value,
            'vcpus': vcpus_value,
            'retry-attempts': retry_value,
        }
        definitions.append(definition)
        definition_names[name] = job_name

    job_definitions = {
        'region': region,
        'job-definitions': definitions,
    }

    return job_definitions, definition_names


def find_command_in_paths(cmd, paths):
    for p in paths:
        exe = os.path.join(p, cmd)
        if os.path.isfile(exe) and os.access(exe, os.X_OK):
            return exe
    return None


def find_command(cmd):
    gopath = os.getenv('GOPATH')
    if gopath is not None:
        candidate_paths = [
            os.path.join(p, 'bin') for p in gopath.split(':')]
        result = find_command_in_paths(cmd, candidate_paths)
        if result is not None:
            return result
    path = os.getenv('PATH')
    if path is not None:
        return find_command_in_paths(cmd, path.split(':'))
    return None


def run_go(cmd, *args, **kwargs):
    """
    Runs the Go executable cmd with the given args. Raises an exception if the
    command failed (return code != 0). If stdout is given as a kwarg, then
    write the command's stdout to that file.

    The cmd is expected to be either in $GOPATH/bin if $GOPATH is set, or in
    $PATH.
    """

    exe = find_command(cmd)
    if exe is None:
        raise RuntimeError("Unable to find executable %r. Did you build "
                           "the Go tileops tools?" % (exe))

    # have to construct an environment which will allow the Go-based
    # command to access the AWS session that this program is using. this
    # means adding a bunch of stuff to the environment, and calling the
    # more complex subprocess.Popen.
    session = boto3.session.Session()
    creds = session.get_credentials()
    env = {
        'AWS_SESSION_TOKEN': creds.token,
        'AWS_ACCESS_KEY_ID': creds.access_key,
        'AWS_SECRET_ACCESS_KEY': creds.secret_key,
        'AWS_DEFAULT_REGION': session.region_name,
        'AWS_PROFILE': session.profile_name,
    }

    stdout = kwargs.get('stdout')
    if stdout is not None:
        stdout = open(stdout, 'w')

    try:
        proc_args = [exe] + list(args)
        proc = Popen(proc_args, env=env, stdout=stdout)
        proc.wait()
        if proc.returncode != 0:
            raise RuntimeError("Command %r failed with return code %d."
                               % (proc_args, proc.returncode))
    finally:
        if stdout is not None:
            stdout.close()


def create_job_definitions(run_id, region, repo_urls, databases, buckets,
                           db_password, memory=1024, vcpus=1,
                           retry_attempts=5, date_prefix=None,
                           meta_date_prefix=None, check_metatile_exists=False,
                           job_env_overrides={}):

    """
    Set up job definitions for all of the repos in repo_urls.

    The parameters will be taken from the memory, vcpus and retry_attempts
    variables. If they are dicts, the image name will be looked up in them, if
    not they will be passed directly to AWS.

    If date_prefix is left as None (the default) it will be generated from
    run_id automatically.

    If meta_date_prefix is specified, a different date prefix will be used in
    the metatile and missing buckets.

    If job_env_overrides is specified, these will be interpreted as
    configuration parameters for the environment of the batch jobs, as written
    into the job definitions. For example, an override of:

      {"META_BATCH__TILEQUEUE__STORE__NAME": "foo"}

    Would override the bucket name in the meta-batch environment.

    Returns a mapping from the keys of repo_urls to the job definition names.
    """

    iam = boto3.client('iam')

    if date_prefix is None:
        date_prefix = run_id

    if meta_date_prefix is None:
        meta_date_prefix = date_prefix

    date_prefixes = Buckets(date_prefix, meta_date_prefix, meta_date_prefix)

    job_definitions, definition_names = make_job_definitions(
        iam, run_id, region, repo_urls, databases, buckets,
        db_password, memory, vcpus, retry_attempts, date_prefixes,
        check_metatile_exists, job_env_overrides)

    tmpdir = tempfile.mkdtemp()
    try:
        yaml_file = os.path.join(tmpdir, "config.yaml")
        with open(yaml_file, 'w') as fh:
            fh.write(yaml.safe_dump(job_definitions))

        run_go('tz-batch-create-job-definition', '-yaml', yaml_file)

    finally:
        shutil.rmtree(tmpdir)

    return definition_names


def terminate_all_jobs(job_queue, reason):
    batch = boto3.client('batch')
    for status in ('SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING',
                   'RUNNING'):
        job_ids = []
        response = batch.list_jobs(
            jobQueue=job_queue,
            jobStatus=status,
        )
        while response.get('jobSummaryList'):
            for j in response['jobSummaryList']:
                job_ids.append(j['jobId'])
            next_token = response.get('nextToken')
            if not next_token:
                break
            response = batch.list_jobs(
                jobQueue=job_queue,
                jobStatus=status,
                nextToken=next_token,
            )
        if status in ('SUBMITTED', 'PENDING', 'RUNNABLE'):
            print("Cancelling %d %r jobs" % (len(job_ids), status))
            for job_id in job_ids:
                batch.cancel_job(jobId=job_id, reason=reason)
        else:
            print("Terminating %d %r jobs" % (len(job_ids), status))
            for job_id in job_ids:
                batch.terminate_job(jobId=job_id, reason=reason)
