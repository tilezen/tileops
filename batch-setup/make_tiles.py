from ecr import ensure_ecr
from rds import ensure_dbs
from batch_setup import batch_setup
from batch import Buckets
from batch import create_job_definitions
from docker import build_and_upload_images
from run_id import assert_run_id_format
import argparse
import os
import yaml
import json
from collections import defaultdict


def vpc_of_sg(sg_id):
    import boto3
    ec2 = boto3.client('ec2')
    result = ec2.describe_security_groups(GroupIds=[sg_id])
    groups = result['SecurityGroups']
    assert len(groups) == 1
    return groups[0]['VpcId']


def _looks_like_an_s3_bucket_name(bucket):
    """
    Return True if bucket looks like a valid S3 bucket name. For this test, we
    assume S3 bucket names should be lower case ASCII alphanumeric,
    dash-delimited.
    """

    # return list of characters in range - inclusive of both ends
    def _chr_range(a, b):
        return [chr(x) for x in range(ord(a), ord(b) + 1)]

    allowed_chars = _chr_range('a', 'z') + _chr_range('0', '9') + ['-']
    for ch in bucket:
        if ch not in allowed_chars:
            return False

    return True


parser = argparse.ArgumentParser('Script to kick off tile creation.')
parser.add_argument('run_id', help='Unique run identifier, used to name '
                    'resources and log out in the batch runs.')
parser.add_argument('--num-db-replicas', default=1, type=int,
                    help='Number of database replicas to create.')
parser.add_argument('rawr_bucket', help='S3 bucket for RAWR tiles')
parser.add_argument('meta_bucket', help='S3 bucket for meta tiles')
parser.add_argument('db_password', help='Database password')
parser.add_argument('--missing-bucket', default=None,
                    help='Bucket for missing meta tile lists. Defaults to the '
                    'meta bucket.')
parser.add_argument('--date-prefix', default=None, help='Date prefix to use '
                    'in S3 buckets. By default, generated from the run ID.')
parser.add_argument('--region', help='AWS region. If not provided, then the '
                    'AWS_DEFAULT_REGION environment variable must be set.')
parser.add_argument('--meta-date-prefix', help='Optional different date '
                    'prefix to be used for the meta and missing buckets.')
parser.add_argument('--check-metatile-exists', default=False,
                    action='store_true', help='Whether to check if the '
                    'metatile exists first before processing the batch job.')
parser.add_argument('--overrides', nargs='*', default=[], help='List of '
                    'KEY=VALUE pairs to use when overriding environment '
                    'variables in Batch jobs. Prefix the KEY with the '
                    'uppercase, underscore-delimited, '
                    'double-underscore-separated name of the batch job, e.g: '
                    'META_BATCH__')
parser.add_argument('--max-vcpus', default=32768, type=int,
                    help='Maximum number of VCPUs to request in the Batch '
                    'compute environment. Note that if this is set so high '
                    'that the Postgres servers are overloaded, you will make '
                    'slower progress than with a lower number. This is the '
                    'number requested, but AWS service limits may mean that a '
                    'lower number is actually supplied.')

args = parser.parse_args()
run_id = args.run_id
assert_run_id_format(run_id)
date_prefix = args.date_prefix or run_id

if args.meta_bucket.startswith('[') and args.meta_bucket.endswith(']'):
    meta_buckets = json.loads(args.meta_bucket)
else:
    meta_buckets = [args.meta_bucket]

assert meta_buckets, "Must configure at least one meta tile storage bucket."

# check that bucket names look like valid bucket names
assert _looks_like_an_s3_bucket_name(args.rawr_bucket), \
    "RAWR bucket name %r doesn't look like an S3 bucket name." \
    % (args.rawr_bucket,)
if args.missing_bucket is not None:
    assert _looks_like_an_s3_bucket_name(args.missing_bucket), \
        "missing bucket name %r doesn't look like an S3 bucket name." \
        % (args.missing_bucket,)
for bucket in meta_buckets:
    assert _looks_like_an_s3_bucket_name(bucket), \
        "meta bucket name %r doesn't look like an S3 bucket name." % (bucket,)


region = args.region or os.environ.get('AWS_DEFAULT_REGION')
if region is None:
    import sys
    print("ERROR: Need environment variable AWS_DEFAULT_REGION to be set.")
    sys.exit(1)

# unpack overrides into a dict, so it's easier to work with
job_env_overrides = {}
for kv in args.overrides:
    key, value = kv.split('=', 1)
    job_env_overrides[key] = value

repo_uris = ensure_ecr(run_id)

# start databases => db_sg & database hostnames
db_sg_id, database_ids = ensure_dbs(run_id, args.num_db_replicas)

# create batch environment and job queue
compute_env_name = 'compute-env-' + run_id
job_queue_name = 'job-queue-' + run_id
vpc_id = vpc_of_sg(db_sg_id)

batch_setup(region, run_id, vpc_id, [db_sg_id], compute_env_name, job_queue_name,
            args.max_vcpus)

# build docker images & upload
build_and_upload_images(repo_uris)

buckets = Buckets(args.rawr_bucket, meta_buckets,
                  args.missing_bucket or meta_buckets[-1])

# set up memory for jobs. this is a starting point, and might need to be
# raised if jobs fail with out-of-memory errors.
memory = {
    'rawr-batch': 8192,
    'meta-batch': 4096,  # 4 GiB
    'meta-low-zoom-batch': 8192,  # 8 GiB
    'missing-meta-tiles-write': 1024,
}
# defaults for the moment. TODO: make them configurable from the command
# line? (argument against is that it'd require pretty large code changes
# before any of our processes could take advantage of >1 cpu, and aren't
# we using Batch to provide the concurrency we want anyway?)
vcpus = 1
retry_attempts = defaultdict(lambda: 3)
# RAWR seems to get a lot of DockerTimeout errors - but i don't think
# they're anything to do with us?
retry_attempts['rawr-batch'] = 10

# create job definitions (references databases, batch setup)
job_def_names = create_job_definitions(
    run_id, region, repo_uris, database_ids, buckets, args.db_password,
    memory=memory, vcpus=vcpus, retry_attempts=retry_attempts,
    date_prefix=date_prefix, meta_date_prefix=args.meta_date_prefix,
    check_metatile_exists=args.check_metatile_exists,
    job_env_overrides=job_env_overrides)

# create config file for tilequeue
for name in ('rawr-batch', 'meta-batch', 'meta-low-zoom-batch',
             'missing-meta-tiles-write'):
    config = {
        'logging': {
            'config': os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                'logging.conf.sample'),
        },
        'batch': {
            'memory': memory[name],
            'run_id': run_id,
            'retry-attempts': retry_attempts[name],
            'queue-zoom': 7,
            'job-name-prefix': ('%s-%s') % (name, run_id),
            'vcpus': vcpus,
            'job-queue': job_queue_name,
            'job-definition': job_def_names[name],
        },
        'rawr': {
            'group-zoom': 10,
        },
    }
    # When enqueueing rawr and metatiles for generation, the job-type needs to
    # be set appropriately. This dictates the size for the batch array option.
    if name in ('rawr-batch', 'meta-batch'):
        config['batch']['job-type'] = 'high'
    elif name == 'meta-low-zoom-batch':
        config['batch']['job-type'] = 'low'
    config_file = 'enqueue-%s.config.yaml' % (name)
    with open(config_file, 'w') as f:
        f.write(yaml.safe_dump(config))

# helpful hint as to where to start next. this script could run it
# automatically, but the setup process is quite long and doesn't need to be
# repeated each time. figuring out whether steps need to be repeated (e.g:
# have the Docker images changed?) would mean duplicating a bunch of
# functionality already in various tools we use. this seemed a good place to
# have a "breakpoint" where we'd "lock in" and not re-run the previous steps.
print("RUN THIS NEXT>> python make_rawr_tiles.py "
      "--config enqueue-rawr-batch.config.yaml %r %r"
      % (buckets.rawr, date_prefix))
