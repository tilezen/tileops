from ecr import ensure_ecr
from rds import ensure_dbs
from batch_setup import batch_setup
from batch import Buckets
from batch import create_job_definitions
from docker import build_and_upload_images
from datetime import datetime
import argparse
import os
import yaml
from collections import defaultdict


def vpc_of_sg(sg_id):
    import boto3
    ec2 = boto3.client('ec2')
    result = ec2.describe_security_groups(GroupIds=[sg_id])
    groups = result['SecurityGroups']
    assert len(groups) == 1
    return groups[0]['VpcId']


parser = argparse.ArgumentParser('Script to kick off tile creation.')
parser.add_argument('date', help='Planet date, YYMMDD')
parser.add_argument('--num-db-replicas', default=1, type=int,
                    help='Number of database replicas to create.')
parser.add_argument('rawr_bucket', help='S3 bucket for RAWR tiles')
parser.add_argument('meta_bucket', help='S3 bucket for meta tiles')
parser.add_argument('db_password', help='Database password')
parser.add_argument('--missing-bucket', default=None,
                    help='Bucket for missing meta tile lists. Defaults to the '
                    'meta bucket.')
parser.add_argument('--run-id', help='Identifying string to log out in the '
                    'batch runs.')
parser.add_argument('--date-prefix', default=None, help='Date prefix to use '
                    'in S3 buckets. By default, generated from the planet '
                    'date.')
parser.add_argument('--region', help='AWS region. If not provided, then the '
                    'AWS_DEFAULT_REGION environment variable must be set.')

args = parser.parse_args()
planet_date = datetime.strptime(args.date, '%y%m%d')
run_id = args.run_id or planet_date.strftime('%Y%m%d')
date_prefix = args.date_prefix or planet_date.strftime('%y%m%d')

region = args.region or os.environ.get('AWS_DEFAULT_REGION')
if region is None:
    import sys
    print "ERROR: Need environment variable AWS_DEFAULT_REGION to be set."
    sys.exit(1)

repo_uris = ensure_ecr(planet_date)

# start databases => db_sg & database hostnames
db_sg_id, database_ids = ensure_dbs(planet_date, args.num_db_replicas)

# create batch environment and job queue
compute_env_name = planet_date.strftime('compute-env-%y%m%d')
job_queue_name = planet_date.strftime('job-queue-%y%m%d')
vpc_id = vpc_of_sg(db_sg_id)

batch_setup(region, vpc_id, [db_sg_id], compute_env_name, job_queue_name)

# build docker images & upload
build_and_upload_images(repo_uris)

buckets = Buckets(args.rawr_bucket, args.meta_bucket,
                  args.missing_bucket or args.meta_bucket)

# set up memory for jobs. this is a starting point, and might need to be
# raised if jobs fail with out-of-memory errors.
memory = {
    'rawr-batch': 8192,
    'meta-batch': 4096,
    'meta-low-zoom-batch': 2048,
    'missing-meta-tiles-write': 1024,
}
# defaults for the moment. TODO: make them configurable from the command
# line? (argument against is that it'd require pretty large code changes
# before any of our processes could take advantage of >1 cpu, and aren't
# we using Batch to provide the concurrency we want anyway?)
vcpus = 1
retry_attempts = defaultdict(lambda: 1)
# RAWR seems to get a lot of DockerTimeout errors - but i don't think
# they're anything to do with us?
retry_attempts['rawr-batch'] = 10

# create job definitions (references databases, batch setup)
job_def_names = create_job_definitions(
    planet_date, region, repo_uris, database_ids, buckets, args.db_password,
    memory=memory, vcpus=vcpus, retry_attempts=retry_attempts,
    date_prefix=date_prefix)

# create config file for tilequeue
for name in ('rawr-batch', 'meta-batch', 'meta-low-zoom-batch',
             'missing-meta-tiles-write'):
    config = {
        'logging': {
            'config': 'logging.conf.sample'
        },
        'batch': {
            'memory': memory[name],
            'run_id': run_id,
            'retry-attempts': retry_attempts[name],
            'queue-zoom': 7,
            'job-name-prefix': name + planet_date.strftime('-%y%m%d'),
            'vcpus': vcpus,
            'job-queue': job_queue_name,
            'job-definition': job_def_names[name],
        }
    }
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
      "--config rawr-enqueue-batch.config.yaml %r %r"
      % (buckets.rawr, date_prefix))
