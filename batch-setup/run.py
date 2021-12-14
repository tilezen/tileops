import argparse

from batch_setup import batch_setup


# Creates a Batch Compute Environment and an associated Batch Job Queue.

parser = argparse.ArgumentParser(
    description='Creates a Batch Compute Environment and an associated Batch '
    'Job Queue')

# The AWS region in which to create these resources.
parser.add_argument('region', help='AWS region in which to create these '
                    'resources')

# The VPC to run the compute environment in. Note that it seems your VPC must
# assign public IPs for the instances to get added to the Elastic Container
# Service cluster that backs the Batch Compute Environment.
parser.add_argument('vpcid', help='VPC to run the compute environment in.')

# An array of Security Groups to attach to the compute environment instances.
parser.add_argument('sg', nargs='+', help='Security groups to attach to the '
                    'compute environment instances.')

# The name of the compute environment
parser.add_argument('--cename', default='raw-tiles',
                    help='The name of the compute environment')

# The name of the job queue
parser.add_argument('--jqname', default='raw-tiles',
                    help='The name of the job queue')

# maximum number of VCPUs to use in the Batch cluster
parser.add_argument('--max-vcpus', default=32768, type=int,
                    help='Maximum number of VPUs to use in the Batch cluster')

args = parser.parse_args()
region_name = args.region
vpc_id = args.vpcid
securityGroupIds = args.sg
computeEnvironmentName = args.cename
jobQueueName = args.jqname
max_vcpus = args.max_vcpus

batch_setup(region_name, vpc_id, securityGroupIds, computeEnvironmentName,
            jobQueueName, max_vcpus)
