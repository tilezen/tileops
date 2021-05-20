import boto3
import json
import time


def get_or_create_role(boto_iam, role_name, role_document, role_path=None):
    """
    Ask IAM for the role by name using `role_name`. If it doesn't exist,
    create it using the `role_document` dict.
    """
    role_path = role_path or '/'

    try:
        response = boto_iam.get_role(
            RoleName=role_name,
        )
        role_arn = response['Role']['Arn']
        print("Found role %s at %s" % (role_name, role_arn))
    except boto_iam.exceptions.NoSuchEntityException:
        response = boto_iam.create_role(
            Path=role_path,
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(role_document),
        )
        role_arn = response['Role']['Arn']
        print("Created role %s at %s" % (role_name, role_arn))

    return role_arn


def get_or_create_instance_profile(boto_iam, boto_ec2, instanceRoleName,
                                   instanceProfileName):
    """
    Find an instance profile named instanceProfileName, or create one and attach
    instanceRoleName to it.
    """

    response = None
    try:
        response = boto_iam.get_instance_profile(
            InstanceProfileName=instanceProfileName)

    except boto_iam.exceptions.NoSuchEntityException:
        response = boto_iam.create_instance_profile(
            InstanceProfileName=instanceProfileName)
        boto_iam.add_role_to_instance_profile(
            InstanceProfileName=instanceProfileName,
            RoleName=instanceRoleName,
        )

    return response['InstanceProfile']['Arn']


def find_policy(boto_iam, policy_name):
    """
    Find an AWS policy by name and return a dict of information about it.

    AWS's existing `list_policies()` API doesn't seem to let you search by
    name, so I iterate over all of them and match on our side.
    """
    paginator = boto_iam.get_paginator('list_policies')
    response_iterator = paginator.paginate()
    for policy_resp in response_iterator:
        for policy in policy_resp['Policies']:
            if policy['PolicyName'] == policy_name:
                return policy
    return None


def batch_setup(region_name, run_id, vpc_id, securityGroupIds, computeEnvironmentName,
                jobQueueName, max_vcpus):
    """
    Set up AWS Batch with a Compute Environment and Job Queue.

    It will be set up in the given region and VPC, with access to all the
    security group IDs (e.g: database access).
    """

    boto_batch = boto3.client('batch', region_name=region_name)
    boto_ec2 = boto3.client('ec2', region_name=region_name)
    boto_iam = boto3.client('iam', region_name=region_name)

    # There should be a VPC already created for the batch instances to run
    # on, so we'll use all the subnets in that VPC

    # Pull the subnets from the specified VPC to use by the compute environment
    response = boto_ec2.describe_subnets(
        Filters=[
            {
                "Name": "vpc-id",
                "Values": [vpc_id]
            }
        ]
    )
    subnet_ids = [s['SubnetId'] for s in response['Subnets']]
    print("Found subnets %s on VPC %s" % (subnet_ids, vpc_id))

    # Create the batch service role
    # https://docs.aws.amazon.com/batch/latest/userguide/service_IAM_role.html
    serviceRoleName = "AWSBatchServiceRole"
    serviceRoleDocument = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "batch.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }

    serviceRoleArn = get_or_create_role(
        boto_iam, serviceRoleName, serviceRoleDocument,
        role_path='/service-role/')
    print("Using batch service role %s" % serviceRoleArn)

    # Look for the AWS-managed "AWSBatchServiceRole" that should be attached to
    # the service role
    serviceRolePolicy = find_policy(boto_iam, 'AWSBatchServiceRole')
    boto_iam.attach_role_policy(
        PolicyArn=serviceRolePolicy['Arn'],
        RoleName=serviceRoleName,
    )
    print("Batch service policy attached to batch service role")

    # Create the instance role
    # https://docs.aws.amazon.com/batch/latest/userguide/instance_IAM_role.html
    instanceRoleName = "ecsInstanceRole"
    instanceRoleDocument = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "ec2.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }
    instanceRoleArn = get_or_create_role(
        boto_iam, instanceRoleName, instanceRoleDocument)
    print("Using ECS instance role %s" % instanceRoleArn)

    instanceProfileArn = get_or_create_instance_profile(
        boto_iam, boto_ec2, instanceRoleName, instanceRoleName)
    print("Using ECS instance profile %s" % instanceProfileArn)

    # Create the spot fleet role
    # https://docs.aws.amazon.com/batch/latest/userguide/spot_fleet_IAM_role.html
    spotIamFleetRoleName = "AmazonEC2SpotFleetRole"
    spotIamRoleDocument = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "spotfleet.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }
    spotIamFleetRoleArn = get_or_create_role(
        boto_iam, spotIamFleetRoleName, spotIamRoleDocument)
    print("Using EC2 Spot Fleet role %s" % spotIamFleetRoleArn)

    spotFleetTaggingRolePolicy = find_policy(
        boto_iam, 'AmazonEC2SpotFleetTaggingRole')
    boto_iam.attach_role_policy(
        PolicyArn=spotFleetTaggingRolePolicy['Arn'],
        RoleName=spotIamFleetRoleName,
    )
    print("Attached Spot Fleet Tagging Role to EC2 Spot Fleet role %s"
          % spotIamFleetRoleArn)

    # Create a compute environment for raw tile rendering
    try:
        response = boto_batch.describe_compute_environments(
            computeEnvironments=[computeEnvironmentName],
        )
        computeEnvironmentInfo = response['computeEnvironments'][0]
        computeEnvironmentArn = computeEnvironmentInfo['computeEnvironmentArn']
        print("Reusing existing batch compute environment %s"
              % computeEnvironmentArn)
    except IndexError:
        response = boto_batch.create_compute_environment(
            computeEnvironmentName=computeEnvironmentName,
            type="MANAGED",
            state="ENABLED",
            serviceRole=serviceRoleArn,
            computeResources=dict(
                type='SPOT',
                minvCpus=0,
                maxvCpus=max_vcpus,
                desiredvCpus=0,
                instanceTypes=["m5"],
                # although this is called "instanceRole", it really wants an instance _profile_ ARN.
                instanceRole=instanceProfileArn,
                tags={
                    'Name': 'Tiles Rendering',
                    'cost_sub_feature': "Tile Build",
                    'cost_resource_group': run_id,
                },
                bidPercentage=60,
                spotIamFleetRole=spotIamFleetRoleArn,
                subnets=subnet_ids,
                securityGroupIds=securityGroupIds,
            )
        )
        computeEnvironmentArn = response['computeEnvironmentArn']
        print("Created batch compute environment %s" % computeEnvironmentArn)

    # each iteration sleeps for sleep_time, and the process will exit with a
    # failure after timeout_time.
    sleep_time = 1.0
    timeout_time = 600.0

    started_at = time.time()
    while True:
        response = boto_batch.describe_compute_environments(
            computeEnvironments=[computeEnvironmentName],
        )
        computeEnvironmentInfo = response['computeEnvironments'][0]
        if computeEnvironmentInfo['status'] == 'VALID':
            print("Compute environment is enabled and valid.")
            break
        elif time.time() > started_at + timeout_time:
            raise RuntimeError(
                "Timed out after %f seconds waiting for compute "
                "enviroment to become valid. Possibly there is some "
                "more information in the AWS console?" %
                (timeout_time))
        else:
            print("Waiting for compute environment to be enabled and valid.")
            time.sleep(sleep_time)

    # Create a job queue to attach to the compute environment
    try:
        response = boto_batch.describe_job_queues(
            jobQueues=[jobQueueName],
        )
        jobQueueInfo = response['jobQueues'][0]
        jobQueueArn = jobQueueInfo['jobQueueArn']
        print("Reusing existing batch job queue %s" % jobQueueArn)
    except IndexError:
        response = boto_batch.create_job_queue(
            jobQueueName=jobQueueName,
            state="ENABLED",
            priority=10,
            computeEnvironmentOrder=[
                dict(order=1, computeEnvironment=computeEnvironmentArn)
            ]
        )
        jobQueueArn = response['jobQueueArn']
        print("Created batch job queue %s" % jobQueueArn)
