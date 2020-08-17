from rds import ensure_dbs
from make_rawr_tiles import wait_for_jobs_to_finish
from batch import terminate_all_jobs
import boto3


def delete_all_job_definitions(batch, run_id):
    job_definitions = []
    suffix = '-' + run_id

    print("Listing job definitions to delete.")
    next_token = None
    response = batch.describe_job_definitions()
    while True:
        for jobdef in response.get('jobDefinitions', []):
            name = jobdef['jobDefinitionName']
            revision = jobdef['revision']
            if name.endswith(suffix):
                job_definitions.append('%s:%d' % (name, revision))
        next_token = response.get('nextToken')
        if next_token is None:
            break
        response = batch.describe_job_definitions(nextToken=next_token)

    for def_name in job_definitions:
        print("Deleting job definition %r" % (def_name,))
        batch.deregister_job_definition(jobDefinition=def_name)


def delete_job_queue(batch, job_queue, terminate):
    response = batch.describe_job_queues(jobQueues=[job_queue])
    if len(response['jobQueues']) == 0:
        return

    if terminate:
        # terminate any jobs which are running
        terminate_all_jobs(job_queue, 'Tearing down the stack')

    # always need to wait for jobs, even if we have terminated them we need to
    # wait for the termination to complete.
    wait_for_jobs_to_finish(job_queue)

    # disable the job queue
    print("Disabling job queue %r" % (job_queue,))
    batch.update_job_queue(jobQueue=job_queue, state='DISABLED')

    print("Waiting for job queue to disable.")
    while True:
        response = batch.describe_job_queues(jobQueues=[job_queue])
        assert len(response['jobQueues']) == 1
        q = response['jobQueues'][0]
        state = q['state']
        status = q['status']
        if status == 'UPDATING':
            print("Queue %r is updating, waiting..." % (job_queue,))

        elif state == 'DISABLED':
            break

        else:
            raise RuntimeError('Expected status=UPDATING or state=DISABLED, '
                               'but status=%r and state=%r' % (status, state))

        # wait a little bit before checking again.
        sleep(15)

    # delete the job queue
    print("Deleting job queue %r" % (job_queue,))
    batch.delete_job_queue(jobQueue=job_queue)

    # wait for queue to be deleted, otherwise it causes issues when we try to
    # delete the compute environment, which is still referred to by the queue.
    print("Waiting for job queue to be deleted...")
    while True:
        response = batch.describe_job_queues(jobQueues=[job_queue])
        if len(response['jobQueues']) == 0:
            break

        # wait a little bit...
        sleep(15)

    print("Deleted job queue.")


def terminate_instances_by_tag(tags):
    ec2 = boto3.client('ec2')
    filters = []
    for k, v in tags.items():
        filters.append(dict(
            Name='tag:' + k,
            Values=[v],
        ))
    assert filters

    response = ec2.describe_instances(Filters=filters)

    for reservation in response['Reservations']:
        for instance in reservation['Instances']:
            instance_id = instance['InstanceId']
            if instance['State']['Name'] == 'running':
                print("Terminating instance %r" % (instance_id,))
                ec2.terminate_instances(InstanceIds=[instance_id])


def delete_role(name):
    iam = boto3.client('iam')

    # first, delete any associated instance profiles
    try:
        iam.get_role(RoleName=name)
    except iam.exceptions.NoSuchEntityException:
        return

    response = iam.list_instance_profiles_for_role(RoleName=name)
    for instance_profile in response['InstanceProfiles']:
        ipname = instance_profile['InstanceProfileName']
        for role in instance_profile['Roles']:
            print("Removing role %r from instance profile %r" %
                  (role['RoleName'], ipname))
            iam.remove_role_from_instance_profile(
                InstanceProfileName=ipname,
                RoleName=role['RoleName'],
            )
        print("Deleting instance profile %r" % (ipname,))
        iam.delete_instance_profile(InstanceProfileName=ipname)

    # next, detach all policies
    paginator = iam.get_paginator('list_attached_role_policies')
    for page in paginator.paginate(RoleName=name):
        for policy in page['AttachedPolicies']:
            print("Detaching role policy %r" % (policy['PolicyName'],))
            iam.detach_role_policy(
                RoleName=name,
                PolicyArn=policy['PolicyArn'],
            )

    # next, delete inline policies
    paginator = iam.get_paginator('list_role_policies')
    for page in paginator.paginate(RoleName=name):
        for policy_name in page['PolicyNames']:
            print("Deleting inline policy %r from role %r" %
                  (policy_name, name))
            iam.delete_role_policy(
                RoleName=name,
                PolicyName=policy_name,
            )

    # finally, delete the role
    print("Deleting role %r" % (name,))
    iam.delete_role(RoleName=name)


def delete_policy(name):
    from batch_setup import find_policy

    iam = boto3.client('iam')
    policy = find_policy(iam, name)

    if policy:
        print("Deleting policy %r (arn=%r)" % (name, policy['Arn']))
        iam.delete_policy(PolicyArn=policy['Arn'])


def delete_compute_env(batch, compute_env):
    # disable the compute environment
    print("Disabling compute environment %r" % (compute_env,))
    batch.update_compute_environment(
        computeEnvironment=compute_env, state='DISABLED')

    print("Waiting for compute environment to disable.")
    while True:
        response = batch.describe_compute_environments(
            computeEnvironments=[compute_env])
        assert len(response['computeEnvironments']) == 1
        env = response['computeEnvironments'][0]
        state = env['state']
        status = env['status']
        if status == 'UPDATING':
            print("Environment %r is updating, waiting..." % (compute_env,))

        elif state == 'DISABLED':
            break

        else:
            raise RuntimeError('Expected status=UPDATING or state=DISABLED, '
                               'but status=%r and state=%r' % (status, state))

        # wait a little bit before checking again.
        sleep(15)

    # delete the compute environment
    print("Deleting compute environment %r" % (compute_env,))
    batch.delete_compute_environment(computeEnvironment=compute_env)


if __name__ == '__main__':
    import argparse
    import os
    from time import sleep
    from run_id import assert_run_id_format

    parser = argparse.ArgumentParser("Tear down a stack")
    parser.add_argument('run_id', help='Unique run identifier.')
    parser.add_argument('--terminate', action='store_true', help='Terminate '
                        'jobs, rather than waiting for them to finish.')
    parser.add_argument('--job-queue', help='Job queue name, default '
                        'job-queue-XXXX with run ID.')
    parser.add_argument('--compute-env', help='Compute environment name, '
                        'default compute-env-XXXX with run ID.')
    parser.add_argument('--region', help='AWS region. Default taken from '
                        'the AWS_DEFAULT_REGION environment variable.')
    parser.add_argument('--leave-databases-running', action='store_true',
                        help='Do not tear down the database replicas, leave '
                        'them running. These can take a long time to stop and '
                        'start, so it can be worth leaving them up if you are '
                        'bouncing the environment.')

    args = parser.parse_args()
    run_id = args.run_id
    assert_run_id_format(run_id)
    job_queue = args.job_queue or ('job-queue-' + run_id)
    compute_env = args.compute_env or ('compute-env-' + run_id)
    region = args.region or os.environ.get('AWS_DEFAULT_REGION')

    if region is None:
        import sys
        print("ERROR: Need environment variable AWS_DEFAULT_REGION to be set.")
        sys.exit(1)

    batch = boto3.client('batch')

    delete_job_queue(batch, job_queue, args.terminate)

    # delete the job definitions - TODO: doesn't look like these _can_ be
    # deleted, only disabled, and just clutters up the output.
    #delete_all_job_definitions(batch, run_id)

    # delete the compute environment
    response = batch.describe_compute_environments(
        computeEnvironments=[compute_env])
    if len(response['computeEnvironments']) > 0:
        delete_compute_env(batch, compute_env)

    if not args.leave_databases_running:
        # shutdown all database replicas
        ensure_dbs(run_id, 0)

    # terminate any running instances - if the osm2pgsql instance is running,
    # then that, also the TPS "orchestration" instance.
    terminate_instances_by_tag(
        {'osm2pgsql-import': run_id})
    terminate_instances_by_tag(
        {'tps-instance': run_id})

    # drop all the roles that we've created for this environment
    for prefix in ('tps-', 'BatchMetaBatch', 'BatchMetaLowZoomBatch',
                   'BatchMissingMetaTilesWrite', 'BatchRawrBatch'):
        delete_role(prefix + run_id)

    # drop policies created for this environment
    for prefix in ('AllowReadAccessTometaBucket',
                   'AllowReadAccessToRAWRBucket',
                   'AllowWriteAccessTometaBucket',
                   'AllowWriteAccessTomissingBucket',
                   'AllowWriteAccessToRAWRBucket'):
        delete_policy(prefix + run_id)
