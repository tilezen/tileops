from rds import ensure_dbs
from make_rawr_tiles import wait_for_jobs_to_finish
from batch import terminate_all_jobs
import boto3


def delete_all_job_definitions(batch, planet_date):
    job_definitions = []
    date_suffix = planet_date.strftime('-%y%m%d')

    print("Listing job definitions to delete.")
    next_token = None
    response = batch.describe_job_definitions()
    while True:
        for jobdef in response.get('jobDefinitions', []):
            name = jobdef['jobDefinitionName']
            revision = jobdef['revision']
            if name.endswith(date_suffix):
                job_definitions.append('%s:%d' % (name, revision))
        next_token = response.get('nextToken')
        if next_token is None:
            break
        response = batch.describe_job_definitions(nextToken=next_token)

    for def_name in job_definitions:
        print("Deleting job definition %r" % (def_name,))
        batch.deregister_job_definition(jobDefinition=def_name)


def delete_job_queue(batch, job_queue, terminate):
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


if __name__ == '__main__':
    import argparse
    from datetime import datetime
    import os
    from time import sleep

    parser = argparse.ArgumentParser("Tear down a stack")
    parser.add_argument('date', help='Planet date, YYMMDD')
    parser.add_argument('--terminate', action='store_true', help='Terminate '
                        'jobs, rather than waiting for them to finish.')
    parser.add_argument('--job-queue', help='Job queue name, default '
                        'job-queue-YYMMDD with planet date.')
    parser.add_argument('--compute-env', help='Compute environment name, '
                        'default compute-env-YYMMDD with planet date.')
    parser.add_argument('--region', help='AWS region. Default taken from '
                        'the AWS_DEFAULT_REGION environment variable.')

    args = parser.parse_args()
    planet_date = datetime.strptime(args.date, '%y%m%d')
    job_queue = args.job_queue or planet_date.strftime('job-queue-%y%m%d')
    compute_env = args.compute_env or planet_date.strftime(
        'compute-env-%y%m%d')
    region = args.region or os.environ.get('AWS_DEFAULT_REGION')

    if region is None:
        import sys
        print "ERROR: Need environment variable AWS_DEFAULT_REGION to be set."
        sys.exit(1)

    batch = boto3.client('batch')

    response = batch.describe_job_queues(jobQueues=[job_queue])
    if len(response['jobQueues']) > 0:
        delete_job_queue(batch, job_queue, args.terminate)

    # delete the job definitions
    delete_all_job_definitions(batch, planet_date)

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

    # shutdown all database replicas
    ensure_dbs(planet_date, 0)