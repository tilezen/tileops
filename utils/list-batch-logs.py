import argparse
import json

import boto3


def list_job_ids(batch, **kwargs):
    next_token = None
    job_ids = []
    while True:
        args = kwargs.copy()
        if next_token:
            args['nextToken'] = next_token
        response = batch.list_jobs(**args)
        for job_summary in response['jobSummaryList']:
            if 'missing-meta-tiles' not in job_summary['jobName']:
                job_ids.append(job_summary['jobId'])
        next_token = response.get('nextToken')
        if next_token is None:
            break

    return job_ids


def print_failures(cwlogs, log_stream_name):
    log_group_name = '/aws/batch/job'
    next_token = None
    while True:
        args = dict(
            logGroupName=log_group_name,
            logStreamName=log_stream_name,
            startFromHead=False,
        )
        if next_token:
            args['nextToken'] = next_token
        response = cwlogs.get_log_events(**args)
        for event in response['events']:
            msg = event['message']
            if len(msg) > 10 and msg[25] == '{' and msg.endswith('}'):
                msg = json.loads(msg[25:])
                if msg.get('type') == 'error':
                    coord = msg['coord']
                    coord_str = '%d/%d/%d' % (coord['z'], coord['x'],
                                              coord['y'])
                    print('%15s: %s' % (coord_str, msg['exception']))
                    trace = msg.get('stacktrace')
                    if trace:
                        print('    ' + trace.replace('|', '\n    '))

        new_token = response.get('nextForwardToken')
        if new_token is None or new_token == next_token:
            break
        next_token = new_token


parser = argparse.ArgumentParser(description="""
Prints errors from CloudWatch Logs from the most recent successful jobs.

A single TPS job typically renders many tiles, some of which might fail.
Because we don't want to stop the whole job because of a single failure, the
job will succeed even when some individual tiles fail. This makes pulling the
failures out of the rest of the logs more difficult.

That's where this tool comes in: Point it at a job queue and it'll list out
all the jobs which had failed tiles, and the stacktrace of the failure.

Batch only retains jobs for about 24h, so you'll want to run this pretty soon
after all the jobs finish - or even several times during the run!
""")
parser.add_argument('job_queue', help='Job queue to list jobs from.')
args = parser.parse_args()

batch = boto3.client('batch')
cwlogs = boto3.client('logs')

job_ids = list_job_ids(batch, jobQueue=args.job_queue, jobStatus='SUCCEEDED')
num_chunks = len(job_ids) / 100
for i in range(0, num_chunks + 1):
    chunk = job_ids[(100 * i):(100 * (i+1))]
    response = batch.describe_jobs(jobs=chunk)
    for job in response['jobs']:
        name = job['jobName']
        last_attempt = job['attempts'][-1]
        log_stream_name = last_attempt['container']['logStreamName']
        print('=== %s ===\n' % (name,))
        print_failures(cwlogs, log_stream_name)
