import argparse

import boto3


parser = argparse.ArgumentParser(description="""
Print the job coordinate and amount of time in milliseconds the job took to
complete. This can help identify long-running jobs (which finished in the
past 24h) and therefore help to identify why they take so long.
""")
parser.add_argument('date', help='Date prefix to use, YYMMDD')
args = parser.parse_args()

job_queue = 'job-queue-' + args.date
prefix = 'meta-batch-%s-' % (args.date,)
batch = boto3.client('batch')

next_token = None
while True:
    kwargs = dict(
        jobQueue=job_queue,
        jobStatus='SUCCEEDED',
    )
    if next_token:
        kwargs['nextToken'] = next_token
    response = batch.list_jobs(**kwargs)

    for job in response['jobSummaryList']:
        array_props = job.get('arrayProperties')
        if array_props:
            continue

        duration = job['stoppedAt'] - job['startedAt']
        name = job['jobName']
        if name.startswith(prefix):
            coord_str = name[len(prefix):].replace('-', '/')
            print('%s\t%d' % (coord_str, duration))

    next_token = response.get('nextToken')
    if not next_token:
        break
