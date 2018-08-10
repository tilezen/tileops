import boto3
import argparse


parser = argparse.ArgumentParser("cancel all the jobs")
parser.add_argument("date", help="Date prefix to use, YYMMDD.")
args = parser.parse_args()

job_queue = 'job-queue-%s' % (args.date,)
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
    print("Terminating %d %r jobs" % (len(job_ids), status))
    for job_id in job_ids:
        batch.terminate_job(jobId=job_id, reason='Terminating this run due to software bugs.')
