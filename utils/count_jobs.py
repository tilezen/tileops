import boto3
import argparse
import datetimeg
import time


def count_jobs(batch, job_queue):
    total = 0
    message = ""

    for status in ('SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING',
                   'RUNNING'):
        response = batch.list_jobs(
            jobQueue=job_queue,
            jobStatus=status,
        )
        status_sum = 0
        while response.get('jobSummaryList'):
            status_sum += len(response['jobSummaryList'])
            next_token = response.get('nextToken')
            if not next_token:
                break
            response = batch.list_jobs(
                jobQueue=job_queue,
                jobStatus=status,
                nextToken=next_token,
            )
        if message:
            message += ", "
        message += "%6d %s" % (status_sum, status)
        total += status_sum

    return "%6d TOTAL (%s)" % (total, message)


def log_msg(msg):
    now = datetime.datetime.now()
    print("[%s] %s" % (now.strftime("%Y-%m-%d %H:%M:%S"), msg))


parser = argparse.ArgumentParser(description="""
Count the jobs in each status in the job queue. This gives you the true count
of all the jobs, rather than the "1000+" that gets shown in the Batch console.
""")
parser.add_argument("date", help="Date prefix to use, YYMMDD.")
parser.add_argument("--interval", type=int, default=300, help="Number of "
                    "seconds between updates.")
args = parser.parse_args()

job_queue = 'job-queue-%s' % (args.date,)
batch = boto3.client('batch')

interval = args.interval
next_time = time.time()
while True:
    msg = count_jobs(batch, job_queue)
    log_msg(msg)

    next_time += interval
    while next_time <= time.time():
        now = datetime.datetime.now()
        log_msg("Count took too long, skipping update.")
        next_time += interval

    time.sleep(next_time - time.time())
