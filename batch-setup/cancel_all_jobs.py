import argparse
from batch import terminate_all_jobs


parser = argparse.ArgumentParser("cancel all the jobs")
parser.add_argument("date", help="Date prefix to use, YYMMDD.")
parser.add_argument("--reason", help="Reason message to use. ",
                    default="Manually terminating run due to bugs or "
                    "misconfiguration.")
args = parser.parse_args()

job_queue = f'job-queue-{args.date}'
terminate_all_jobs(job_queue, args.reason)
