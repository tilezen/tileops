import boto3
import time
import argparse


parser = argparse.ArgumentParser(
    description="""
Stream the latest lines from a log stream by ingestion time. This acts a bit
like "tailing" a log file. It's by ingestion time rather than event time, as
the TPS logs don't seem to get the right event time right now.
""")
parser.add_argument('group', help='Name of the log group for TPS logs')
parser.add_argument('--stream', help='Name of log stream. Optional, defaults '
                    'to the stream in the group with the most recent events')
parser.add_argument('--interval', default=60, type=int,
                    help='Interval between log updates.')
args = parser.parse_args()

logs = boto3.client('logs')
log_group = args.group
log_stream = args.stream
if log_stream is None:
    response = logs.describe_log_streams(
        logGroupName=log_group,
        orderBy='LastEventTime',
        descending=True,
        limit=1,
    )

    streams = response['logStreams']
    assert streams
    log_stream = streams[0]['logStreamName']

last_time = None
while True:
    response = logs.get_log_events(
        logGroupName=log_group, logStreamName=log_stream, limit=150,
        startFromHead=False)

    for event in response['events']:
        ingest_time = event['ingestionTime']
        # any integer is greater than None, so we don't need to check for
        # last_time is None
        if ingest_time > last_time:
            print(event['message'])
            last_time = ingest_time

    time.sleep(args.interval)
