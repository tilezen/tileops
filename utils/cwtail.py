import boto3
import time
import argparse
from datetime import datetime


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
parser.add_argument('--date', help='Planet date. If --stream is not supplied, '
                    'look for a TPS instance tagged with this date. YYMMDD.')
args = parser.parse_args()

logs = boto3.client('logs')
log_group = args.group
log_stream = args.stream

if log_stream is None and args.date:
    planet_date = datetime.strptime(args.date, '%y%m%d')
    long_date = planet_date.strftime('%Y-%m-%d')
    ec2 = boto3.client('ec2')
    response = ec2.describe_instances(
        Filters=[dict(Name='tag:tps-instance', Values=[long_date])],
    )
    assert response['Reservations']
    assert response['Reservations'][0]['Instances']
    log_stream = response['Reservations'][0]['Instances'][0]['InstanceId']

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
        logGroupName=log_group, logStreamName=log_stream, limit=200,
        startFromHead=False)

    max_ingest_time = None
    for event in response['events']:
        ingest_time = event['ingestionTime']
        # any integer is greater than None, so we don't need to check for
        # last_time / max_ingest_time is None
        if ingest_time > last_time and ingest_time >= max_ingest_time:
            print(event['message'])
        if ingest_time > max_ingest_time:
            max_ingest_time = ingest_time
    if max_ingest_time > last_time:
        last_time = max_ingest_time

    time.sleep(args.interval)
