from ModestMaps.Core import Coordinate
from collections import namedtuple
from contextlib import contextmanager
from tilequeue.command import make_config_from_argparse
from tilequeue.command import tilequeue_batch_enqueue
from tilequeue.tile import deserialize_coord
from tilequeue.tile import serialize_coord
import boto3
import os.path
import shutil
import tempfile
import yaml


# this struct exists to be passed into tilequeue's tilequeue_batch_enqueue
# command. it replicates the expected arguments, which would normally come
# from argparse. we only use the 'config' and 'file' parts of it, but the
# others need to exist to avoid AttributeError.
BatchEnqueueArgs = namedtuple('BatchEnqueueArgs', 'config tile file pyramid')


# parse a coordinate from an S3 key.
# TODO: don't we have an implementation of this somewhere else to reuse???
def coord_from_key(key):
    ext = '.zip'
    assert key.endswith(ext)
    # skip backward through the string's '/'. this is an attempt to avoid the
    # overhead of allocating a bunch of strings by splitting on '/' when we
    # are just going to join them back together anyway.
    i = key.rfind('/')
    i = key.rfind('/', 0, i)
    i = key.rfind('/', 0, i)
    return deserialize_coord(key[(i+1):-len(ext)])


def ls(bucket, date_prefix):
    """
    Generate the coordinates of all the tiles in the bucket with the given
    date prefix.
    """

    from sys import stderr

    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    response_iter = paginator.paginate(
        Bucket=bucket,
        Prefix=date_prefix,
    )

    # this takes a while, so it's nice to have some output to tell us it's
    # doing something and allows us to roughly figure how long left to go.
    total_keys = 0
    stderr.write("Listed %d tiles..." % (total_keys))
    for response in response_iter:
        num_keys = response['KeyCount']
        if num_keys == 0:
            continue

        for obj in response['Contents']:
            key = obj['Key']
            assert key.startswith(date_prefix)
            coord = coord_from_key(key)
            yield coord

        total_keys += num_keys
        stderr.write("\rListed %d tiles..." % (total_keys))


def all_tiles_at(zoom):
    """
    Generate all the coordinates at a given zoom level.
    """

    max_coord = 2 ** zoom
    for x in xrange(max_coord):
        for y in xrange(max_coord):
            yield Coordinate(zoom=zoom, column=x, row=y)


def missing_tiles(bucket, date_prefix, zoom):
    print("Finding missing tiles in s3://%s/%s/..." % (bucket, date_prefix))

    present = set()
    for coord in ls(bucket, date_prefix):
        if coord.zoom == zoom:
            present.add(coord)

    missing = set(all_tiles_at(zoom)) - set(present)
    return missing


@contextmanager
def missing_jobs(bucket, date_prefix, tile_zoom=10, job_zoom=7):
    """
    Write and yield file containing a z/x/y coordinate for each job (at the
    job zoom) corresponding to a missing tile (at the tile zoom) in the bucket.

    Cleans up the temporary directory after the yielded-to code returns.
    """

    tiles = missing_tiles(bucket, date_prefix, tile_zoom)
    jobs = set(coord.zoomTo(job_zoom).container() for coord in tiles)

    print("Missing %d tiles (%d jobs)" % (len(tiles), len(jobs)))

    tmpdir = tempfile.mkdtemp()
    try:
        missing_file = os.path.join(tmpdir, "missing_jobs.txt")

        with open(missing_file, 'w') as fh:
            for coord in sorted(jobs):
                fh.write(serialize_coord(coord) + "\n")

        yield missing_file

    finally:
        shutil.rmtree(tmpdir)


def wc_line(filename):
    """
    Returns a count of the number of lines in the file, similar to the command
    line utility `wc -l`.
    """

    with open(filename, 'r') as fh:
        count = sum(1 for _ in fh)
    return count


def any_jobs_with_status(batch, job_queue, status):
    """
    Returns True if there are any jobs in the queue with the same status.
    """

    response = batch.list_jobs(
        jobQueue=job_queue, jobStatus=status, maxResults=1)
    return len(response['jobSummaryList']) > 0


def wait_for_jobs_to_finish(job_queue, wait_time=60):
    """
    Wait until there are no jobs in the queue with a non-finished status.

    By "non-finished" we mean anything that is running, or could potentially
    run, so. When there are no jobs in any of these statuses, we know that all
    the jobs we submitted have either succeeded or failed, and we can check S3
    to find out if we need to re-run anything.
    """

    import time

    batch = boto3.client('batch')

    jobs_remaining = True
    while jobs_remaining:
        jobs_remaining = False
        for status in ('SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING',
                       'RUNNING'):
            if any_jobs_with_status(batch, job_queue, status):
                jobs_remaining = True
                print("Still have jobs left in queue.")
                time.sleep(wait_time)
                break
    print("All jobs finished (either SUCCEEDED or FAILED)")


def make_rawr_tiles(config_file, bucket, date_prefix, retry_attempts,
                    tile_zoom=10):
    """
    Finds out which jobs need to be run to have a complete RAWR tiles bucket,
    runs them and waits for them to complete. If the bucket still isn't
    complete, repeats until it is complete or the given number of retries is
    exceeded.
    """

    assert os.path.isfile(config_file)
    with open(config_file, 'r') as fh:
        config = yaml.load(fh.read())
        job_zoom = config['batch']['queue-zoom']
        logging_config = config['logging']['config']
        assert os.path.isfile(logging_config)
        job_queue = config['batch']['job-queue']

    for attempt in range(retry_attempts):
        with missing_tiles(bucket, date_prefix, tile_zoom,
                           job_zoom) as missing_file:
            num_missing = wc_line(missing_file)
            if num_missing == 0:
                print("Successfully generated all the RAWR tiles after "
                      "%d re-enqueues!" % (attempt))
                return

            args = BatchEnqueueArgs(config_file, None, missing_file, None)
            with open(args.config) as fh:
                cfg = make_config_from_argparse(fh)
            tilequeue_batch_enqueue(cfg, args)

            wait_for_jobs_to_finish(job_queue)

    with missing_tiles(bucket, date_prefix, tile_zoom,
                       job_zoom) as missing_file:
        missing_file_notmp = 'missing_tiles.txt'
        num_missing = wc_line(missing_file)
        shutil.copyfile(missing_file, missing_file_notmp)
        print("Ran %d times, but still have %d missing tiles. The list is "
              "in %r for you. Good luck!" %
              (retry_attempts, num_missing, missing_file_notmp))


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser("Render missing RAWR tiles")
    parser.add_argument('bucket', help="Bucket with RAWR tiles in")
    parser.add_argument('date_prefix', help="Date prefix in bucket")
    parser.add_argument('--retries', default=5, type=int, help="Number "
                        "of times to retry enqueueing the remaining jobs "
                        "before giving up.")
    parser.add_argument('--config', default='enqueue-rawr-batch.config.yaml',
                        help="Configuration file written out by make_tiles.py")
    parser.add_argument('--tile-zoom', default=10, type=int,
                        help="Zoom level at which tiles are saved -- not the "
                        "same thing as the zoom level jobs are run at!")

    args = parser.parse_args()
    make_rawr_tiles(args.config, args.bucket, args.date_prefix,
                    args.retries, args.tile_zoom)
