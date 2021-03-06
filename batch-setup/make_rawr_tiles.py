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


def all_tiles_at(zoom):
    """
    Generate all the coordinates at a given zoom level.
    """

    max_coord = 2 ** zoom
    for x in range(max_coord):
        for y in range(max_coord):
            yield Coordinate(zoom=zoom, column=x, row=y)


def missing_tiles(missing_bucket, rawr_bucket, date_prefix, region,
                  key_format_type, config, zoom):
    from make_meta_tiles import MissingTileFinder

    present = set()

    finder = MissingTileFinder(
        missing_bucket, rawr_bucket, date_prefix, date_prefix, region,
        key_format_type, config, zoom)

    with finder.present_tiles() as present_file:
        with open(present_file) as fh:
            for line in fh:
                coord = deserialize_coord(line)
                if coord.zoom == zoom:
                    present.add(coord)

    missing = set(all_tiles_at(zoom)) - set(present)
    return missing


@contextmanager
def missing_jobs(missing_bucket, rawr_bucket, date_prefix, region, config,
                 tile_zoom=10, job_zoom=7, key_format_type='prefix-hash'):
    """
    Write and yield file containing a z/x/y coordinate for each job (at the
    job zoom) corresponding to a missing tile (at the tile zoom) in the bucket.

    Cleans up the temporary directory after the yielded-to code returns.
    """

    tiles = missing_tiles(
        missing_bucket, rawr_bucket, date_prefix, region, key_format_type,
        config, tile_zoom)
    jobs = set(coord.zoomTo(job_zoom).container() for coord in tiles)

    print("[make_rawr_tiles] Missing %d tiles (%d jobs)" % (len(tiles), len(jobs)))

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


def head_lines(filename, n_lines):
    """
    Returns an array of the first n_lines lines of filename, similar to the command
    line utility `head -n`.
    """

    sample = []

    with open(filename, 'r') as fh:
        try:
            for _ in range(n_lines):
                sample.append(next(fh).strip())
        except StopIteration:
            pass

    return sample


def any_jobs_with_status(batch, job_queue, status):
    """
    Returns True if there are any jobs in the queue with the same status.
    """

    response = batch.list_jobs(
        jobQueue=job_queue, jobStatus=status, maxResults=1)
    return len(response['jobSummaryList']) > 0


def wait_for_jobs_to_finish(job_queue, wait_time=300):
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
                print("[%s] Still have jobs left in queue." % (time.ctime()))
                time.sleep(wait_time)
                break
    print("[make_rawr_tiles] All jobs finished (either SUCCEEDED or FAILED)")


def make_rawr_tiles(rawr_config_file, missing_config_file, missing_bucket,
                    rawr_bucket, region, date_prefix, retry_attempts,
                    tile_zoom=10, key_format_type='prefix-hash'):
    """
    Finds out which jobs need to be run to have a complete RAWR tiles bucket,
    runs them and waits for them to complete. If the bucket still isn't
    complete, repeats until it is complete or the given number of retries is
    exceeded.
    """

    assert os.path.isfile(rawr_config_file), rawr_config_file
    with open(rawr_config_file, 'r') as fh:
        config = yaml.load(fh.read())
        job_zoom = config['batch']['queue-zoom']
        logging_config = config['logging']['config']
        assert os.path.isfile(logging_config), logging_config
        job_queue = config['batch']['job-queue']

    for attempt in range(retry_attempts):
        with missing_jobs(
                missing_bucket, rawr_bucket, date_prefix, region,
                missing_config_file, tile_zoom, job_zoom, key_format_type
        ) as missing_file:
            num_missing = wc_line(missing_file)
            if num_missing == 0:
                print("[make_rawr_tiles] Successfully generated all the RAWR "
                      "tiles after "
                      "%d re-enqueues!" % (attempt))
                return

            args = BatchEnqueueArgs(rawr_config_file, None, missing_file, None)
            with open(args.config) as fh:
                cfg = make_config_from_argparse(fh)
            tilequeue_batch_enqueue(cfg, args)

            wait_for_jobs_to_finish(job_queue)

    tiles = missing_tiles(missing_bucket, rawr_bucket, date_prefix, region,
                          key_format_type, config, tile_zoom)
    print("[make_rawr_tiles] Ran %d times, but still have %d missing tiles. "
          "Good luck!" %
          (retry_attempts, len(tiles)))


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
    parser.add_argument('--key-format-type', default='prefix-hash',
                        help="Key format type, either 'prefix-hash' or "
                        "'hash-prefix', controls whether the S3 key is "
                        "prefixed with the date or hash first.")
    parser.add_argument('--region', help='AWS region. If not provided, then '
                        'the AWS_DEFAULT_REGION environment variable must be '
                        'set.')
    parser.add_argument('--missing-config',
                        default='enqueue-missing-meta-tiles-write.config.yaml',
                        help="Configuration file for missing tile enumeration "
                        "written out by make_tiles.py")
    parser.add_argument('missing_bucket', help="Bucket to store tile "
                        "enumeration logs in while calculating missing tiles.")

    args = parser.parse_args()
    assert args.key_format_type in ('prefix-hash', 'hash-prefix')
    assert args.bucket
    assert args.missing_bucket

    region = args.region or os.environ.get('AWS_DEFAULT_REGION')
    if region is None:
        import sys
        print("[make_rawr_tiles] ERROR: Need environment variable "
              "AWS_DEFAULT_REGION to be set.")
        sys.exit(1)

    make_rawr_tiles(args.config, args.missing_config, args.missing_bucket,
                    args.bucket, region, args.date_prefix, args.retries,
                    args.tile_zoom, args.key_format_type)
