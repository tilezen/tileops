from batch import Buckets
from batch import run_go
from datetime import datetime
import yaml
from make_rawr_tiles import wait_for_jobs_to_finish
from make_rawr_tiles import wc_line
from contextlib import contextmanager
from collections import namedtuple
import boto3
import shutil
import tempfile


MissingTiles = namedtuple('MissingTiles', 'count low_zoom_file high_zoom_file')


class MissingTileFinder(object):
    """
    Finds tiles missing from an S3 bucket and provides convenience methods to
    navigate them.
    """

    def __init__(self, planet_date, buckets, date_prefix, region, config):
        self.planet_date = planet_date
        self.buckets = buckets
        self.date_prefix = date_prefix
        self.region = region

        with open(config, 'r') as fh:
            conf = yaml.load(fh.read())
            self.job_queue_name = conf['batch']['job-queue']
            self.job_definition = conf['batch']['job-definition']

        self.s3 = boto3.client('s3')

    def clear_old_missing_logs(self):
        # wait for any currently-running jobs to finish, so that we don't have
        # things which are possibly still running and writing to S3.
        wait_for_jobs_to_finish(self.job_queue_name)

        paginator = self.s3.get_paginator('list_objects_v2')
        page_iter = paginator.paginate(
            Bucket=self.buckets.missing,
            Prefix=date_prefix,
        )

        print("Listing logs to delete.")
        keys = []
        for page in page_iter:
            if page['KeyCount'] == 0:
                break

            for obj in page['Contents']:
                keys.append(obj['Key'])

        # from AWS documentation, we can delete up to 1000 at a time.
        max_keys_per_chunk = 1000

        print("Deleting old logs.")
        for idx in xrange(0, len(keys), max_keys_per_chunk):
            chunk = keys[idx:idx+max_keys_per_chunk]
            response = self.s3.delete_objects(
                Bucket=self.buckets.missing,
                Delete=dict(
                    Objects=[dict(Key=key) for key in chunk],
                ),
            )

            errors = response.get('Errors')
            if errors:
                raise RuntimeError("Unable to delete some files: %r" % errors)

    @contextmanager
    def missing_tiles(self):
        """
        To be used in a with-statement. Yields a MissingTiles object, giving
        information about the tiles which are missing.
        """

        # before we start, delete any data that exists under this date prefix
        print("Clearing out old missing tile logs")
        self.clear_old_missing_logs()

        # enqueue the jobs to find all the existing meta tiles.
        print("Running Batch job to enumerate existing meta tiles.")
        run_go('tz-batch-submit-missing-meta-tiles',
               '-job-queue', self.job_queue_name,
               '-job-definition', self.job_definition,
               '-dest-bucket', self.buckets.missing,
               '-dest-date-prefix', self.date_prefix,
               '-src-bucket', self.buckets.meta,
               '-src-date-prefix', self.date_prefix,
               '-region', self.region)

        print("Waiting for jobs to finish...")
        wait_for_jobs_to_finish(self.job_queue_name)

        tmpdir = tempfile.mkdtemp()
        try:
            missing_meta_file = os.path.join(tmpdir, 'missing_meta.txt')
            missing_low_file = os.path.join(tmpdir, 'missing.low.txt')
            missing_high_file = os.path.join(tmpdir, 'missing.high.txt')

            print("Reading existing meta tiles")
            run_go('tz-missing-meta-tiles-read',
                   '-bucket', self.buckets.missing,
                   '-date-prefix', self.date_prefix,
                   '-region', self.region,
                   stdout=missing_meta_file)

            print("Splitting into high and low zoom lists")
            # TODO: split zoom and zoom max should come from config.
            run_go('tz-batch-tiles-split-low-high',
                   '-high-zoom-file', missing_high_file,
                   '-low-zoom-file', missing_low_file,
                   '-split-zoom', '10',
                   '-tiles-file', missing_meta_file,
                   '-zoom-max', '7')

            count = wc_line(missing_low_file) + wc_line(missing_high_file)
            yield MissingTiles(count, missing_low_file, missing_high_file)

        finally:
            shutil.rmtree(tmpdir)


def enqueue_tiles(config_file, tile_list_file):
    from tilequeue.command import make_config_from_argparse
    from tilequeue.command import tilequeue_batch_enqueue
    from make_rawr_tiles import BatchEnqueueArgs

    args = BatchEnqueueArgs(config_file, None, tile_list_file, None)
    with open(args.config) as fh:
        cfg = make_config_from_argparse(fh)
    tilequeue_batch_enqueue(cfg, args)


if __name__ == '__main__':
    import argparse
    import os

    parser = argparse.ArgumentParser("Render missing meta tiles")
    parser.add_argument('rawr_bucket', help="Bucket with RAWR tiles in")
    parser.add_argument('meta_bucket', help="Bucket with meta tiles in")
    parser.add_argument('--missing-bucket', help="Bucket to store missing "
                        "tile logs in")
    parser.add_argument('date', help='Planet date, YYMMDD')
    parser.add_argument('--date-prefix', help="Date prefix in bucket, "
                        "defaults to planet date.")
    parser.add_argument('--retries', default=5, type=int, help="Number "
                        "of times to retry enqueueing the remaining jobs "
                        "before giving up.")
    parser.add_argument('--config',
                        default='enqueue-missing-meta-tiles-write.config.yaml',
                        help="Configuration file written out by make_tiles.py")
    parser.add_argument('--region', help='AWS region. If not provided, then '
                        'the AWS_DEFAULT_REGION environment variable must be '
                        'set.')
    parser.add_argument('--low-zoom-config',
                        default='enqueue-meta-low-zoom-batch.config.yaml',
                        help="Low zoom batch configuration file written out "
                        "by make_tiles.py")
    parser.add_argument('--high-zoom-config',
                        default='enqueue-meta-batch.config.yaml',
                        help="High zoom batch configuration file written out "
                        "by make_tiles.py")

    args = parser.parse_args()
    planet_date = datetime.strptime(args.date, '%y%m%d')
    buckets = Buckets(args.rawr_bucket, args.meta_bucket,
                      args.missing_bucket or args.meta_bucket)
    date_prefix = args.date_prefix or planet_date.strftime('%y%m%d')

    region = args.region or os.environ.get('AWS_DEFAULT_REGION')
    if region is None:
        import sys
        print "ERROR: Need environment variable AWS_DEFAULT_REGION to be set."
        sys.exit(1)

    tile_finder = MissingTileFinder(
        planet_date, buckets, date_prefix, region, args.config)

    for retry_number in xrange(0, args.retries):
        with tile_finder.missing_tiles() as missing:
            if missing.count == 0:
                print("All done!")
                break

            # enqueue jobs for missing tiles
            print("Enqueueing low zoom tiles")
            enqueue_tiles(args.low_zoom_config, missing.low_zoom_file)

            print("Enqueueing high zoom tiles")
            enqueue_tiles(args.high_zoom_config, missing.high_zoom_file)

    else:
        with tile_finder.missing_tiles() as missing:
            print("FAILED! %d tiles still missing after %d tries"
                  % (missing.count, args.retries))
