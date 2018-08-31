from batch import Buckets
from batch import run_go
from datetime import datetime
import yaml
from make_rawr_tiles import wait_for_jobs_to_finish
from make_rawr_tiles import wc_line
from contextlib import contextmanager
from collections import namedtuple
import boto3
import os
import shutil
import tempfile
from tilequeue.tile import metatile_zoom_from_size


MissingTiles = namedtuple('MissingTiles', 'low_zoom_file high_zoom_file')


class MissingTileFinder(object):
    """
    Finds tiles missing from an S3 bucket and provides convenience methods to
    navigate them.
    """

    def __init__(self, missing_bucket, tile_bucket, date_prefix, region,
                 key_format_type, config, max_zoom):
        self.missing_bucket = missing_bucket
        self.tile_bucket = tile_bucket
        self.date_prefix = date_prefix
        self.region = region
        self.key_format_type = key_format_type
        self.max_zoom = max_zoom

        assert self.missing_bucket
        assert self.tile_bucket
        assert self.date_prefix
        assert self.region
        assert self.key_format_type is not None

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
            Bucket=self.missing_bucket,
            Prefix=self.date_prefix,
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
                Bucket=self.missing_bucket,
                Delete=dict(
                    Objects=[dict(Key=key) for key in chunk],
                ),
            )

            errors = response.get('Errors')
            if errors:
                raise RuntimeError("Unable to delete some files: %r" % errors)

    def run_batch_job(self):
        # before we start, delete any data that exists under this date prefix
        print("Clearing out old missing tile logs")
        self.clear_old_missing_logs()

        # enqueue the jobs to find all the existing meta tiles.
        print("Running Batch job to enumerate existing meta tiles.")
        run_go(
            'tz-batch-submit-missing-meta-tiles',
            '-job-queue', self.job_queue_name,
            '-job-definition', self.job_definition,
            '-dest-bucket', self.missing_bucket,
            '-dest-date-prefix', self.date_prefix,
            '-src-bucket', self.tile_bucket,
            '-src-date-prefix', self.date_prefix,
            '-region', self.region,
            '-key-format-type', self.key_format_type,
        )

        print("Waiting for jobs to finish...")
        wait_for_jobs_to_finish(self.job_queue_name)

    def read_metas_to_file(self, filename, present=False, compress=False):
        if present:
            print("Reading existing meta tiles")
        else:
            print("Reading missing meta tiles")

        run_go('tz-missing-meta-tiles-read',
               '-bucket', self.missing_bucket,
               '-date-prefix', self.date_prefix,
               '-region', self.region,
               '-present=%r' % (bool(present),),
               '-compress-output=%r' % (bool(compress),),
               '-max-zoom', str(self.max_zoom),
               stdout=filename)

    @contextmanager
    def missing_tiles_split(self, split_zoom, zoom_max):
        """
        To be used in a with-statement. Yields a MissingTiles object, giving
        information about the tiles which are missing.
        """

        self.run_batch_job()
        tmpdir = tempfile.mkdtemp()
        try:
            missing_meta_file = os.path.join(tmpdir, 'missing_meta.txt')
            missing_low_file = os.path.join(tmpdir, 'missing.low.txt')
            missing_high_file = os.path.join(tmpdir, 'missing.high.txt')

            self.read_metas_to_file(missing_meta_file, compress=True)

            print("Splitting into high and low zoom lists")
            run_go('tz-batch-tiles-split-low-high',
                   '-high-zoom-file', missing_high_file,
                   '-low-zoom-file', missing_low_file,
                   '-split-zoom', str(split_zoom),
                   '-tiles-file', missing_meta_file,
                   '-zoom-max', str(zoom_max),
                   '-compressed=True')

            # was this debugging code???
            #with open(missing_low_file, 'w') as fh:
            #    for z in xrange(0, 8):
            #        max_coord = 1 << z
            #        for x in xrange(0, max_coord):
            #            for y in xrange(0, max_coord):
            #                fh.write("%d/%d/%d\n" % (z, x, y))

            yield MissingTiles(missing_low_file, missing_high_file)

        finally:
            shutil.rmtree(tmpdir)

    @contextmanager
    def present_tiles(self):
        """
        To be used in a with-statement. Yields the name of a file containing a
        list of all the tile coordinates which _are present_ in the S3 bucket.
        """

        self.run_batch_job()
        tmpdir = tempfile.mkdtemp()
        try:
            tile_file = os.path.join(tmpdir, 'tiles.txt')
            self.read_metas_to_file(tile_file, present=True)
            yield tile_file

        finally:
            shutil.rmtree(tmpdir)


def enqueue_tiles(config_file, tile_list_file, check_metatile_exists):
    from tilequeue.command import make_config_from_argparse
    from tilequeue.command import tilequeue_batch_enqueue
    from make_rawr_tiles import BatchEnqueueArgs

    args = BatchEnqueueArgs(config_file, None, tile_list_file, None)
    os.environ['TILEQUEUE__BATCH__CHECK-METATILE-EXISTS'] = (
        str(check_metatile_exists).lower())
    with open(args.config) as fh:
        cfg = make_config_from_argparse(fh)
    tilequeue_batch_enqueue(cfg, args)


if __name__ == '__main__':
    import argparse

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
    parser.add_argument('--key-format-type', default='prefix-hash',
                        help="Key format type, either 'prefix-hash' or "
                        "'hash-prefix', controls whether the S3 key is "
                        "prefixed with the date or hash first.")
    parser.add_argument('--metatile-size', default=8, type=int,
                        help='Metatile size (in 256px tiles).')

    args = parser.parse_args()
    planet_date = datetime.strptime(args.date, '%y%m%d')
    buckets = Buckets(args.rawr_bucket, args.meta_bucket,
                      args.missing_bucket or args.meta_bucket)
    date_prefix = args.date_prefix or planet_date.strftime('%y%m%d')
    assert args.key_format_type in ('prefix-hash', 'hash-prefix')

    # TODO: split zoom and zoom max should come from config.
    split_zoom = 10
    zoom_max = 7

    region = args.region or os.environ.get('AWS_DEFAULT_REGION')
    if region is None:
        import sys
        print "ERROR: Need environment variable AWS_DEFAULT_REGION to be set."
        sys.exit(1)

    # check that metatile_size is within a sensible range
    assert args.metatile_size > 0
    assert args.metatile_size < 100
    metatile_max_zoom = 16 - metatile_zoom_from_size(args.metatile_size)
    tile_finder = MissingTileFinder(
        buckets.missing, buckets.meta, date_prefix, region,
        args.key_format_type, args.config, metatile_max_zoom)

    for retry_number in xrange(0, args.retries):
        with tile_finder.missing_tiles_split(split_zoom, zoom_max) as missing:
            low_count = wc_line(missing.low_zoom_file)
            high_count = wc_line(missing.high_zoom_file)

            if low_count == 0 and high_count == 0:
                print("All done!")
                break

            # On the first run, don't check if the metatile already exists,
            # since it most likely won't. On subsequent runs when we're trying
            # to generate the remaining tiles, we want to check first to avoid
            # repeat processing.
            check_metatile_exists = retry_number > 0

            # enqueue jobs for missing tiles
            if low_count > 0:
                print("Enqueueing low zoom tiles")
                enqueue_tiles(args.low_zoom_config, missing.low_zoom_file,
                              check_metatile_exists)

            if high_count > 0:
                print("Enqueueing high zoom tiles")
                enqueue_tiles(args.high_zoom_config, missing.high_zoom_file,
                              check_metatile_exists)

    else:
        with tile_finder.missing_tiles_split(split_zoom, zoom_max) as missing:
            low_count = wc_line(missing.low_zoom_file)
            high_count = wc_line(missing.high_zoom_file)
            total = low_count + high_count
            print("FAILED! %d tiles still missing after %d tries"
                  % (total, args.retries))
