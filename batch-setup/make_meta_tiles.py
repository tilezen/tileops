from batch import Buckets
from batch import run_go
import yaml
from make_rawr_tiles import wait_for_jobs_to_finish
from make_rawr_tiles import wc_line
from run_id import assert_run_id_format
from contextlib import contextmanager
from collections import namedtuple
import boto3
import os
import shutil
import tempfile
from tilequeue.tile import metatile_zoom_from_size
from rds import ensure_dbs
from tilequeue.tile import deserialize_coord
from tilequeue.tile import serialize_coord
import gzip
from tileset import CoordSet
from tilequeue.store import make_s3_tile_key_generator
from ModestMaps.Core import Coordinate
from multiprocessing import Pool


MissingTiles = namedtuple('MissingTiles', 'low_zoom_file high_zoom_file')


class MissingTileFinder(object):
    """
    Finds tiles missing from an S3 bucket and provides convenience methods to
    navigate them.
    """

    def __init__(self, missing_bucket, tile_bucket, src_date_prefix,
                 dst_date_prefix, region, key_format_type, config, max_zoom):
        self.missing_bucket = missing_bucket
        self.tile_bucket = tile_bucket
        self.src_date_prefix = src_date_prefix
        self.dst_date_prefix = dst_date_prefix
        self.region = region
        self.key_format_type = key_format_type
        self.max_zoom = max_zoom

        assert self.missing_bucket
        assert self.tile_bucket
        assert self.src_date_prefix
        assert self.dst_date_prefix
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
            Prefix=self.dst_date_prefix,
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
        for idx in range(0, len(keys), max_keys_per_chunk):
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
            '-dest-date-prefix', self.dst_date_prefix,
            '-src-bucket', self.tile_bucket,
            '-src-date-prefix', self.src_date_prefix,
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
               '-date-prefix', self.dst_date_prefix,
               '-region', self.region,
               '-present=%r' % (bool(present),),
               '-compress-output=%r' % (bool(compress),),
               '-max-zoom', str(self.max_zoom),
               stdout=filename)

    @contextmanager
    def missing_tiles_split(self, split_zoom, zoom_max, big_jobs):
        """
        To be used in a with-statement. Yields a MissingTiles object, giving
        information about the tiles which are missing.

        High zoom jobs are output either at split_zoom (RAWR tile granularity)
        or zoom_max (usually lower, e.g: 7) depending on whether big_jobs
        contains a truthy value for the RAWR tile. The big_jobs are looked up
        at zoom_max.
        """

        self.run_batch_job()
        tmpdir = tempfile.mkdtemp()
        try:
            missing_meta_file = os.path.join(tmpdir, 'missing_meta.txt')
            missing_low_file = os.path.join(tmpdir, 'missing.low.txt')
            missing_high_file = os.path.join(tmpdir, 'missing.high.txt')

            self.read_metas_to_file(missing_meta_file, compress=True)

            print("Splitting into high and low zoom lists")

            # contains zooms 0 until group zoom. the jobs between the group
            # zoom and RAWR zoom are merged into the parent at group zoom.
            missing_low = CoordSet(zoom_max)

            # contains job coords at either zoom_max or split_zoom only.
            # zoom_max is a misnomer here, as it's always less than or equal to
            # split zoom?
            missing_high = CoordSet(split_zoom, min_zoom=zoom_max)

            with gzip.open(missing_meta_file, 'r') as fh:
                for line in fh:
                    c = deserialize_coord(line)
                    if c.zoom < split_zoom:
                        # in order to not have too many jobs in the queue, we
                        # group the low zoom jobs to the zoom_max (usually 7)
                        if c.zoom > zoom_max:
                            c = c.zoomTo(zoom_max).container()

                        missing_low[c] = True

                    else:
                        # if the group of jobs at zoom_max would be too big
                        # (according to big_jobs[]) then enqueue the original
                        # coordinate. this is to prevent a "long tail" of huge
                        # job groups.
                        job_coord = c.zoomTo(zoom_max).container()
                        if not big_jobs[job_coord]:
                            c = job_coord

                        missing_high[c] = True

            with open(missing_low_file, 'w') as fh:
                for coord in missing_low:
                    fh.write(serialize_coord(coord) + "\n")

            with open(missing_high_file, 'w') as fh:
                for coord in missing_high:
                    fh.write(serialize_coord(coord) + "\n")

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


class _JobSizer(object):
    """
    Individual instance of a callable which can evaluate the size of a job
    (i.e: grouped set of RAWR tiles).
    """

    def __init__(self, bucket, prefix, key_format_type, rawr_zoom):
        self.bucket = bucket
        self.prefix = prefix
        self.key_format_type = key_format_type
        self.rawr_zoom = rawr_zoom

    def __call__(self, parent):
        s3 = boto3.client('s3')
        gen = make_s3_tile_key_generator({
            'key-format-type': self.key_format_type,
        })

        dz = self.rawr_zoom - parent.zoom
        assert dz >= 0
        width = 1 << dz

        size = 0
        for dx in range(width):
            for dy in range(width):
                coord = Coordinate(
                    zoom=self.rawr_zoom,
                    column=((parent.column << dz) + dx),
                    row=((parent.row << dz) + dy))
                key = gen(self.prefix, coord, 'zip')
                response = s3.head_object(Bucket=self.bucket, Key=key)
                size += response['ContentLength']

        return parent, size


def _big_jobs(rawr_bucket, prefix, key_format_type, rawr_zoom, group_zoom,
              size_threshold, pool_size=30):
    """
    Look up the RAWR tiles in the rawr_bucket under the prefix and with the
    given key format, group the RAWR tiles (usually at zoom 10) by the job
    group zoom (usually 7) and sum their sizes. Return a map-like object
    which has a truthy value for those Coordinates at group_zoom which sum
    to size_threshold or more.

    A pool size of 30 seems to work well; the point of the pool is to hide
    the latency of S3 requests, so pretty quickly hits the law of diminishing
    returns with extra concurrency (without the extra network cards that Batch
    would provide).
    """

    p = Pool(pool_size)
    job_sizer = _JobSizer(rawr_bucket, prefix, key_format_type, rawr_zoom)

    big_jobs = CoordSet(group_zoom, min_zoom=group_zoom)

    # loop over each column individually. this limits the number of concurrent
    # tasks, which means we don't waste memory maintaining a huge queue of
    # pending tasks. and when something goes wrong, the stacktrace isn't buried
    # in a million others.
    num_coords = 1 << group_zoom
    for x in range(num_coords):
        # kick off tasks async. each one knows its own coordinate, so we only
        # need to track the handle to know when its finished.
        tasks = []
        for y in range(num_coords):
            coord = Coordinate(zoom=group_zoom, column=x, row=y)
            tasks.append(p.apply_async(job_sizer, (coord,)))

        # collect tasks and put them into the big jobs list.
        for task in tasks:
            coord, size = task.get()
            if size >= size_threshold:
                big_jobs[coord] = True

    return big_jobs


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


# adaptor class for MissingTiles to see just the high zoom parts, this is used
# along with the LowZoomLense to loop over missing tiles generically but
# separately.
class HighZoomLense(object):
    def __init__(self, config):
        self.config = config
        self.description = "high zoom tiles"

    def missing_file(self, missing):
        return missing.high_zoom_file


class LowZoomLense(object):
    def __init__(self, config):
        self.config = config
        self.description = "low zoom tiles"

    def missing_file(self, missing):
        return missing.low_zoom_file


# abstracts away the logic for a re-rendering loop, splitting between high and
# low zoom tiles and stopping if all the tiles aren't rendered within a
# certain number of retries.
class TileRenderer(object):

    def __init__(self, tile_finder, big_jobs, split_zoom, zoom_max):
        self.tile_finder = tile_finder
        self.big_jobs = big_jobs
        self.split_zoom = split_zoom
        self.zoom_max = zoom_max

    def _missing(self):
        return self.tile_finder.missing_tiles_split(
            self.split_zoom, self.zoom_max, self.big_jobs)

    def render(self, num_retries, lense):
        for retry_number in range(0, num_retries):
            with self._missing() as missing:
                missing_tile_file = lense.missing_file(missing)
                count = wc_line(missing_tile_file)

                if count == 0:
                    print("All %s done!" % (lense.description,))
                    break

                check_metatile_exists = retry_number > 0

                # enqueue jobs for missing tiles
                if count > 0:
                    print("Enqueueing %s tiles" % (lense.description,))
                    enqueue_tiles(lense.config, missing_tile_file,
                                  check_metatile_exists)

        else:
            with self._missing() as missing:
                missing_tile_file = lense.missing_file(missing)
                count = wc_line(missing_tile_file)
                raise RuntimeError(
                    "FAILED! %d %s still missing after %d tries"
                    % (count, lense.description, num_retries))


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser("Render missing meta tiles")
    parser.add_argument('rawr_bucket', help="Bucket with RAWR tiles in")
    parser.add_argument('meta_bucket', help="Bucket with meta tiles in")
    parser.add_argument('--missing-bucket', help="Bucket to store missing "
                        "tile logs in")
    parser.add_argument('run_id', help='Unique identifier for run.')
    parser.add_argument('--date-prefix', help="Date prefix in bucket, "
                        "defaults to run ID.")
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
    parser.add_argument('--size-threshold', default=350000000, type=int,
                        help='If all the RAWR tiles grouped together are '
                        'bigger than this, split the job up into individual '
                        'RAWR tiles.')

    args = parser.parse_args()
    assert_run_id_format(args.run_id)
    buckets = Buckets(args.rawr_bucket, args.meta_bucket,
                      args.missing_bucket or args.meta_bucket)
    date_prefix = args.date_prefix or args.run_id
    missing_bucket_date_prefix = args.run_id
    assert args.key_format_type in ('prefix-hash', 'hash-prefix')

    # TODO: split zoom and zoom max should come from config.
    split_zoom = 10
    zoom_max = 7

    region = args.region or os.environ.get('AWS_DEFAULT_REGION')
    if region is None:
        import sys
        print("ERROR: Need environment variable AWS_DEFAULT_REGION to be set.")
        sys.exit(1)

    # check that metatile_size is within a sensible range
    assert args.metatile_size > 0
    assert args.metatile_size < 100
    metatile_max_zoom = 16 - metatile_zoom_from_size(args.metatile_size)
    tile_finder = MissingTileFinder(
        buckets.missing, buckets.meta, date_prefix, missing_bucket_date_prefix,
        region, args.key_format_type, args.config, metatile_max_zoom)

    big_jobs = _big_jobs(
        buckets.rawr, missing_bucket_date_prefix, args.key_format_type,
        split_zoom, zoom_max, args.size_threshold)

    tile_renderer = TileRenderer(tile_finder, big_jobs, split_zoom, zoom_max)

    tile_renderer.render(args.retries, LowZoomLense(args.low_zoom_config))

    # turn off databases by saying we want to ensure that zero are running.
    ensure_dbs(args.run_id, 0)

    tile_renderer.render(args.retries, HighZoomLense(args.high_zoom_config))
