import queue

from batch import Buckets
from batch import run_go
import yaml
from make_rawr_tiles import wait_for_jobs_to_finish
from make_rawr_tiles import wc_line, head_lines
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
import time
import sys

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
    def missing_tiles_split(self, split_zoom, zoom_max, job_set):
        """
        To be used in a with-statement. Yields a MissingTiles object, giving
        information about the tiles which are missing.

        High zoom jobs are output between split_zoom (RAWR tile granularity) and zoom_max
        (usually lower, e.g: 6) depending on whether job_list
        contains a truthy value for the RAWR tile. The job_list jobs are are looked up
        at starting at zoom_max, then at increasing zooms until split_zoom.
        """

        self.run_batch_job()
        tmpdir = tempfile.mkdtemp()
        try:
            missing_meta_file = os.path.join(tmpdir, 'missing_meta.txt')
            missing_low_file = os.path.join(tmpdir, 'missing.low.txt')
            missing_high_file = os.path.join(tmpdir, 'missing.high.txt')

            self.read_metas_to_file(missing_meta_file, compress=True)

            print("[%s] Splitting into high and low zoom lists" % (time.ctime()))

            # contains zooms 0 until group zoom. the jobs between the group
            # zoom and RAWR zoom are merged into the parent at group zoom.
            missing_low = CoordSet(zoom_max)

            # contains job coords at either zoom_max or split_zoom only.
            # zoom_max is a misnomer here, as it's always less than or equal to
            # split zoom?
            missing_high = CoordSet(split_zoom, min_zoom=zoom_max)

            with gzip.open(missing_meta_file, 'r') as fh:
                for line in fh:
                    this_coord = deserialize_coord(line)
                    if this_coord.zoom < split_zoom:
                        # in order to not have too many jobs in the queue, we
                        # group the low zoom jobs to the zoom_max (usually 7)
                        if this_coord.zoom > zoom_max:
                            this_coord = this_coord.zoomTo(zoom_max).container()

                        missing_low[this_coord] = True

                    else:
                        # check the job set at every zoom starting from
                        # zoom max - if we don't find it at a lower zoom
                        # register it at split_zoom
                        job_registered = False
                        for this_zoom in range(zoom_max, split_zoom):
                            lower_zoom_job_coord = this_coord.zoomTo(this_zoom)
                            if job_set[lower_zoom_job_coord]:
                                print("REMOVEME: Registered %s at lower zoom coord %s" % (serialize_coord(this_coord), serialize_coord(lower_zoom_job_coord)))
                                missing_high[lower_zoom_job_coord] = True
                                job_registered = True
                                break

                        if not job_registered:
                            print("REMOVEME: Fell back to registering %s at split zoom: %s" % (serialize_coord(this_coord), serialize_coord(lower_zoom_job_coord)))
                            missing_high[this_coord] = True

            with open(missing_low_file, 'w') as fh:
                for coord in missing_low:
                    fh.write(serialize_coord(coord) + "\n")

            with open(missing_high_file, 'w') as fh:
                for coord in missing_high:
                    fh.write(serialize_coord(coord) + "\n")

            print("[%s] Done splitting into high and low zoom lists" % (time.ctime()))
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

        sizes = {}
        for dx in range(width):
            for dy in range(width):
                coord = Coordinate(
                    zoom=self.rawr_zoom,
                    column=((parent.column << dz) + dx),
                    row=((parent.row << dz) + dy))
                key = gen(self.prefix, coord, 'zip')
                response = s3.head_object(Bucket=self.bucket, Key=key)
                sizes[coord] = response['ContentLength']

        # now sum the sizes from rawr zoom to parent zoom
        for coord in sizes.keys():
            for zoom in range(self.rawr_zoom, parent.zoom, -1):
                parent_coord = coord.zoomTo(zoom-1).container()
                if parent_coord not in sizes:
                    sizes[parent_coord] = 0
                sizes[parent_coord] += sizes[coord]
        print("Completed parent %s" % serialize_coord(parent_coord))

        return parent, sizes


class TileSpecifier(object):
    ORDER_KEY = "order"
    MEM_GB_KEY = "mem_gb"

    """
    Provides the ability to sort tiles based on an ordering and specify memory reqs
    """

    def __init__(self, default_mem_gb=8, spec_dict={}):
        """
        :param default_mem_gb:
        :param spec_dict: keys are of form "<zoom>/<x>/<y>" value is a map with keys "mem_gb" and "order".  e.g.
                {"7/3/10": {"mem_gb": 2.5, "order": 12},
                "10/12/3": {"mem_gb": 0.3, "order": 10}}
        """
        self.default_mem_gb = default_mem_gb
        self.spec_dict = spec_dict

    @staticmethod
    def from_ordering_file(filename, default_mem_gb=8):
        """
        Expects a csv with at least these columns:
        zoom, x, y, mem_gb
        The lines in the file should be ordered in the desired queue ordering (e.g. first data line contains first tile to enqueue)
        :return:
        TileSpecifier with ordering and memory requirements expressed in <filename>
        """
        import csv

        spec_dict = {}
        with open(filename) as fh:
            order_idx = 0
            reader = csv.DictReader(fh)
            for row in reader:
                try:
                    # TODO: add checks for keys
                    coord_str = serialize_coord(Coordinate(int(row['y']), int(row['x']), int(row['zoom'])))
                    spec_dict[coord_str] = \
                        {TileSpecifier.MEM_GB_KEY: float(row[TileSpecifier.MEM_GB_KEY]), TileSpecifier.ORDER_KEY: order_idx}
                    order_idx += 1
                except:
                    print("Error for line parsed as %s in TileSpecifier.from_ordering_file - details %s" % (row, sys.exc_info()[0]))
                    continue

        return TileSpecifier(default_mem_gb, spec_dict)
    @staticmethod
    def from_coord_list(coord_list, default_mem_gb):
        spec_dict = {}
        for i in range(len(coord_list)):
            coord_str = serialize_coord(coord_list[i])
            spec_dict[coord_str] = {TileSpecifier.ORDER_KEY: i}

        return TileSpecifier(default_mem_gb, spec_dict)

    def reorder(self, coord_list):
        """
        Using the sort ordering for this specifier, reorders the tiles in coord_list.
        coords that are in the coord_list that aren't mentioned in the ordering will go first.
        :return:
        """
        return sorted(coord_list, key=lambda coord: self.get_ordering_val(coord))

    def get_ordering_val(self, coord):
        """
        coord is type string
        returns ordering location for coord.  The lower it is the earlier in the order.
        If there is no ordering specified for this coordinate, returns 0
        """
        if coord in self.spec_dict:
            return self.spec_dict[coord][self.ORDER_KEY]

        return 0

    def get_mem_reqs_mb(self, coord_str):
        """
        returns the specified memory requirement in megabytes for the coordinate.  If none are specified,
        returns default_gb
        """
        if coord_str in self.spec_dict:
            return self.spec_dict[coord_str][self.MEM_GB_KEY] * 1024
        else:
            return self.default_mem_gb * 1024


def _distribute_jobs_by_raw_tile_size(rawr_bucket, prefix, key_format_type, rawr_zoom, group_zoom,
                                      size_threshold, pool_size=30):
    """
    Look up the RAWR tiles in the rawr_bucket under the prefix and with the
    given key format, group the RAWR tiles (usually at zoom 10) by the job
    group zoom (usually 7) and sum their sizes. Return an ordered list of job coordinates
    by descending raw size sum.

    A pool size of 30 seems to work well; the point of the pool is to hide
    the latency of S3 requests, so pretty quickly hits the law of diminishing
    returns with extra concurrency (without the extra network cards that Batch
    would provide).
    """

    p = Pool(pool_size)
    job_sizer = _JobSizer(rawr_bucket, prefix, key_format_type, rawr_zoom)

    grouped_by_rawr_tile_size = []
    print("[%s] Bucketizing jobs by zoom using size of raw tiles" % time.ctime())
    # loop over each column individually. this limits the number of concurrent
    # tasks, which means we don't waste memory maintaining a huge queue of
    # pending tasks. and when something goes wrong, the stacktrace isn't buried
    # in a million others.
    num_coords = 1 << group_zoom
    all_sizes = {}
    grouping_queue = queue.Queue()
    for x in range(num_coords):
        # kick off tasks async. each one knows its own coordinate, so we only
        # need to track the handle to know when its finished.
        tasks = []
        for y in range(num_coords):
            coord = Coordinate(zoom=group_zoom, column=x, row=y)
            tasks.append(p.apply_async(job_sizer, (coord,)))
            grouping_queue.put_nowait(coord)  # queue for future size counting

        # collect tasks and put them into the big jobs list.
        for task in tasks:
            coord, sizes = task.get()
            all_sizes.update(sizes)
        print("REMOVEME: Finished the %s column" % x)

    # now use all_sizes plus the size_threshold to find the lowest zoom we can group the tiles in
    counts_at_zoom = {}
    while not grouping_queue.empty():
        this_coord = grouping_queue.get_nowait()
        this_size = all_sizes[this_coord]

        if this_size <= size_threshold or this_coord.zoom == rawr_zoom:
            grouped_by_rawr_tile_size.append(this_coord)
            grouping_queue.task_done()

            if not this_coord.zoom in counts_at_zoom:
                counts_at_zoom[this_coord.zoom] = 0
            counts_at_zoom += 1
        else:
            top_left_child = this_coord.zoomBy(1)

            grouping_queue.put_nowait(top_left_child)
            grouping_queue.put_nowait(top_left_child.down(1))
            grouping_queue.put_nowait(top_left_child.right(1))
            grouping_queue.put_nowait(top_left_child.down(1).right(1))

    print("[%s] Done bucketizing jobs - count by zoom %s" % (time.ctime(), counts_at_zoom))
    ordered_job_list = sorted(grouped_by_rawr_tile_size, key=lambda coord: all_sizes[coord], reverse=True)
    return ordered_job_list


def viable_container_overrides(mem_mb):
    """
    Turns a number into the next highest even multiple that AWS will accept, and the min number of CPUs you need for that amount
    :param mem_mb: (int) the megabytes of memory you'd request in an ideal world
    :return: the amount of mem you need to request for AWS batch to honor it, the amount of vcpus you must request
    """
    mem_mb = int(mem_mb)

    if mem_mb < 512:
        return 512, 1

    if mem_mb % 1024 == 0:
        return mem_mb, 1

    mem_gb_truncated = int(mem_mb / 1024)
    next_gb = mem_gb_truncated + 1
    desired_mem_mb = next_gb * 1024

    max_mem_per_vcpu = 8 * 1024
    vcpus = 1 + int((desired_mem_mb - 1)/max_mem_per_vcpu)

    return desired_mem_mb, vcpus


def enqueue_tiles(config_file, tile_list_file, check_metatile_exists, tile_specifier=TileSpecifier(), mem_multiplier=1.0, mem_max=32 * 1024):
    from tilequeue.command import make_config_from_argparse
    from tilequeue.command import tilequeue_batch_enqueue
    from make_rawr_tiles import BatchEnqueueArgs

    os.environ['TILEQUEUE__BATCH__CHECK-METATILE-EXISTS'] = (
        str(check_metatile_exists).lower())
    with open(config_file) as fh:
        cfg = make_config_from_argparse(fh)

    with open(tile_list_file, 'r') as tile_list:
        coord_lines = [line.strip() for line in tile_list.readlines()]

    reordered_lines = tile_specifier.reorder(coord_lines)

    print("[%s] Starting to enqueue %d tile batches" % (time.ctime(), len(reordered_lines)))
    for coord_line in reordered_lines:
        # override memory requirements for this job with what the tile_specifier tells us
        mem_mb = int(tile_specifier.get_mem_reqs_mb(coord_line))
        adjusted_mem = mem_mb * mem_multiplier

        # now that we know what we want, pick something AWS actually supports
        viable_mem_request, required_min_cpus = viable_container_overrides(adjusted_mem)

        update_container_overrides(cfg, viable_mem_request, mem_max, required_min_cpus)

        args = BatchEnqueueArgs(config_file, coord_line, None, None)
        tilequeue_batch_enqueue(cfg, args)
    print("[%s] Done enqueuing tile batches" % time.ctime())


def update_container_overrides(cfg, mem_mb, mem_max, cpus):
    cfg.yml["batch"]["memory"] = int(min(mem_mb, mem_max))
    cfg.yml["batch"]["vcpus"] = cpus


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

    def __init__(self, tile_finder, high_zoom_job_set, split_zoom, zoom_max, tile_specifier=TileSpecifier(), allowed_missing_tiles=0):
        self.tile_finder = tile_finder
        self.high_zoom_job_set = high_zoom_job_set
        self.split_zoom = split_zoom
        self.zoom_max = zoom_max
        self.allowed_missing_tiles = allowed_missing_tiles
        self.tile_specifier = tile_specifier

    def _missing(self):
        return self.tile_finder.missing_tiles_split(
            self.split_zoom, self.zoom_max, self.high_zoom_job_set)

    def render(self, num_retries, lense):
        mem_max = 32 * 1024  # 32 GiB

        for retry_number in range(0, num_retries):
            mem_multiplier = 1.5 ** retry_number
            with self._missing() as missing:
                missing_tile_file = lense.missing_file(missing)
                count = wc_line(missing_tile_file)

                if count <= self.allowed_missing_tiles:
                    sample = head_lines(missing_tile_file, 10)
                    print("All %s done with %d missing tiles, %d allowed. e.g. %s" %
                          (lense.description, count, self.allowed_missing_tiles,
                           ', '.join(sample)))
                    break

                check_metatile_exists = retry_number > 0

                # enqueue jobs for missing tiles
                if count > 0:
                    sample = head_lines(missing_tile_file, 10)
                    print("Enqueueing %d %s tiles (e.g. %s)" %
                          (count, lense.description, ', '.join(sample)))

                    enqueue_tiles(lense.config, missing_tile_file,
                                  check_metatile_exists, self.tile_specifier, mem_multiplier, mem_max)

        else:
            with self._missing() as missing:
                missing_tile_file = lense.missing_file(missing)
                count = wc_line(missing_tile_file)
                sample = head_lines(missing_tile_file, 10)
                raise RuntimeError(
                    "FAILED! %d %s still missing after %d tries (e.g. %s)"
                    % (count, lense.description, num_retries, ', '.join(sample)))


def create_tile_specifier(tile_specifier_file):
    tile_specifier = TileSpecifier()
    if tile_specifier_file is not None:
        try:
            tile_specifier = TileSpecifier.from_ordering_file(tile_specifier_file, default_mem_gb=8)
        except:
            print("Error creating TileSpecifier, will use the default.  Error details: {}".format(sys.exc_info()[0]))

    return tile_specifier


def make_coord_set(coords, max_zoom, min_zoom):
    coord_set = CoordSet(max_zoom, min_zoom)
    for coord in coords:
         coord_set[coord] = True
    return coord_set

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
    parser.add_argument('--size-threshold', default=80000000, type=int,
                        help='If all the RAWR tiles grouped together are '
                        'bigger than this, split the job up into individual '
                        'RAWR tiles.')
    parser.add_argument('--allowed-missing-tiles', default=2, type=int,
                        help='The maximum number of missing metatiles allowed '
                        'to continue the build process.')
    parser.add_argument('--tile-specifier-file',
                        help="optional csv containing lines in desired queue order with columns "
                             "for zoom, x, y, and mem_gb to specify ordering and memory requirements")

    args = parser.parse_args()
    assert_run_id_format(args.run_id)
    buckets = Buckets(args.rawr_bucket, args.meta_bucket,
                      args.missing_bucket or args.meta_bucket)
    date_prefix = args.date_prefix or args.run_id
    missing_bucket_date_prefix = args.run_id
    assert args.key_format_type in ('prefix-hash', 'hash-prefix')

    # TODO: split zoom and zoom max should come from config.
    split_zoom = 10
    zoom_max = 6

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

    jobs_list = _distribute_jobs_by_raw_tile_size(
        buckets.rawr, missing_bucket_date_prefix, args.key_format_type,
        split_zoom, zoom_max, args.size_threshold)

    sys.exit()

    tile_specifier = TileSpecifier.from_coord_list(jobs_list, 4)

    tile_renderer = TileRenderer(tile_finder, make_coord_set(jobs_list, split_zoom, zoom_max), split_zoom, zoom_max, tile_specifier, args.allowed_missing_tiles)

    tile_renderer.render(args.retries, LowZoomLense(args.low_zoom_config))

    # turn off databases by saying we want to ensure that zero are running.
    ensure_dbs(args.run_id, 0)

    tile_renderer.render(args.retries, HighZoomLense(args.high_zoom_config))
