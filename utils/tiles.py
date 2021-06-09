from ModestMaps.Core import Coordinate
from mercantile import tiles
from mercantile import tile
import boto3
from utils.constants import MAX_TILE_ZOOM
from utils.constants import MIN_TILE_ZOOM
from tilequeue.store import make_s3_tile_key_generator
from tilequeue.command import find_job_coords_for
from tilequeue.tile import coord_children_range


class TileCoordinatesGenerator(object):
    """
    Generate tiles EPSG:3857 coordinates from a list of zooms within a range
    """
    def __init__(self, min_zoom, max_zoom):
        # type: (int, int) -> None
        """
        :param min_zoom: the minimum zoom(inclusive) it can generate tiles for
        :param max_zoom: the maximum zoom(inclusive) it can generate tiles for
        """
        assert min_zoom <= max_zoom
        self.min_zoom = min_zoom
        self.max_zoom = max_zoom

    def filter_zooms_in_range(self, zooms):
        # type: (List[int]) -> List[int]
        """ given a list of zooms return zooms within min_zoom and max_zoom"""
        if bool(zooms):
            ranged_zoom = [z for z in zooms if
                           self.min_zoom <= z <= self.max_zoom]
        else:
            ranged_zoom = [z for z in range(self.min_zoom, self.max_zoom+1)]
        return ranged_zoom

    def generate_tiles_coordinates(self, zooms):
        # type: (List[int]) -> Iterator[Coordinate]
        """ Generate all tiles coordinates in the specific zoom or
        all tiles coordinates if zoom is empty"""

        for zoom in self.filter_zooms_in_range(zooms):
            max_coord = 2 ** zoom
            for x in range(max_coord):
                for y in range(max_coord):
                    yield Coordinate(zoom=zoom, column=x, row=y)


class BoundingBoxTileCoordinatesGenerator(TileCoordinatesGenerator):
    """ Generate the tiles overlapped by a geographic bounding box within a
    range """

    def __init__(self, min_x, min_y, max_x, max_y, min_zoom=MIN_TILE_ZOOM,
                 max_zoom=MAX_TILE_ZOOM):
        # type: (float, float, float, float, int, int) -> None
        """
        :param min_x: longitude of the west boundary
        :param min_y: latitude of the south boundary
        :param max_x: longitude of the east boundary
        :param max_y: latitude of the north boundary
        :param min_zoom: the minimum zoom(inclusive) it can generate tiles for
        :param max_zoom: the maximum zoom(inclusive) it can generate tiles for
        """
        super(BoundingBoxTileCoordinatesGenerator, self).__init__(min_zoom=min_zoom, max_zoom=max_zoom)
        self.min_x = min_x
        self.min_y = min_y
        self.max_x = max_x
        self.max_y = max_y

    @staticmethod
    def convert_mercantile(m_tile):
        # type: (tile) -> Coordinate
        return Coordinate(column=m_tile.x, row=m_tile.y, zoom=m_tile.z)

    def generate_tiles_coordinates(self, zooms):
        # type: (List[int]) -> Iterator[Coordinate]
        """ Get the tiles overlapped by a geographic bounding box """
        for m_tile in tiles(self.min_x, self.min_y, self.max_x, self.max_y,
                            self.filter_zooms_in_range(zooms), True):
            yield BoundingBoxTileCoordinatesGenerator.\
                convert_mercantile(m_tile)


class S3TileVerifier(object):
    """
    Used the logic of tilequeue to expand tile coordinates to the rawr and meta
    tiles S3 locations that tilequeue would write to.

    It is useful for arbitrary tile build verification because we can use the
    methods provided here to pre-generate the s3 objects' paths that we need
    to verify later.
    """

    def __init__(self,
                 date_prefix,
                 key_format_type_str,
                 rawr_s3_bucket,
                 meta_s3_bucket,
                 rawr_tile_filename,
                 high_zoom_tile_filename,
                 low_zoom_tile_filename):
        # type: (str, str, str, str, str, str, str) -> None
        """
        :param date_prefix: the build prefix string
        :param key_format_type_str: the style of S3 key prefix
        :param rawr_s3_bucket: the s3 bucket for storing rawr tiles
        :param meta_s3_bucket: the s3 bucket for storing meta tiles
        :param rawr_tile_filename: the local file name to store rawr tiles coords and its last updated time on S3
        :param high_zoom_tile_filename: the local file name to store high_zoom meta tiles coords and its last updated time on S3
        :param low_zoom_tile_filename: the local file name to store low_zoom meta tiles coords and its last updated time on S3
        """
        self.date_prefix = date_prefix
        self.key_format_type_str = key_format_type_str
        self.rawr_tile_filename = rawr_tile_filename
        self.high_zoom_tile_filename = high_zoom_tile_filename
        self.low_zoom_tile_filename = low_zoom_tile_filename
        self.s3 = boto3.client('s3')
        self.rawr_s3_bucket = rawr_s3_bucket
        self.meta_s3_bucket = meta_s3_bucket

    # https://github.com/tilezen/tilequeue/blob/9644d916a864bd7d97448c40823158016e5e6dd2/tilequeue/command.py#L1861
    def generate_tile_coords_rebuild_paths_rawr(self, job_coords, group_by_zoom):
        """
        Generate the s3 paths of the rawr tiles that need to be rebuilt.
        """
        job_coords_int = [Coordinate(int(jc.row), int(jc.column), int(jc.zoom))
                          for jc in job_coords]
        extension = 'zip'  # TODO: for now this should match the one defined
        s3_key_gen = make_s3_tile_key_generator({
            'key-format-type': self.key_format_type_str,
        })
        all_coords_paths = []  # type: List[str]
        for coord in job_coords_int:
            # TODO: write to a file to not use to much memory
            coords_paths = [s3_key_gen(self.date_prefix, c, extension) for c in
                            find_job_coords_for(coord, group_by_zoom)]
            all_coords_paths.extend(coords_paths)

        with open(self.rawr_tile_filename, 'a') as fh:
            for path in all_coords_paths:
                response = self.s3.head_object(
                    Bucket=self.rawr_s3_bucket,
                    Key=path
                )
                datetime_value = response["LastModified"]
                fh.write(path + "," + datetime_value.strftime("%Y-%m-%dT%H:%M:%S%z") + "\n")

        return all_coords_paths

    # The tile expansion logic references
    # https://github.com/tilezen/tilequeue/blob/9644d916a864bd7d97448c40823158016e5e6dd2/tilequeue/command.py#L2074
    def generate_tile_coords_rebuild_paths_high_zoom(self, job_coords, queue_zoom, group_by_zoom):
        """
            Generate the s3 paths of the high_zoom meta tiles that need to be
            rebuilt.
        """
        job_coords_int = [Coordinate(int(jc.row), int(jc.column), int(jc.zoom))
                          for jc in job_coords]
        zoom_stop = 13
        assert zoom_stop > group_by_zoom, 'zoom_stop is not greater than ' \
                                          'group_by_zoom'

        s3_key_gen = make_s3_tile_key_generator({
            'key-format-type': self.key_format_type_str,
        })

        extension = 'zip'
        all_coords = []
        for job_coord_int in job_coords_int:
            assert queue_zoom <= job_coord_int.zoom <= group_by_zoom, \
                'Unexpected zoom: %d, zoom should be between %d and %d' % (
                job_coord_int.zoom, queue_zoom, group_by_zoom)
            more_job_coords = find_job_coords_for(job_coord_int, group_by_zoom)
            for mjc in more_job_coords:
                pyramid_coords = [mjc]
                pyramid_coords.extend(coord_children_range(mjc, zoom_stop))
                all_coords.extend(pyramid_coords)

        all_coords_paths = [s3_key_gen(self.date_prefix, c, extension) for c in
                            all_coords]

        with open(self.high_zoom_tile_filename, 'a') as fh:
            for path in all_coords_paths:
                response = self.s3.head_object(
                    Bucket=self.meta_s3_bucket,
                    Key=path
                )
                datetime_value = response["LastModified"]
                fh.write(path + "," + datetime_value.strftime("%Y-%m-%dT%H:%M:%S%z") + "\n")

        return all_coords_paths

    # The tile expansion logic references
    # https://github.com/tilezen/tilequeue/blob/9644d916a864bd7d97448c40823158016e5e6dd2/tilequeue/command.py#L2199
    def generate_tile_coords_rebuild_paths_low_zoom(self,
                                                    job_coords,
                                                    queue_zoom,
                                                    group_by_zoom):
        """
            Generate the s3 paths of the low_zoom meta tiles that need to be
            rebuilt.
        """
        assert queue_zoom < group_by_zoom, 'queue_zoom is not less than ' \
                                           'group_by_zoom'
        job_coords_int = [Coordinate(int(jc.row), int(jc.column), int(jc.zoom))
                          for jc in job_coords]

        s3_key_gen = make_s3_tile_key_generator({
            'key-format-type': self.key_format_type_str,
        })

        extension = 'zip'
        coords = []

        for job_coord_int in job_coords_int:
            assert 0 <= job_coord_int.zoom <= queue_zoom
            if job_coord_int.zoom == queue_zoom and job_coord_int.zoom < \
                    group_by_zoom - 1:
                coords.extend(
                    coord_children_range(job_coord_int, group_by_zoom - 1))

        all_coords_paths = [s3_key_gen(self.date_prefix, c, extension) for c in
                            coords]
        with open(self.low_zoom_tile_filename, 'a') as fh:
            for path in all_coords_paths:
                response = self.s3.head_object(
                    Bucket=self.meta_s3_bucket,
                    Key=path
                )
                datetime_value = response["LastModified"]
                fh.write(path + "," + datetime_value.strftime("%Y-%m-%dT%H:%M:%S%z") + "\n")
        return all_coords_paths

    def verify_tiles_rebuild_time(self, tile_type):
        """
        Read the csv file to get the s3 path the object that it needs to
        verify and then call s3 to get the object updated time to compare with
        the the old updated time which is in the csv file.
        It checks whether the newly fetched time are all greater than the ones
        in the csv file.
        """
        all_verified = True
        if tile_type == 'rawr':
            file_name = self.rawr_tile_filename
            bucket = self.rawr_s3_bucket
        elif tile_type == 'meta_high_zoom':
            file_name = self.high_zoom_tile_filename
            bucket = self.meta_s3_bucket
        elif tile_type == 'meta_low_zoom':
            file_name = self.low_zoom_tile_filename
            bucket = self.meta_s3_bucket
        else:
            raise Exception(
                'tile_type:{tt} is not supported.'.format(tt=tile_type))

        print('verifying {tile_type} tiles rebulid time...'.format(
            tile_type=tile_type))
        with open(file_name, 'r') as fh:
            for line in fh:
                elements = line.rstrip().split(',')
                assert len(elements) == 2
                s3_key_path = elements[0]
                modified_time_before_rebuild = elements[1]
                try:
                    response = self.s3.head_object(
                        Bucket=bucket,
                        Key=s3_key_path
                    )
                    datetime_value = response['LastModified']
                    datetime_str = datetime_value.strftime(
                        '%Y-%m-%dT%H:%M:%S%z')
                    assert datetime_str > modified_time_before_rebuild, \
                        '{tile_type} {key} new time:{new_time} is not newer ' \
                        'than old time:{old_time}'.format(
                        tile_type=tile_type, key=s3_key_path,
                        new_time=datetime_str,
                        old_time=modified_time_before_rebuild)
                except Exception as e:
                    print(
                        'exceptions encountered during verifying {tile_type} '
                        'tile object:{path} exception:{e}'.format(
                            tile_type=tile_type, path=s3_key_path, e=e))
                    all_verified = False
        if all_verified:
            print('all {tile_type} tiles successfully verified'.format(
                tile_type=tile_type))
        else:
            print('{tile_type} tiles failed to be verified.'.format(tile_type=tile_type))
