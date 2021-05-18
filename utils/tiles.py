from ModestMaps.Core import Coordinate
from mercantile import tiles
from mercantile import tile
#from typing import Iterator
#from typing import List
from utils.constants import MAX_TILE_ZOOM
from utils.constants import MIN_TILE_ZOOM


class TilesCoordinateGenerator(object):
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


class BoundingBoxTilesCoordinateGenerator(TilesCoordinateGenerator):
    """ Generate the tiles overlapped by a geographic bounding box within a
    range """

    def __init__(self, west, south, east, north, min_zoom=MIN_TILE_ZOOM,
                 max_zoom=MAX_TILE_ZOOM):
        # type: (float, float, float, float, int, int) -> None
        """
        :param west: longitude of the west boundary
        :param south: latitude of the south boundary
        :param east: longitude of the east boundary
        :param north: latitude of the north boundary
        :param min_zoom: the minimum zoom(inclusive) it can generate tiles for
        :param max_zoom: the maximum zoom(inclusive) it can generate tiles for
        """
        super(BoundingBoxTilesCoordinateGenerator, self).__init__(min_zoom=min_zoom, max_zoom=max_zoom)
        self.west = west
        self.south = south
        self.east = east
        self.north = north

    @staticmethod
    def convert_mercantile(m_tile):
        # type: (tile) -> Coordinate
        return Coordinate(column=m_tile.x, row=m_tile.y, zoom=m_tile.z)

    def generate_tiles_coordinates(self, zooms):
        # type: (List[int]) -> Iterator[Coordinate]
        """ Get the tiles overlapped by a geographic bounding box """
        for m_tile in tiles(self.west, self.south, self.east, self.north,
                            self.filter_zooms_in_range(zooms), True):
            yield BoundingBoxTilesCoordinateGenerator.\
                convert_mercantile(m_tile)
