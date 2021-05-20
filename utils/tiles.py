from ModestMaps.Core import Coordinate
from mercantile import tiles
from mercantile import tile
from utils.constants import MAX_TILE_ZOOM
from utils.constants import MIN_TILE_ZOOM


class TileCoordinatesGenerator:
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
        super().__init__(min_zoom=min_zoom, max_zoom=max_zoom)
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
