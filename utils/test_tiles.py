from utils.tiles import BoundingBoxTilesCoordinateGenerator
from ModestMaps.Core import Coordinate


def test_tiles_within_bbox():
    generator = BoundingBoxTilesCoordinateGenerator(-4.1494623,
                                                    38.350205,
                                                    3.321241,
                                                    47.790958)
    res = generator.generate_tiles_coordinates([5])
    expected = [Coordinate(11, 15, 5),
                Coordinate(12, 15, 5),
                Coordinate(11, 16, 5),
                Coordinate(12, 16, 5)]
    assert expected == [c for c in res]
