from utils.tiles import BoundingBoxTilesCoordinateGenerator
from ModestMaps.Core import Coordinate


def test_tiles_within_bbox():
    generator1 = BoundingBoxTilesCoordinateGenerator(-4.1494623,
                                                    38.350205,
                                                    3.321241,
                                                    47.790958)
    res = generator1.generate_tiles_coordinates([5])
    expected = [Coordinate(11, 15, 5),
                Coordinate(12, 15, 5),
                Coordinate(11, 16, 5),
                Coordinate(12, 16, 5)]
    assert expected == [c for c in res]

    generator2 = BoundingBoxTilesCoordinateGenerator(-122.185508,
                                                     47.587435,
                                                     -122.168342,
                                                     47.602600)
    res = generator2.generate_tiles_coordinates([10])
    expected = [Coordinate(357, 164, 10)]
    assert expected == [c for c in res]

    generator2 = BoundingBoxTilesCoordinateGenerator(-122.188295,
                                                     47.556570,
                                                     -122.187670,
                                                     47.556808)
    res = generator2.generate_tiles_coordinates([15])
    expected = [Coordinate(11450, 5262, 15)]
    assert expected == [c for c in res]
