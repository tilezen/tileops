from ModestMaps.Core import Coordinate
from itertools import chain


class BitSet(object):
    """
    A single-zoom set of Coordinates or, equivalently, a mapping from
    Coordinates to booleans.

    This is done in a fairly memory-efficient manner by using a single bit per
    Coordinate and storing those in a bytearray. This means that total memory
    usage should be roughly equal to (4 ** zoom) / 8 bytes, plus some noise for
    pointers.

    Bits can be set or cleared using a getitem/setitem interface, e.g:

    >>> b = BitSet(0)
    >>> b[Coordinate(zoom=0, column=0, row=0)] = True
    >>> b[Coordinate(zoom=0, column=0, row=0)]
    True

    The Coordinates which are set to True can be iterated using the usual
    idiom:

    >>> b = BitSet(0)
    >>> b[Coordinate(zoom=0, column=0, row=0)] = True
    >>> list(b) == [Coordinate(zoom=0, column=0, row=0)]
    True

    """

    def __init__(self, zoom):
        self.zoom = zoom
        num_tiles = 1 << (2 * zoom)

        # one bit per tile takes up ceil(tiles / 8) bytes, and we might waste
        # up to seven trailing bits.
        num_bytes = num_tiles // 8
        if num_tiles % 8 > 0:
            num_bytes += 1

        self.buf = bytearray(num_bytes)

    def _bit_index(self, coord):
        assert coord.zoom == self.zoom
        bit_index = (int(coord.column) << self.zoom) + int(coord.row)
        return (bit_index // 8, bit_index % 8)

    def __getitem__(self, coord):
        byte_index, bit_index = self._bit_index(coord)
        byte = self.buf[byte_index]
        return byte & (1 << bit_index) > 0

    def __setitem__(self, coord, value):
        byte_index, bit_index = self._bit_index(coord)
        byte = self.buf[byte_index]
        if value:
            byte |= 1 << bit_index
        else:
            byte &= ~(1 << bit_index)
        self.buf[byte_index] = byte

    def __iter__(self):
        return BitSetIterator(self)


class BitSetIterator(object):
    """
    Helper class to iterate over the True bits in a BitSet, yielding their
    Coordinates.
    """

    def __init__(self, container):
        self.container = container
        self.index = 0
        self.max_index = 1 << (2 * self.container.zoom)

    def __iter__(self):
        return self

    def next(self):
        while True:
            if self.index >= self.max_index:
                raise StopIteration()

            index = self.index
            self.index += 1

            byte = self.container.buf[index // 8]
            if byte & (1 << (index % 8)) > 0:
                coord = Coordinate(
                    zoom=self.container.zoom,
                    column=(index >> self.container.zoom),
                    row=(index & ((1 << self.container.zoom) - 1)))
                return coord


class CoordSet(object):
    """
    A set of Coordinates, implemented as a mapping of Coordinate to True/False.

    Can contain Coordinates over range of zooms, and modify Coordinates with a
    zoom greater than the maximum to be in range.
    """

    def __init__(self, max_zoom, min_zoom=0, drop_over_bounds=False):
        """
        Construct a set of Coordinates covering max_zoom through min_zoom,
        inclusive.

        Coordinates under the min_zoom will be dropped. Coordinates over the
        max zoom will be moved into range if drop_over_bounds is False, or
        dropped if it is True.
        """
        self.min_zoom = min_zoom
        self.max_zoom = max_zoom
        self.drop_over_bounds = drop_over_bounds

        self.zooms = {}
        for z in range(min_zoom, max_zoom + 1):
            self.zooms[z] = BitSet(z)

    def _zoom_for(self, coord):
        if coord.zoom > self.max_zoom and not self.drop_over_bounds:
            coord = coord.zoomTo(self.max_zoom).container()

        return self.zooms.get(coord.zoom), coord

    def __getitem__(self, coord):
        bits, coord = self._zoom_for(coord)
        if bits is not None:
            return bits[coord]
        else:
            raise KeyError(coord)

    def __setitem__(self, coord, value):
        bits, coord = self._zoom_for(coord)
        if bits is not None:
            bits[coord] = value

    def __iter__(self):
        iters = [z.__iter__() for z in self.zooms.values()]
        return chain(*iters)


if __name__ == '__main__':
    b = BitSet(0)
    assert list(b) == []
    zoom0_coord = Coordinate(zoom=0, column=0, row=0)
    b[zoom0_coord] = True
    assert list(b) == [zoom0_coord]

    b = BitSet(10)
    assert list(b) == []
    zoom10_coord = Coordinate(zoom=10, column=123, row=456)
    b[zoom10_coord] = True
    assert list(b) == [zoom10_coord]

    c = CoordSet(10)
    assert list(c) == []
    c[zoom0_coord] = True
    c[zoom10_coord] = True
    assert set(c) == set([zoom0_coord, zoom10_coord])

    c = CoordSet(10, min_zoom=7)
    c[Coordinate(zoom=11, column=3, row=1)] = True
    assert list(c) == [Coordinate(zoom=10, column=1, row=0)]

    c = CoordSet(10, min_zoom=7, drop_over_bounds=True)
    c[Coordinate(zoom=11, column=3, row=1)] = True
    assert list(c) == []
