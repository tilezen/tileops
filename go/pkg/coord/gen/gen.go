package gen

import (
	"math"
	"tzops/go/pkg/coord"
)

// Generator provides an interface for yielding successive coordinates.
type Generator interface {
	Next() *coord.Coord
}

func dimRange(zoom uint) uint {
	return uint(math.Pow(2, float64(zoom)))
}

type zoomRangeState struct {
	next         coord.Coord
	end          uint
	curZoomRange uint
}

// NewZoomRange returns a Generator that yields all coordinates from begin zoom
// to end zoom. The end zoom is inclusive.
func NewZoomRange(zoomBegin uint, zoomEndInclusive uint) Generator {
	return &zoomRangeState{
		next:         coord.Coord{Z: zoomBegin, X: 0, Y: 0},
		end:          zoomEndInclusive,
		curZoomRange: dimRange(zoomBegin),
	}
}

func (g *zoomRangeState) Next() *coord.Coord {
	if g.next.Z > g.end {
		return nil
	}
	result := g.next
	nextCoord := &g.next
	nextCoord.X++
	if nextCoord.X == g.curZoomRange {
		nextCoord.X = 0
		nextCoord.Y++
		if nextCoord.Y == g.curZoomRange {
			nextCoord.Y = 0
			nextCoord.Z++
			g.curZoomRange = dimRange(nextCoord.Z)
		}
	}
	return &result
}

type sliceState struct {
	idx    uint
	coords []coord.Coord
}

// NewSlice returns a Generator that yields all coordinates in the slice.
func NewSlice(coords []coord.Coord) Generator {
	return &sliceState{0, coords}
}

func (g *sliceState) Next() *coord.Coord {
	if g.idx >= uint(len(g.coords)) {
		return nil
	}
	result := g.coords[g.idx]
	g.idx++
	return &result
}
