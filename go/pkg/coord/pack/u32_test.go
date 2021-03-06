package pack

import (
	"github.com/tilezen/tileops.git/go/pkg/coord"
	"testing"
	"testing/quick"
)

func TestPackU32Symmetric(t *testing.T) {
	maxZoomToGen := uint(14)
	cfg := quick.Config{
		MaxCount: 1000,
		Values:   newValidCoordGenerator(maxZoomToGen),
	}
	f := func(c *coord.Coord) bool {
		packed, err := ToU32(*c)
		if err != nil {
			panic(err)
		}
		unpackedCoord := FromU32(packed)
		return unpackedCoord == *c
	}
	if err := quick.Check(f, &cfg); err != nil {
		t.Error(err)
	}
}
