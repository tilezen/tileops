package pack

import (
	"testing"
	"testing/quick"
	"tzops/go/pkg/coord"
)

func TestPackU64Symmetric(t *testing.T) {
	maxZoomToGen := uint(29)
	cfg := quick.Config{
		MaxCount: 1000,
		Values:   newValidCoordGenerator(maxZoomToGen),
	}
	f := func(c *coord.Coord) bool {
		packed, err := ToU64(*c)
		if err != nil {
			panic(err)
		}
		unpackedCoord := FromU64(packed)
		return unpackedCoord == *c
	}
	if err := quick.Check(f, &cfg); err != nil {
		t.Error(err)
	}
}
