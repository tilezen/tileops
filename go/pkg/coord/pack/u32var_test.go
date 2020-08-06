package pack

import (
	"github.com/tilezen/tileops.git/go/pkg/coord"
	"testing"
	"testing/quick"
)

func TestPackU32VarSymmetric(t *testing.T) {
	maxZoomToGen := uint(15)
	cfg := quick.Config{
		MaxCount: 1000,
		Values:   newValidCoordGenerator(maxZoomToGen),
	}
	f := func(c *coord.Coord) bool {
		packed, err := ToU32Var(*c)
		if err != nil {
			panic(err)
		}
		unpackedCoord, err := FromU32Var(packed)
		return err == nil && unpackedCoord == *c
	}
	if err := quick.Check(f, &cfg); err != nil {
		t.Error(err)
	}
}
