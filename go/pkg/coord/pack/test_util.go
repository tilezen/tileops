package pack

import (
	"fmt"
	"math/rand"
	"reflect"

	"github.com/tilezen/tileops.git/go/pkg/coord"
)

// This contains test utility functions for testing in the pack package

func newValidCoordGenerator(maxZoomInclusive uint) func([]reflect.Value, *rand.Rand) {
	return func(values []reflect.Value, rand *rand.Rand) {
		if len(values) != 1 {
			panic(fmt.Errorf("unexpected number of values to gen: %d", len(values)))
		}
		zoom := uint(rand.Intn(int(maxZoomInclusive) + 1))
		dim := 1 << zoom
		c := coord.Coord{
			Z: zoom,
			X: uint(rand.Intn(dim)),
			Y: uint(rand.Intn(dim)),
		}
		values[0] = reflect.ValueOf(&c)
	}
}
