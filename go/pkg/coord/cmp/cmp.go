package cmp

import (
	"tzops/go/pkg/coord"
	"tzops/go/pkg/coord/gen"
)

// FindMissingTiles compares two coordinate generators to find the missing tiles.
// It assumes that the first generator is the exhaustive list of what's
// expected, and reports the coordinates that are missing from the second
// generator. These generators must yield tiles in sorted order.
func FindMissingTiles(exp gen.Generator, act gen.Generator) []coord.Coord {
	var result []coord.Coord
	expC := exp.Next()
	actC := act.Next()
	for {
		if expC == nil {
			break
		}
		if actC == nil {
			if expC != nil {
				result = append(result, *expC)
				expC = exp.Next()
			}
		} else if expC.LessZYX(*actC) {
			result = append(result, *expC)
			expC = exp.Next()
		} else if !actC.LessZYX(*expC) {
			expC = exp.Next()
			actC = act.Next()
		}
	}
	return result
}
