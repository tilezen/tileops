package pack

import (
	"fmt"
	"tzops/go/pkg/coord"
)

// ToU64 will pack a coordinate into a u64. The maximum zoom handled is 29.
// Coordinates with a higher zoom will result in an error.
func ToU64(c coord.Coord) (uint64, error) {
	if c.Z > 29 {
		return 0, fmt.Errorf("cannot pack coordinate into u64, z=%d > 14", c.Z)
	}
	return (uint64(c.Z) << 58) | (uint64(c.X) << 29) | uint64(c.Y), nil
}

// FromU64 will take a u64 and return a coordinate from that representation.
// It's expected that the u64 was created from a call to ToU64.
func FromU64(val uint64) coord.Coord {
	return coord.Coord{
		Z: uint((val >> 58) & ((1 << 5) - 1)),
		X: uint((val >> 29) & ((1 << 29) - 1)),
		Y: uint(val & ((1 << 29) - 1)),
	}
}
