package pack

import (
	"fmt"
	"tzops/go/pkg/coord"
)

// ToU32 will pack a coordinate into a u32. The maximum zoom handled is 14.
// Coordinates with a higher zoom will result in an error.
func ToU32(c coord.Coord) (uint32, error) {
	if c.Z > 14 {
		return 0, fmt.Errorf("cannot pack coordinate into u32, z=%d > 14", c.Z)
	}
	return uint32(c.Z<<28) | uint32(c.X<<14) | uint32(c.Y), nil
}

// FromU32 will take a u32 and return a coordinate from that representation.
// It's expected that the u32 was created from a call to ToU32.
func FromU32(val uint32) coord.Coord {
	return coord.Coord{
		Z: uint((val >> 28) & ((1 << 4) - 1)),
		X: uint((val >> 14) & ((1 << 14) - 1)),
		Y: uint(val & ((1 << 14) - 1)),
	}
}
