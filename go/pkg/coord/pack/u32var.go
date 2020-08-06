package pack

import (
	"fmt"
	"github.com/tilezen/tileops.git/go/pkg/coord"
	"math/bits"
)

// ToU32Var will pack the coordinate into a u32. The max coordinate zoom that
// can be handled is 15. An error is returned for coordinates with higher
// zooms.
//
// This function should be used when the zoom range is know to max
// out at 15. For zooms < 15, you might want to consider the more
// straightforward PackU32 function instead.
func ToU32Var(c coord.Coord) (uint32, error) {
	if c.Z > 15 {
		return 0, fmt.Errorf("cannot pack coordinate into u32, z=%d > 15", c.Z)
	}
	return uint32(1<<(2*c.Z)) | uint32(c.X<<c.Z) | uint32(c.Y), nil
}

// FromU32Var unpacks the u32 back into a coordinate. It's expected that the
// coordinate was originally packed with the ToU32Var function.
func FromU32Var(val uint32) (coord.Coord, error) {
	zeros := bits.LeadingZeros32(val)
	if zeros&1 == 0 {
		return coord.Coord{}, fmt.Errorf("tile value %d has %d leading zeros, which isn't valid", val, zeros)
	}
	z := uint((31 - zeros) >> 1)
	x := uint((val >> z) & ((1 << z) - 1))
	y := uint(val & ((1 << z) - 1))
	return coord.Coord{Z: z, X: x, Y: y}, nil
}
