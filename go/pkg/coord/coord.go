package coord

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Coord contains the Z, X, Y coordinate for a particular tile.
type Coord struct {
	Z, X, Y uint
}

// ZoomTo returns a new coordinate with the new zoom.
func (c Coord) ZoomTo(z uint) Coord {
	var result Coord
	if c.Z == z {
		result = c
	} else if c.Z < z {
		delta := z - c.Z
		result = Coord{z, c.X << delta, c.Y << delta}
	} else {
		delta := c.Z - z
		result = Coord{z, c.X >> delta, c.Y >> delta}
	}
	return result
}

// String is the Coord Stringer implementation.
// It returns the coordinate in z/x/y.
func (c Coord) String() string {
	return fmt.Sprintf("%d/%d/%d", c.Z, c.X, c.Y)
}

// LessZYX returns true if the coordinate is "less than" the argument.
// First z is considered, then y, and finally x.
func (c Coord) LessZYX(o Coord) bool {
	if c.Z < o.Z {
		return true
	} else if c.Z == o.Z {
		if c.Y < o.Y {
			return true
		} else if c.Y == o.Y {
			if c.X < o.X {
				return true
			}
		}
	}
	return false
}

// ByZYX is a wrapper type used for sorting.
type ByZYX []Coord

func (a ByZYX) Len() int      { return len(a) }
func (a ByZYX) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByZYX) Less(i, j int) bool {
	x := a[i]
	y := a[j]
	return x.LessZYX(y)
}

// Decode parses a coordinate from a string.
// It expects the string to be in the form z/x/y.
func Decode(coordSpec string) (*Coord, error) {
	fields := strings.Split(coordSpec, "/")
	if len(fields) != 3 {
		return nil, errors.New("Invalid number of fields")
	}
	z, err := strconv.ParseUint(fields[0], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("Invalid z: %#v %s", fields[0], err)
	}
	x, err := strconv.ParseUint(fields[1], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("Invalid x: %#v %s", fields[1], err)
	}
	y, err := strconv.ParseUint(fields[2], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("Invalid y: %#v %s", fields[2], err)
	}
	return &Coord{uint(z), uint(x), uint(y)}, nil
}
