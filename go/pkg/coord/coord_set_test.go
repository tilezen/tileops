package coord

import (
	"testing"
)

func TestEmptyGet(t *testing.T) {
	// the coord set should be empty when first initialised
	cs := NewCoordSet()
	c := Coord{16, 32768, 2244}
	if cs.Get(c) != false {
		t.Errorf("Expecting cs.Get(%#v) to be false, but got %#v", c, cs.Get(c))
	}
}

func TestSetGet(t *testing.T) {
	// we should be able to read what we wrote
	cs := NewCoordSet()
	c := Coord{16, 32768, 2244}

	cs.Set(c, true)
	if cs.Get(c) != true {
		t.Errorf("Expecting cs.Get(%#v) to be true, but got %#v", c, cs.Get(c))
	}
}

func TestResetGet(t *testing.T) {
	// we should be able to read what we wrote
	cs := NewCoordSet()
	c := Coord{16, 32768, 2244}

	cs.Set(c, true)
	cs.Set(c, false)

	if cs.Get(c) != false {
		t.Errorf("Expecting cs.Get(%#v) to be false, but got %#v", c, cs.Get(c))
	}
}

func TestMaxCoords(t *testing.T) {
	cs := NewCoordSet()
	for z := uint(0); z <= 16; z++ {
		max_coord := (uint(1) << z) - 1
		min := Coord{z, 0, 0}
		max := Coord{z, max_coord, max_coord}

		for _, coord := range []Coord{min, max} {
			if cs.Get(coord) != false {
				t.Errorf("Expecting cs.Get(%#v) to be false, but got %#v", coord, cs.Get(coord))
			}
			cs.Set(coord, true)
			if cs.Get(coord) != true {
				t.Errorf("Expecting cs.Get(%#v) to be true, but got %#v", coord, cs.Get(coord))
			}
			// reset to its old value (because min=max at zoom 0!)
			cs.Set(coord, false)
		}
	}
}
