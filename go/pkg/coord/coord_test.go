package coord

import (
	"sort"
	"testing"
)

func TestZoomTo(t *testing.T) {
	z4 := Coord{4, 8, 7}

	z2 := z4.ZoomTo(2)
	exp := Coord{2, 2, 1}
	if z2 != exp {
		t.Fail()
	}

	z5 := z2.ZoomTo(5)
	exp = Coord{5, 16, 8}
	if z5 != exp {
		t.Fail()
	}
}

func TestLessZYX(t *testing.T) {
	a := Coord{1, 2, 3}
	b := Coord{1, 3, 2}
	if a.LessZYX(b) {
		t.Fail()
	}
}

func TestSort(t *testing.T) {
	coords := []Coord{
		Coord{3, 2, 1},
		Coord{2, 1, 2},
		Coord{2, 2, 1},
		Coord{1, 2, 3},
	}
	sort.Sort(ByZYX(coords))
	for i := 0; i < len(coords)-1; i++ {
		a := coords[i]
		b := coords[i+1]
		if !a.LessZYX(b) {
			t.Fail()
		}
	}
}

func TestDecode(t *testing.T) {
	s := "3/2/1"
	c, err := Decode(s)
	exp := Coord{3, 2, 1}
	if c == nil || err != nil || *c != exp {
		t.Fail()
	}

	bad := "3/2/foo"
	c, err = Decode(bad)
	if c != nil || err == nil {
		t.Fail()
	}
}
