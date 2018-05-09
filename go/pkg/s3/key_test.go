package s3

import (
	"testing"
	"tzops/go/pkg/coord"
)

func TestParseCoordFromKey(t *testing.T) {
	// too few characters
	c, err := ParseCoordFromKey("123")
	if c != nil || err == nil {
		t.Fail()
	}

	// missing extension
	c, err = ParseCoordFromKey("abcdefg")
	if c != nil || err == nil {
		t.Fail()
	}

	// missing fields
	c, err = ParseCoordFromKey("a/b.zip")
	if c != nil || err == nil {
		t.Fail()
	}

	// invalid coords
	c, err = ParseCoordFromKey("a/b/c.zip")
	if c != nil || err == nil {
		t.Fail()
	}

	// valid coord
	c, err = ParseCoordFromKey("1/2/3.zip")
	if c == nil || err != nil {
		t.FailNow()
	}
	exp := coord.Coord{Z: 1, X: 2, Y: 3}
	if *c != exp {
		t.Fail()
	}

	// valid coord with prefix
	c, err = ParseCoordFromKey("foo/bar/baz/quux/1/2/3.zip")
	if c == nil || err != nil {
		t.FailNow()
	}
	exp = coord.Coord{Z: 1, X: 2, Y: 3}
	if *c != exp {
		t.Fail()
	}
}

func TestMd5Hash(t *testing.T) {
	hashed := HashString("10/941/1011.zip")
	if hashed != "00018" {
		t.Fail()
	}
}
