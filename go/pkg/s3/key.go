package s3

import (
	"crypto/md5"
	"errors"
	"fmt"
	"strings"
	tzc "tzops/go/pkg/coord"
)

// specific logic around s3, eg understanding our tile paths and
// prefixes for different types of buckets

// ParseCoordFromKey parses a coordinate from an s3 path.
func ParseCoordFromKey(key string) (*tzc.Coord, error) {
	// this can probably be made more general if needed, but right
	// now just making it work for our known cases

	// sanity check to see if it's even possible
	if len(key) < 4 {
		return nil, errors.New("Too few characters")
	}
	// assume that we have an extension that we're trimming off
	extIdx := strings.LastIndexByte(key, '.')
	if extIdx < 0 {
		return nil, errors.New("Missing extension")
	}

	var slashCount uint
	var idx int
	for idx = extIdx - 1; idx >= 0; idx-- {
		if key[idx] == '/' {
			slashCount++
			if slashCount == 3 {
				break
			}
		}
	}
	if slashCount == 3 || (slashCount == 2 && idx == -1) {
		coordStr := key[idx+1 : extIdx]
		return tzc.Decode(coordStr)
	}
	return nil, errors.New("Missing fields")
}

// HashString returns the first 5 characters of the md5 hash.
// This is what gets used as s3 path prefixes.
func HashString(s string) string {
	// returns the 5 char hash for a particular string
	md5Hash := md5.Sum([]byte(s))
	if len(md5Hash) < 3 {
		panic(errors.New("Invalid md5 hash"))
	}
	hex := fmt.Sprintf("%x", md5Hash)
	hash := hex[:5]
	return hash
}

// MetaTileHashPathForCoord returns the hashed s3 path for metatiles.
func MetaTileHashPathForCoord(datePrefix string, coord tzc.Coord) string {
	pathToHash := fmt.Sprintf("%d/%d/%d.zip", coord.Z, coord.X, coord.Y)
	hash := HashString(pathToHash)
	result := fmt.Sprintf("%s/%s/%s", hash, datePrefix, pathToHash)
	return result
}

// RawrTileHashPathForCoord returns the hashed s3 path for rawr tiles.
func RawrTileHashPathForCoord(datePrefix string, coord tzc.Coord) string {
	pathToHash := fmt.Sprintf("%d/%d/%d.zip", coord.Z, coord.X, coord.Y)
	hash := HashString(pathToHash)
	result := fmt.Sprintf("%s/%s/%s", hash, datePrefix, pathToHash)
	return result
}
