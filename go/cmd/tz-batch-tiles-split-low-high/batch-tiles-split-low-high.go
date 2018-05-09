package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"tzops/go/pkg/cmd"
	"tzops/go/pkg/coord"
)

func mustOpenForWriting(path string) io.WriteCloser {
	result, err := os.Create(path)
	if err != nil {
		panic(fmt.Errorf("Error opening %#v for writing: %s", path, err))
	}
	return result
}

func mustClose(wr io.Closer) {
	err := wr.Close()
	if err != nil {
		panic(err)
	}
}

func writeTiles(wg *sync.WaitGroup, wr io.Writer, tiles map[coord.Coord]interface{}) {
	defer wg.Done()
	for c := range tiles {
		fmt.Fprintf(wr, "%s\n", c)
	}
}

func main() {
	var tilesPath, lowZoomPath, highZoomPath string
	var splitZoom, zoomMax uint
	flag.StringVar(&tilesPath, "tiles-file", "", "path to tiles file to read")
	flag.StringVar(&lowZoomPath, "low-zoom-file", "", "path to low zoom file to write")
	flag.StringVar(&highZoomPath, "high-zoom-file", "", "path to high zoom file to write")
	flag.UintVar(&splitZoom, "split-zoom", 10, "zoom level that a tile is considered to be high")
	flag.UintVar(&zoomMax, "zoom-max", 7, "tiles are clamped to this zoom")

	flag.Parse()

	if tilesPath == "" || lowZoomPath == "" || highZoomPath == "" {
		cmd.DieWithUsage()
	}

	tf, err := os.Open(tilesPath)
	if err != nil {
		panic(fmt.Errorf("Error opening %#v for reading: %s", tilesPath, err))
	}
	defer mustClose(tf)

	lzf := mustOpenForWriting(lowZoomPath)
	defer mustClose(lzf)
	hzf := mustOpenForWriting(highZoomPath)
	defer mustClose(hzf)

	// because we are zooming tiles, track sets to only write unique tiles
	highZoomTiles := make(map[coord.Coord]interface{})
	lowZoomTiles := make(map[coord.Coord]interface{})

	scanner := bufio.NewScanner(tf)
	var tilesSet map[coord.Coord]interface{}
	for scanner.Scan() {
		line := scanner.Text()
		coord, err := coord.Decode(line)
		if err != nil {
			panic(fmt.Errorf("Failed to read coord from %#v: %s", line, err))
		}
		if coord.Z >= splitZoom {
			tilesSet = highZoomTiles
		} else {
			tilesSet = lowZoomTiles
		}
		if coord.Z > zoomMax {
			*coord = coord.ZoomTo(zoomMax)
		}
		tilesSet[*coord] = struct{}{}
	}
	if err = scanner.Err(); err != nil {
		panic(err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go writeTiles(&wg, lzf, lowZoomTiles)
	go writeTiles(&wg, hzf, highZoomTiles)
	wg.Wait()
}
