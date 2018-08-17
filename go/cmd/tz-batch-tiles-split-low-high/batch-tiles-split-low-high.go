package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"tzops/go/pkg/cmd"
	"tzops/go/pkg/coord"
)

func mustOpenForWriting(path string) io.WriteCloser {
	result, err := os.Create(path)
	if err != nil {
		log.Fatalf("Error opening %#v for writing: %s", path, err)
	}
	return result
}

func mustClose(wr io.Closer) {
	err := wr.Close()
	if err != nil {
		log.Fatalf("Unable to close: %s", err.Error())
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
	var isCompressed bool

	flag.StringVar(&tilesPath, "tiles-file", "", "path to tiles file to read")
	flag.StringVar(&lowZoomPath, "low-zoom-file", "", "path to low zoom file to write")
	flag.StringVar(&highZoomPath, "high-zoom-file", "", "path to high zoom file to write")
	flag.UintVar(&splitZoom, "split-zoom", 10, "zoom level that a tile is considered to be high")
	flag.UintVar(&zoomMax, "zoom-max", 7, "tiles are clamped to this zoom")
	flag.BoolVar(&isCompressed, "compressed", false, "Input tiles-file is a gzip compressed file.")

	flag.Parse()

	if tilesPath == "" || lowZoomPath == "" || highZoomPath == "" {
		cmd.DieWithUsage()
	}

	var tf io.ReadCloser
	tf, err := os.Open(tilesPath)
	if err != nil {
		log.Fatalf("Error opening %#v for reading: %s", tilesPath, err.Error())
	}
	defer mustClose(tf)

	if isCompressed {
		tf, err = gzip.NewReader(tf)
		if err != nil {
			log.Fatalf("Error decompressing %#v: %s", tilesPath, err.Error())
		}
	}

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
