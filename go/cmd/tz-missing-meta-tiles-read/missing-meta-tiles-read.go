package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"sort"
	"tzops/go/pkg/cmd"
	"tzops/go/pkg/coord"
	"tzops/go/pkg/coord/cmp"
	"tzops/go/pkg/coord/gen"
	"tzops/go/pkg/util"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func listObjects(keysChan chan<- string, svc *s3.S3, bucket string, datePrefix string) {
	err := svc.ListObjectsPages(&s3.ListObjectsInput{
		Bucket: &bucket,
		Prefix: &datePrefix,
	}, func(output *s3.ListObjectsOutput, lastPage bool) bool {
		for _, obj := range output.Contents {
			keysChan <- *obj.Key
		}
		return true
	})
	if err != nil {
		panic(err)
	}
	close(keysChan)
}

func readKey(keysChan <-chan string, coordsChan chan<- []coord.Coord, svc *s3.S3, bucket string, concurrency uint) {
	util.Concurrently(concurrency, func() {
		for key := range keysChan {
			obj, err := svc.GetObject(&s3.GetObjectInput{
				Bucket: &bucket,
				Key:    &key,
			})
			if err != nil {
				panic(err)
			}
			var coords []coord.Coord
			scanner := bufio.NewScanner(obj.Body)
			for scanner.Scan() {
				line := scanner.Text()
				c, err := coord.Decode(line)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to parse tile coordinate from key=%#v line=%#v: %s\n", key, line, err)
					continue
				}
				coords = append(coords, *c)
			}
			if err = obj.Body.Close(); err != nil {
				panic(err)
			}
			if err = scanner.Err(); err != nil {
				panic(err)
			}
			coordsChan <- coords
		}
	})
	close(coordsChan)
}

func readAllCoords(coordsChan <-chan []coord.Coord) []coord.Coord {
	var result []coord.Coord
	for cs := range coordsChan {
		result = append(result, cs...)
	}
	return result
}

func findMissingCoords(coordsChan <-chan []coord.Coord, doneChan chan<- interface{}, min_zoom, max_zoom uint) {
	allCoordsGen := gen.NewZoomRange(min_zoom, max_zoom)
	// need to realize the channel to get all coordinates
	// in a slice first for sorting purposes
	coords := readAllCoords(coordsChan)
	sort.Sort(coord.ByZYX(coords))
	coordsGen := gen.NewSlice(coords)
	missing := cmp.FindMissingTiles(allCoordsGen, coordsGen)
	for _, coord := range missing {
		fmt.Println(coord)
	}
	doneChan <- struct{}{}
}

func printCoords(coordsChan <-chan []coord.Coord, doneChan chan<- interface{}) {
	for coordArray := range coordsChan {
		for _, coord := range coordArray {
			fmt.Println(coord)
		}
	}
	doneChan <- struct{}{}
}

func main() {
	var bucket string
	var datePrefix string
	var concurrency, min_zoom, max_zoom uint
	var region string
	var present bool

	flag.StringVar(&bucket, "bucket", "", "s3 bucket containing tile listing from missing-meta-tiles-write command")
	flag.StringVar(&datePrefix, "date-prefix", "", "date prefix")
	flag.UintVar(&concurrency, "concurrency", 16, "number of goroutines listing bucket per hash prefix")
	flag.StringVar(&region, "region", "us-east-1", "region")
	flag.BoolVar(&present, "present", false, "If set, return tiles which are present rather than missing. The default (false) is to return tiles which are missing within the zoom range.")
	flag.UintVar(&min_zoom, "min-zoom", 0, "Minimum zoom to check for missing tiles (inclusive). (default 0)")
	flag.UintVar(&max_zoom, "max-zoom", 14, "Maximum zoom to check for missing tiles (inclusive).")

	flag.Parse()

	if bucket == "" || datePrefix == "" || concurrency == 0 {
		cmd.DieWithUsage()
	}

	if max_zoom < min_zoom {
		fmt.Fprintf(os.Stderr, "Max zoom must be >= min zoom.\n")
		cmd.DieWithUsage()
	}

	sess := session.Must(session.NewSession(&aws.Config{
		Region:     &region,
		MaxRetries: aws.Int(10),
	}))
	svc := s3.New(sess)

	keysChan := make(chan string, concurrency)
	coordsChan := make(chan []coord.Coord, concurrency)
	doneChan := make(chan interface{})

	go listObjects(keysChan, svc, bucket, datePrefix)
	go readKey(keysChan, coordsChan, svc, bucket, concurrency)
	if present {
		go printCoords(coordsChan, doneChan)
	} else {
		go findMissingCoords(coordsChan, doneChan, min_zoom, max_zoom)
	}
	<-doneChan
}
