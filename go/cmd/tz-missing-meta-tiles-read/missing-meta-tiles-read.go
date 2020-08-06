package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/tilezen/tileops.git/go/pkg/cmd"
	"github.com/tilezen/tileops.git/go/pkg/coord"
	"github.com/tilezen/tileops.git/go/pkg/coord/gen"
	"github.com/tilezen/tileops.git/go/pkg/util"

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

const (
	COORD_BATCH_SIZE = 1024
)

func findMissingCoords(coordsChan <-chan []coord.Coord, outputChan chan<- []coord.Coord, min_zoom, max_zoom uint) {
	cs := coord.NewCoordSet()

	// set all the ones we expect to find to true
	allCoordsGen := gen.NewZoomRange(min_zoom, max_zoom)
	for {
		c := allCoordsGen.Next()
		if c == nil {
			break
		}
		cs.Set(*c, true)
	}

	// then clear all the ones we actually found - any left as true must be missing
	for coordArray := range coordsChan {
		for _, c := range coordArray {
			cs.Set(c, false)
		}
	}

	coords := make([]coord.Coord, 0, COORD_BATCH_SIZE)
	listGen := gen.NewZoomRange(min_zoom, max_zoom)
	for {
		c := listGen.Next()
		if c == nil {
			break
		}
		val := cs.Get(*c)
		if val {
			if cap(coords) == len(coords) {
				outputChan <- coords
				coords = make([]coord.Coord, 0, COORD_BATCH_SIZE)
			}
			coords = append(coords, *c)
		}
	}
	if len(coords) > 0 {
		outputChan <- coords
	}
	close(outputChan)
}

func printCoords(coordsChan <-chan []coord.Coord, doneChan chan<- error, compressOutput bool) {
	var err error

	var output io.WriteCloser = os.Stdout
	if compressOutput {
		output = gzip.NewWriter(output)
	}

	// buffer output so we make fewer system calls.
	buf := bufio.NewWriter(output)
	for coordArray := range coordsChan {
		for _, coord := range coordArray {
			// don't immediately exit when we get an error - instead we want to train the coordsChan, so keep reading until the range is done, just don't write anything to the buffer.
			if err == nil {
				_, err = buf.WriteString(coord.String())
			}
			if err == nil {
				_, err = buf.WriteString("\n")
			}
		}
	}
	err = buf.Flush()
	if compressOutput && err == nil {
		// note: can't defer this, as then it would execute after the doneChan write. because the doneChan write unblocks the main thread, which will then exit, this thread might not execute anything further. (this only happens on the compressed Writer because stdout doesn't need to be closed)
		err = output.Close()
	}
	doneChan <- err
}

func main() {
	var bucket string
	var datePrefix string
	var concurrency, min_zoom, max_zoom uint
	var region string
	var present, compressOutput bool

	flag.StringVar(&bucket, "bucket", "", "s3 bucket containing tile listing from missing-meta-tiles-write command")
	flag.StringVar(&datePrefix, "date-prefix", "", "date prefix")
	flag.UintVar(&concurrency, "concurrency", 16, "number of goroutines listing bucket per hash prefix")
	flag.StringVar(&region, "region", "us-east-1", "region")
	flag.BoolVar(&present, "present", false, "If set, return tiles which are present rather than missing. The default (false) is to return tiles which are missing within the zoom range.")
	flag.UintVar(&min_zoom, "min-zoom", 0, "Minimum zoom to check for missing tiles (inclusive). (default 0)")
	flag.UintVar(&max_zoom, "max-zoom", 14, "Maximum zoom to check for missing tiles (inclusive).")
	flag.BoolVar(&compressOutput, "compress-output", false, "If set, compress the output file with gzip.")

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
	doneChan := make(chan error)

	go listObjects(keysChan, svc, bucket, datePrefix)
	go readKey(keysChan, coordsChan, svc, bucket, concurrency)
	if !present {
		missingChan := make(chan []coord.Coord, concurrency)
		go findMissingCoords(coordsChan, missingChan, min_zoom, max_zoom)
		coordsChan = missingChan
	}
	go printCoords(coordsChan, doneChan, compressOutput)

	err := <-doneChan
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s\n", err.Error())
		os.Exit(1)
	}
}
