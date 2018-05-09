package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"tzops/go/pkg/cmd"
	"tzops/go/pkg/coord"
	tzs3 "tzops/go/pkg/s3"
	"tzops/go/pkg/util"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func isHexChar(c rune) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')
}

func isValidHexPrefix(hexPrefix string) bool {
	if len(hexPrefix) != 3 {
		return false
	}
	for _, c := range hexPrefix {
		if !isHexChar(c) {
			return false
		}
	}
	return true
}

func genHexPrefixes(hexPrefixChan chan<- string, hexPrefix string) {
	totalToGen := 16 * 16
	for i := 0; i < totalToGen; i++ {
		hexValue := fmt.Sprintf("%s%02x", hexPrefix, i)
		hexPrefixChan <- hexValue
	}
	close(hexPrefixChan)
}

func listPrefix(hexPrefixChan <-chan string, coordsChan chan<- []coord.Coord, svc *s3.S3, srcBucket string, srcDatePrefix string, concurrency uint) {
	util.Concurrently(concurrency, func() {
		for hexPrefix := range hexPrefixChan {
			prefix := fmt.Sprintf("%s/%s/", srcDatePrefix, hexPrefix)
			input := s3.ListObjectsInput{
				Bucket: &srcBucket,
				Prefix: &prefix,
			}
			err := svc.ListObjectsPages(&input, func(output *s3.ListObjectsOutput, lastPage bool) bool {
				coords := make([]coord.Coord, 0, len(output.Contents))
				for _, obj := range output.Contents {
					key := *obj.Key
					c, err := tzs3.ParseCoordFromKey(key)
					if err != nil {
						fmt.Fprintf(os.Stderr, "Could not parse coordinate from %#v: %s\n", key, err)
					} else {
						coords = append(coords, *c)
					}
				}
				coordsChan <- coords
				return true
			})
			if err != nil {
				panic(err)
			}
		}
	})
	close(coordsChan)
}

func writeCoords(coordsChan <-chan []coord.Coord, doneChan chan<- interface{}, svc *s3.S3, destBucket string, destDatePrefix string, hexPrefix string) {
	// store all coords that were enumerated in memory
	var allCoords []coord.Coord
	for coords := range coordsChan {
		allCoords = append(allCoords, coords...)
	}

	var coordEncBuf bytes.Buffer
	for _, c := range allCoords {
		coordEnc := c.String()
		coordEncBuf.WriteString(coordEnc)
		coordEncBuf.WriteByte('\n')
	}
	s3Key := fmt.Sprintf("%s/%s", destDatePrefix, hexPrefix)
	rdr := bytes.NewReader(coordEncBuf.Bytes())
	contentLength := rdr.Len()
	input := s3.PutObjectInput{
		Body:          rdr,
		Bucket:        &destBucket,
		ContentType:   aws.String("text/plain"),
		ContentLength: aws.Int64(int64(contentLength)),
		Key:           &s3Key,
	}
	_, err := svc.PutObject(&input)
	if err != nil {
		panic(err)
	}
	doneChan <- struct{}{}
}

func main() {
	var srcBucket string
	var srcDatePrefix string
	var destBucket string
	var destDatePrefix string
	var hexPrefix string
	var concurrency uint
	var region string

	// TODO is this better to put in a yaml file?
	flag.StringVar(&srcBucket, "src-bucket", "", "source s3 bucket to enumerate tiles")
	flag.StringVar(&srcDatePrefix, "src-date-prefix", "", "source date prefix")
	flag.StringVar(&destBucket, "dest-bucket", "", "dest s3 bucket to write tiles")
	flag.StringVar(&destDatePrefix, "dest-date-prefix", "", "dest date prefix to write tiles found")
	flag.StringVar(&hexPrefix, "hex-prefix", "", "hex prefix for job, must be 3 lowercase hexadecimal characters")
	flag.UintVar(&concurrency, "concurrency", 16, "number of goroutines listing bucket per hash prefix")
	flag.StringVar(&region, "region", "us-east-1", "region")

	flag.Parse()

	if srcBucket == "" || srcDatePrefix == "" || destBucket == "" || destDatePrefix == "" || hexPrefix == "" {
		cmd.DieWithUsage()
	}

	if !isValidHexPrefix(hexPrefix) {
		fmt.Fprintf(os.Stderr, "Invalid hex prefix: %#v\n", hexPrefix)
		cmd.DieWithUsage()
	}

	sess := session.Must(session.NewSession(&aws.Config{
		Region:     &region,
		MaxRetries: aws.Int(10),
	}))
	svc := s3.New(sess)

	hexPrefixChan := make(chan string, concurrency)
	coordsChan := make(chan []coord.Coord, concurrency)
	doneChan := make(chan interface{})

	go genHexPrefixes(hexPrefixChan, hexPrefix)
	go listPrefix(hexPrefixChan, coordsChan, svc, srcBucket, srcDatePrefix, concurrency)
	go writeCoords(coordsChan, doneChan, svc, destBucket, destDatePrefix, hexPrefix)

	<-doneChan
}
