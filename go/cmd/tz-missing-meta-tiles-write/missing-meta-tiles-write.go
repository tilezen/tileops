package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/tilezen/tileops.git/go/pkg/cmd"
	"github.com/tilezen/tileops.git/go/pkg/coord"
	tzs3 "github.com/tilezen/tileops.git/go/pkg/s3"
	"github.com/tilezen/tileops.git/go/pkg/util"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// the order of date and hash as a prefix for the tile coordinate to list.
// having the date first makes things easier to list and interact with in the AWS UI, since the top-level grouping is "human-readable". the hash first is better for S3's sharding, but can make it harder to interact with.
type KeyFormatType int

const (
	PREFIX_HASH KeyFormatType = iota // date, then hash.
	HASH_PREFIX                      // hash then date.
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

// formatKey returns the prefix to be used for all keys, given a date and hash along with the format type.
func formatKey(hash, date string, keyFormatType KeyFormatType) string {
	if keyFormatType == PREFIX_HASH {
		return fmt.Sprintf("%s/%s/", date, hash)
	} else {
		return fmt.Sprintf("%s/%s/", hash, date)
	}
}

// listBucketPrefix lists all the coordinates it finds while scanning a given bucket in the given prefix to the coordsChan channel.
func listBucketPrefix(coordsChan chan<- []coord.Coord, svc *s3.S3, srcBucket, prefix string) {
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

func listPrefix(hexPrefixChan <-chan string, coordsChan chan<- []coord.Coord, svc *s3.S3, srcBuckets []string, srcDatePrefix string, concurrency uint, keyFormatType KeyFormatType) {
	util.Concurrently(concurrency, func() {
		for hexPrefix := range hexPrefixChan {
			prefix := formatKey(hexPrefix, srcDatePrefix, keyFormatType)
			for _, srcBucket := range srcBuckets {
				listBucketPrefix(coordsChan, svc, srcBucket, prefix)
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

// isS3BucketName checks if the string looks like an S3 bucket name.
//
// According to the docs, more than just a-z, 0-9 and '-' are allowed (e.g: %, &, / - see https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/DomainNameFormat.html#domain-name-format-hosted-zones) but these look terribly confusing and I haven't seen them in widespread use. Underscores and upper case letters are now not allowed in bucket names, so I think we can assume they'd not be used either.
func isS3BucketName(input string) bool {
	for _, ch := range input {
		if (ch < 'a' || ch > 'z') &&
			(ch < '0' || ch > '9') &&
			ch != '-' {
			return false
		}
	}
	return true
}

func main() {
	var srcBucket string
	var srcDatePrefix string
	var destBucket string
	var destDatePrefix string
	var hexPrefix string
	var concurrency uint
	var region string
	var keyFormatTypeStr string
	var keyFormatType KeyFormatType
	var allBuckets bool

	// TODO is this better to put in a yaml file?
	flag.StringVar(&srcBucket, "src-bucket", "", "source s3 bucket to enumerate tiles. If multiple buckets, format as a JSON array _without spaces_.")
	flag.StringVar(&srcDatePrefix, "src-date-prefix", "", "source date prefix")
	flag.StringVar(&destBucket, "dest-bucket", "", "dest s3 bucket to write tiles")
	flag.StringVar(&destDatePrefix, "dest-date-prefix", "", "dest date prefix to write tiles found")
	flag.StringVar(&hexPrefix, "hex-prefix", "", "hex prefix for job, must be 3 lowercase hexadecimal characters")
	flag.UintVar(&concurrency, "concurrency", 16, "number of goroutines listing bucket per hash prefix")
	flag.StringVar(&region, "region", "us-east-1", "region")
	flag.StringVar(&keyFormatTypeStr, "key-format-type", "", "Either 'prefix-hash' or 'hash-prefix' to control the order of the date prefix and hash in the src S3 key.")
	flag.BoolVar(&allBuckets, "all-buckets", false, "If true, check all buckets in list, not just the last one.")

	flag.Parse()

	if srcBucket == "" || srcDatePrefix == "" || destBucket == "" || destDatePrefix == "" || hexPrefix == "" {
		cmd.DieWithUsage()
	}

	if keyFormatTypeStr == "prefix-hash" {
		keyFormatType = PREFIX_HASH

	} else if keyFormatTypeStr == "hash-prefix" {
		keyFormatType = HASH_PREFIX

	} else if keyFormatTypeStr != "" {
		fmt.Fprintf(os.Stderr, "Unknown value %#v for -key-format-type argument.\n", keyFormatTypeStr)
		cmd.DieWithUsage()
	}

	if !isValidHexPrefix(hexPrefix) {
		fmt.Fprintf(os.Stderr, "Invalid hex prefix: %#v\n", hexPrefix)
		cmd.DieWithUsage()
	}

	// we support multiple buckets as src, in which case we check the last one.
	var srcBuckets []string

	// decode src bucket argument if it looks like an array.
	if strings.HasPrefix(srcBucket, "[") && strings.HasSuffix(srcBucket, "]") {
		err := json.Unmarshal([]byte(srcBucket), &srcBuckets)
		if err != nil {
			fmt.Fprintf(os.Stderr, "-src-bucket looks like JSON array, but there was an error while decoding it: %s\n", err.Error())
			cmd.DieWithUsage()
		}

		numBuckets := len(srcBuckets)
		if numBuckets < 1 {
			fmt.Fprintf(os.Stderr, "Must provide at least one bucket to -src-bucket when using JSON array syntax\n")
			cmd.DieWithUsage()
		}

	} else {
		// add the single command line argument to the array
		srcBuckets = append(srcBuckets, srcBucket)
	}

	for i, bucket := range srcBuckets {
		if !isS3BucketName(bucket) {
			fmt.Fprintf(os.Stderr, "-src-bucket %dth argument, %#v, does not look like an S3 bucket name. It should be all lower case ASCII letters and digits separated by hyphens.\n", i, bucket)
			cmd.DieWithUsage()
		}
	}

	if !allBuckets {
		// we use only the last bucket to check whether the tile has been written. this is because tilequeue will write in order, so if it wrote the last one then it seems reasonable to assume it wrote all the previous ones.
		srcBuckets = []string{srcBuckets[len(srcBuckets) - 1]}
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
	go listPrefix(hexPrefixChan, coordsChan, svc, srcBuckets, srcDatePrefix, concurrency, keyFormatType)
	go writeCoords(coordsChan, doneChan, svc, destBucket, destDatePrefix, hexPrefix)

	<-doneChan
}
