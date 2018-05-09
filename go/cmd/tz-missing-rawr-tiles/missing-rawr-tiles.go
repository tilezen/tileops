package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"tzops/go/pkg/cmd"
	tzc "tzops/go/pkg/coord"
	"tzops/go/pkg/coord/cmp"
	"tzops/go/pkg/coord/gen"
	tzs3 "tzops/go/pkg/s3"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func main() {
	var bucket string
	var datePrefix string
	var region string
	var printZ10 bool

	flag.StringVar(&bucket, "bucket", "", "s3 bucket")
	flag.StringVar(&datePrefix, "date-prefix", "", "date prefix")
	flag.StringVar(&region, "region", "us-east-1", "region")
	flag.BoolVar(&printZ10, "z10", false, "print z10 coordinates that are missing")

	flag.Parse()

	if bucket == "" || datePrefix == "" {
		cmd.DieWithUsage()
	}

	sess := session.Must(session.NewSession(&aws.Config{
		Region:     &region,
		MaxRetries: aws.Int(3),
	}))
	svc := s3.New(sess)
	input := s3.ListObjectsInput{
		Bucket: &bucket,
		Prefix: &datePrefix,
	}
	var rawrCoords []tzc.Coord
	err := svc.ListObjectsPages(&input, func(output *s3.ListObjectsOutput, lastPage bool) bool {
		for _, obj := range output.Contents {
			key := *obj.Key
			coord, _ := tzs3.ParseCoordFromKey(key)
			if coord != nil {
				rawrCoords = append(rawrCoords, *coord)
			}
		}
		return true
	})
	if err != nil {
		panic(err)
	}
	if len(rawrCoords) == 0 {
		fmt.Fprintf(os.Stderr, "No rawr tiles found!\n")
		os.Exit(2)
	}
	sort.Sort(tzc.ByZYX(rawrCoords))

	expGen := gen.NewZoomRange(10, 10)
	actGen := gen.NewSlice(rawrCoords)
	missing := cmp.FindMissingTiles(expGen, actGen)
	if len(missing) == 0 {
		return
	}
	// group by z7 parent
	z7map := make(map[tzc.Coord][]tzc.Coord)
	for _, c := range missing {
		parent := c.ZoomTo(7)
		z7map[parent] = append(z7map[parent], c)
	}
	for z7, z10s := range z7map {
		fmt.Println(z7)
		if printZ10 {
			for _, z10 := range z10s {
				fmt.Printf("  %s\n", z10)
			}
		}
	}
}
