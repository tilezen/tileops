package main

import (
	"flag"
	"fmt"
	"os"
	"tzops/go/pkg/cmd"
	"tzops/go/pkg/coord"
	"tzops/go/pkg/s3"
)

func main() {
	var bucket string
	var prefix string
	var rawr bool
	var tileStr string

	flag.StringVar(&bucket, "bucket", "", "s3 bucket")
	flag.StringVar(&prefix, "prefix", "", "s3 bucket prefix")
	flag.StringVar(&tileStr, "tile", "", "tile coordinate")
	flag.BoolVar(&rawr, "rawr", false, "generate rawr path")

	flag.Parse()

	if prefix == "" || tileStr == "" {
		cmd.DieWithUsage()
	}

	c, err := coord.Decode(tileStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid tile %s: %s\n", tileStr, err)
		cmd.DieWithUsage()
	}

	var path string
	if rawr {
		path = s3.RawrTileHashPathForCoord(prefix, *c)
	} else {
		path = s3.MetaTileHashPathForCoord(prefix, *c)
	}

	if bucket != "" {
		fmt.Printf("s3://%s/%s\n", bucket, path)
	} else {
		fmt.Printf("%s\n", path)
	}
}
