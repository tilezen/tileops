Go tileops commands
===================

Collection of utility commands to support Tilezen build operations.

Install
-------

### tldr

    go install ./...

### Go Modules

The `go install` command will use [Go Modules](https://blog.golang.org/using-go-modules) to download the dependencies for the commands. 

Commands
--------

### tz-batch-check-failures

Walk through all batch job failures in a particular queue, and print out the reason that they failed.

```
Usage: tz-batch-check-failures
  -queue string
        job queue
  -region string
        region (default "us-east-1")
```

### tz-batch-create-job-definition

Create a job definition in aws batch.

```
Usage: tz-batch-create-job-definition
  -yaml string
        path to yaml file
```

### tz-batch-submit-missing-meta-tiles

Submit jobs to aws batch to find missing meta tiles.

```
Usage: tz-batch-submit-missing-meta-tiles
  -concurrency uint
        number of goroutines submitting jobs (default 2)
  -dest-bucket string
        destination bucket
  -dest-date-prefix string
        destination date prefix
  -job-definition string
        job definition
  -job-queue string
        batch job queue
  -region string
        region (default "us-east-1")
  -src-bucket string
        source bucket
  -src-date-prefix string
        source date prefix
```

### tz-batch-tiles-split-low-high

Split a list of missing meta tiles into a low/high zoom files, which can be used to submit back into aws batch.

```
Usage: tz-batch-tiles-split-low-high
  -high-zoom-file string
        path to high zoom file to write
  -low-zoom-file string
        path to low zoom file to write
  -split-zoom uint
        zoom level that a tile is considered to be high (default 10)
  -tiles-file string
        path to tiles file to read
  -zoom-max uint
        tiles are clamped to this zoom (default 7)
```

### tz-missing-meta-tiles-read

Read the list of generated meta tiles from an s3 location, and determine which tiles are missing.

```
Usage: tz-missing-meta-tiles-read
  -bucket string
        s3 bucket containing tile listing from missing-meta-tiles-write command
  -concurrency uint
        number of goroutines listing bucket per hash prefix (default 16)
  -date-prefix string
        date prefix
  -region string
        region (default "us-east-1")
```

### tz-missing-meta-tiles-write

Walk the s3 bucket that has metatiles generated, and write the list that was seen into an s3 location. This command is intended to be run from aws batch.

```
Usage: tz-missing-meta-tiles-write
  -concurrency uint
        number of goroutines listing bucket per hash prefix (default 16)
  -dest-bucket string
        dest s3 bucket to write tiles
  -dest-date-prefix string
        dest date prefix to write tiles found
  -hex-prefix string
        hex prefix for job, must be 3 lowercase hexadecimal characters
  -region string
        region (default "us-east-1")
  -src-bucket string
        source s3 bucket to enumerate tiles
  -src-date-prefix string
        source date prefix
```

### tz-missing-rawr-tiles

Walk the s3 bucket where rawr tiles are stored via ListObjects and print out the zoom 7 parents that don't have all zoom 10 children generated.

```
Usage: tz-missing-rawr-tiles
  -bucket string
        s3 bucket
  -date-prefix string
        date prefix
  -region string
        region (default "us-east-1")
  -z10
        print z10 coordinates that are missing
```

### tz-s3-path

Print out the s3 path for a particular tile.

```
Usage: tz-s3-path
  -bucket string
        s3 bucket
  -prefix string
        s3 bucket prefix
  -rawr
        generate rawr path
  -tile string
        tile coordinate
```

Lint
----

Use the [gometalinter](https://github.com/alecthomas/gometalinter) to lint code.

    gometalinter --disable-all --enable=vet --enable=golint --enable=errcheck ./...
