package main

import (
	"flag"
	"fmt"
	"sync"
	"tzops/go/pkg/cmd"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/batch"
)

type s3Cfg struct {
	destBucket, destDatePrefix, srcBucket, srcDatePrefix string
}

type batchCfg struct {
	jobQueue, jobDefinition string
}

func genPrefixes(prefixesChan chan<- string) {
	totalPrefixes := 16 * 16 * 16
	for i := 0; i < totalPrefixes; i++ {
		hexValue := fmt.Sprintf("%03x", i)
		prefixesChan <- hexValue
	}
	close(prefixesChan)
}

func newParams(s3Cfg *s3Cfg, prefix string) map[string]*string {
	return map[string]*string{
		"dest_bucket":      &s3Cfg.destBucket,
		"dest_date_prefix": &s3Cfg.destDatePrefix,
		"src_bucket":       &s3Cfg.srcBucket,
		"src_date_prefix":  &s3Cfg.srcDatePrefix,
		"hex_prefix":       &prefix,
	}
}

func submitJobs(prefixesChan <-chan string, doneChan chan<- interface{}, s3Cfg *s3Cfg, batchCfg *batchCfg, svc *batch.Batch, concurrency uint) {
	var wg sync.WaitGroup
	wg.Add(int(concurrency))
	for i := uint(0); i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for prefix := range prefixesChan {
				params := newParams(s3Cfg, prefix)
				jobName := fmt.Sprintf("missing-meta-tiles-%s", prefix)
				_, err := svc.SubmitJob(&batch.SubmitJobInput{
					JobDefinition: &batchCfg.jobDefinition,
					JobName:       &jobName,
					JobQueue:      &batchCfg.jobQueue,
					Parameters:    params,
				})
				if err != nil {
					panic(err)
				}
			}
		}()
	}
	wg.Wait()
	close(doneChan)
}

func main() {
	var jobQueue,
		jobDefinition,
		destBucket,
		destDatePrefix,
		srcBucket,
		srcDatePrefix,
		region string
	var concurrency uint

	// TODO is this better to put in a yaml file?
	flag.StringVar(&jobQueue, "job-queue", "", "batch job queue")
	flag.StringVar(&jobDefinition, "job-definition", "", "job definition")
	flag.StringVar(&destBucket, "dest-bucket", "", "destination bucket")
	flag.StringVar(&destDatePrefix, "dest-date-prefix", "", "destination date prefix")
	flag.StringVar(&srcBucket, "src-bucket", "", "source bucket")
	flag.StringVar(&srcDatePrefix, "src-date-prefix", "", "source date prefix")
	flag.UintVar(&concurrency, "concurrency", 2, "number of goroutines submitting jobs")
	flag.StringVar(&region, "region", "us-east-1", "region")

	flag.Parse()

	if jobQueue == "" || jobDefinition == "" || destBucket == "" || destDatePrefix == "" || srcBucket == "" || srcDatePrefix == "" {
		cmd.DieWithUsage()
	}

	s3Cfg := s3Cfg{
		destBucket:     destBucket,
		destDatePrefix: destDatePrefix,
		srcBucket:      srcBucket,
		srcDatePrefix:  srcDatePrefix,
	}
	batchCfg := batchCfg{
		jobQueue:      jobQueue,
		jobDefinition: jobDefinition,
	}

	sess := session.Must(session.NewSession(&aws.Config{
		Region:     &region,
		MaxRetries: aws.Int(3),
	}))
	svc := batch.New(sess)

	prefixesChan := make(chan string, concurrency)
	doneChan := make(chan interface{})

	go genPrefixes(prefixesChan)
	go submitJobs(prefixesChan, doneChan, &s3Cfg, &batchCfg, svc, concurrency)
	<-doneChan

}
