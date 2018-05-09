package main

import (
	"flag"
	"fmt"
	"tzops/go/pkg/cmd"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/batch"
)

func main() {
	var jobQueue string
	var region string

	flag.StringVar(&jobQueue, "queue", "", "job queue")
	flag.StringVar(&region, "region", "us-east-1", "region")

	flag.Parse()

	if jobQueue == "" {
		cmd.DieWithUsage()
	}

	// track job ids we need to look up more details on in a
	// subsequent request
	var jobIDLookups []string

	sess := session.Must(session.NewSession(&aws.Config{
		Region:     &region,
		MaxRetries: aws.Int(3),
	}))
	svc := batch.New(sess)
	var nextToken *string
	for {
		input := batch.ListJobsInput{
			JobQueue:  &jobQueue,
			JobStatus: aws.String(batch.JobStatusFailed),
			NextToken: nextToken,
		}
		output, err := svc.ListJobs(&input)
		if err != nil {
			panic(err)
		}

		for _, jobSummary := range output.JobSummaryList {
			jobID := *jobSummary.JobId
			var containerReason string
			if jobSummary.Container != nil {
				containerSummary := jobSummary.Container
				if containerSummary.Reason != nil {
					containerReason = *containerSummary.Reason
				}
			}
			if containerReason == "" {
				// we aren't getting the status reason in these
				// results, but if we ask for it separately in a
				// describejobs call we do get it
				jobIDLookups = append(jobIDLookups, jobID)
			} else {
				fmt.Printf("%s: %s\n", jobID, containerReason)
			}
		}

		nextToken = output.NextToken
		if nextToken == nil {
			break
		}
	}
	// now we issue requests for the job ids where we need further
	// information

	// request 100 jobs at a time
	step := 100
	for idx := 0; idx < len(jobIDLookups); idx += step {
		upperBound := idx + step
		if upperBound > len(jobIDLookups) {
			upperBound = len(jobIDLookups)
		}
		jobIDs := jobIDLookups[idx:upperBound]
		jobIDPtrs := make([]*string, len(jobIDs))
		for i := 0; i < len(jobIDs); i++ {
			jobIDPtrs[i] = &jobIDs[i]
		}
		input := batch.DescribeJobsInput{
			Jobs: jobIDPtrs,
		}
		output, err := svc.DescribeJobs(&input)
		if err != nil {
			panic(err)
		}
		for _, jobDetail := range output.Jobs {
			var statusReason string
			if jobDetail.StatusReason != nil {
				statusReason = *jobDetail.StatusReason
			}
			jobID := *jobDetail.JobId
			fmt.Printf("%s: %s\n", jobID, statusReason)
		}
	}
}
