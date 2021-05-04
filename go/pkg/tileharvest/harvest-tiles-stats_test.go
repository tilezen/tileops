package tileharvest

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/aclements/go-moremath/stats"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	rawrTileMatch           = "Rawr tile generation finished"
	metaTileMatch           = "batch process run end"
	metaLowZoomTileMatch    = "low zoom tile run end"
	jobTypeRawrBatch        = "rawr-batch"
	jobTypeMetaBatch        = "meta-batch"
	jobTypeMetaLowZoomBatch = "meta-low-zoom-batch"
)

// Update these!!
const (
	runId            = "20210426-ordered"
	jobType          = jobTypeMetaLowZoomBatch
	maxLastUpdateAge = 290 * time.Minute
)

const (
	filename = "/tmp/tilestats-" + runId + "-" + jobType
)



func getLogNames(svc *cloudwatchlogs.CloudWatchLogs, jobType string, runId string, logNameChan chan<- string) {
	logGroupName := "/aws/batch/job"

	limit := int64(50)
	var token *string
	var count int
	var done bool
	for {
		input := cloudwatchlogs.DescribeLogStreamsInput{
			Descending:          aws.Bool(true),
			Limit:               &limit,
			LogGroupName:        &logGroupName,
			// LogStreamNamePrefix: &logGroupNamePrefix,
			NextToken:           token,
			OrderBy:             aws.String("LastEventTime"),
		}
		streams, err := svc.DescribeLogStreams(&input)
		if err != nil {
			fmt.Printf("Error %v\n", err)
		}

		token = streams.NextToken

		logGroupNamePrefix := jobType + "-" + runId
		for _, stream := range streams.LogStreams {
			lastEventTime := time.Unix(*stream.LastEventTimestamp / 1000, 0)
			if time.Since(lastEventTime) > maxLastUpdateAge {
				fmt.Printf("Bailing after %d logs - this LastEventTimestamp is %s which is greater than %s ago\n", count, lastEventTime.String(), maxLastUpdateAge.String())
				done = true
				break
			}

			if !strings.Contains(*stream.LogStreamName, logGroupNamePrefix) {
				continue
			}
			if count % 1000 == 0 {
				fmt.Printf("%dth Stream: %s - last event is at %s\n", count, *stream.LogStreamName, lastEventTime.String())
			}
			logNameChan <- *stream.LogStreamName
			count++
		}

		if token == nil || done {
			close(logNameChan)
			break
		}
	}
}

func getTileInfo(svc *cloudwatchlogs.CloudWatchLogs, logName string) (TileInfo, error) {
	logGroupName := aws.String("/aws/batch/job")
	limit := aws.Int64(2)
	startFromHead := aws.Bool(false)

	events, err := svc.GetLogEvents(&cloudwatchlogs.GetLogEventsInput{
		Limit:         limit,
		LogGroupName:  logGroupName,
		LogStreamName: aws.String(logName),
		StartFromHead: startFromHead,
	})
	if err != nil {
		return TileInfo{}, err
	}

	// TODO: Add "batch process run begin" decode for first line (add second call to get things ordered chrono)

	var stats TileStats
	var spec TileSpec
	for _, event := range events.Events {
		eventMessage := *event.Message
		if strings.Contains(eventMessage, "max_resident_kb") {
			stats, err = extractTileStats(eventMessage)
			if err != nil {
				return TileInfo{}, err
			}
		} else if strings.Contains(eventMessage, rawrTileMatch) || strings.Contains(eventMessage, metaTileMatch) || strings.Contains(eventMessage, metaLowZoomTileMatch){
			spec, err = extractTileSpec(eventMessage)
			if err != nil {
				return TileInfo{}, err
			}
		}
	}

	if stats.isNilObj() {
		return TileInfo{}, fmt.Errorf("missing stats")
	} else if spec.isNilObj() {
		return TileInfo{}, fmt.Errorf("missing spec")
	}

	return TileInfo{
		Spec:  spec,
		Stats: stats,
	}, nil
}

func extractTileStats(uglyMessage string) (TileStats, error) {
	strMessage := removeGarbage(uglyMessage)

	var stats interface{}
	err := json.Unmarshal([]byte(strMessage), &stats)
	if err != nil {
		fmt.Errorf("unmarshal error %w", err)
		return TileStats{}, fmt.Errorf("unmarshal error %w", err)
	}

	percent := stats.(map[string]interface{})["cpu_percent"].(string)
	percent = percent[0:len(percent) - 1]
	atoi, err := strconv.Atoi(percent)
	if err != nil {
		return TileStats{}, nil
	}

	return TileStats{
		CPUPercent:           atoi,
		WallTimeSeconds:      int(stats.(map[string]interface{})["wall_time_seconds"].(float64)),
		MaxResidentKilobytes: int(stats.(map[string]interface{})["max_resident_kb"].(float64)),
	}, nil
}

func removeGarbage(message string) string {
	degarbaged := strings.ReplaceAll(message, "?\\", "")
	return degarbaged[1 : len(degarbaged)-1]
}

func extractTileSpec(eventMessage string) (TileSpec, error) {
	startIndex := strings.Index(eventMessage, "{")
	s := (eventMessage)[startIndex:]

	var message interface{}
	err := json.Unmarshal([]byte(s), &message)
	if err != nil {
		fmt.Errorf("unmarshal error %w", err)
		return TileSpec{}, fmt.Errorf("unmarshal error %w", err)
	}

	parent := message.(map[string]interface{})["parent"]
	zoom := parent.(map[string]interface{})["z"].(float64)
	x := parent.(map[string]interface{})["x"].(float64)
	y := parent.(map[string]interface{})["y"].(float64)
	return TileSpec{
		Zoom: int(zoom),
		X:    int(x),
		Y:    int(y),
	}, nil
}

func processLogNames(i int, svc *cloudwatchlogs.CloudWatchLogs, logNameChan chan string, retryNameChan chan string, resultsChan chan<- TileInfo) {
	for logName := range logNameChan {
		retrieveTileInfo(i, svc, logName, retryNameChan, resultsChan)
	}
	for logName := range retryNameChan {
		retrieveTileInfo(i, svc, logName, retryNameChan, resultsChan)
	}
}

func retrieveTileInfo(i int, svc *cloudwatchlogs.CloudWatchLogs, logName string, retryNameChan chan string, resultsChan chan<- TileInfo) {
	tileInfo, err := getTileInfo(svc, logName)
	if err != nil {
		fmt.Printf("%d: Putting %s in retry queue - it wasn't ready: error is %s\n", i+1, logName, err.Error())
		retryNameChan <- logName
		return
	} else if tileInfo.isNilObj() {
		fmt.Printf("%d: Putting %s in retry queue - it was empty\n", i+1, logName)
		retryNameChan <- logName
		return
	}
	resultsChan <- tileInfo
}

func dumpTileInfo(filename string, resultsChan <-chan TileInfo) {
	f, err := os.Create(filename)
	if err != nil {
		panic (err)
	}
	defer f.Close()

	var count int
	for tileInfo := range resultsChan {
		output, err := json.Marshal(tileInfo)
		if err != nil {
			fmt.Println("Error in outputting!")
		}

		f.WriteString(string(output) + "\n")
		if count % 1000 == 0 {
			fmt.Printf("%dth - output: %s\n", count, output)
		}
		count ++
	}
}

func TestHarvestTileData(t *testing.T) {
	region := "us-east-1"

	verbose := true
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region:     &region,
			MaxRetries: aws.Int(10),
			Retryer: client.DefaultRetryer{
				NumMaxRetries:    5,
				MaxRetryDelay:    5 * time.Second,
				MaxThrottleDelay: 30 * time.Second,
			},
			CredentialsChainVerboseErrors: &verbose,
		},
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := cloudwatchlogs.New(sess)

	logNameChan := make(chan string, 1000000)
	go getLogNames(svc, jobType, runId, logNameChan)

	workerCount := 20
	resultsChan := make(chan TileInfo, workerCount)
	retryNameChan := make(chan string, 1000000)
	for workerId := 0; workerId < workerCount; workerId++ {
		go processLogNames(workerId, svc, logNameChan, retryNameChan, resultsChan)
	}

	go func() {
		for {
			time.Sleep(1 * time.Minute)
			fmt.Printf("There are %d logNames in the channel\n", len(logNameChan))
			fmt.Printf("There are %d logNames in the retry channel\n", len(retryNameChan))
		}
	}()

	dumpTileInfo(filename, resultsChan)
}

func TestStatsForTileHarvest(t *testing.T) {
	f, err := os.Open(filename)
	if err != nil {
		panic (err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)

	tiles := make(map[string]TileInfo)

	for scanner.Scan() {
		var tile TileInfo
		text := scanner.Text()
		err = json.Unmarshal([]byte(text), &tile)
		if err != nil {
			fmt.Printf("Error with line '%s', skipping\n", text)
		}

		key := tile.Spec.makeKey()
		if _, existsAlready := tiles[key]; !existsAlready {
			tiles[key] = tile
		} else {
			// fmt.Printf("Tile %s exists already! -- %s\n ", key, text)
		}
	}

	fmt.Printf("There are %d keys in the map\n", len(tiles))

	statsNames := []string{"zoom7-cpu-percent", "zoom10-cpu-percent", "zoom7-mem-kb", "zoom10-mem-kb", "zoom7-time-seconds", "zoom10-time-seconds"}
	identity := func(f float64)float64 { return f}
	kbToGb := func(f float64)float64 {return f / 1E6}
	secondsToMinutes := func(f float64)float64 {return f / 60}
	statsFuncs := []func(f float64)float64{identity, identity, kbToGb, kbToGb, secondsToMinutes, secondsToMinutes}
	statsLabels := []string{"%", "%", "GB", "GB", " minutes", " minutes"}

	var count7, count10 int
	statsMap := make(map[string][]float64)
	for _, statsName := range statsNames {
		statsMap[statsName] = make([]float64, 0, 0)
	}

	for k, v := range tiles {
		if v.Spec.Zoom == 7 {
			count7++
			statsMap["zoom7-cpu-percent"] = append(statsMap["zoom7-cpu-percent"], float64(v.Stats.CPUPercent))
			statsMap["zoom7-mem-kb"] = append(statsMap["zoom7-mem-kb"], float64(v.Stats.MaxResidentKilobytes))
			statsMap["zoom7-time-seconds"] = append(statsMap["zoom7-time-seconds"], float64(v.Stats.WallTimeSeconds))
		} else if v.Spec.Zoom == 10 {
			count10++
			statsMap["zoom10-cpu-percent"] = append(statsMap["zoom10-cpu-percent"], float64(v.Stats.CPUPercent))
			statsMap["zoom10-mem-kb"] = append(statsMap["zoom10-mem-kb"], float64(v.Stats.MaxResidentKilobytes))
			statsMap["zoom10-time-seconds"] = append(statsMap["zoom10-time-seconds"], float64(v.Stats.WallTimeSeconds))
		} else {
			fmt.Printf("weird zoom for tile %s\n", k)
		}
	}

	sampleMap := make(map[string]stats.Sample)
	for _, statsName := range statsNames {
		sampleMap[statsName] = stats.Sample{
			Xs:   statsMap[statsName],
			Sorted:  false,
		}
	}

	for i, statsName := range statsNames {
		fmt.Printf("-------------start %s--------------------\n", statsName)
		for _, val := range []float64{0.0, 0.10, 0.25, 0.50, 0.75, 0.90, 0.99, 1.0} {
			quantile := sampleMap[statsName].Quantile(val)
			fmt.Printf("%0.fth percentile is %0.f or %0.2f%s\n", val * 100, quantile, statsFuncs[i](quantile), statsLabels[i])
		}
	}


	// fmt.Printf("There are %d zoom 7 tiles and %d zoom 10 tiles in the map\n", count7, count10)
}