package tileharvest

import (
	"bufio"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"math"
	"math/rand"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	thisRunId      = "20210903"
	prefixLength   = 5
	chanSizePeriod = 1 * time.Minute
	reportPeriod   = 1 * time.Minute
	dumpPeriod     = 8 * time.Minute
	zoomMax        = 13
	regionName     = "us-east-1"
	bucketName     = "sc-snapzen-meta-tiles-" + regionName
	numWorkers = 2048
)

var (
	re = regexp.MustCompile(`...../` + thisRunId + `/(\d+)/(\d+)/(\d+)\.zip`)
	numPrefixes    = int(math.Pow(16, prefixLength))
	missingFilename = fmt.Sprintf("%s-%s-missing.txt", thisRunId, bucketName)
)


func TestCountFoundS3(t *testing.T) {
	missingTiles := newMissingTiles(zoomMax, thisRunId)
	missingTiles.report()

	s3 := makeS3()

	var workerWaitGroup sync.WaitGroup
	workerWaitGroup.Add(numWorkers)

	prefixChan := make(chan string, numPrefixes)
	for i := 0; i < numWorkers; i++ {
		makePrefixWorker(i + 1, missingTiles, prefixChan, s3, workerWaitGroup)
	}

	loadPrefixChan(prefixChan, missingTiles)

	trackPercentComplete(missingTiles, prefixChan, missingTiles.done)

	workerWaitGroup.Wait()
	fmt.Printf("Done waiting on workers")

	missingTiles.done <- true
	missingTiles.writeToFile()
}

func trackPercentComplete(missingTiles MissingTiles, chanOfChoice chan string, done chan bool) {
	var startingChanSize = numPrefixes
	chanSizeTicker := time.NewTicker(chanSizePeriod)
	go func() {
		startTime := time.Now()
		for {
			select {
			case <-chanSizeTicker.C:
				currentChanSize := len(chanOfChoice)
				percentRemaining := float64(currentChanSize) * 100 / float64(startingChanSize)
				minutesElapsed := time.Now().Sub(startTime).Minutes()
				fmt.Printf("****Channel Size %d / %d - %2.3f%% remaining after %0.f minutes****\n",
					currentChanSize, startingChanSize, percentRemaining, minutesElapsed)
			case <-done:
				minutesElapsed := time.Now().Sub(startTime).Minutes()
				fmt.Printf("Channel complete after %0.f minutes\n", minutesElapsed)
				return
			}
		}
	}()
}

type CoordWorker struct {
	id           int
	missingTiles MissingTiles
	coordChan    chan string
	s3           *s3.S3
	doneChan 	 chan bool
}

type PrefixWorker struct {
	id           int
	missingTiles MissingTiles
	prefixChan    chan string
	s3           *s3.S3
	doneChan 	 chan bool
}

func makePrefixWorker(id int, tiles MissingTiles, prefixChan chan string, s3 *s3.S3, group sync.WaitGroup) PrefixWorker {
	worker := PrefixWorker{
		id:           id,
		missingTiles: tiles,
		prefixChan:   prefixChan,
		s3:           s3,
	}

	go func() {
		var started bool
		sleepCount := 0
		for true {

			select {
			case prefix := <-worker.prefixChan:
				err := worker.processPrefix(prefix)
				if err != nil {
					fmt.Printf("Re-enqueueing prefix %s due to an error\n", prefix)
					worker.prefixChan <- prefix
				}
				started = true
			default:
				if sleepCount > 3 {
					fmt.Printf("Worker %d never got any work. Stopping\n", worker.id)
					group.Done()
					return
				} else if !started {
					fmt.Printf("Worker %d waiting to start \n", worker.id)
					time.Sleep(1 * time.Second)
					sleepCount++
				} else {
					fmt.Printf("Worker %d done\n", worker.id)
					group.Done()
					return
				}
			}
		}
	}()

	return worker
}


func makeCoordWorker(id int, tiles MissingTiles, coordChan chan string, s3 *s3.S3, group sync.WaitGroup) CoordWorker {
	worker := CoordWorker{
		id:           id,
		missingTiles: tiles,
		coordChan:    coordChan,
		s3:           s3,
	}

	go func() {
		var started bool
		for true {
			select {
			case coord := <-worker.coordChan:
				worker.processCoord(coord)
				started = true
			default:
				if !started {
					fmt.Printf("Worker %d waiting to start \n", worker.id)
					time.Sleep(1 * time.Second)
				} else {
					group.Done()
					return
				}
			}
		}
	}()

	return worker
}

func (p *PrefixWorker) processPrefix(thisPrefix string) error {
	if rand.Intn(numWorkers) == 1 {
		fmt.Printf("%d: Trying to find prefix %s\n", p.id, thisPrefix)
	}

	resultChan := make(chan []*s3.Object)
	go func() {
		//	resultCount := 0
		for result := range resultChan {
			for _, obj := range result {
				name := *obj.Key
				if strings.Contains(name, p.missingTiles.runId) {
					match := re.FindStringSubmatch(name)
					zoom := mustAtoi(match[1])
					x := mustAtoi(match[2])
					y := mustAtoi(match[3])
					foundCoord := fmt.Sprintf("%d/%d/%d", zoom, x, y)
					//TODO: check zoom here for faster debugging at lower zoom inclusives
					p.missingTiles.remove(foundCoord)
				}
			}
		}
	}()

	return queryForPrefix(thisPrefix, p.s3, resultChan)
}

func (c *CoordWorker) processCoord(thisCoord string) {
	if !c.missingTiles.contains(thisCoord) {
		return
	}

	if rand.Intn(1000) == 1 {
		fmt.Printf("%d: Trying to find %s\n", c.id, thisCoord)
	}

	objects := queryForCoord(thisCoord, c.s3, c.missingTiles.runId)
	if objects == nil {
		return
	}

	for _, obj := range objects.Contents {
		name := *obj.Key
		if strings.Contains(name, c.missingTiles.runId) {
			match := re.FindStringSubmatch(name)
			zoom := mustAtoi(match[1])
			x := mustAtoi(match[2])
			y := mustAtoi(match[3])
			foundCoord := fmt.Sprintf("%d/%d/%d", zoom, x, y)

			c.missingTiles.remove(foundCoord)
		}
	}
}

func makeS3() *s3.S3 {
	os.Setenv("AWS_PROFILE", "snapzen")

	region := aws.String(regionName)
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region:     region,
			MaxRetries: aws.Int(10),
			Retryer: client.DefaultRetryer{
				NumMaxRetries:    5,
				MaxRetryDelay:    5 * time.Second,
				MaxThrottleDelay: 30 * time.Second,
			},
			CredentialsChainVerboseErrors: aws.Bool(true),
		},
		SharedConfigState: session.SharedConfigEnable,
	}))

	s := s3.New(sess)
	return s
}

func extractIntCoords(coordStr string) (int, int, int) {
	coord := strings.Split(coordStr, "/")
	return mustAtoi(coord[0]), mustAtoi(coord[1]), mustAtoi(coord[2])
}

func queryForPrefix(prefix string, s *s3.S3, resultChan chan []*s3.Object) error {
	defer close(resultChan)

	var prefixParamString string
	if prefixLength < 5 {
		prefixParamString = prefix
	} else {
		prefixParamString = prefix + "/" + thisRunId
	}

	nextMarker := aws.String("")
	for {
		tempObjects, err := s.ListObjects(&s3.ListObjectsInput{
			Bucket:  aws.String(bucketName),
			Prefix:  aws.String(prefixParamString),
			Marker: nextMarker,
		})

		if err != nil {
			return err
			break
		}

		resultChan <- tempObjects.Contents

		if !*tempObjects.IsTruncated {
			break
		}
		nextMarker = tempObjects.Contents[len(tempObjects.Contents) - 1].Key
	}

	return nil
}


func queryForCoord(coordStr string, s *s3.S3, runId string) *s3.ListObjectsOutput {
	z, x, y := extractIntCoords(coordStr)
	prefix := s3Hash(z, x, y)

	objects, err := s.ListObjects(&s3.ListObjectsInput{
		Bucket:  aws.String( bucketName),
		Marker:  aws.String(""),
		MaxKeys: aws.Int64(5000),
		Prefix:  aws.String(prefix + "/" + runId),
	})

	if err != nil {
		fmt.Println("error talking to s3 for coord %s" + coordStr)
		return nil
	}
	return objects
}

func mustAtoi(coord string) int {
	atoi, err := strconv.Atoi(coord)
	if err!= nil {
		fmt.Errorf("error turning %s into an int", coord)
		os.Exit(1)
	}
	return atoi
}

type MissingTiles struct {
	TilesMap      []map[string]bool
	runId         string
	zoomInclusive int
	removeChan    chan string
	done          chan bool
	foundCount    int
	locks         []sync.RWMutex
	writeLock	  sync.Mutex
	fromFile	  bool
}


func newMissingTiles(zoomInclusive int, runId string) MissingTiles {
	tiles := MissingTiles{
		TilesMap:      nil,
		runId:         runId,
		zoomInclusive: zoomInclusive,
		removeChan:    make(chan string, 10000000),
		done:          make(chan bool),
	}

	if tiles.restoreMissingTilesFromFile() {
		tiles.fromFile = true
	} else {
		tiles.TilesMap = initMissingTiles(zoomInclusive)
	}

	tiles.locks = make([]sync.RWMutex, zoomInclusive + 1, zoomInclusive + 1)

	processRemoves := func() {
		for true {
			select {
				case toRemove := <-tiles.removeChan:
					tiles._remove(toRemove)
				default:
					time.Sleep(1 * time.Second)
			}
		}
	}
	go processRemoves()

	reportTicker := time.NewTicker(reportPeriod)
	dumpTicker := time.NewTicker(dumpPeriod)
	go func() {
		for {
			select {
			case <-tiles.done:
				fmt.Println("Stopping ticker")
				reportTicker.Stop()
				dumpTicker.Stop()
				return
			case <-reportTicker.C:
				tiles.report()
			case <- dumpTicker.C:
				go func() {
					tiles.writeToFile()
				}()
			}
		}
	}()

	return tiles
}

func initMissingTiles(zoomInclusive int) []map[string]bool {
	fmt.Println("Initing a new tile set")
	missingTiles := make([]map[string]bool, 0, zoomInclusive)
	for z := 0; z <= zoomInclusive; z++ {
		mapForThisZoom := make(map[string]bool, int(math.Pow(4, float64(z))))
		missingTiles = append(missingTiles, mapForThisZoom)
		for x := 0; x < int(math.Pow(2, float64(z))); x++ {
			for y := 0; y < int(math.Pow(2, float64(z))); y++ {
				coordString := fmt.Sprintf("%d/%d/%d", z, x, y)
				missingTiles[z][coordString] = true
			}
		}
		fmt.Printf("---Done initing zoom %d\n", z)
	}
	return missingTiles
}

func (m *MissingTiles) writeToFile() {
	// this is so gross
	startedWaiting := time.Now()

	m.writeLock.Lock()
	defer m.writeLock.Unlock()

	doneWaiting := time.Now()
	waitTime := doneWaiting.Sub(startedWaiting)

	list := m.list()

	if waitTime > dumpPeriod {
		fmt.Printf("--Skipping Writing %d tiles to file after waiting %0.1f minutes\n", len(list), waitTime.Minutes())
		return
	}

	outputFile, _ := os.Create(missingFilename)
	defer outputFile.Close()

	start := time.Now()
	fmt.Printf("--Writing %d lines to %s\n", len(list), missingFilename)

	if len(list) == 0 {
		fmt.Printf("Looks like we found everything!\n")
		outputFile.WriteString("Found everything!!")
	}

	for _, coordStr := range list {
		outputFile.WriteString(coordStr + "\n")
	}
	fmt.Printf("--Done writing %d lines to output file.  Took %0.1f minutes \n", len(list), time.Now().Sub(start).Minutes())


}

func (m *MissingTiles) restoreMissingTilesFromFile() bool {
	open, err := os.Open(missingFilename)
	if err != nil {
		return false
	}
	defer open.Close()

	m.TilesMap = make([]map[string]bool, 0, m.zoomInclusive)
	for z := 0; z <= m.zoomInclusive; z++ {
		mapForThisZoom := make(map[string]bool, int(math.Pow(4, float64(z))))
		m.TilesMap = append(m.TilesMap, mapForThisZoom)
	}

	scanner := bufio.NewScanner(open)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		zoom := mustAtoi(strings.Split(line, "/")[0])

		m.TilesMap[zoom][line] = true
	}

	return true
}

func (m *MissingTiles) report() {


	fmt.Printf("--->\n")
	fmt.Printf("Summary: Found a total of %d tiles we were looking for so far\n", m.foundCount)
	var foundSum, missingSum, expectedSum int
	for z, missing := range m.TilesMap {
		m.locks[z].RLock()
		expectedCount := int(math.Pow(4, float64(z)))
		missingCount := len(missing)
		foundCount := expectedCount- missingCount
		percentDone := float64(foundCount) / float64(expectedCount) * 100

		foundSum += foundCount
		missingSum += missingCount
		expectedSum += expectedCount
		fmt.Printf("---%d | %d | %d | %0.f%%\n", z, foundCount, missingCount, percentDone)
		m.locks[z].RUnlock()
	}
	fmt.Printf("---Total | %d | %d | %0.f%%\n", foundSum, missingSum, float64(foundSum)/float64(expectedSum)*100)
	fmt.Printf("<---\n")


}

func (m *MissingTiles) length() int {
	length := 0
	for z, missing := range m.TilesMap {
		m.locks[z].RLock()
		length += len(missing)
		m.locks[z].RUnlock()
	}
	return length
}

func (m *MissingTiles) remove(coordString string) {
	m.removeChan <- coordString
}

func (m *MissingTiles) _remove(coordString string) {
	zoom := mustAtoi(strings.Split(coordString, "/")[0])

	if zoom >= len(m.TilesMap) {
		return
	}
	m.foundCount++

	m.locks[zoom].Lock()

	m.unsafeRemove(coordString, zoom)

	m.locks[zoom].Unlock()
}

func (m *MissingTiles) unsafeRemove(coordString string, zoom int) {
	if _, ok := m.TilesMap[zoom][coordString]; ok {
		delete((m.TilesMap)[zoom], coordString)
	}
}

func (m *MissingTiles) contains(coordString string) bool {
	zoom := mustAtoi(strings.Split(coordString, "/")[0])

	m.locks[zoom].RLock()
	defer m.locks[zoom].RUnlock()
	_, contains := (m.TilesMap)[zoom][coordString]

	return contains
}

func (m *MissingTiles) list() []string {
	list := make([]string, 0, m.length())

	for z, missing := range m.TilesMap {
		m.locks[z].RLock()
		for coordStr, _ := range missing {
			list = append(list, coordStr)
		}
		m.locks[z].RUnlock()
	}

	return list
}

func loadPrefixChan(prefixChan chan string, tiles MissingTiles) {
	if tiles.fromFile {
		loadFromMissingList(tiles, prefixChan)
	} else {
		loadEntireSet(prefixChan)
	}
}

func loadFromMissingList(tiles MissingTiles, prefixChan chan string) {
	prefixMap := make(map[string]bool, 0)
	for _, tile := range tiles.list() {
		z, x, y := extractIntCoords(tile)
		prefix := s3Hash(z, x, y)[0:prefixLength]
		if _, found := prefixMap[prefix]; !found {
			prefixMap[prefix] = true
			prefixChan <- prefix
		}
	}
}

func loadEntireSet(prefixChan chan string) {
	endIndex := 16
	startIndex := endIndex - prefixLength
	for i := uint64(0); i < uint64(numPrefixes); i++ {
		bytes := make([]byte, 8)
		binary.BigEndian.PutUint64(bytes, uint64(i))
		prefix := hex.EncodeToString(bytes)[startIndex:endIndex]
		prefixChan <- prefix
	}
}

func (m *MissingTiles) listToChan(coordChan chan string) {

	for z := 0; z < len(m.TilesMap); z++ {
		missing := m.TilesMap[z]
		m.locks[z].RLock()
		for coordStr, _ := range missing {
			coordChan <- coordStr
		}
		m.locks[z].RUnlock()
	}

}

func (m *MissingTiles) lockAll() {
	for _, lock := range m.locks {
		lock.Lock()
	}

}

func (m *MissingTiles) unlockAll() {
	for _, lock := range m.locks {
		lock.Unlock()
	}
}

func s3Hash(zoom int, x int, y int) string {
	toHash := fmt.Sprintf("%d/%d/%d.%s", zoom, x, y, "zip")
	hash := md5.Sum([]byte(toHash))

	return fmt.Sprintf("%x", hash)[0:5]
}