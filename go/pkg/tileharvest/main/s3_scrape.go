package main

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
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	thisRunId      = "20211206-full"
	prefixLength   = 5
	chanSizePeriod = 1 * time.Minute
	reportPeriod   = 1 * time.Minute
	dumpPeriod     = 1 * time.Minute
	zoomMax        = 13
	regionName     = "us-east-1"
	bucketName     = "sc-snapzen-meta-tiles-" + regionName
	numWorkers = 128
)

var (
	tileNamesFromThisRun = regexp.MustCompile(`...../` + thisRunId + `/(\d+)/(\d+)/(\d+)\.zip`)
	extractRunId = regexp.MustCompile(`...../([A-Za-z0-9-_]+)/\d+/\d+/\d+\.zip`)
	numPrefixes          = int(math.Pow(16, prefixLength))
	missingFilename = fmt.Sprintf("%s-%s-missing.txt", thisRunId, bucketName)

	deleteOld = false
	// nothing will be deleted if above is false
	oldIfBefore = time.Date(2021, time.September, 2, 0, 0, 0,0, time.Local)
)

func main() {
	deletedTilesTracker := newDeletedTilesTracker()
	if deleteOld {
		fmt.Println("!!!!! This will delete tiles !!!!")
	} else {
		fmt.Println("Dry run.  Won't delete tiles")
	}

	missingTiles := newMissingTiles(zoomMax, thisRunId, &deletedTilesTracker)
	missingTiles.report()

	s3 := makeS3()

	var workerWaitGroup sync.WaitGroup
	workerWaitGroup.Add(numWorkers)

	prefixChan := make(chan string, numPrefixes)
	for i := 0; i < numWorkers; i++ {
		makePrefixWorker(i + 1, missingTiles, prefixChan, s3, workerWaitGroup, &deletedTilesTracker)
	}

	loadPrefixChan(prefixChan, missingTiles)

	trackPercentComplete(prefixChan, missingTiles.done)

	workerWaitGroup.Wait()
	fmt.Printf("Done waiting on workers")

	deletedTilesTracker.markDone()

	missingTiles.markDone()
	missingTiles.writeToFile()


}

func trackPercentComplete(chanOfChoice chan string, done chan bool) {
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

type PrefixWorker struct {
	id           int
	missingTiles MissingTiles
	prefixChan   chan string
	s3           *s3.S3
	doneChan     chan bool
	deletedTiles *DeletedTilesTracker
}

func makePrefixWorker(id int, tiles MissingTiles, prefixChan chan string, s3 *s3.S3, group sync.WaitGroup, tracker *DeletedTilesTracker) PrefixWorker {
	worker := PrefixWorker{
		id:           id,
		missingTiles: tiles,
		prefixChan:   prefixChan,
		s3:           s3,
		deletedTiles: tracker,
	}

	go func() {
		var started bool
		sleepCount := 0
		for true {

			select {
			case prefix := <-worker.prefixChan:
				if rand.Intn(102) != 1 {
					worker.prefixChan <- prefix
					break
				}
				err := worker.processPrefix(prefix)
				if err != nil {
					fmt.Printf("Re-enqueueing prefix %s due to an error\n", prefix)
					worker.prefixChan <- prefix
				}
				started = true
			default:
				if sleepCount > 10 {
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

func (p *PrefixWorker) processPrefix(thisPrefix string) error {
	// if rand.Intn(numWorkers) == 1 {
	// 	fmt.Printf("%d: Trying to find prefix %s\n", p.id, thisPrefix)
	// }

	resultChan := make(chan []*s3.Object)
	deleteChan := make(chan []*s3.Object)
	go func() {
		//	resultCount := 0
		for result := range resultChan {
			toDelete := make([]*s3.Object, 0, 0)
			for _, obj := range result {
				name := *obj.Key
				if strings.Contains(name, p.missingTiles.runId) {
					match := tileNamesFromThisRun.FindStringSubmatch(name)
					zoom := mustAtoi(match[1])
					x := mustAtoi(match[2])
					y := mustAtoi(match[3])
					foundCoord := fmt.Sprintf("%d/%d/%d", zoom, x, y)
					p.missingTiles.markFound(foundCoord)
				} else if obj.LastModified.Before(oldIfBefore) {
					toDelete = append(toDelete, obj)
				}
			}
			deleteChan <- toDelete
		}
		close(deleteChan)
	}()


	go func() {
		runIDCounts := make(map[string]int)
		for objsToDelete := range deleteChan {
			objIDs := make([]*s3.ObjectIdentifier, 0, len(objsToDelete))
			for _, obj := range objsToDelete {
				objIDs = append(objIDs, &s3.ObjectIdentifier{
					Key:       obj.Key,
				})

				submatch := extractRunId.FindStringSubmatch(*obj.Key)
				if len(submatch) < 2 {
					fmt.Printf("!!!!!!!!! Error extracting runId from key %s !!!!!!!!!\n", *obj.Key)
					continue
				}
				runIDCounts[submatch[1]]++
			}

			if deleteOld && len(objIDs) > 0 {
				if len(objIDs) > 1000 {
					fmt.Println("More than 1000 to delete for prefix " + thisPrefix)
				}
				d := &s3.DeleteObjectsInput{
					Bucket:                    aws.String(bucketName),
					Delete: &s3.Delete{
						Objects: objIDs,
						Quiet: aws.Bool(true),
					},
				}

				maxRetries := 10
				i := 0
				for ; i < maxRetries; i++ {
					_, err := p.s3.DeleteObjects(d)

					if err == nil && i > 0 {
						p.deletedTiles.countSuccessOnRetry()
						break
					}
					sleepMultiplier := time.Duration(math.Pow(2, float64(i)))
					time.Sleep(sleepMultiplier * 10 * time.Millisecond)
				}

				if i == maxRetries {
					p.deletedTiles.countErrorOnRetry()
					p.deletedTiles.countError()
					fmt.Printf("!! Unfixable error on prefix %s after %d attempts\n", thisPrefix, maxRetries)
				} else {
					p.deletedTiles.countDeleteBatches(true)
					p.deletedTiles.addDeletedCount(runIDCounts)
				}
			} else if len(objIDs) == 0 {
				p.deletedTiles.countDeleteBatches(false)
			}
		}
	}()

	return queryForPrefix(thisPrefix, p.s3, resultChan)
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

	// Commented this out because I wanted more granular chunks that could still be deleted
	// var prefixParamString string
	// if prefixLength < 5 {
	// 	prefixParamString = prefix
	// } else {
	// 	prefixParamString = prefix + "/" + thisRunId
	// }

	prefixParamString := prefix

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

func mustAtoi(coord string) int {
	atoi, err := strconv.Atoi(coord)
	if err!= nil {
		fmt.Errorf("error turning %s into an int", coord)
		os.Exit(1)
	}
	return atoi
}

type DeletedTilesTracker struct {
	PrefixMap              map[string]int
	deletedChan            chan map[string]int
	doneChan               chan bool
	lock                   sync.Mutex
	nothingToDeleteCount   uint32
	somethingToDeleteCount uint32
	errorCount             uint32
	errorOnRetryCount      uint32
	successOnRetryCount    uint32
}

func (t DeletedTilesTracker) report() {
	fmt.Printf("===>\n")

	batchCount := float32(t.somethingToDeleteCount + t.nothingToDeleteCount + t.errorCount)
	deleteBatchPercent := float32(t.somethingToDeleteCount) / batchCount * 100
	errorBatchPercent := float32(t.errorCount) / batchCount * 100
	retrySuccessBatchPercent := float32(t.successOnRetryCount) / float32(t.errorOnRetryCount + t.successOnRetryCount) * 100

	t.lock.Lock()
	fmt.Printf("Delete Summary: %0.1E tiles so far. \n", float32(t.count()))
	fmt.Printf("Batches: delete %d, nothing %d, percent deletes %2.2f\n", t.somethingToDeleteCount, t.nothingToDeleteCount, deleteBatchPercent)
	fmt.Printf("Error on delete Batches: %d, percent %2.2f\n", t.errorCount, errorBatchPercent)
	fmt.Printf("Retry on delete success: %d, error %d, percent retry successful %2.2f\n", t.successOnRetryCount, t.errorOnRetryCount, retrySuccessBatchPercent)

	strings := make([]string, 0, len(t.PrefixMap))
	for prefix, count := range t.PrefixMap {
		str := fmt.Sprintf("---%s | %0.1E \n", prefix, float64(count))
		strings = append(strings, str)
	}

	// sort these things so I can tell everything is ok faster
	sort.Strings(strings)
	for _, s := range strings {
		fmt.Print(s)
	}

	t.lock.Unlock()
	fmt.Printf("<===\n")
}

func (t DeletedTilesTracker) count() int {
	sum := 0
	for _, count := range t.PrefixMap {
		sum += count
	}

	return sum
}

func (t *DeletedTilesTracker) addDeletedCount(prefixMap map[string]int) {
	//t.deletedChan <- prefixMap
}

func newDeletedTilesTracker() DeletedTilesTracker {
	deletedTiles := DeletedTilesTracker{
		PrefixMap:   make(map[string]int),
		deletedChan: make(chan map[string]int, 1000000),
		doneChan:    make(chan bool),
		lock:        sync.Mutex{},
	}

	go func() {
		for true {
			select {
			case toCount := <-deletedTiles.deletedChan:
				deletedTiles.lock.Lock()
				for prefix, count := range toCount {
					deletedTiles.PrefixMap[prefix] += count
				}
				deletedTiles.lock.Unlock()
			case <- deletedTiles.doneChan:
				fmt.Println("No longer counting deleted tiles")
				return
			default:
				time.Sleep(1 * time.Second)
			}
		}
	}()

	return deletedTiles
}

func (t *DeletedTilesTracker) markDone() {
	waitCount := 2
	for i := 0; i < waitCount; i++ {
		t.doneChan <- true
	}
}

func (t *DeletedTilesTracker) countDeleteBatches(hadSomething bool) {
	if hadSomething {
		atomic.AddUint32(&t.somethingToDeleteCount, 1)
	} else {
		atomic.AddUint32(&t.nothingToDeleteCount, 1)
	}
}

func (t *DeletedTilesTracker) countError() {
	atomic.AddUint32(&t.errorCount, 1)
}

func (t *DeletedTilesTracker) countErrorOnRetry() {
	atomic.AddUint32(&t.errorOnRetryCount, 1)
}

func (t *DeletedTilesTracker) countSuccessOnRetry() {
	atomic.AddUint32(&t.successOnRetryCount, 1)
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

func newMissingTiles(zoomInclusive int, runId string, tracker *DeletedTilesTracker) MissingTiles {
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
				case <- tiles.done:
					fmt.Println("Done marking tiles found")
					return
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
				tracker.report()
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

func (m *MissingTiles) markDone() {
	waitCount := 2
	for i := 0; i < waitCount; i++ {
		m.done <- true
	}
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

	outputFile, _ := os.CreateTemp(".", missingFilename)
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

	fmt.Printf("--Done writing %d lines to output file %s.  Took %0.1f minutes \n", len(list), outputFile.Name(), time.Now().Sub(start).Minutes())
	os.Rename("./" + outputFile.Name(), "./" + missingFilename)
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
		fmt.Printf("---%d | %d | %d | %0.2f%%\n", z, foundCount, missingCount, percentDone)
		m.locks[z].RUnlock()
	}
	fmt.Printf("---Total | %0.2E | %0.2E | %0.f%%\n", float64(foundSum), float64(missingSum), float64(foundSum)/float64(expectedSum)*100)
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

func (m *MissingTiles) markFound(coordString string) {
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