package util

import "sync"

// Concurrently helps with factoring out the logic to run a particular function with multiple goroutines.
// It will be run once per concurrency level. The function output is expected to be sent to a channel as part of its implementation. This will block until all function invocations terminate.
func Concurrently(concurrency uint, thunk func()) {
	var wg sync.WaitGroup
	wg.Add(int(concurrency))
	for i := uint(0); i < concurrency; i++ {
		go func() {
			defer wg.Done()
			thunk()
		}()
	}
	wg.Wait()
}
