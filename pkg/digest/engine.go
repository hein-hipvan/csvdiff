package digest

import (
	"encoding/csv"
	"runtime"
	"sync"

	"github.com/cespare/xxhash"
)

// Engine to create a FileDigest
type Engine struct {
	config Config
	lock   *sync.Mutex
}

// NewEngine instantiates an engine
func NewEngine(config Config) *Engine {
	return &Engine{
		config: config,
		lock:   &sync.Mutex{},
	}
}

// GenerateFileDigest generates FileDigest with thread safety
func (e Engine) GenerateFileDigest() (*FileDigest, error) {
	e.lock.Lock()
	defer e.lock.Unlock()

	fd := NewFileDigest()

	digestChannel, errorChannel := e.StreamDigests()

	for digests := range digestChannel {
		for _, digest := range digests {
			fd.Append(digest)
		}
	}

	if err := <-errorChannel; err != nil {
		return nil, err
	}

	return fd, nil
}

// StreamDigests starts creating digests in the background
// Returns 2 buffered channels, a digestChannel and an errorChannel
//
// digestChannel has all digests
// errorChannel has any errors created during processing
//
// If there are any errors while processing csv, all existing go routines
// to creates digests are waited to be closed and the digestChannel is closed at the end.
// Only after that an error is created on the errorChannel.
func (e Engine) StreamDigests() (chan []Digest, chan error) {
	maxProcs := runtime.NumCPU()
	digestChannel := make(chan []Digest, bufferSize*maxProcs)
	errorChannel := make(chan error, 1)

	go func(digestChannel chan []Digest, errorChannel chan error) {
		wg := &sync.WaitGroup{}
		reader := csv.NewReader(e.config.Reader)
		reader.Comma = e.config.Separator
		reader.LazyQuotes = e.config.LazyQuotes
		separator := string(e.config.Separator)
		var occCounts map[uint64]uint32
		if e.config.AllowDuplicateKeys {
			occCounts = make(map[uint64]uint32)
		}
		for {
			lines, eofReached, err := getNextNLines(reader)

			if err != nil {
				wg.Wait()
				close(digestChannel)
				errorChannel <- err
				close(errorChannel)
				return
			}

			var occurrences []uint32
			if e.config.AllowDuplicateKeys {
				occurrences = make([]uint32, len(lines))
				for i, line := range lines {
					origKey := xxhash.Sum64String(e.config.Key.Join(line, separator))
					occurrences[i] = occCounts[origKey]
					occCounts[origKey]++
				}
			}

			wg.Add(1)
			go e.digestForLines(lines, occurrences, digestChannel, wg)

			if eofReached {
				break
			}
		}
		wg.Wait()
		close(digestChannel)
		errorChannel <- nil
		close(errorChannel)
	}(digestChannel, errorChannel)

	return digestChannel, errorChannel

}

func (e Engine) digestForLines(lines [][]string, occurrences []uint32, digestChannel chan []Digest, wg *sync.WaitGroup) {
	output := make([]Digest, 0, len(lines))
	separator := string(e.config.Separator)
	for i, line := range lines {
		if e.config.AllowDuplicateKeys {
			output = append(output, CreateDigestWithOccurrence(line, separator, e.config.Key, e.config.Value, occurrences[i], e.config.NormalizeNumeric, e.config.Equivalences))
		} else {
			output = append(output, CreateDigest(line, separator, e.config.Key, e.config.Value, e.config.NormalizeNumeric, e.config.Equivalences))
		}
	}

	digestChannel <- output
	wg.Done()
}
