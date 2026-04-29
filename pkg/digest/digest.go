package digest

import (
	"encoding/csv"
	"runtime"
	"sync"

	"github.com/cespare/xxhash"
)

// Digest represents the binding of the key of each csv line
// and the digest that gets created for the entire line
type Digest struct {
	Key    uint64
	Value  uint64
	Source []string
}

// CreateDigest creates a Digest for each line of csv.
// There will be one Digest per line.
//
// When normalizeNumeric is true or equivalences is non-nil, cells used in
// the row digest are canonicalized before hashing so that values the user
// considers equal produce the same value hash. Numeric normalization runs
// first (so e.g. "0.0" → "0"), then equivalence mapping (so a group like
// ["0", "N/A"] still matches "0.0" with --numeric on). The primary-key
// hash and the returned Source slice always use the original, unmodified
// cells.
func CreateDigest(csv []string, separator string, pKey Positions, pRow Positions, normalizeNumeric bool, equivalences *Equivalences) Digest {
	key := xxhash.Sum64String(pKey.Join(csv, separator))

	rowCells := csv
	if normalizeNumeric || equivalences != nil {
		rowCells = normalizedCellsForRow(csv, pRow, normalizeNumeric, equivalences)
	}
	digest := xxhash.Sum64String(pRow.Join(rowCells, separator))

	return Digest{Key: key, Value: digest, Source: csv}
}

// normalizedCellsForRow returns a shallow copy of csv with cells at the
// row-digest positions replaced by their canonical form. When pRow is
// empty, every cell is normalized (mirroring Positions.Join's "all
// columns" semantics for an empty Positions). Numeric normalization is
// applied before equivalence mapping.
func normalizedCellsForRow(csv []string, pRow Positions, normalizeNumeric bool, equivalences *Equivalences) []string {
	out := make([]string, len(csv))
	copy(out, csv)
	transform := func(cell string) string {
		if normalizeNumeric {
			cell = normalizeNumericValue(cell)
		}
		return equivalences.Canonicalize(cell)
	}
	if len(pRow) == 0 {
		for i := range out {
			out[i] = transform(out[i])
		}
		return out
	}
	for _, pos := range pRow {
		out[pos] = transform(out[pos])
	}
	return out
}

const bufferSize = 512

// Create can create a Digest using the Configurations passed.
// It returns the digest as a map[uint64]uint64.
// It can also keep track of the Source line.
func Create(config *Config) (map[uint64]uint64, map[uint64][]string, error) {
	maxProcs := runtime.NumCPU()
	reader := csv.NewReader(config.Reader)
	reader.Comma = config.Separator
	reader.LazyQuotes = config.LazyQuotes
	output := make(map[uint64]uint64)
	sourceMap := make(map[uint64][]string)

	digestChannel := make(chan []Digest, bufferSize*maxProcs)
	errorChannel := make(chan error)
	defer close(errorChannel)

	go readAndProcess(config, reader, digestChannel, errorChannel)

	for digests := range digestChannel {
		for _, digest := range digests {
			output[digest.Key] = digest.Value
			sourceMap[digest.Key] = digest.Source
		}
	}

	if err := <-errorChannel; err != nil {
		return nil, nil, err
	}

	return output, sourceMap, nil
}

func readAndProcess(config *Config, reader *csv.Reader, digestChannel chan<- []Digest, errorChannel chan<- error) {
	var wg sync.WaitGroup
	for {
		lines, eofReached, err := getNextNLines(reader)
		if err != nil {
			wg.Wait()
			close(digestChannel)
			errorChannel <- err
			return
		}

		wg.Add(1)
		go createDigestForNLines(lines, config, digestChannel, &wg)

		if eofReached {
			break
		}
	}
	wg.Wait()
	close(digestChannel)
	errorChannel <- nil
}

func createDigestForNLines(lines [][]string,
	config *Config,
	digestChannel chan<- []Digest,
	wg *sync.WaitGroup,
) {
	output := make([]Digest, len(lines))
	separator := string(config.Separator)
	for i, line := range lines {
		output[i] = CreateDigest(line, separator, config.Key, config.Value, config.NormalizeNumeric, config.Equivalences)
	}

	digestChannel <- output
	wg.Done()
}
