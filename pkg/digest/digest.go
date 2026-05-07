package digest

import (
	"encoding/binary"
	"encoding/csv"
	"runtime"
	"strings"
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
// Cells used in the row digest are canonicalized before hashing so values
// the user considers equal produce the same value hash. The transform
// chain on each value cell is: comma-separated multi-value cells are
// sorted (numerically when every whitespace-trimmed token parses as a
// float, else lexicographically) so that "68863,68093" matches
// "68093,68863"; then, when normalizeNumeric is true, single-number
// cells are canonicalized (so e.g. "0.0" → "0"); then, when equivalences
// is non-nil, equivalence groups are applied (so a group like
// ["0", "N/A"] still matches "0.0" with --numeric on). Multi-value
// canonicalization always runs. The primary-key hash and the returned
// Source slice always use the original, unmodified cells.
func CreateDigest(csv []string, separator string, pKey Positions, pRow Positions, normalizeNumeric bool, equivalences *Equivalences) Digest {
	key := xxhash.Sum64String(pKey.Join(csv, separator))

	rowCells := normalizedCellsForRow(csv, pRow, normalizeNumeric, equivalences)
	digest := xxhash.Sum64String(pRow.Join(rowCells, separator))

	return Digest{Key: key, Value: digest, Source: csv}
}

// CreateDigestWithOccurrence is like CreateDigest but mixes a per-file
// occurrence index into the resulting Key. This lets duplicate primary
// keys be distinguished: the Nth occurrence of a primary key in base is
// matched against the Nth occurrence in delta.
//
// The mix uses xxhash over [origKey 8 LE bytes][0x00][occurrence 4 LE bytes]
// rather than string concatenation, so it cannot collide with user-supplied
// key content.
func CreateDigestWithOccurrence(csv []string, separator string, pKey Positions, pRow Positions, occurrence uint32, normalizeNumeric bool, equivalences *Equivalences) Digest {
	d := CreateDigest(csv, separator, pKey, pRow, normalizeNumeric, equivalences)
	d.Key = mixOccurrence(d.Key, occurrence)
	return d
}

func mixOccurrence(key uint64, occurrence uint32) uint64 {
	var buf [13]byte
	binary.LittleEndian.PutUint64(buf[0:8], key)
	buf[8] = 0x00
	binary.LittleEndian.PutUint32(buf[9:13], occurrence)
	return xxhash.Sum64(buf[:])
}

// normalizedCellsForRow returns a shallow copy of csv with cells at the
// row-digest positions replaced by their canonical form. When pRow is
// empty, every cell is normalized (mirroring Positions.Join's "all
// columns" semantics for an empty Positions). Multi-value cells (those
// containing a comma) are sorted; numeric normalization runs next; then
// equivalence mapping. When no transformation would apply (no comma in
// any value cell, normalizeNumeric off, equivalences nil), csv is
// returned directly without copying.
func normalizedCellsForRow(csv []string, pRow Positions, normalizeNumeric bool, equivalences *Equivalences) []string {
	if !normalizeNumeric && equivalences == nil && !anyValueCellHasComma(csv, pRow) {
		return csv
	}
	out := make([]string, len(csv))
	copy(out, csv)
	transform := func(cell string) string {
		cell = sortMultivalueCell(cell)
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

func anyValueCellHasComma(csv []string, pRow Positions) bool {
	if len(pRow) == 0 {
		for _, c := range csv {
			if strings.IndexByte(c, ',') >= 0 {
				return true
			}
		}
		return false
	}
	for _, pos := range pRow {
		if pos >= 0 && pos < len(csv) && strings.IndexByte(csv[pos], ',') >= 0 {
			return true
		}
	}
	return false
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
