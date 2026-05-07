package digest_test

import (
	"encoding/csv"
	"strings"
	"testing"

	"github.com/aswinkarthik/csvdiff/pkg/digest"
	"github.com/cespare/xxhash"
	"github.com/stretchr/testify/assert"
)

func TestCreateDigestWithSource(t *testing.T) {
	firstLine := "1,someline"
	firstKey := xxhash.Sum64String("1")
	firstLineDigest := xxhash.Sum64String(firstLine)

	expectedDigest := digest.Digest{
		Key:    firstKey,
		Value:  firstLineDigest,
		Source: strings.Split(firstLine, comma),
	}

	actualDigest := digest.CreateDigest(strings.Split(firstLine, comma), comma, []int{0}, []int{}, false, nil)

	assert.Equal(t, expectedDigest, actualDigest)
}

func TestDigestForFile(t *testing.T) {
	firstLine := "1,first-line,some-columne,friday"
	firstKey := xxhash.Sum64String("1")
	firstDigest := xxhash.Sum64String(firstLine)
	fridayDigest := xxhash.Sum64String("friday")

	secondLine := "2,second-line,nobody-needs-this,saturday"
	secondKey := xxhash.Sum64String("2")
	secondDigest := xxhash.Sum64String(secondLine)
	saturdayDigest := xxhash.Sum64String("saturday")

	t.Run("should create digest for given key and all values", func(t *testing.T) {
		testConfig := &digest.Config{
			Reader:    strings.NewReader(firstLine + "\n" + secondLine),
			Key:       []int{0},
			Separator: ',',
		}

		actualDigest, sourceMap, err := digest.Create(testConfig)

		expectedDigest := map[uint64]uint64{firstKey: firstDigest, secondKey: secondDigest}

		assert.NoError(t, err)
		assert.Len(t, sourceMap, 2)
		assert.Equal(t, expectedDigest, actualDigest)
	})

	t.Run("should create digest for given key and given values", func(t *testing.T) {
		testConfig := &digest.Config{
			Reader:    strings.NewReader(firstLine + "\n" + secondLine),
			Key:       []int{0},
			Value:     []int{3},
			Separator: ',',
		}

		actualDigest, _, err := digest.Create(testConfig)
		expectedDigest := map[uint64]uint64{firstKey: fridayDigest, secondKey: saturdayDigest}

		assert.NoError(t, err)
		assert.Equal(t, expectedDigest, actualDigest)
	})

	t.Run("should return ParseError if csv reading fails", func(t *testing.T) {
		testConfig := &digest.Config{
			Reader:    strings.NewReader(firstLine + "\n" + "some-random-line"),
			Key:       []int{0},
			Value:     []int{3},
			Separator: ',',
		}

		actualDigest, _, err := digest.Create(testConfig)

		assert.Error(t, err)

		_, isParseError := err.(*csv.ParseError)

		assert.True(t, isParseError)
		assert.Nil(t, actualDigest)
	})
}

func TestCreateDigest_NumericNormalization(t *testing.T) {
	baseLine := strings.Split("1,0", comma)
	deltaLine := strings.Split("1,0.0", comma)

	t.Run("same value hash when normalizeNumeric is true", func(t *testing.T) {
		base := digest.CreateDigest(baseLine, comma, []int{0}, []int{1}, true, nil)
		delta := digest.CreateDigest(deltaLine, comma, []int{0}, []int{1}, true, nil)

		assert.Equal(t, base.Key, delta.Key)
		assert.Equal(t, base.Value, delta.Value, "'0' and '0.0' should hash equally when normalized")
	})

	t.Run("different value hash when normalizeNumeric is false", func(t *testing.T) {
		base := digest.CreateDigest(baseLine, comma, []int{0}, []int{1}, false, nil)
		delta := digest.CreateDigest(deltaLine, comma, []int{0}, []int{1}, false, nil)

		assert.Equal(t, base.Key, delta.Key)
		assert.NotEqual(t, base.Value, delta.Value, "raw '0' and '0.0' should hash differently by default")
	})

	t.Run("source slice preserves original cells", func(t *testing.T) {
		delta := digest.CreateDigest(deltaLine, comma, []int{0}, []int{1}, true, nil)
		assert.Equal(t, deltaLine, delta.Source)
	})
}

func TestCreateDigest_Equivalences(t *testing.T) {
	t.Run("equivalence applied to value cells", func(t *testing.T) {
		eq := digest.NewEquivalences([][]string{{"N/A", "null", ""}}, false)

		base := digest.CreateDigest(strings.Split("1,N/A", comma), comma, []int{0}, []int{1}, false, eq)
		delta := digest.CreateDigest(strings.Split("1,null", comma), comma, []int{0}, []int{1}, false, eq)

		assert.Equal(t, base.Key, delta.Key)
		assert.Equal(t, base.Value, delta.Value, "values in same equivalence group should hash equally")
	})

	t.Run("equivalence not applied to PK cells", func(t *testing.T) {
		eq := digest.NewEquivalences([][]string{{"A", "B"}}, false)

		base := digest.CreateDigest(strings.Split("A,x", comma), comma, []int{0}, []int{1}, false, eq)
		delta := digest.CreateDigest(strings.Split("B,x", comma), comma, []int{0}, []int{1}, false, eq)

		assert.NotEqual(t, base.Key, delta.Key, "PK cells must not be canonicalized via equivalences")
	})

	t.Run("numeric normalization runs before equivalence", func(t *testing.T) {
		eq := digest.NewEquivalences([][]string{{"0", "N/A"}}, false)

		base := digest.CreateDigest(strings.Split("1,0.0", comma), comma, []int{0}, []int{1}, true, eq)
		delta := digest.CreateDigest(strings.Split("1,N/A", comma), comma, []int{0}, []int{1}, true, eq)

		assert.Equal(t, base.Value, delta.Value, "'0.0' should normalize to '0' then map to canonical via equivalence")
	})

	t.Run("source slice preserves original cells", func(t *testing.T) {
		eq := digest.NewEquivalences([][]string{{"N/A", "null"}}, false)
		line := strings.Split("1,null", comma)

		d := digest.CreateDigest(line, comma, []int{0}, []int{1}, false, eq)
		assert.Equal(t, line, d.Source)
	})
}

func TestNewConfig(t *testing.T) {
	r := strings.NewReader("a,csv,as,str")
	primaryColumns := digest.Positions{0}
	values := digest.Positions{0, 1, 2}
	include := digest.Positions{0, 1}

	t.Run("should create config from given params", func(t *testing.T) {
		conf := digest.NewConfig(r, primaryColumns, values, include, ',', false, false, nil, false)
		expectedConf := digest.Config{
			Reader:           r,
			Key:              primaryColumns,
			Value:            values,
			Include:          include,
			Separator:        ',',
			LazyQuotes:       false,
			NormalizeNumeric: false,
		}

		assert.Equal(t, expectedConf, *conf)
	})

	t.Run("should use valueColumns as includeColumns for includes not specified", func(t *testing.T) {
		conf := digest.NewConfig(r, primaryColumns, values, nil, ',', false, false, nil, false)
		expectedConf := digest.Config{
			Reader:           r,
			Key:              primaryColumns,
			Value:            values,
			Include:          values,
			Separator:        ',',
			LazyQuotes:       false,
			NormalizeNumeric: false,
		}

		assert.Equal(t, expectedConf, *conf)
	})
}
