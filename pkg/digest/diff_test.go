package digest_test

import (
	"fmt"
	"github.com/aswinkarthik/csvdiff/pkg/digest"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestDiff(t *testing.T) {
	base := `1,col-1,col-2,col-3,one-value
2,col-1,col-2,col-3,two-value
3,col-1,col-2,col-3,three-value
100,col-1,col-2,col-3,hundred-value
`

	delta := `1,col-1,col-2,col-3,one-value
2,col-1,col-2,col-3,two-value-modified
4,col-1,col-2,col-3,four-value-added
100,col-1-modified,col-2,col-3,hundred-value-modified
5,col-1,col-2,col-3,five-value-added
`

	t.Run("default config", func(t *testing.T) {
		separators := []string{",", "\t", "|"}
		for _, sep := range separators {
			t.Run(fmt.Sprintf("should support \"%s\" as separator", sep), func(t *testing.T) {
				sepRune, _ := utf8.DecodeRuneInString(sep)
				baseConfig := &digest.Config{
					Reader:     strings.NewReader(strings.ReplaceAll(base, ",", sep)),
					Key:        []int{0},
					Separator:  sepRune,
					LazyQuotes: false,
				}

				deltaConfig := &digest.Config{
					Reader:     strings.NewReader(strings.ReplaceAll(delta,",", sep)),
					Key:        []int{0},
					Separator:  sepRune,
					LazyQuotes: false,
				}

				expected := digest.Differences{
					Additions: []digest.Addition{
						strings.Split("4,col-1,col-2,col-3,four-value-added", ","),
						strings.Split("5,col-1,col-2,col-3,five-value-added", ","),
					},
					Modifications: []digest.Modification{
						{
							Current:  strings.Split("2,col-1,col-2,col-3,two-value-modified", ","),
							Original: strings.Split("2,col-1,col-2,col-3,two-value", ","),
						},
						{
							Current:  strings.Split("100,col-1-modified,col-2,col-3,hundred-value-modified", ","),
							Original: strings.Split("100,col-1,col-2,col-3,hundred-value", ","),
						},
					},
					Deletions: []digest.Deletion{
						strings.Split("3,col-1,col-2,col-3,three-value", ","),
					},
				}

				actual, err := digest.Diff(*baseConfig, *deltaConfig)
				assert.NoError(t, err)
				assert.Equal(t, expected, actual)
			})
		}
	})

	deltaLazyQuotes := `1,col-1,col-2,col-3,one-value
2,col-1,col-2,col-3,two-value-modified
4,col-1,col-2,col-3,four"-added
100,col-1-modified,col-2,col-3,hundred-value-modified
5,col-1,col-2,col-3,five"-added
`

	t.Run("lazy quotes in delta config", func(t *testing.T) {
		baseConfig := &digest.Config{
			Reader:     strings.NewReader(base),
			Key:        []int{0},
			Separator:  ',',
			LazyQuotes: false,
		}

		deltaConfig := &digest.Config{
			Reader:     strings.NewReader(deltaLazyQuotes),
			Key:        []int{0},
			Separator:  ',',
			LazyQuotes: true,
		}

		expected := digest.Differences{
			Additions: []digest.Addition{
				strings.Split("4,col-1,col-2,col-3,four\"-added", ","),
				strings.Split("5,col-1,col-2,col-3,five\"-added", ","),
			},
			Modifications: []digest.Modification{
				{
					Current:  strings.Split("2,col-1,col-2,col-3,two-value-modified", ","),
					Original: strings.Split("2,col-1,col-2,col-3,two-value", ","),
				},
				{
					Current:  strings.Split("100,col-1-modified,col-2,col-3,hundred-value-modified", ","),
					Original: strings.Split("100,col-1,col-2,col-3,hundred-value", ","),
				},
			},
			Deletions: []digest.Deletion{
				strings.Split("3,col-1,col-2,col-3,three-value", ","),
			},
		}

		actual, err := digest.Diff(*baseConfig, *deltaConfig)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	})

	t.Run("numeric normalization treats 0 and 0.0 as equal", func(t *testing.T) {
		baseCSV := "1,0\n2,hello\n3, -0.00 \n"
		deltaCSV := "1,0.0\n2,hello\n3,0\n"

		baseConfig := &digest.Config{
			Reader:           strings.NewReader(baseCSV),
			Key:              []int{0},
			Separator:        ',',
			NormalizeNumeric: true,
		}
		deltaConfig := &digest.Config{
			Reader:           strings.NewReader(deltaCSV),
			Key:              []int{0},
			Separator:        ',',
			NormalizeNumeric: true,
		}

		actual, err := digest.Diff(*baseConfig, *deltaConfig)
		assert.NoError(t, err)
		assert.Empty(t, actual.Additions)
		assert.Empty(t, actual.Deletions)
		assert.Empty(t, actual.Modifications, "numerically-equal rows should not be reported as modifications")
	})

	t.Run("numeric normalization off reports 0 vs 0.0 as modification", func(t *testing.T) {
		baseCSV := "1,0\n"
		deltaCSV := "1,0.0\n"

		baseConfig := &digest.Config{
			Reader:    strings.NewReader(baseCSV),
			Key:       []int{0},
			Separator: ',',
		}
		deltaConfig := &digest.Config{
			Reader:    strings.NewReader(deltaCSV),
			Key:       []int{0},
			Separator: ',',
		}

		actual, err := digest.Diff(*baseConfig, *deltaConfig)
		assert.NoError(t, err)
		assert.Len(t, actual.Modifications, 1, "default behavior should still flag the difference")
	})

	t.Run("equivalence groups treat user-defined values as equal", func(t *testing.T) {
		baseCSV := "1,N/A\n2,seen\n"
		deltaCSV := "1,null\n2,seen\n"
		eq := digest.NewEquivalences([][]string{{"N/A", "null", ""}}, false)

		baseConfig := &digest.Config{
			Reader:       strings.NewReader(baseCSV),
			Key:          []int{0},
			Separator:    ',',
			Equivalences: eq,
		}
		deltaConfig := &digest.Config{
			Reader:       strings.NewReader(deltaCSV),
			Key:          []int{0},
			Separator:    ',',
			Equivalences: eq,
		}

		actual, err := digest.Diff(*baseConfig, *deltaConfig)
		assert.NoError(t, err)
		assert.Empty(t, actual.Additions)
		assert.Empty(t, actual.Deletions)
		assert.Empty(t, actual.Modifications, "values in same equivalence group should not be reported as modifications")
	})

	t.Run("equivalence does not affect primary keys", func(t *testing.T) {
		baseCSV := "N/A,row\n"
		deltaCSV := "null,row\n"
		eq := digest.NewEquivalences([][]string{{"N/A", "null"}}, false)

		baseConfig := &digest.Config{
			Reader:       strings.NewReader(baseCSV),
			Key:          []int{0},
			Separator:    ',',
			Equivalences: eq,
		}
		deltaConfig := &digest.Config{
			Reader:       strings.NewReader(deltaCSV),
			Key:          []int{0},
			Separator:    ',',
			Equivalences: eq,
		}

		actual, err := digest.Diff(*baseConfig, *deltaConfig)
		assert.NoError(t, err)
		assert.Len(t, actual.Additions, 1, "differing PKs should be treated as add+delete, not equivalence")
		assert.Len(t, actual.Deletions, 1)
	})
}

func TestDiff_AllowDuplicateKeys(t *testing.T) {
	t.Run("identical duplicates in both files produce no differences", func(t *testing.T) {
		csv := "a,1\na,2\na,3\n"

		baseConfig := digest.Config{
			Reader:             strings.NewReader(csv),
			Key:                []int{0},
			Separator:          ',',
			AllowDuplicateKeys: true,
		}
		deltaConfig := digest.Config{
			Reader:             strings.NewReader(csv),
			Key:                []int{0},
			Separator:          ',',
			AllowDuplicateKeys: true,
		}

		actual, err := digest.Diff(baseConfig, deltaConfig)
		assert.NoError(t, err)
		assert.Empty(t, actual.Additions)
		assert.Empty(t, actual.Modifications)
		assert.Empty(t, actual.Deletions)
	})

	t.Run("modifying the 2nd of three duplicates yields one modification with correct pairing", func(t *testing.T) {
		base := "a,1\na,2\na,3\n"
		delta := "a,1\na,9\na,3\n"

		baseConfig := digest.Config{
			Reader:             strings.NewReader(base),
			Key:                []int{0},
			Separator:          ',',
			AllowDuplicateKeys: true,
		}
		deltaConfig := digest.Config{
			Reader:             strings.NewReader(delta),
			Key:                []int{0},
			Separator:          ',',
			AllowDuplicateKeys: true,
		}

		actual, err := digest.Diff(baseConfig, deltaConfig)
		assert.NoError(t, err)
		assert.Empty(t, actual.Additions)
		assert.Empty(t, actual.Deletions)
		assert.Len(t, actual.Modifications, 1)
		assert.Equal(t, []string{"a", "2"}, actual.Modifications[0].Original)
		assert.Equal(t, []string{"a", "9"}, actual.Modifications[0].Current)
	})

	t.Run("an extra duplicate in delta is reported as addition not modification", func(t *testing.T) {
		base := "a,1\na,2\n"
		delta := "a,1\na,2\na,3\n"

		baseConfig := digest.Config{
			Reader:             strings.NewReader(base),
			Key:                []int{0},
			Separator:          ',',
			AllowDuplicateKeys: true,
		}
		deltaConfig := digest.Config{
			Reader:             strings.NewReader(delta),
			Key:                []int{0},
			Separator:          ',',
			AllowDuplicateKeys: true,
		}

		actual, err := digest.Diff(baseConfig, deltaConfig)
		assert.NoError(t, err)
		assert.Empty(t, actual.Modifications)
		assert.Empty(t, actual.Deletions)
		assert.Len(t, actual.Additions, 1)
		assert.Equal(t, digest.Addition{"a", "3"}, actual.Additions[0])
	})

	t.Run("a missing duplicate in delta is reported as deletion not modification", func(t *testing.T) {
		base := "a,1\na,2\na,3\n"
		delta := "a,1\na,2\n"

		baseConfig := digest.Config{
			Reader:             strings.NewReader(base),
			Key:                []int{0},
			Separator:          ',',
			AllowDuplicateKeys: true,
		}
		deltaConfig := digest.Config{
			Reader:             strings.NewReader(delta),
			Key:                []int{0},
			Separator:          ',',
			AllowDuplicateKeys: true,
		}

		actual, err := digest.Diff(baseConfig, deltaConfig)
		assert.NoError(t, err)
		assert.Empty(t, actual.Additions)
		assert.Empty(t, actual.Modifications)
		assert.Len(t, actual.Deletions, 1)
		assert.Equal(t, digest.Deletion{"a", "3"}, actual.Deletions[0])
	})

	t.Run("flag off with duplicates produces wrong pairing (regression guard)", func(t *testing.T) {
		base := "a,1\na,2\na,3\n"
		delta := "a,1\na,9\na,3\n"

		baseConfig := digest.Config{
			Reader:    strings.NewReader(base),
			Key:       []int{0},
			Separator: ',',
		}
		deltaConfig := digest.Config{
			Reader:    strings.NewReader(delta),
			Key:       []int{0},
			Separator: ',',
		}

		actual, err := digest.Diff(baseConfig, deltaConfig)
		assert.NoError(t, err)
		// Without --allow-duplicate-keys, duplicates silently overwrite each
		// other in the digest map. The resulting diff is NOT what the flag-on
		// test produced (single mod 2 → 9). Confirm the buggy default still
		// reports something different so we'd notice if defaults change.
		clean := actual.Modifications != nil &&
			len(actual.Modifications) == 1 &&
			len(actual.Modifications[0].Original) == 2 &&
			actual.Modifications[0].Original[1] == "2" &&
			len(actual.Modifications[0].Current) == 2 &&
			actual.Modifications[0].Current[1] == "9"
		assert.False(t, clean, "without --allow-duplicate-keys, duplicates should NOT produce the cleanly-paired modification")
	})

	t.Run("flag on with no duplicates matches flag-off behavior", func(t *testing.T) {
		base := "1,a\n2,b\n3,c\n"
		delta := "1,a\n2,B\n4,d\n"

		makeConfigs := func(allow bool) (digest.Config, digest.Config) {
			return digest.Config{
					Reader:             strings.NewReader(base),
					Key:                []int{0},
					Separator:          ',',
					AllowDuplicateKeys: allow,
				}, digest.Config{
					Reader:             strings.NewReader(delta),
					Key:                []int{0},
					Separator:          ',',
					AllowDuplicateKeys: allow,
				}
		}

		off, err := digest.Diff(makeConfigs(false))
		assert.NoError(t, err)
		on, err := digest.Diff(makeConfigs(true))
		assert.NoError(t, err)

		assert.ElementsMatch(t, off.Additions, on.Additions)
		assert.ElementsMatch(t, off.Modifications, on.Modifications)
		assert.ElementsMatch(t, off.Deletions, on.Deletions)
	})

	t.Run("repeated runs produce identical effective keys despite worker concurrency", func(t *testing.T) {
		var b strings.Builder
		for i := 0; i < 2000; i++ {
			b.WriteString("a,row\n")
		}
		csv := b.String()

		run := func() digest.Differences {
			baseConfig := digest.Config{
				Reader:             strings.NewReader(csv),
				Key:                []int{0},
				Separator:          ',',
				AllowDuplicateKeys: true,
			}
			deltaConfig := digest.Config{
				Reader:             strings.NewReader(csv),
				Key:                []int{0},
				Separator:          ',',
				AllowDuplicateKeys: true,
			}
			d, err := digest.Diff(baseConfig, deltaConfig)
			assert.NoError(t, err)
			return d
		}

		first := run()
		for i := 0; i < 5; i++ {
			next := run()
			assert.Empty(t, next.Additions)
			assert.Empty(t, next.Modifications)
			assert.Empty(t, next.Deletions)
			assert.Equal(t, first, next)
		}
	})
}
