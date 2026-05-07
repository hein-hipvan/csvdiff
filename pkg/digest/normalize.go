package digest

import (
	"sort"
	"strconv"
	"strings"
)

// normalizeNumericValue returns a canonical representation of cell when it
// parses as a float64. Surrounding whitespace is trimmed, the value is
// re-emitted via strconv.FormatFloat with precision -1 (shortest round-trip),
// and -0 collapses to "0". Non-numeric cells are returned unchanged.
func normalizeNumericValue(cell string) string {
	trimmed := strings.TrimSpace(cell)
	if trimmed == "" {
		return cell
	}
	f, err := strconv.ParseFloat(trimmed, 64)
	if err != nil {
		return cell
	}
	if f == 0 {
		return "0"
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// sortMultivalueCell canonicalizes a value cell that packs multiple
// comma-separated tokens into a single field. Each token is whitespace-
// trimmed; if every trimmed token parses as a float the tokens are sorted
// by numeric value (so "10,2" canonicalizes to "2,10"), otherwise they are
// sorted lexicographically. Cells without a comma are returned unchanged.
func sortMultivalueCell(cell string) string {
	if strings.IndexByte(cell, ',') < 0 {
		return cell
	}
	parts := strings.Split(cell, ",")
	tokens := make([]string, len(parts))
	nums := make([]float64, len(parts))
	allNumeric := true
	for i, p := range parts {
		tokens[i] = strings.TrimSpace(p)
		if allNumeric {
			v, err := strconv.ParseFloat(tokens[i], 64)
			if err != nil {
				allNumeric = false
			} else {
				nums[i] = v
			}
		}
	}
	if allNumeric {
		idx := make([]int, len(tokens))
		for i := range idx {
			idx[i] = i
		}
		sort.SliceStable(idx, func(i, j int) bool {
			return nums[idx[i]] < nums[idx[j]]
		})
		sorted := make([]string, len(tokens))
		for i, j := range idx {
			sorted[i] = tokens[j]
		}
		tokens = sorted
	} else {
		sort.Strings(tokens)
	}
	return strings.Join(tokens, ",")
}
