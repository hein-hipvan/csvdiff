package digest

import (
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
