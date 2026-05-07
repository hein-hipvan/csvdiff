package digest

import "io"

// Config represents configurations that can be passed
// to create a Digest.
//
// Key: The primary key positions
// Value: The Value positions that needs to be compared for diff
// Include: Include these positions in output. It is Value positions by default.
// NormalizeNumeric: When true, numerically-equal value cells hash equally
// (e.g. "0" == "0.0"). Primary-key cells are unaffected.
// Equivalences: Optional precomputed lookup of user-defined equal values.
// Applied to value cells only. Primary-key cells are unaffected. nil is a
// valid no-op.
// AllowDuplicateKeys: When true, the Nth occurrence of a primary key in
// each file is treated as a distinct row. The 1st occurrence in base is
// matched against the 1st occurrence in delta, the 2nd against the 2nd,
// and so on. Without this flag, duplicate primary keys silently overwrite
// each other in the digest map.
type Config struct {
	Key                Positions
	Value              Positions
	Include            Positions
	Reader             io.Reader
	Separator          rune
	LazyQuotes         bool
	NormalizeNumeric   bool
	Equivalences       *Equivalences
	AllowDuplicateKeys bool
}

// NewConfig creates an instance of Config struct.
func NewConfig(
	r io.Reader,
	primaryKey Positions,
	valueColumns Positions,
	includeColumns Positions,
	separator rune,
	lazyQuotes bool,
	normalizeNumeric bool,
	equivalences *Equivalences,
	allowDuplicateKeys bool,
) *Config {
	if len(includeColumns) == 0 {
		includeColumns = valueColumns
	}

	return &Config{
		Reader:             r,
		Key:                primaryKey,
		Value:              valueColumns,
		Include:            includeColumns,
		Separator:          separator,
		LazyQuotes:         lazyQuotes,
		NormalizeNumeric:   normalizeNumeric,
		Equivalences:       equivalences,
		AllowDuplicateKeys: allowDuplicateKeys,
	}
}
