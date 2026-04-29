package digest

import "strings"

// Equivalences holds a precomputed lookup that maps every member of an
// equivalence group to a canonical replacement (the first member of its
// group). It is consulted during row-digest creation so that user-defined
// equal values hash to the same Value.
//
// A nil *Equivalences is valid and behaves as a no-op.
//
// When ignoreCase is true the lookup keys are stored in lowercase and
// incoming cells are lowercased only for the lookup; the canonical
// replacement is emitted exactly as the user wrote it.
//
// If the same value appears in two groups, the later group wins.
type Equivalences struct {
	lookup     map[string]string
	ignoreCase bool
}

// NewEquivalences builds an *Equivalences from groups of mutually-equal
// values. Groups with fewer than two members are skipped (no-op). Returns
// nil when no usable group is provided so callers can keep the no-op fast
// path.
func NewEquivalences(groups [][]string, ignoreCase bool) *Equivalences {
	lookup := make(map[string]string)
	for _, group := range groups {
		if len(group) < 2 {
			continue
		}
		canonical := group[0]
		for _, member := range group {
			key := member
			if ignoreCase {
				key = strings.ToLower(key)
			}
			lookup[key] = canonical
		}
	}
	if len(lookup) == 0 {
		return nil
	}
	return &Equivalences{lookup: lookup, ignoreCase: ignoreCase}
}

// Canonicalize returns the canonical form of cell if it belongs to a
// configured equivalence group, otherwise it returns cell unchanged.
// Safe to call on a nil receiver.
func (e *Equivalences) Canonicalize(cell string) string {
	if e == nil {
		return cell
	}
	key := cell
	if e.ignoreCase {
		key = strings.ToLower(key)
	}
	if canonical, ok := e.lookup[key]; ok {
		return canonical
	}
	return cell
}
