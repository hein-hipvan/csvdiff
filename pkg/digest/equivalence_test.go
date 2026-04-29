package digest

import "testing"

func TestNewEquivalences(t *testing.T) {
	t.Run("nil when no groups", func(t *testing.T) {
		if e := NewEquivalences(nil, false); e != nil {
			t.Fatalf("expected nil, got %+v", e)
		}
	})

	t.Run("nil when only single-element groups", func(t *testing.T) {
		if e := NewEquivalences([][]string{{"only"}}, false); e != nil {
			t.Fatalf("expected nil for unusable groups, got %+v", e)
		}
	})

	t.Run("nil for empty group", func(t *testing.T) {
		if e := NewEquivalences([][]string{{}}, false); e != nil {
			t.Fatalf("expected nil for empty group, got %+v", e)
		}
	})
}

func TestEquivalences_Canonicalize(t *testing.T) {
	cases := []struct {
		name       string
		groups     [][]string
		ignoreCase bool
		in         string
		want       string
	}{
		{"nil receiver passthrough", nil, false, "anything", "anything"},
		{"hit returns canonical", [][]string{{"N/A", "null", ""}}, false, "null", "N/A"},
		{"empty string member matches", [][]string{{"N/A", "null", ""}}, false, "", "N/A"},
		{"first member maps to itself", [][]string{{"N/A", "null", ""}}, false, "N/A", "N/A"},
		{"miss returns input unchanged", [][]string{{"N/A", "null"}}, false, "something", "something"},
		{"case-sensitive miss", [][]string{{"null", "N/A"}}, false, "NULL", "NULL"},
		{"case-insensitive hit", [][]string{{"null", "N/A"}}, true, "NULL", "null"},
		{"case-insensitive preserves canonical casing", [][]string{{"Null", "N/A"}}, true, "n/a", "Null"},
		{"multiple groups", [][]string{{"N/A", "null"}, {"yes", "1", "true"}}, false, "true", "yes"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var e *Equivalences
			if tc.groups != nil {
				e = NewEquivalences(tc.groups, tc.ignoreCase)
			}
			got := e.Canonicalize(tc.in)
			if got != tc.want {
				t.Errorf("Canonicalize(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestEquivalences_OverlapLastWins(t *testing.T) {
	groups := [][]string{
		{"A", "shared"},
		{"B", "shared"},
	}
	e := NewEquivalences(groups, false)
	if got := e.Canonicalize("shared"); got != "B" {
		t.Errorf("on overlap, expected last group to win (B), got %q", got)
	}
}
