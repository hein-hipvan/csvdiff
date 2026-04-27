package digest

import "testing"

func TestNormalizeNumericValue(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"zero", "0", "0"},
		{"zero decimal", "0.0", "0"},
		{"zero trailing zeros", "0.00", "0"},
		{"negative zero", "-0", "0"},
		{"negative zero decimal", "-0.0", "0"},
		{"positive sign", "+5", "5"},
		{"trailing zero after decimal", "1.50", "1.5"},
		{"scientific exponent", "1e2", "100"},
		{"whitespace trimmed", " 0 ", "0"},
		{"whitespace around number", "  42  ", "42"},
		{"leading zero integer", "007", "7"},
		{"non-numeric", "foo", "foo"},
		{"empty string", "", ""},
		{"whitespace only", "   ", "   "},
		{"date-like", "2025-04-24", "2025-04-24"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := normalizeNumericValue(tc.in)
			if got != tc.want {
				t.Errorf("normalizeNumericValue(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
