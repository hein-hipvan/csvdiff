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

func TestSortMultivalueCell(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"no comma noop", "abc", "abc"},
		{"empty noop", "", ""},
		{"already sorted numeric", "68093,68863", "68093,68863"},
		{"reordered numeric", "68863,68093", "68093,68863"},
		{"numeric beats lex", "10,2", "2,10"},
		{"three numbers", "30,10,20", "10,20,30"},
		{"trim whitespace numeric", " 68863, 68093", "68093,68863"},
		{"lex sort", "b,a,c", "a,b,c"},
		{"lex sort already sorted", "a,b,c", "a,b,c"},
		{"mixed numeric and string", "1,abc", "1,abc"},
		{"mixed reordered", "abc,1", "1,abc"},
		{"floats", "1.5,0.5,1.25", "0.5,1.25,1.5"},
		{"empty token forces lex", "a,,b", ",a,b"},
		{"trailing comma", "abc,", ",abc"},
		{"negative numbers", "1,-1,0", "-1,0,1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sortMultivalueCell(tc.in)
			if got != tc.want {
				t.Errorf("sortMultivalueCell(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
