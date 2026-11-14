package utils

import "testing"

func TestNonNegativeDelta(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		after, before uint64
		want          uint64
	}{
		{name: "increase", after: 9, before: 4, want: 5},
		{name: "unchanged", after: 4, before: 4, want: 0},
		{name: "decrease after GC", after: 3, before: 4, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := nonNegativeDelta(tt.after, tt.before); got != tt.want {
				t.Fatalf("nonNegativeDelta(%d, %d) = %d, want %d", tt.after, tt.before, got, tt.want)
			}
		})
	}
}
