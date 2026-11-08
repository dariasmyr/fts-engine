package semantic

import (
	"math"
	"testing"
)

func TestCandidateMergeCapacity(t *testing.T) {
	tests := []struct {
		name                string
		liveCount           int
		candidatesPerSearch int
		queryCount          int
		segmentCount        int
		expected            int
	}{
		{name: "single query and segment", liveCount: 4096, candidatesPerSearch: 16, queryCount: 1, segmentCount: 1, expected: 16},
		{name: "multiple queries and segments", liveCount: 4096, candidatesPerSearch: 16, queryCount: 2, segmentCount: 4, expected: 128},
		{name: "bounded by live vectors", liveCount: 100, candidatesPerSearch: 16, queryCount: 2, segmentCount: 4, expected: 100},
		{name: "overflow protected", liveCount: math.MaxInt, candidatesPerSearch: math.MaxInt, queryCount: math.MaxInt, segmentCount: math.MaxInt, expected: math.MaxInt},
		{name: "no active segments", liveCount: 100, candidatesPerSearch: 16, queryCount: 1, segmentCount: 0, expected: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := candidateMergeCapacity(
				test.liveCount,
				test.candidatesPerSearch,
				test.queryCount,
				test.segmentCount,
			)
			if got != test.expected {
				t.Fatalf("candidateMergeCapacity() = %d, want %d", got, test.expected)
			}
		})
	}
}
