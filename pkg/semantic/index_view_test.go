package semantic

import (
	"context"
	"errors"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"
)

func TestIndexViewRejectsSearchOverridesAboveConfiguredMaximum(t *testing.T) {
	index := newTestIndex(t)
	tests := []struct {
		name    string
		options QueryConfig
	}{
		{name: "candidate chunks", options: QueryConfig{CandidateChunks: 17}},
		{name: "ef search", options: QueryConfig{EfSearch: 17}},
		{name: "visit limit", options: QueryConfig{VisitLimit: 33}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := index.Search(
				t.Context(),
				[]ChunkEmbedding{testChunk("query", "q-1", 1, 0)},
				1,
				test.options,
			)
			if !errors.Is(err, ErrInvalidQueryConfig) {
				t.Fatalf("Search() error = %v, want ErrInvalidQueryConfig", err)
			}
		})
	}
}

func TestIndexViewRejectsRequiredEfSearchAboveConfiguredMaximum(t *testing.T) {
	t.Parallel()

	index, err := NewIndex(IndexConfig{
		Dimensions: 2,
		Metric:     vector.MetricL2Squared,
		Build:      hnsw.BuildConfig{MaxNeighbors: 2, EfConstruction: 4, Seed: 1},
		Operational: OperationalLimits{
			Capacity:       CapacityLimits{MaxLiveVectors: 32, MaxStaleVectors: 32, MaxSegments: 8},
			Ingestion:      IngestionLimits{MaxChunksPerDocument: 4},
			Search:         SearchLimits{MaxResults: 4, MaxCandidateChunks: 30, MaxChunksPerResult: 2, MaxEfSearch: 20},
			SearchDefaults: hnsw.SearchConfig{EfSearch: 8, VisitLimit: 32},
		},
	})
	if err != nil {
		t.Fatalf("NewIndex() error = %v", err)
	}
	_, err = index.Search(
		t.Context(),
		[]ChunkEmbedding{testChunk("query", "q-1", 1, 0)},
		1,
		QueryConfig{CandidateChunks: 21, EfSearch: 8},
	)
	if !errors.Is(err, ErrInvalidQueryConfig) {
		t.Fatalf("Search() error = %v, want ErrInvalidQueryConfig", err)
	}
}

func TestIndexViewVisitBudgetIsSharedAcrossSegments(t *testing.T) {
	t.Parallel()

	index := newTestIndex(t)
	addTestDocument(t, index, "doc-a", testChunk("doc-a", "a-1", 0, 0))
	mustCommit(t, index)
	addTestDocument(t, index, "doc-b", testChunk("doc-b", "b-1", 1, 0))
	mustCommit(t, index)

	tests := []struct {
		name            string
		visitLimit      int
		candidateChunks int
		wantHits        int
		wantVisited     int
		wantIncomplete  bool
		wantTermination string
	}{
		{
			name:            "one visit reaches one segment",
			visitLimit:      1,
			candidateChunks: 2,
			wantHits:        1,
			wantVisited:     1,
			wantIncomplete:  true,
			wantTermination: hnsw.TerminationVisitLimit,
		},
		{
			name:            "two visits reach both segments",
			visitLimit:      2,
			candidateChunks: 2,
			wantHits:        2,
			wantVisited:     2,
			wantIncomplete:  false,
			wantTermination: hnsw.TerminationComplete,
		},
		{
			name:            "candidate budget truncates merged hits",
			visitLimit:      2,
			candidateChunks: 1,
			wantHits:        1,
			wantVisited:     2,
			wantIncomplete:  true,
			wantTermination: "candidate_limit",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := index.Search(
				context.Background(),
				[]ChunkEmbedding{testChunk("query", "q-1", 0, 0)},
				2,
				QueryConfig{
					CandidateChunks: tt.candidateChunks,
					EfSearch:        2,
					VisitLimit:      tt.visitLimit,
				},
			)
			if err != nil {
				t.Fatalf("Search() error = %v", err)
			}
			if len(result.Hits) != tt.wantHits {
				t.Fatalf("Search() hit count = %d, want %d", len(result.Hits), tt.wantHits)
			}
			if result.Stats.VisitedNodes != tt.wantVisited {
				t.Fatalf("Search() visited nodes = %d, want %d", result.Stats.VisitedNodes, tt.wantVisited)
			}
			if result.HNSWSearches != tt.wantVisited {
				t.Fatalf("Search() HNSW searches = %d, want %d", result.HNSWSearches, tt.wantVisited)
			}
			if result.GroupingIncomplete != tt.wantIncomplete {
				t.Fatalf("Search() grouping incomplete = %v, want %v", result.GroupingIncomplete, tt.wantIncomplete)
			}
			if result.Stats.Termination != tt.wantTermination {
				t.Fatalf("Search() termination = %q, want %q", result.Stats.Termination, tt.wantTermination)
			}
		})
	}
}
