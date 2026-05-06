package ftsstats

import (
	"errors"
	"testing"
	"time"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

func TestSearchStatsObserveAggregatesByStrategy(t *testing.T) {
	stats := NewSearchStats(4)
	stats.ObserveSearch("alpha beta", &fts.QueryDiagnostics{
		LogicalQueryType:   "boolean",
		ExecutionStrategy:  "bool_or_wand",
		StrategySkipReason: "and_fast_no_must_terms",
		PostingEntriesRead: 12,
		MatchedDocs:        3,
		ReturnedDocs:       2,
		Timings:            map[string]time.Duration{"total": 8 * time.Millisecond},
	}, nil)
	stats.ObserveSearch("alpha", &fts.QueryDiagnostics{
		LogicalQueryType:   "term",
		ExecutionStrategy:  "term",
		PostingEntriesRead: 4,
		MatchedDocs:        0,
		ReturnedDocs:       0,
		Timings:            map[string]time.Duration{"total": 2 * time.Millisecond},
	}, nil)

	snap := stats.Snapshot()
	if snap.TotalSearches != 2 {
		t.Fatalf("TotalSearches = %d, want 2", snap.TotalSearches)
	}
	if snap.ZeroResults != 1 {
		t.Fatalf("ZeroResults = %d, want 1", snap.ZeroResults)
	}
	if got := snap.ByStrategy["bool_or_wand"]; got.Count != 1 || got.TotalPostings != 12 || got.CumulativeDuration != 8*time.Millisecond {
		t.Fatalf("bool_or_wand stats = %+v, want count=1 postings=12 duration=8ms", got)
	}
	if got := snap.ByStrategy["bool_or_wand"].AvgDuration(); got != 8*time.Millisecond {
		t.Fatalf("AvgDuration = %v, want 8ms", got)
	}
	if got := snap.ByStrategy["bool_or_wand"].AvgPostings(); got != 12 {
		t.Fatalf("AvgPostings = %v, want 12", got)
	}
	if len(snap.Recent) != 2 {
		t.Fatalf("Recent len = %d, want 2", len(snap.Recent))
	}
	if snap.Recent[0].LogicalQueryType != "term" || snap.Recent[1].LogicalQueryType != "boolean" {
		t.Fatalf("unexpected recent order: %+v", snap.Recent)
	}
	if snap.Recent[1].StrategySkipReason != "and_fast_no_must_terms" {
		t.Fatalf("StrategySkipReason = %q, want and_fast_no_must_terms", snap.Recent[1].StrategySkipReason)
	}
	if snap.Recent[1].QueryHash == "" || snap.Recent[1].QueryHash == "alpha beta" {
		t.Fatalf("expected hashed query, got %q", snap.Recent[1].QueryHash)
	}
}

func TestSearchStatsObserveErrorWithoutDiagnostics(t *testing.T) {
	stats := NewSearchStats(2)
	stats.ObserveSearch("broken", nil, errors.New("boom"))

	snap := stats.Snapshot()
	if snap.TotalSearches != 1 || snap.ErrorsTotal != 1 {
		t.Fatalf("unexpected snapshot totals: %+v", snap)
	}
	if len(snap.ByStrategy) != 0 {
		t.Fatalf("ByStrategy = %+v, want empty", snap.ByStrategy)
	}
	if len(snap.Recent) != 1 || snap.Recent[0].Error != "boom" {
		t.Fatalf("unexpected recent events: %+v", snap.Recent)
	}
}

func TestSearchStatsRecentLimit(t *testing.T) {
	stats := NewSearchStats(2)
	stats.ObserveSearch("one", &fts.QueryDiagnostics{Timings: map[string]time.Duration{}}, nil)
	stats.ObserveSearch("two", &fts.QueryDiagnostics{Timings: map[string]time.Duration{}}, nil)
	stats.ObserveSearch("three", &fts.QueryDiagnostics{Timings: map[string]time.Duration{}}, nil)

	recent := stats.Recent(2)
	if len(recent) != 2 {
		t.Fatalf("Recent len = %d, want 2", len(recent))
	}
	if recent[0].QueryHash != queryHash("three") || recent[1].QueryHash != queryHash("two") {
		t.Fatalf("unexpected recent order: %+v", recent)
	}
}
