package quality

import (
	"math"
	"testing"

	"github.com/dariasmyr/fts-engine/benchmarks/internal/harness"
)

func TestRecall(t *testing.T) {
	hits := []harness.SearchHit{{DocID: "a"}, {DocID: "b"}, {DocID: "x"}}
	relevant := map[string]int{"a": 1, "b": 1, "c": 1, "d": 1}
	if got := Recall(hits, relevant, 3); !approx(got, 0.5) {
		t.Fatalf("Recall() = %v, want 0.5", got)
	}
}

func TestMRR(t *testing.T) {
	hits := []harness.SearchHit{{DocID: "x"}, {DocID: "b"}, {DocID: "a"}}
	relevant := map[string]int{"a": 1, "b": 1}
	if got := MRR(hits, relevant); !approx(got, 0.5) {
		t.Fatalf("MRR() = %v, want 0.5", got)
	}
}

func TestNDCG(t *testing.T) {
	hits := []harness.SearchHit{{DocID: "a"}, {DocID: "x"}, {DocID: "b"}}
	relevant := map[string]int{"a": 1, "b": 1}
	got := NDCG(hits, relevant, 3)
	want := (1/math.Log2(2) + 1/math.Log2(4)) / (1/math.Log2(2) + 1/math.Log2(3))
	if !approx(got, want) {
		t.Fatalf("NDCG() = %v, want %v", got, want)
	}
}

func TestNDCGUsesGradedRelevanceForIdealRanking(t *testing.T) {
	hits := []harness.SearchHit{{DocID: "low"}, {DocID: "high"}}
	relevant := map[string]int{"low": 1, "high": 3}
	got := NDCG(hits, relevant, 2)
	want := (1/math.Log2(2) + 7/math.Log2(3)) / (7/math.Log2(2) + 1/math.Log2(3))
	if !approx(got, want) {
		t.Fatalf("NDCG() = %v, want %v", got, want)
	}
}

func TestNDCGPenalizesIncompleteResults(t *testing.T) {
	hits := []harness.SearchHit{{DocID: "a"}}
	relevant := map[string]int{"a": 1, "b": 1}
	got := NDCG(hits, relevant, 2)
	want := 1 / (1 + 1/math.Log2(3))
	if !approx(got, want) {
		t.Fatalf("NDCG() = %v, want %v", got, want)
	}
}

func TestCompute(t *testing.T) {
	results := []harness.QueryResult{{
		QueryID: "q-1",
		Hits:    []harness.SearchHit{{DocID: "a"}, {DocID: "x"}},
	}}
	scores := Compute(results, Qrels{"q-1": {"a": 1, "b": 1}}, 2)
	if scores == nil {
		t.Fatal("Compute() = nil, want scores")
	}
	if scores.NumScored != 1 {
		t.Fatalf("NumScored = %d, want 1", scores.NumScored)
	}
	if !approx(scores.MRR, 1) {
		t.Fatalf("MRR = %v, want 1", scores.MRR)
	}
}

func approx(got, want float64) bool {
	return math.Abs(got-want) < 1e-9
}
