package bench

import (
	"math"
	"testing"
	"time"
)

func approxEq(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestNDCGPerfect(t *testing.T) {
	rel := NewRelevanceSet([]string{"a", "b", "c"})
	got := NDCG([]string{"a", "b", "c", "x"}, rel, 10)
	if !approxEq(got, 1.0) {
		t.Fatalf("NDCG() = %v, want 1.0", got)
	}
}

func TestNDCGNoHits(t *testing.T) {
	rel := NewRelevanceSet([]string{"a", "b"})
	if got := NDCG([]string{"x", "y", "z"}, rel, 10); got != 0 {
		t.Fatalf("NDCG() = %v, want 0", got)
	}
}

func TestNDCGPartial(t *testing.T) {
	rel := NewRelevanceSet([]string{"a"})
	got := NDCG([]string{"x", "a", "y"}, rel, 10)
	want := 1.0 / math.Log2(3)
	if !approxEq(got, want) {
		t.Fatalf("NDCG() = %v, want %v", got, want)
	}
}

func TestMRR(t *testing.T) {
	rel := NewRelevanceSet([]string{"b"})
	if got := MRR([]string{"a", "b", "c"}, rel); !approxEq(got, 0.5) {
		t.Fatalf("MRR() = %v, want 0.5", got)
	}
	if got := MRR([]string{"a", "c"}, rel); got != 0 {
		t.Fatalf("MRR() = %v, want 0", got)
	}
}

func TestRecall(t *testing.T) {
	rel := NewRelevanceSet([]string{"a", "b", "c", "d"})
	if got := Recall([]string{"a", "b", "x"}, rel, 3); !approxEq(got, 0.5) {
		t.Fatalf("Recall() = %v, want 0.5", got)
	}
	if got := Recall([]string{"a"}, NewRelevanceSet(nil), 3); got != 0 {
		t.Fatalf("Recall() = %v, want 0", got)
	}
}

func TestPercentile(t *testing.T) {
	durations := []time.Duration{10, 20, 30, 40, 50}
	if got := Percentile(durations, 0.0); got != 10 {
		t.Fatalf("Percentile(0.0) = %v, want 10", got)
	}
	if got := Percentile(durations, 0.5); got != 30 {
		t.Fatalf("Percentile(0.5) = %v, want 30", got)
	}
	if got := Percentile(durations, 1.0); got != 50 {
		t.Fatalf("Percentile(1.0) = %v, want 50", got)
	}
	if got := Percentile(durations, 0.25); got != 20 {
		t.Fatalf("Percentile(0.25) = %v, want 20", got)
	}
	if got := Percentile(durations, 0.1); got != 14 {
		t.Fatalf("Percentile(0.1) = %v, want 14", got)
	}
}

func TestPercentileEmpty(t *testing.T) {
	if got := Percentile(nil, 0.5); got != 0 {
		t.Fatalf("Percentile(nil) = %v, want 0", got)
	}
}
