package ftseval

import (
	"context"
	"math"
	"reflect"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

func TestMetricsAtK(t *testing.T) {
	ranked := []fts.DocID{"doc-a", "doc-b", "doc-c"}
	relevant := map[fts.DocID]float64{
		"doc-b": 1,
		"doc-c": 1,
		"doc-x": 1,
	}

	if got := MRR(ranked, relevant); got != 0.5 {
		t.Fatalf("MRR() = %v, want 0.5", got)
	}
	if got := PrecisionAtK(ranked, relevant, 2); got != 0.5 {
		t.Fatalf("PrecisionAtK() = %v, want 0.5", got)
	}
	if got := RecallAtK(ranked, relevant, 2); !close(got, 1.0/3.0) {
		t.Fatalf("RecallAtK() = %v, want %v", got, 1.0/3.0)
	}
	if got := NDCGAtK(ranked, relevant, 3); got <= 0 || got >= 1 {
		t.Fatalf("NDCGAtK() = %v, want between 0 and 1", got)
	}
}

func TestNDCGAtKWithIdealRanking(t *testing.T) {
	ranked := []fts.DocID{"doc-a", "doc-b", "doc-c"}
	relevant := map[fts.DocID]float64{
		"doc-a": 3,
		"doc-b": 2,
		"doc-c": 1,
	}

	if got := NDCGAtK(ranked, relevant, 3); got != 1 {
		t.Fatalf("NDCGAtK() = %v, want 1", got)
	}
}

func TestMetricsIgnoreDuplicateRankedIDs(t *testing.T) {
	ranked := []fts.DocID{"doc-a", "doc-a", "doc-b"}
	relevant := map[fts.DocID]float64{"doc-a": 1, "doc-b": 1}

	if got := PrecisionAtK(ranked, relevant, 3); !close(got, 2.0/3.0) {
		t.Fatalf("PrecisionAtK() = %v, want %v", got, 2.0/3.0)
	}
	if got := RecallAtK(ranked, relevant, 3); got != 1 {
		t.Fatalf("RecallAtK() = %v, want 1", got)
	}
}

func TestEvaluateAggregatesQueryReports(t *testing.T) {
	queries := []Query{
		{Name: "q1", Query: "alpha", Relevant: map[fts.DocID]float64{"doc-a": 1}},
		{Name: "q2", Query: "beta", Relevant: map[fts.DocID]float64{"doc-c": 1}},
	}
	search := func(_ context.Context, query string, _ int) ([]fts.DocID, error) {
		switch query {
		case "alpha":
			return []fts.DocID{"doc-a", "doc-b"}, nil
		case "beta":
			return []fts.DocID{"doc-b", "doc-c"}, nil
		default:
			return nil, nil
		}
	}

	report, err := Evaluate(context.Background(), queries, 2, search)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if report.QueryCount != 2 || report.K != 2 || len(report.Queries) != 2 {
		t.Fatalf("unexpected report shape: %+v", report)
	}
	if got := report.MRR; got != 0.75 {
		t.Fatalf("report.MRR = %v, want 0.75", got)
	}
	if got := report.PrecisionAtK; got != 0.5 {
		t.Fatalf("report.PrecisionAtK = %v, want 0.5", got)
	}
	if got := report.RecallAtK; got != 1 {
		t.Fatalf("report.RecallAtK = %v, want 1", got)
	}
	if !reflect.DeepEqual(report.Queries[0].Ranked, []fts.DocID{"doc-a", "doc-b"}) {
		t.Fatalf("unexpected ranked copy: %+v", report.Queries[0].Ranked)
	}
}

func TestEvaluateRejectsInvalidInputs(t *testing.T) {
	if _, err := Evaluate(context.Background(), nil, 0, func(context.Context, string, int) ([]fts.DocID, error) { return nil, nil }); err == nil {
		t.Fatalf("expected k validation error")
	}
	if _, err := Evaluate(context.Background(), nil, 10, nil); err == nil {
		t.Fatalf("expected nil search validation error")
	}
}

func close(a, b float64) bool {
	return math.Abs(a-b) < 1e-9
}
