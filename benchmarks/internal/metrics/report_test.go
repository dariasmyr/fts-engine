package metrics

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/dariasmyr/fts-engine/benchmarks/internal/harness"
	"github.com/dariasmyr/fts-engine/benchmarks/internal/quality"
)

func TestWriteJSONStableSchema(t *testing.T) {
	rep := &harness.Report{
		Engine:        "fts-engine",
		NumDocs:       2,
		NumQueries:    3,
		IndexBuildDur: 2 * time.Second,
		IndexBytes:    12345,
		RetainedHeap:  &harness.HeapStats{AllocBytes: 12_345_678, Objects: 4567},
		Latencies: []time.Duration{
			10 * time.Millisecond,
			20 * time.Millisecond,
			30 * time.Millisecond,
		},
		Wall: 90 * time.Millisecond,
	}
	rec := Build(rep, &quality.Scores{
		K:         10,
		NumScored: 3,
		RecallAtK: 0.5,
		MRR:       0.75,
		NDCGAtK:   0.6,
	}, RunMeta{
		Timestamp:   time.Date(2026, time.June, 12, 10, 11, 12, 0, time.UTC),
		GoVersion:   "go-test",
		GOOS:        "darwin",
		GOARCH:      "arm64",
		NumCPU:      8,
		Concurrency: 2,
		BatchSize:   1000,
		WarmupFrac:  0.10,
	})
	rec.Dataset = DatasetMeta{
		Name:       "synthetic",
		NumDocs:    2,
		NumQueries: 3,
		Params: map[string]any{
			"seed":          float64(42),
			"synth_docs":    float64(2),
			"synth_queries": float64(3),
		},
	}
	rec.Config = map[string]any{"index": "slicedradix", "persist": "none"}
	rec.Extras = map[string]any{"diagnostics_enabled": false}

	var out bytes.Buffer
	if err := WriteJSON(&out, []Record{rec}); err != nil {
		t.Fatalf("WriteJSON() error = %v", err)
	}

	got := out.String()
	checks := []string{
		`"schema_version": "benchmarks.v1alpha2"`,
		`"engine": "fts-engine"`,
		`"dataset": {`,
		`"name": "synthetic"`,
		`"batch_size": 1000`,
		`"warmup_frac": 0.1`,
		`"index": {`,
		`"build_duration_ms": 2000`,
		`"retained_heap": {`,
		`"alloc_bytes": 12345678`,
		`"objects": 4567`,
		`"quality": {`,
		`"diagnostics_enabled": false`,
	}
	for _, check := range checks {
		if !strings.Contains(got, check) {
			t.Fatalf("WriteJSON() output missing %q\n%s", check, got)
		}
	}
	if rec.Index.RetainedHeap == nil || rec.Index.RetainedHeap.AllocBytes != 12_345_678 || rec.Index.RetainedHeap.Objects != 4567 {
		t.Fatalf("Build() retained heap = %+v", rec.Index.RetainedHeap)
	}
}

func TestWriteTableGroupsByQueryClass(t *testing.T) {
	recs := []Record{
		{Engine: "fts-engine", QueryClass: "phrase", Index: IndexStats{BuildDurationMS: 1000, DocsPerSec: 100}, Latency: LatencyStats{P50MS: 1, P95MS: 2, P99MS: 3, QPS: 10}},
		{Engine: "bleve", QueryClass: "term", Index: IndexStats{BuildDurationMS: 1000, DocsPerSec: 100}, Latency: LatencyStats{P50MS: 1, P95MS: 2, P99MS: 3, QPS: 10}},
		{Engine: "fts-engine", QueryClass: "term", Index: IndexStats{BuildDurationMS: 1000, DocsPerSec: 100, RetainedHeap: &HeapStats{AllocBytes: 12_300_000, Objects: 456}}, Latency: LatencyStats{P50MS: 1, P95MS: 2, P99MS: 3, QPS: 10}},
		{Engine: "bleve", QueryClass: "phrase", Index: IndexStats{BuildDurationMS: 1000, DocsPerSec: 100}, Latency: LatencyStats{P50MS: 1, P95MS: 2, P99MS: 3, QPS: 10}},
	}

	var out bytes.Buffer
	if err := WriteTable(&out, recs); err != nil {
		t.Fatalf("WriteTable() error = %v", err)
	}

	got := out.String()
	checks := []string{
		"QUERY",
		"ENGINE",
		"HEAP(MB)",
		"HEAP_OBJS",
		"12.3",
		"456",
		"term      bleve",
		"term      fts-engine",
		"\n\nphrase    bleve",
		"phrase    fts-engine",
	}
	for _, check := range checks {
		if !strings.Contains(got, check) {
			t.Fatalf("WriteTable() output missing %q\n%s", check, got)
		}
	}
	if strings.Index(got, "term   bleve") > strings.Index(got, "phrase  bleve") {
		t.Fatalf("term rows should appear before phrase rows\n%s", got)
	}
}
