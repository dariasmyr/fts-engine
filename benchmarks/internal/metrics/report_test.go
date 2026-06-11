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
		`"schema_version": "benchmarks.v1alpha1"`,
		`"engine": "fts-engine"`,
		`"dataset": {`,
		`"name": "synthetic"`,
		`"batch_size": 1000`,
		`"warmup_frac": 0.1`,
		`"index": {`,
		`"build_duration_ms": 2000`,
		`"quality": {`,
		`"diagnostics_enabled": false`,
	}
	for _, check := range checks {
		if !strings.Contains(got, check) {
			t.Fatalf("WriteJSON() output missing %q\n%s", check, got)
		}
	}
}
