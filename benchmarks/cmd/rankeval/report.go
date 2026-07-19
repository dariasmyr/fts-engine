package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"text/tabwriter"

	"github.com/dariasmyr/fts-engine/pkg/ftseval"
)

type evalRun struct {
	Name    string           `json:"name"`
	Kind    string           `json:"kind"`
	Base    string           `json:"base"`
	File    string           `json:"file,omitempty"`
	Profile *rankProfileFile `json:"profile,omitempty"`
	Metrics metrics          `json:"metrics"`
	Delta   *metricsDelta    `json:"delta,omitempty"`
	Queries *queryDelta      `json:"queries,omitempty"`
}

type metrics struct {
	K            int     `json:"k"`
	QueryCount   int     `json:"query_count"`
	MRR          float64 `json:"mrr"`
	PrecisionAtK float64 `json:"precision_at_k"`
	RecallAtK    float64 `json:"recall_at_k"`
	NDCGAtK      float64 `json:"ndcg_at_k"`
}

type metricsDelta struct {
	MRR                 float64 `json:"mrr"`
	MRRPercent          float64 `json:"mrr_percent"`
	PrecisionAtK        float64 `json:"precision_at_k"`
	PrecisionAtKPercent float64 `json:"precision_at_k_percent"`
	RecallAtK           float64 `json:"recall_at_k"`
	RecallAtKPercent    float64 `json:"recall_at_k_percent"`
	NDCGAtK             float64 `json:"ndcg_at_k"`
	NDCGAtKPercent      float64 `json:"ndcg_at_k_percent"`
}

type queryDelta struct {
	Improved    int     `json:"improved"`
	Worse       int     `json:"worse"`
	Same        int     `json:"same"`
	MaxNDCGGain float64 `json:"max_ndcg_gain"`
	MaxNDCGLoss float64 `json:"max_ndcg_loss"`
}

type reportFile struct {
	SchemaVersion string         `json:"schema_version"`
	Dataset       map[string]any `json:"dataset"`
	Runs          []evalRun      `json:"runs"`
}

func metricsFromReport(report *ftseval.Report) metrics {
	return metrics{
		K:            report.K,
		QueryCount:   report.QueryCount,
		MRR:          report.MRR,
		PrecisionAtK: report.PrecisionAtK,
		RecallAtK:    report.RecallAtK,
		NDCGAtK:      report.NDCGAtK,
	}
}

func deltaMetrics(candidate metrics, baseline metrics) *metricsDelta {
	return &metricsDelta{
		MRR:                 candidate.MRR - baseline.MRR,
		MRRPercent:          percentDelta(candidate.MRR, baseline.MRR),
		PrecisionAtK:        candidate.PrecisionAtK - baseline.PrecisionAtK,
		PrecisionAtKPercent: percentDelta(candidate.PrecisionAtK, baseline.PrecisionAtK),
		RecallAtK:           candidate.RecallAtK - baseline.RecallAtK,
		RecallAtKPercent:    percentDelta(candidate.RecallAtK, baseline.RecallAtK),
		NDCGAtK:             candidate.NDCGAtK - baseline.NDCGAtK,
		NDCGAtKPercent:      percentDelta(candidate.NDCGAtK, baseline.NDCGAtK),
	}
}

func percentDelta(candidate float64, baseline float64) float64 {
	if baseline == 0 {
		return 0
	}
	return (candidate - baseline) / baseline * 100
}

func compareQueries(candidate *ftseval.Report, baseline *ftseval.Report) *queryDelta {
	if candidate == nil || baseline == nil {
		return nil
	}
	baselineByName := make(map[string]ftseval.QueryReport, len(baseline.Queries))
	for _, query := range baseline.Queries {
		baselineByName[query.Name] = query
	}
	out := &queryDelta{}
	for _, query := range candidate.Queries {
		base, ok := baselineByName[query.Name]
		if !ok {
			continue
		}
		delta := query.NDCGAtK - base.NDCGAtK
		switch {
		case delta > 1e-12:
			out.Improved++
			if delta > out.MaxNDCGGain {
				out.MaxNDCGGain = delta
			}
		case delta < -1e-12:
			out.Worse++
			if delta < out.MaxNDCGLoss {
				out.MaxNDCGLoss = delta
			}
		default:
			out.Same++
		}
	}
	return out
}

func writeJSON(path string, report reportFile) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(report)
}

func writeTable(w io.Writer, runs []evalRun) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "PROFILE\tKIND\tBASE\tnDCG@k\tΔnDCG\tΔnDCG%\tQ+\tQ-\tQ=\tMRR\tΔMRR\tRecall@k\tΔRecall\tPrecision@k\tΔPrecision")
	for _, run := range runs {
		delta := run.Delta
		if delta == nil {
			delta = &metricsDelta{}
		}
		queries := run.Queries
		if queries == nil {
			queries = &queryDelta{}
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%.4f\t%s\t%s\t%s\t%s\t%s\t%.4f\t%s\t%.4f\t%s\t%.4f\t%s\n",
			run.Name,
			run.Kind,
			run.Base,
			run.Metrics.NDCGAtK,
			formatDelta(run.Delta, delta.NDCGAtK),
			formatPercentDelta(run.Delta, delta.NDCGAtKPercent),
			formatQueryCount(run.Queries, queries.Improved),
			formatQueryCount(run.Queries, queries.Worse),
			formatQueryCount(run.Queries, queries.Same),
			run.Metrics.MRR,
			formatDelta(run.Delta, delta.MRR),
			run.Metrics.RecallAtK,
			formatDelta(run.Delta, delta.RecallAtK),
			run.Metrics.PrecisionAtK,
			formatDelta(run.Delta, delta.PrecisionAtK),
		)
	}
	return tw.Flush()
}

func formatDelta(delta *metricsDelta, value float64) string {
	if delta == nil {
		return "-"
	}
	return fmt.Sprintf("%+.4f", value)
}

func formatPercentDelta(delta *metricsDelta, value float64) string {
	if delta == nil {
		return "-"
	}
	return fmt.Sprintf("%+.2f%%", value)
}

func formatQueryCount(delta *queryDelta, value int) string {
	if delta == nil {
		return "-"
	}
	return fmt.Sprintf("%d", value)
}
