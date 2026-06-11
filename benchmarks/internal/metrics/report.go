package metrics

import (
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"text/tabwriter"
	"time"

	"github.com/dariasmyr/fts-engine/benchmarks/internal/harness"
	"github.com/dariasmyr/fts-engine/benchmarks/internal/quality"
)

type Record struct {
	Engine  string        `json:"engine"`
	Run     RunMeta       `json:"run"`
	Index   IndexStats    `json:"index"`
	Latency LatencyStats  `json:"latency"`
	Quality *QualityStats `json:"quality,omitempty"`
}

type RunMeta struct {
	Timestamp   time.Time `json:"timestamp"`
	GoVersion   string    `json:"go_version"`
	GOOS        string    `json:"goos"`
	GOARCH      string    `json:"goarch"`
	NumCPU      int       `json:"num_cpu"`
	Dataset     string    `json:"dataset"`
	Concurrency int       `json:"concurrency"`
	NumDocs     int       `json:"num_docs"`
	NumQueries  int       `json:"num_queries"`
}

type IndexStats struct {
	BuildDurationMS int64   `json:"build_duration_ms"`
	DocsPerSec      float64 `json:"docs_per_sec"`
	IndexBytes      int64   `json:"index_bytes_on_disk"`
}

type LatencyStats struct {
	P50MS  float64 `json:"p50_ms"`
	P95MS  float64 `json:"p95_ms"`
	P99MS  float64 `json:"p99_ms"`
	MaxMS  float64 `json:"max_ms"`
	MeanMS float64 `json:"mean_ms"`
	QPS    float64 `json:"qps"`
}

type QualityStats struct {
	K         int     `json:"k"`
	NumScored int     `json:"num_scored"`
	RecallAtK float64 `json:"recall_at_k"`
	MRR       float64 `json:"mrr"`
	NDCGAtK   float64 `json:"ndcg_at_k"`
}

func Build(rep *harness.Report, q *quality.Scores, meta RunMeta) Record {
	pcts := harness.Percentiles(rep.Latencies, 0.50, 0.95, 0.99, 1.0)
	var sum time.Duration
	for _, d := range rep.Latencies {
		sum += d
	}
	meanMS := 0.0
	if len(rep.Latencies) > 0 {
		meanMS = float64(sum.Nanoseconds()) / float64(len(rep.Latencies)) / 1e6
	}
	qps := 0.0
	if rep.Wall > 0 {
		qps = float64(len(rep.Latencies)) / rep.Wall.Seconds()
	}
	docsPS := 0.0
	if rep.IndexBuildDur > 0 {
		docsPS = float64(rep.NumDocs) / rep.IndexBuildDur.Seconds()
	}

	meta.Timestamp = time.Now().UTC()
	meta.GoVersion = runtime.Version()
	meta.GOOS = runtime.GOOS
	meta.GOARCH = runtime.GOARCH
	meta.NumCPU = runtime.NumCPU()
	meta.NumDocs = rep.NumDocs
	meta.NumQueries = rep.NumQueries

	rec := Record{
		Engine: rep.Engine,
		Run:    meta,
		Index: IndexStats{
			BuildDurationMS: rep.IndexBuildDur.Milliseconds(),
			DocsPerSec:      docsPS,
			IndexBytes:      rep.IndexBytes,
		},
		Latency: LatencyStats{
			P50MS:  ms(pcts[0]),
			P95MS:  ms(pcts[1]),
			P99MS:  ms(pcts[2]),
			MaxMS:  ms(pcts[3]),
			MeanMS: meanMS,
			QPS:    qps,
		},
	}
	if q != nil && q.NumScored > 0 {
		rec.Quality = &QualityStats{
			K:         q.K,
			NumScored: q.NumScored,
			RecallAtK: q.RecallAtK,
			MRR:       q.MRR,
			NDCGAtK:   q.NDCGAtK,
		}
	}
	return rec
}

func WriteJSON(w io.Writer, recs []Record) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(recs)
}

func WriteTable(w io.Writer, recs []Record) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "ENGINE\tBUILD(s)\tdocs/s\tINDEX(MB)\tp50(ms)\tp95(ms)\tp99(ms)\tQPS\tRecall@k\tnDCG@k\tMRR"); err != nil {
		return err
	}
	for _, r := range recs {
		recall, ndcg, mrr := "-", "-", "-"
		if r.Quality != nil {
			recall = fmt.Sprintf("%.4f", r.Quality.RecallAtK)
			ndcg = fmt.Sprintf("%.4f", r.Quality.NDCGAtK)
			mrr = fmt.Sprintf("%.4f", r.Quality.MRR)
		}
		if _, err := fmt.Fprintf(tw, "%s\t%.2f\t%.0f\t%.1f\t%.3f\t%.3f\t%.3f\t%.0f\t%s\t%s\t%s\n",
			r.Engine,
			float64(r.Index.BuildDurationMS)/1000.0,
			r.Index.DocsPerSec,
			float64(r.Index.IndexBytes)/1e6,
			r.Latency.P50MS, r.Latency.P95MS, r.Latency.P99MS, r.Latency.QPS,
			recall, ndcg, mrr,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func ms(d time.Duration) float64 { return float64(d.Nanoseconds()) / 1e6 }
