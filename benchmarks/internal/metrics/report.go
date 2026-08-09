package metrics

import (
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"sort"
	"time"

	"github.com/dariasmyr/fts-engine/benchmarks/internal/harness"
	"github.com/dariasmyr/fts-engine/benchmarks/internal/quality"
)

const SchemaVersion = "benchmarks.v1alpha2"

type ReportFile struct {
	SchemaVersion string   `json:"schema_version"`
	Records       []Record `json:"records"`
}

type Record struct {
	Engine     string         `json:"engine"`
	QueryClass string         `json:"query_class,omitempty"`
	Run        RunMeta        `json:"run"`
	Dataset    DatasetMeta    `json:"dataset"`
	Config     map[string]any `json:"config,omitempty"`
	Index      IndexStats     `json:"index"`
	Latency    LatencyStats   `json:"latency"`
	Quality    *QualityStats  `json:"quality,omitempty"`
	Extras     map[string]any `json:"extras,omitempty"`
}

type RunMeta struct {
	Timestamp   time.Time `json:"timestamp"`
	GoVersion   string    `json:"go_version"`
	GOOS        string    `json:"goos"`
	GOARCH      string    `json:"goarch"`
	NumCPU      int       `json:"num_cpu"`
	Concurrency int       `json:"concurrency"`
	BatchSize   int       `json:"batch_size,omitempty"`
	WarmupFrac  float64   `json:"warmup_frac,omitempty"`
}

type DatasetMeta struct {
	Name       string         `json:"name"`
	NumDocs    int            `json:"num_docs"`
	NumQueries int            `json:"num_queries"`
	Params     map[string]any `json:"params,omitempty"`
}

type IndexStats struct {
	BuildDurationMS int64      `json:"build_duration_ms"`
	DocsPerSec      float64    `json:"docs_per_sec"`
	IndexBytes      int64      `json:"index_bytes_on_disk"`
	RetainedHeap    *HeapStats `json:"retained_heap,omitempty"`
}

type HeapStats struct {
	AllocBytes int64 `json:"alloc_bytes"`
	Objects    int64 `json:"objects"`
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

	meta = withRuntimeDefaults(meta)

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
	if rep.RetainedHeap != nil {
		rec.Index.RetainedHeap = &HeapStats{
			AllocBytes: rep.RetainedHeap.AllocBytes,
			Objects:    rep.RetainedHeap.Objects,
		}
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
	return enc.Encode(ReportFile{SchemaVersion: SchemaVersion, Records: recs})
}

func WriteTable(w io.Writer, recs []Record) error {
	ordered := append([]Record(nil), recs...)
	sort.SliceStable(ordered, func(i, j int) bool {
		leftQuery, rightQuery := normalizedQueryClass(ordered[i]), normalizedQueryClass(ordered[j])
		leftRank, rightRank := queryClassRank(leftQuery), queryClassRank(rightQuery)
		if leftRank != rightRank {
			return leftRank < rightRank
		}
		if leftQuery != rightQuery {
			return leftQuery < rightQuery
		}
		return ordered[i].Engine < ordered[j].Engine
	})

	if _, err := fmt.Fprintf(w, "%-8s  %-11s  %8s  %7s  %9s  %9s  %10s  %7s  %7s  %7s  %7s  %9s  %7s  %7s\n",
		"QUERY", "ENGINE", "BUILD(s)", "docs/s", "INDEX(MB)", "HEAP(MB)", "HEAP_OBJS", "p50(ms)", "p95(ms)", "p99(ms)", "QPS", "Recall@k", "nDCG@k", "MRR",
	); err != nil {
		return err
	}
	prevQuery := ""
	for i, r := range ordered {
		recall, ndcg, mrr := "-", "-", "-"
		if r.Quality != nil {
			recall = fmt.Sprintf("%.4f", r.Quality.RecallAtK)
			ndcg = fmt.Sprintf("%.4f", r.Quality.NDCGAtK)
			mrr = fmt.Sprintf("%.4f", r.Quality.MRR)
		}
		heapMB, heapObjects := "-", "-"
		if r.Index.RetainedHeap != nil {
			heapMB = fmt.Sprintf("%.1f", float64(r.Index.RetainedHeap.AllocBytes)/1e6)
			heapObjects = fmt.Sprintf("%d", r.Index.RetainedHeap.Objects)
		}
		queryClass := normalizedQueryClass(r)
		if i > 0 && queryClass != prevQuery {
			if _, err := fmt.Fprintln(w); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintf(w, "%-8s  %-11s  %8.2f  %7.0f  %9.1f  %9s  %10s  %7.3f  %7.3f  %7.3f  %7.0f  %9s  %7s  %7s\n",
			queryClass,
			r.Engine,
			float64(r.Index.BuildDurationMS)/1000.0,
			r.Index.DocsPerSec,
			float64(r.Index.IndexBytes)/1e6,
			heapMB,
			heapObjects,
			r.Latency.P50MS, r.Latency.P95MS, r.Latency.P99MS, r.Latency.QPS,
			recall, ndcg, mrr,
		); err != nil {
			return err
		}
		prevQuery = queryClass
	}
	return nil
}

func normalizedQueryClass(r Record) string {
	if r.QueryClass == "" {
		return "-"
	}
	return r.QueryClass
}

func queryClassRank(class string) int {
	switch class {
	case "term":
		return 0
	case "and-hh":
		return 1
	case "and-hl":
		return 2
	case "or-hh":
		return 3
	case "phrase":
		return 4
	case "prefix":
		return 5
	case "-":
		return 100
	default:
		return 50
	}
}

func ms(d time.Duration) float64 { return float64(d.Nanoseconds()) / 1e6 }

func withRuntimeDefaults(meta RunMeta) RunMeta {
	if meta.Timestamp.IsZero() {
		meta.Timestamp = time.Now().UTC()
	}
	if meta.GoVersion == "" {
		meta.GoVersion = runtime.Version()
	}
	if meta.GOOS == "" {
		meta.GOOS = runtime.GOOS
	}
	if meta.GOARCH == "" {
		meta.GOARCH = runtime.GOARCH
	}
	if meta.NumCPU == 0 {
		meta.NumCPU = runtime.NumCPU()
	}
	return meta
}
