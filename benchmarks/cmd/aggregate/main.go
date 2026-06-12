package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/dariasmyr/fts-engine/benchmarks/internal/metrics"
)

type scenario struct {
	kind  string
	label string
	path  string
}

func main() {
	dir := "results/full"
	if len(os.Args) > 1 {
		dir = os.Args[1]
	}
	if err := run(dir, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(dir string, w io.Writer) error {
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return fmt.Errorf("aggregate: list json files: %w", err)
	}
	if len(files) == 0 {
		return fmt.Errorf("aggregate: no JSON files found in %s (run ./benchmarks/scripts/run-suite.sh first or point to a directory with benchmark -out JSON files)", dir)
	}
	sort.Strings(files)

	groups := map[string]map[string][]metrics.Record{}
	for _, path := range files {
		recs, err := loadRecords(path)
		if err != nil {
			return fmt.Errorf("aggregate: load %s: %w", path, err)
		}
		sc := classify(path)
		if groups[sc.kind] == nil {
			groups[sc.kind] = make(map[string][]metrics.Record)
		}
		groups[sc.kind][sc.label] = recs
	}

	renderVariability(w, groups["var"])
	renderAxis(w, "Concurrency", groups["conc"])
	renderAxis(w, "Scale", groups["scale"])
	renderAxis(w, "Synthetic", groups["synth"])
	renderAxis(w, "Other", groups["other"])
	return nil
}

func loadRecords(path string) ([]metrics.Record, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var report metrics.ReportFile
	if err := json.Unmarshal(data, &report); err == nil && len(report.Records) > 0 {
		return report.Records, nil
	}
	var legacy []metrics.Record
	if err := json.Unmarshal(data, &legacy); err == nil && len(legacy) > 0 {
		return legacy, nil
	}
	return nil, fmt.Errorf("unsupported benchmark JSON format")
}

func classify(path string) scenario {
	name := strings.TrimSuffix(filepath.Base(path), ".json")
	switch {
	case strings.HasPrefix(name, "var-"):
		return scenario{kind: "var", label: strings.TrimPrefix(name, "var-"), path: path}
	case strings.HasPrefix(name, "conc-"):
		return scenario{kind: "conc", label: strings.TrimPrefix(name, "conc-"), path: path}
	case strings.HasPrefix(name, "scale-"):
		return scenario{kind: "scale", label: strings.TrimPrefix(name, "scale-"), path: path}
	case strings.HasPrefix(name, "synth") || strings.HasPrefix(name, "synthetic"):
		return scenario{kind: "synth", label: name, path: path}
	default:
		return scenario{kind: "other", label: name, path: path}
	}
}

func renderVariability(w io.Writer, runs map[string][]metrics.Record) {
	if len(runs) == 0 {
		return
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "\nVariability")
	_, _ = fmt.Fprintln(tw, "ENGINE\tQUERY\tBUILD(s)\tp50(ms)\tp95(ms)\tQPS\tRecall@k\tnDCG@k\tMRR")
	perEngine := map[string]map[string][]float64{}
	for _, recs := range runs {
		for _, r := range recs {
			label := recordLabel(r)
			if perEngine[label] == nil {
				perEngine[label] = make(map[string][]float64)
			}
			appendMetric(perEngine[label], "build_s", float64(r.Index.BuildDurationMS)/1000.0)
			appendMetric(perEngine[label], "p50", r.Latency.P50MS)
			appendMetric(perEngine[label], "p95", r.Latency.P95MS)
			appendMetric(perEngine[label], "qps", r.Latency.QPS)
			if r.Quality != nil {
				appendMetric(perEngine[label], "recall", r.Quality.RecallAtK)
				appendMetric(perEngine[label], "ndcg", r.Quality.NDCGAtK)
				appendMetric(perEngine[label], "mrr", r.Quality.MRR)
			}
		}
	}
	engines := sortedKeys(perEngine)
	for _, engine := range engines {
		metricsMap := perEngine[engine]
		base, query := splitRecordLabel(engine)
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			base,
			query,
			formatRange(metricsMap["build_s"], 2),
			formatRange(metricsMap["p50"], 3),
			formatRange(metricsMap["p95"], 3),
			formatRange(metricsMap["qps"], 0),
			formatRange(metricsMap["recall"], 4),
			formatRange(metricsMap["ndcg"], 4),
			formatRange(metricsMap["mrr"], 4),
		)
	}
	_ = tw.Flush()
}

func renderAxis(w io.Writer, title string, runs map[string][]metrics.Record) {
	if len(runs) == 0 {
		return
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintf(tw, "\n%s\n", title)
	_, _ = fmt.Fprintln(tw, "SCENARIO\tENGINE\tQUERY\tBUILD(s)\tp50(ms)\tp95(ms)\tp99(ms)\tQPS\tRecall@k\tnDCG@k\tMRR")
	labels := sortedKeys(runs)
	for _, label := range labels {
		recs := append([]metrics.Record(nil), runs[label]...)
		sort.Slice(recs, func(i, j int) bool { return recordLabel(recs[i]) < recordLabel(recs[j]) })
		for _, r := range recs {
			recall, ndcg, mrr := "-", "-", "-"
			if r.Quality != nil {
				recall = fmt.Sprintf("%.4f", r.Quality.RecallAtK)
				ndcg = fmt.Sprintf("%.4f", r.Quality.NDCGAtK)
				mrr = fmt.Sprintf("%.4f", r.Quality.MRR)
			}
			queryClass := r.QueryClass
			if queryClass == "" {
				queryClass = "-"
			}
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%.2f\t%.3f\t%.3f\t%.3f\t%.0f\t%s\t%s\t%s\n",
				label,
				r.Engine,
				queryClass,
				float64(r.Index.BuildDurationMS)/1000.0,
				r.Latency.P50MS,
				r.Latency.P95MS,
				r.Latency.P99MS,
				r.Latency.QPS,
				recall,
				ndcg,
				mrr,
			)
		}
	}
	_ = tw.Flush()
}

func appendMetric(dst map[string][]float64, key string, value float64) {
	dst[key] = append(dst[key], value)
}

func sortedKeys[M ~map[string]V, V any](m M) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func formatRange(values []float64, decimals int) string {
	if len(values) == 0 {
		return "-"
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	med := median(sorted)
	lo := sorted[0]
	hi := sorted[len(sorted)-1]
	fmtNum := func(v float64) string {
		return fmt.Sprintf("%.*f", decimals, v)
	}
	return fmt.Sprintf("%s [%s..%s]", fmtNum(med), fmtNum(lo), fmtNum(hi))
}

func recordLabel(r metrics.Record) string {
	if r.QueryClass == "" {
		return r.Engine
	}
	return r.Engine + "|" + r.QueryClass
}

func splitRecordLabel(label string) (string, string) {
	parts := strings.SplitN(label, "|", 2)
	if len(parts) == 1 {
		return label, "-"
	}
	return parts[0], parts[1]
}

func median(values []float64) float64 {
	if len(values)%2 == 1 {
		return values[len(values)/2]
	}
	mid := len(values) / 2
	return (values[mid-1] + values[mid]) / 2
}
