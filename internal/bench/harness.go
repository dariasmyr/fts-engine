package bench

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/dariasmyr/fts-engine/internal/domain/models"
	pkgfts "github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftsstats"
)

type Corpus []models.Document

func (c Corpus) TitleIndex() map[string]string {
	out := make(map[string]string, len(c))
	for _, doc := range c {
		key := strings.ToLower(strings.TrimSpace(doc.Title))
		if key == "" {
			continue
		}
		out[key] = doc.ID
	}
	return out
}

type ContentSelector func(models.Document) string

func SelectAbstract(d models.Document) string { return d.Abstract }
func SelectExtract(d models.Document) string  { return d.Extract }
func SelectTitle(d models.Document) string    { return d.Title }

type IndexReport struct {
	DocumentCount int
	Duration      time.Duration
	HeapAllocMB   uint64
}

type QueryReport struct {
	Query                   string
	Returned                int
	Relevant                int
	NDCG                    float64
	MRR                     float64
	Recall                  float64
	Latency                 time.Duration
	ExecutionStrategy       string
	StrategySkipReason      string
	IndexSearches           int
	PostingEntriesRead      int
	DiagnosticsTotal        time.Duration
	DiagnosticsSearchTokens time.Duration
}

type StrategyReport struct {
	Count             int
	TotalDuration     time.Duration
	TotalSearchTokens time.Duration
	TotalPostingsRead int
	TotalIndexSearch  int
}

type RunQueryOptions struct {
	Diagnostics bool
	Observer    *ftsstats.SearchStats
	Repeat      int
	Warmup      int
	Shuffle     bool
}

type Report struct {
	DiagnosticsEnabled     bool
	K                      int
	Index                  IndexReport
	Queries                []QueryReport
	LatencyP50             time.Duration
	LatencyP95             time.Duration
	LatencyP99             time.Duration
	DiagnosticsTotalP50    time.Duration
	DiagnosticsTotalP95    time.Duration
	DiagnosticsTotalP99    time.Duration
	DiagnosticsSearchP50   time.Duration
	DiagnosticsSearchP95   time.Duration
	DiagnosticsSearchP99   time.Duration
	MeanNDCG               float64
	MeanMRR                float64
	MeanRecall             float64
	MeanPostingEntriesRead float64
	MeanIndexSearches      float64
	Strategies             map[string]StrategyReport
}

func IndexCorpus(ctx context.Context, svc *pkgfts.Service, corpus Corpus, content ContentSelector) (IndexReport, error) {
	var report IndexReport
	if content == nil {
		content = SelectAbstract
	}

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	start := time.Now()
	for _, doc := range corpus {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		if err := svc.IndexDocument(ctx, pkgfts.DocID(doc.ID), content(doc)); err != nil {
			return report, fmt.Errorf("index %q: %w", doc.ID, err)
		}
	}

	report.Duration = time.Since(start)
	report.DocumentCount = len(corpus)

	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	report.HeapAllocMB = after.HeapAlloc / (1024 * 1024)

	return report, nil
}

func ResolveRelevant(q Query, titleIdx map[string]string) []string {
	out := make([]string, 0, len(q.RelevantIDs)+len(q.RelevantTitles))
	out = append(out, q.RelevantIDs...)
	for _, title := range q.RelevantTitles {
		key := strings.ToLower(strings.TrimSpace(title))
		if id, ok := titleIdx[key]; ok {
			out = append(out, id)
		}
	}
	return out
}

func CountMissingTitles(gt *GroundTruth, titleIdx map[string]string) int {
	missing := 0
	for _, q := range gt.Queries {
		for _, title := range q.RelevantTitles {
			key := strings.ToLower(strings.TrimSpace(title))
			if _, ok := titleIdx[key]; !ok {
				missing++
			}
		}
	}
	return missing
}

func RunQueries(ctx context.Context, svc *pkgfts.Service, gt *GroundTruth, titleIdx map[string]string, k int, opts RunQueryOptions) ([]QueryReport, error) {
	if opts.Repeat <= 0 {
		opts.Repeat = 1
	}
	if opts.Warmup < 0 {
		opts.Warmup = 0
	}
	rng := rand.New(rand.NewSource(1))

	reports := make([]QueryReport, 0, len(gt.Queries)*opts.Repeat)
	for remaining := opts.Warmup; remaining > 0 && len(gt.Queries) > 0; {
		batch := orderedQueries(gt.Queries, opts.Shuffle, rng)
		if remaining < len(batch) {
			batch = batch[:remaining]
		}
		for _, q := range batch {
			if _, err := runQuery(ctx, svc, q, titleIdx, k, opts.Diagnostics, nil); err != nil {
				return nil, fmt.Errorf("warmup search %q: %w", q.Query, err)
			}
		}
		remaining -= len(batch)
	}

	for run := 0; run < opts.Repeat; run++ {
		for _, q := range orderedQueries(gt.Queries, opts.Shuffle, rng) {
			queryReport, err := runQuery(ctx, svc, q, titleIdx, k, opts.Diagnostics, opts.Observer)
			if err != nil {
				return nil, fmt.Errorf("search %q: %w", q.Query, err)
			}
			reports = append(reports, queryReport)
		}
	}
	return reports, nil
}

func orderedQueries(in []Query, shuffle bool, rng *rand.Rand) []Query {
	out := append([]Query(nil), in...)
	if !shuffle || len(out) < 2 {
		return out
	}
	rng.Shuffle(len(out), func(i, j int) {
		out[i], out[j] = out[j], out[i]
	})
	return out
}

func runQuery(ctx context.Context, svc *pkgfts.Service, q Query, titleIdx map[string]string, k int, diagnostics bool, observer *ftsstats.SearchStats) (QueryReport, error) {
	if err := ctx.Err(); err != nil {
		return QueryReport{}, err
	}

	searchCtx := ctx
	if diagnostics {
		searchCtx = pkgfts.WithDiagnostics(searchCtx)
	}

	relevant := NewRelevanceSet(ResolveRelevant(q, titleIdx))
	start := time.Now()
	res, err := svc.SearchDocuments(searchCtx, q.Query, k)
	elapsed := time.Since(start)
	if observer != nil {
		observer.ObserveResult(q.Query, res, err)
	}
	if err != nil {
		return QueryReport{}, err
	}

	ranked := make([]string, 0, len(res.Results))
	for _, item := range res.Results {
		ranked = append(ranked, string(item.ID))
	}

	queryReport := QueryReport{
		Query:    q.Query,
		Returned: len(ranked),
		Relevant: relevant.Size(),
		NDCG:     NDCG(ranked, relevant, k),
		MRR:      MRR(ranked, relevant),
		Recall:   Recall(ranked, relevant, k),
		Latency:  elapsed,
	}
	if diag := res.Diagnostics; diag != nil {
		queryReport.ExecutionStrategy = diag.ExecutionStrategy
		queryReport.StrategySkipReason = diag.StrategySkipReason
		queryReport.IndexSearches = diag.IndexSearches
		queryReport.PostingEntriesRead = diag.PostingEntriesRead
		queryReport.DiagnosticsTotal = diag.Timings.Total
		queryReport.DiagnosticsSearchTokens = diag.Timings.SearchTokens
	}

	return queryReport, nil
}

func Aggregate(k int, idx IndexReport, queries []QueryReport, diagnosticsEnabled bool) Report {
	report := Report{DiagnosticsEnabled: diagnosticsEnabled, K: k, Index: idx, Queries: queries, Strategies: make(map[string]StrategyReport)}
	if len(queries) == 0 {
		return report
	}

	latencies := make([]time.Duration, 0, len(queries))
	diagnosticsTotals := make([]time.Duration, 0, len(queries))
	searchTimings := make([]time.Duration, 0, len(queries))
	var sumNDCG, sumMRR, sumRecall float64
	var totalPostingsRead, totalIndexSearches int
	for _, q := range queries {
		latencies = append(latencies, q.Latency)
		if diagnosticsEnabled {
			diagnosticsTotals = append(diagnosticsTotals, q.DiagnosticsTotal)
			searchTimings = append(searchTimings, q.DiagnosticsSearchTokens)
		}
		sumNDCG += q.NDCG
		sumMRR += q.MRR
		sumRecall += q.Recall
		if diagnosticsEnabled {
			totalPostingsRead += q.PostingEntriesRead
			totalIndexSearches += q.IndexSearches
		}

		if diagnosticsEnabled {
			strategy := q.ExecutionStrategy
			if strategy == "" {
				strategy = "unknown"
			}
			st := report.Strategies[strategy]
			st.Count++
			st.TotalDuration += q.DiagnosticsTotal
			st.TotalSearchTokens += q.DiagnosticsSearchTokens
			st.TotalPostingsRead += q.PostingEntriesRead
			st.TotalIndexSearch += q.IndexSearches
			report.Strategies[strategy] = st
		}
	}

	n := float64(len(queries))
	report.MeanNDCG = sumNDCG / n
	report.MeanMRR = sumMRR / n
	report.MeanRecall = sumRecall / n
	report.LatencyP50 = Percentile(latencies, 0.50)
	report.LatencyP95 = Percentile(latencies, 0.95)
	report.LatencyP99 = Percentile(latencies, 0.99)
	if diagnosticsEnabled {
		report.MeanPostingEntriesRead = float64(totalPostingsRead) / n
		report.MeanIndexSearches = float64(totalIndexSearches) / n
		report.DiagnosticsTotalP50 = Percentile(diagnosticsTotals, 0.50)
		report.DiagnosticsTotalP95 = Percentile(diagnosticsTotals, 0.95)
		report.DiagnosticsTotalP99 = Percentile(diagnosticsTotals, 0.99)
		report.DiagnosticsSearchP50 = Percentile(searchTimings, 0.50)
		report.DiagnosticsSearchP95 = Percentile(searchTimings, 0.95)
		report.DiagnosticsSearchP99 = Percentile(searchTimings, 0.99)
	}

	sort.SliceStable(report.Queries, func(i, j int) bool {
		return report.Queries[i].NDCG < report.Queries[j].NDCG
	})
	return report
}

func WriteReport(w io.Writer, r Report, topWorst int) {
	fmt.Fprintf(w, "Indexed:   %d docs in %s (heap %d MB)\n", r.Index.DocumentCount, r.Index.Duration, r.Index.HeapAllocMB)
	fmt.Fprintf(w, "Queries:   %d   (k=%d)\n", len(r.Queries), r.K)
	fmt.Fprintf(w, "  nDCG@%d:   %.4f\n", r.K, r.MeanNDCG)
	fmt.Fprintf(w, "  MRR:       %.4f\n", r.MeanMRR)
	fmt.Fprintf(w, "  Recall@%d: %.4f\n", r.K, r.MeanRecall)
	fmt.Fprintf(w, "  latency:   p50=%s p95=%s p99=%s\n", r.LatencyP50, r.LatencyP95, r.LatencyP99)
	if r.DiagnosticsEnabled {
		fmt.Fprintf(w, "  diag.total: p50=%s p95=%s p99=%s\n", r.DiagnosticsTotalP50, r.DiagnosticsTotalP95, r.DiagnosticsTotalP99)
		fmt.Fprintf(w, "  diag.search_tokens: p50=%s p95=%s p99=%s\n", r.DiagnosticsSearchP50, r.DiagnosticsSearchP95, r.DiagnosticsSearchP99)
		fmt.Fprintf(w, "  avg internal work: postings=%.1f index_lookups=%.1f\n", r.MeanPostingEntriesRead, r.MeanIndexSearches)
	}

	if r.DiagnosticsEnabled && len(r.Strategies) > 0 {
		keys := make([]string, 0, len(r.Strategies))
		for key := range r.Strategies {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		fmt.Fprintln(w, "  strategies:")
		for _, key := range keys {
			st := r.Strategies[key]
			avgTotal := time.Duration(0)
			avgSearch := time.Duration(0)
			avgPostings := 0.0
			avgLookups := 0.0
			if st.Count > 0 {
				avgTotal = st.TotalDuration / time.Duration(st.Count)
				avgSearch = st.TotalSearchTokens / time.Duration(st.Count)
				avgPostings = float64(st.TotalPostingsRead) / float64(st.Count)
				avgLookups = float64(st.TotalIndexSearch) / float64(st.Count)
			}
			fmt.Fprintf(w, "    %s: count=%d avg_total=%s avg_search_tokens=%s avg_postings=%.1f avg_lookups=%.1f\n", key, st.Count, avgTotal, avgSearch, avgPostings, avgLookups)
		}
	}

	if topWorst <= 0 || len(r.Queries) == 0 {
		return
	}
	if topWorst > len(r.Queries) {
		topWorst = len(r.Queries)
	}

	fmt.Fprintf(w, "\nWorst %d queries by nDCG@%d:\n", topWorst, r.K)
	for i := 0; i < topWorst; i++ {
		q := r.Queries[i]
		if r.DiagnosticsEnabled {
			fmt.Fprintf(w, "  ndcg=%.3f mrr=%.3f recall=%.3f lat=%s strategy=%s postings=%d lookups=%d  %q\n", q.NDCG, q.MRR, q.Recall, q.Latency, q.ExecutionStrategy, q.PostingEntriesRead, q.IndexSearches, q.Query)
			continue
		}
		fmt.Fprintf(w, "  ndcg=%.3f mrr=%.3f recall=%.3f lat=%s  %q\n", q.NDCG, q.MRR, q.Recall, q.Latency, q.Query)
	}
}
