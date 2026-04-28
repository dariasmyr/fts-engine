package bench

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/dariasmyr/fts-engine/internal/domain/models"
	pkgfts "github.com/dariasmyr/fts-engine/pkg/fts"
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
	Query    string
	Returned int
	Relevant int
	NDCG     float64
	MRR      float64
	Recall   float64
	Latency  time.Duration
}

type Report struct {
	K          int
	Index      IndexReport
	Queries    []QueryReport
	LatencyP50 time.Duration
	LatencyP95 time.Duration
	LatencyP99 time.Duration
	MeanNDCG   float64
	MeanMRR    float64
	MeanRecall float64
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

func RunQueries(ctx context.Context, svc *pkgfts.Service, gt *GroundTruth, titleIdx map[string]string, k int) ([]QueryReport, error) {
	reports := make([]QueryReport, 0, len(gt.Queries))
	for _, q := range gt.Queries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		relevant := NewRelevanceSet(ResolveRelevant(q, titleIdx))
		start := time.Now()
		res, err := svc.SearchDocuments(ctx, q.Query, k)
		elapsed := time.Since(start)
		if err != nil {
			return nil, fmt.Errorf("search %q: %w", q.Query, err)
		}

		ranked := make([]string, 0, len(res.Results))
		for _, item := range res.Results {
			ranked = append(ranked, string(item.ID))
		}

		reports = append(reports, QueryReport{
			Query:    q.Query,
			Returned: len(ranked),
			Relevant: relevant.Size(),
			NDCG:     NDCG(ranked, relevant, k),
			MRR:      MRR(ranked, relevant),
			Recall:   Recall(ranked, relevant, k),
			Latency:  elapsed,
		})
	}
	return reports, nil
}

func Aggregate(k int, idx IndexReport, queries []QueryReport) Report {
	report := Report{K: k, Index: idx, Queries: queries}
	if len(queries) == 0 {
		return report
	}

	latencies := make([]time.Duration, 0, len(queries))
	var sumNDCG, sumMRR, sumRecall float64
	for _, q := range queries {
		latencies = append(latencies, q.Latency)
		sumNDCG += q.NDCG
		sumMRR += q.MRR
		sumRecall += q.Recall
	}

	n := float64(len(queries))
	report.MeanNDCG = sumNDCG / n
	report.MeanMRR = sumMRR / n
	report.MeanRecall = sumRecall / n
	report.LatencyP50 = Percentile(latencies, 0.50)
	report.LatencyP95 = Percentile(latencies, 0.95)
	report.LatencyP99 = Percentile(latencies, 0.99)

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

	if topWorst <= 0 || len(r.Queries) == 0 {
		return
	}
	if topWorst > len(r.Queries) {
		topWorst = len(r.Queries)
	}

	fmt.Fprintf(w, "\nWorst %d queries by nDCG@%d:\n", topWorst, r.K)
	for i := 0; i < topWorst; i++ {
		q := r.Queries[i]
		fmt.Fprintf(w, "  ndcg=%.3f mrr=%.3f recall=%.3f lat=%s  %q\n", q.NDCG, q.MRR, q.Recall, q.Latency, q.Query)
	}
}
