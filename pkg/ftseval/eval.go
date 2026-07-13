package ftseval

import (
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

type Query struct {
	Name     string
	Query    string
	Relevant map[fts.DocID]float64
}

type SearchFunc func(context.Context, string, int) ([]fts.DocID, error)

type Report struct {
	K            int
	QueryCount   int
	MRR          float64
	PrecisionAtK float64
	RecallAtK    float64
	NDCGAtK      float64
	Queries      []QueryReport
}

type QueryReport struct {
	Name         string
	Query        string
	Ranked       []fts.DocID
	MRR          float64
	PrecisionAtK float64
	RecallAtK    float64
	NDCGAtK      float64
}

func Evaluate(ctx context.Context, queries []Query, k int, search SearchFunc) (*Report, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if k <= 0 {
		return nil, fmt.Errorf("ftseval: k must be positive")
	}
	if search == nil {
		return nil, fmt.Errorf("ftseval: search function is nil")
	}

	report := &Report{K: k, QueryCount: len(queries), Queries: make([]QueryReport, 0, len(queries))}
	for _, q := range queries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ranked, err := search(ctx, q.Query, k)
		if err != nil {
			return nil, fmt.Errorf("ftseval: query %q: %w", q.Query, err)
		}

		qr := QueryReport{
			Name:         q.Name,
			Query:        q.Query,
			Ranked:       append([]fts.DocID(nil), ranked...),
			MRR:          MRR(ranked, q.Relevant),
			PrecisionAtK: PrecisionAtK(ranked, q.Relevant, k),
			RecallAtK:    RecallAtK(ranked, q.Relevant, k),
			NDCGAtK:      NDCGAtK(ranked, q.Relevant, k),
		}
		report.Queries = append(report.Queries, qr)
		report.MRR += qr.MRR
		report.PrecisionAtK += qr.PrecisionAtK
		report.RecallAtK += qr.RecallAtK
		report.NDCGAtK += qr.NDCGAtK
	}

	if report.QueryCount > 0 {
		n := float64(report.QueryCount)
		report.MRR /= n
		report.PrecisionAtK /= n
		report.RecallAtK /= n
		report.NDCGAtK /= n
	}
	return report, nil
}

func MRR(ranked []fts.DocID, relevant map[fts.DocID]float64) float64 {
	if len(ranked) == 0 || len(relevant) == 0 {
		return 0
	}
	seen := make(map[fts.DocID]struct{}, len(ranked))
	for i, id := range ranked {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		if relevant[id] > 0 {
			return 1 / float64(i+1)
		}
	}
	return 0
}

func PrecisionAtK(ranked []fts.DocID, relevant map[fts.DocID]float64, k int) float64 {
	if k <= 0 || len(ranked) == 0 || len(relevant) == 0 {
		return 0
	}
	limit := min(k, len(ranked))
	seen := make(map[fts.DocID]struct{}, limit)
	hits := 0
	for _, id := range ranked[:limit] {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		if relevant[id] > 0 {
			hits++
		}
	}
	return float64(hits) / float64(k)
}

func RecallAtK(ranked []fts.DocID, relevant map[fts.DocID]float64, k int) float64 {
	if k <= 0 || len(ranked) == 0 || len(relevant) == 0 {
		return 0
	}
	limit := min(k, len(ranked))
	seen := make(map[fts.DocID]struct{}, limit)
	hits := 0
	for _, id := range ranked[:limit] {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		if relevant[id] > 0 {
			hits++
		}
	}
	return float64(hits) / float64(relevantCount(relevant))
}

func NDCGAtK(ranked []fts.DocID, relevant map[fts.DocID]float64, k int) float64 {
	if k <= 0 || len(ranked) == 0 || len(relevant) == 0 {
		return 0
	}
	idcg := idealDCG(relevant, k)
	if idcg == 0 {
		return 0
	}
	return dcg(ranked, relevant, k) / idcg
}

func dcg(ranked []fts.DocID, relevant map[fts.DocID]float64, k int) float64 {
	limit := min(k, len(ranked))
	seen := make(map[fts.DocID]struct{}, limit)
	var sum float64
	for i, id := range ranked[:limit] {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		grade := relevant[id]
		if grade <= 0 {
			continue
		}
		sum += (math.Pow(2, grade) - 1) / math.Log2(float64(i+2))
	}
	return sum
}

func idealDCG(relevant map[fts.DocID]float64, k int) float64 {
	grades := make([]float64, 0, len(relevant))
	for _, grade := range relevant {
		if grade > 0 {
			grades = append(grades, grade)
		}
	}
	sort.Slice(grades, func(i, j int) bool { return grades[i] > grades[j] })
	limit := min(k, len(grades))
	var sum float64
	for i, grade := range grades[:limit] {
		sum += (math.Pow(2, grade) - 1) / math.Log2(float64(i+2))
	}
	return sum
}

func relevantCount(relevant map[fts.DocID]float64) int {
	var count int
	for _, grade := range relevant {
		if grade > 0 {
			count++
		}
	}
	return count
}
