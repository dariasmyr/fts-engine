package quality

import (
	"github.com/dariasmyr/fts-engine/benchmarks/internal/harness"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftseval"
)

type Qrels map[string]map[string]int

type Scores struct {
	K            int
	NumScored    int
	RecallAtK    float64
	MRR          float64
	NDCGAtK      float64
	PrecisionAtK float64
}

func Compute(results []harness.QueryResult, qrels Qrels, k int) *Scores {
	if len(qrels) == 0 {
		return nil
	}
	var sumRecall, sumMRR, sumNDCG, sumPrecision float64
	numScored := 0
	for _, qr := range results {
		rel, ok := qrels[qr.QueryID]
		if !ok || len(rel) == 0 {
			continue
		}
		numScored++
		sumRecall += Recall(qr.Hits, rel, k)
		sumMRR += MRR(qr.Hits, rel)
		sumNDCG += NDCG(qr.Hits, rel, k)
		sumPrecision += Precision(qr.Hits, rel, k)
	}
	if numScored == 0 {
		return nil
	}
	denom := float64(numScored)
	return &Scores{
		K:            k,
		NumScored:    numScored,
		RecallAtK:    sumRecall / denom,
		MRR:          sumMRR / denom,
		NDCGAtK:      sumNDCG / denom,
		PrecisionAtK: sumPrecision / denom,
	}
}

func Precision(hits []harness.SearchHit, relevant map[string]int, k int) float64 {
	return ftseval.PrecisionAtK(rankedDocIDs(hits), relevantDocIDs(relevant), k)
}

func Recall(hits []harness.SearchHit, relevant map[string]int, k int) float64 {
	return ftseval.RecallAtK(rankedDocIDs(hits), relevantDocIDs(relevant), k)
}

func MRR(hits []harness.SearchHit, relevant map[string]int) float64 {
	return ftseval.MRR(rankedDocIDs(hits), relevantDocIDs(relevant))
}

func NDCG(hits []harness.SearchHit, relevant map[string]int, k int) float64 {
	return ftseval.NDCGAtK(rankedDocIDs(hits), relevantDocIDs(relevant), k)
}

func rankedDocIDs(hits []harness.SearchHit) []fts.DocID {
	out := make([]fts.DocID, 0, len(hits))
	for _, hit := range hits {
		out = append(out, fts.DocID(hit.DocID))
	}
	return out
}

func relevantDocIDs(relevant map[string]int) map[fts.DocID]float64 {
	out := make(map[fts.DocID]float64, len(relevant))
	for id, grade := range relevant {
		out[fts.DocID(id)] = float64(grade)
	}
	return out
}
