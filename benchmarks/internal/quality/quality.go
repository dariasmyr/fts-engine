package quality

import (
	"math"

	"github.com/dariasmyr/fts-engine/benchmarks/internal/harness"
)

type Qrels map[string]map[string]int

type Scores struct {
	K         int
	NumScored int
	RecallAtK float64
	MRR       float64
	NDCGAtK   float64
}

func Compute(results []harness.QueryResult, qrels Qrels, k int) *Scores {
	if len(qrels) == 0 {
		return nil
	}
	var sumRecall, sumMRR, sumNDCG float64
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
	}
	if numScored == 0 {
		return nil
	}
	denom := float64(numScored)
	return &Scores{
		K:         k,
		NumScored: numScored,
		RecallAtK: sumRecall / denom,
		MRR:       sumMRR / denom,
		NDCGAtK:   sumNDCG / denom,
	}
}

func Recall(hits []harness.SearchHit, relevant map[string]int, k int) float64 {
	if len(relevant) == 0 {
		return 0
	}
	if k > len(hits) {
		k = len(hits)
	}
	matches := 0
	for i := 0; i < k; i++ {
		if relevant[hits[i].DocID] > 0 {
			matches++
		}
	}
	return float64(matches) / float64(len(relevant))
}

func MRR(hits []harness.SearchHit, relevant map[string]int) float64 {
	for i, hit := range hits {
		if relevant[hit.DocID] > 0 {
			return 1 / float64(i+1)
		}
	}
	return 0
}

func NDCG(hits []harness.SearchHit, relevant map[string]int, k int) float64 {
	if len(relevant) == 0 || k <= 0 {
		return 0
	}
	if k > len(hits) {
		k = len(hits)
	}
	dcg := 0.0
	for i := 0; i < k; i++ {
		rel := relevant[hits[i].DocID]
		if rel <= 0 {
			continue
		}
		dcg += (math.Pow(2, float64(rel)) - 1) / math.Log2(float64(i)+2)
	}
	idealHits := len(relevant)
	if idealHits > k {
		idealHits = k
	}
	idcg := 0.0
	for i := 0; i < idealHits; i++ {
		idcg += 1 / math.Log2(float64(i)+2)
	}
	if idcg == 0 {
		return 0
	}
	return dcg / idcg
}
