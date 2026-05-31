package bench

import (
	"math"
	"sort"
	"time"
)

type RelevanceSet map[string]struct{}

func NewRelevanceSet(ids []string) RelevanceSet {
	rs := make(RelevanceSet, len(ids))
	for _, id := range ids {
		rs[id] = struct{}{}
	}
	return rs
}

func (rs RelevanceSet) Contains(id string) bool {
	_, ok := rs[id]
	return ok
}

func (rs RelevanceSet) Size() int { return len(rs) }

func NDCG(ranked []string, relevant RelevanceSet, k int) float64 {
	if k <= 0 || len(relevant) == 0 {
		return 0
	}
	if k > len(ranked) {
		k = len(ranked)
	}

	dcg := 0.0
	for i := 0; i < k; i++ {
		if relevant.Contains(ranked[i]) {
			dcg += 1.0 / math.Log2(float64(i)+2.0)
		}
	}

	idealHits := len(relevant)
	if idealHits > k {
		idealHits = k
	}
	idcg := 0.0
	for i := 0; i < idealHits; i++ {
		idcg += 1.0 / math.Log2(float64(i)+2.0)
	}
	if idcg == 0 {
		return 0
	}
	return dcg / idcg
}

func MRR(ranked []string, relevant RelevanceSet) float64 {
	for i, id := range ranked {
		if relevant.Contains(id) {
			return 1.0 / float64(i+1)
		}
	}
	return 0
}

func Recall(ranked []string, relevant RelevanceSet, k int) float64 {
	if len(relevant) == 0 {
		return 0
	}
	if k > len(ranked) {
		k = len(ranked)
	}
	hits := 0
	for i := 0; i < k; i++ {
		if relevant.Contains(ranked[i]) {
			hits++
		}
	}
	return float64(hits) / float64(len(relevant))
}

func Percentile(durations []time.Duration, p float64) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(durations))
	copy(sorted, durations)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	switch {
	case p <= 0:
		return sorted[0]
	case p >= 1:
		return sorted[len(sorted)-1]
	}

	idx := p * float64(len(sorted)-1)
	lo := int(math.Floor(idx))
	hi := int(math.Ceil(idx))
	if lo == hi {
		return sorted[lo]
	}
	frac := idx - float64(lo)
	return sorted[lo] + time.Duration(float64(sorted[hi]-sorted[lo])*frac)
}
