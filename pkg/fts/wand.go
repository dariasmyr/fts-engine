package fts

import (
	"container/heap"
	"context"
	"math"
	"sort"
)

func (s *Service) tryExecBooleanOrWand(ctx context.Context, q *BooleanQuery, candidateLimit int, scope queryFieldScope) (map[DocID]docAccum, bool, error) {
	if candidateLimit <= 0 || s.scorer == nil {
		return nil, false, nil
	}

	shouldTerms, mustNots, ok := parseFastOrClauses(q)
	if !ok || len(shouldTerms) == 0 {
		return nil, false, nil
	}

	exclude, err := s.buildExcludeSet(ctx, mustNots, scope)
	if err != nil {
		return nil, false, err
	}

	plan := make([]fastMust, 0, len(shouldTerms))
	for _, term := range shouldTerms {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		group, err := s.collectTermPostings(ctx, term, scope)
		if err != nil {
			return nil, false, err
		}
		if group.totalDocs == 0 {
			continue
		}
		plan = append(plan, group)
	}
	if len(plan) == 0 {
		return map[DocID]docAccum{}, true, nil
	}
	if !allSingleExpansionInSameField(plan) || !allClausesHaveStrictSeq(plan) {
		return nil, false, nil
	}

	clauses := make([]*wandClause, 0, len(plan))
	for i := range plan {
		exp := &plan[i].expansions[0]
		clauses = append(clauses, &wandClause{
			exp:    exp,
			ub:     clauseUpperBound(exp, s),
			cursor: 0,
		})
	}

	h := &candidateHeap{}
	heap.Init(h)
	var theta float64

	for {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}

		clauses = compactWandClauses(clauses)
		if len(clauses) == 0 {
			break
		}

		sort.Slice(clauses, func(i, j int) bool {
			return clauses[i].currentSeq() < clauses[j].currentSeq()
		})

		pivot := -1
		var cum float64
		for i, c := range clauses {
			cum += c.ub
			if cum >= theta {
				pivot = i
				break
			}
		}
		if pivot < 0 {
			break
		}

		pivotSeq := clauses[pivot].currentSeq()
		if clauses[0].currentSeq() == pivotSeq {
			matchedDocID := clauses[0].currentDocID()
			var accum docAccum
			for _, c := range clauses {
				if c.currentSeq() != pivotSeq {
					continue
				}
				d := c.currentDoc()
				accum.UniqueMatches++
				accum.TotalMatches += int(d.Count)
				accum.Score += s.scoreTermExpansionDoc(*c.exp, d)
			}

			if _, skip := exclude[matchedDocID]; !skip {
				hit := wandHit{id: matchedDocID, accum: accum}
				if h.Len() < candidateLimit {
					heap.Push(h, hit)
					if h.Len() == candidateLimit {
						theta = (*h)[0].accum.Score
					}
				} else if betterWandHit(hit, (*h)[0]) {
					(*h)[0] = hit
					heap.Fix(h, 0)
					theta = (*h)[0].accum.Score
				}
			}

			for _, c := range clauses {
				if c.currentSeq() == pivotSeq {
					c.cursor++
				}
			}
			continue
		}

		for i := 0; i <= pivot; i++ {
			c := clauses[i]
			if c.currentSeq() < pivotSeq {
				for c.cursor < len(c.exp.docs) && c.exp.docs[c.cursor].Seq < pivotSeq {
					c.cursor++
				}
				break
			}
		}
	}

	out := make(map[DocID]docAccum, h.Len())
	for _, hit := range *h {
		out[hit.id] = hit.accum
	}
	return out, true, nil
}

type wandClause struct {
	exp    *termExpansion
	ub     float64
	cursor int
}

func (c *wandClause) currentDoc() DocRef  { return c.exp.docs[c.cursor] }
func (c *wandClause) currentSeq() uint32  { return c.exp.docs[c.cursor].Seq }
func (c *wandClause) currentDocID() DocID { return c.exp.docs[c.cursor].ID }
func (c *wandClause) exhausted() bool     { return c.cursor >= len(c.exp.docs) }

func compactWandClauses(cs []*wandClause) []*wandClause {
	out := cs[:0]
	for _, c := range cs {
		if !c.exhausted() {
			out = append(out, c)
		}
	}
	return out
}

func clauseUpperBound(exp *termExpansion, s *Service) float64 {
	if s.scorer == nil {
		return math.Inf(1)
	}
	var maxTF uint32
	for i := range exp.docs {
		if exp.docs[i].Count > maxTF {
			maxTF = exp.docs[i].Count
		}
	}
	if maxTF == 0 {
		return 0
	}

	ts := TermStats{Field: exp.field, Term: exp.term, TF: maxTF, DF: exp.df}
	ds := DocStats{ID: "", Length: 1}
	return s.scorer.Score(ts, ds, exp.fieldStats)
}

func allClausesHaveStrictSeq(plan []fastMust) bool {
	for i := range plan {
		docs := plan[i].expansions[0].docs
		for j := 1; j < len(docs); j++ {
			if docs[j-1].Seq >= docs[j].Seq {
				return false
			}
		}
	}
	return true
}

type wandHit struct {
	id    DocID
	accum docAccum
}

func betterWandHit(a, b wandHit) bool {
	if a.accum.Score != b.accum.Score {
		return a.accum.Score > b.accum.Score
	}
	if a.accum.UniqueMatches != b.accum.UniqueMatches {
		return a.accum.UniqueMatches > b.accum.UniqueMatches
	}
	if a.accum.TotalMatches != b.accum.TotalMatches {
		return a.accum.TotalMatches > b.accum.TotalMatches
	}
	return a.id < b.id
}

type candidateHeap []wandHit

func (h candidateHeap) Len() int { return len(h) }

func (h candidateHeap) Less(i, j int) bool {
	return betterWandHit(h[j], h[i])
}

func (h candidateHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *candidateHeap) Push(x any) {
	*h = append(*h, x.(wandHit))
}

func (h *candidateHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
