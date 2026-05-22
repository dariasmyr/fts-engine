package fts

import (
	"container/heap"
	"context"
	"math"
	"sort"
)

func (s *Service) tryExecBooleanOrWand(ctx context.Context, q *BooleanQuery, candidateLimit int, scope queryFieldScope) (map[DocOrd]docAccum, bool, error) {
	if exec := diagnosticsFromContext(ctx); exec != nil {
		exec.updateBooleanDiagnostics(func(b *BooleanDiagnostics) {
			b.WAND.TopK = candidateLimit
		})
	}
	if candidateLimit <= 0 || s.scorer == nil {
		if exec := diagnosticsFromContext(ctx); exec != nil {
			reason := "wand_disabled_no_topk"
			if candidateLimit <= 0 {
				exec.setSkipReasonIfEmpty(reason)
			} else {
				reason = "wand_disabled_no_scorer"
				exec.setSkipReasonIfEmpty(reason)
			}
			exec.recordFastPathSkip(reason)
			exec.updateBooleanDiagnostics(func(b *BooleanDiagnostics) {
				b.WAND.SkipReason = reason
			})
		}
		return nil, false, nil
	}

	shouldTerms, mustNots, ok, _ := parseFastOrClauses(q)
	if !ok || len(shouldTerms) == 0 {
		if exec := diagnosticsFromContext(ctx); exec != nil {
			reason := "wand_not_or_terms_only"
			exec.setSkipReasonIfEmpty(reason)
			exec.recordFastPathSkip(reason)
			exec.updateBooleanDiagnostics(func(b *BooleanDiagnostics) {
				b.WAND.SkipReason = reason
			})
		}
		return nil, false, nil
	}
	if exec := diagnosticsFromContext(ctx); exec != nil {
		exec.updateBooleanDiagnostics(func(b *BooleanDiagnostics) {
			b.WAND.ClauseCount = len(shouldTerms)
		})
	}

	exclude, err := s.buildExcludeSet(ctx, mustNots, scope)
	if err != nil {
		return nil, false, err
	}

	plan := make([]fastMust, 0, len(shouldTerms))
	postingsPerClause := make([]int, 0, len(shouldTerms))
	for _, term := range shouldTerms {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		group, err := s.collectTermPostings(ctx, term, scope)
		if err != nil {
			return nil, false, err
		}
		postingsPerClause = append(postingsPerClause, group.totalDocs)
		if group.totalDocs == 0 {
			continue
		}
		plan = append(plan, group)
	}
	if exec := diagnosticsFromContext(ctx); exec != nil {
		exec.updateBooleanDiagnostics(func(b *BooleanDiagnostics) {
			b.WAND.PostingsPerClause = append([]int(nil), postingsPerClause...)
		})
	}
	if len(plan) == 0 {
		if exec := diagnosticsFromContext(ctx); exec != nil {
			exec.setStrategy(strategyBoolOrWAND)
			exec.updateBooleanDiagnostics(func(b *BooleanDiagnostics) {
				b.WAND.Eligible = true
				b.WAND.Used = true
				b.WAND.HeapSize = 0
			})
		}
		return map[DocOrd]docAccum{}, true, nil
	}
	if !allSingleExpansionInSameField(plan) || !allClausesHaveStrictSeq(plan) {
		if exec := diagnosticsFromContext(ctx); exec != nil {
			reason := "wand_multiple_expansions_or_fields"
			if !allSingleExpansionInSameField(plan) {
				exec.setSkipReasonIfEmpty(reason)
			} else {
				reason = "wand_non_strict_seq"
				exec.setSkipReasonIfEmpty(reason)
			}
			exec.recordFastPathSkip(reason)
			exec.updateBooleanDiagnostics(func(b *BooleanDiagnostics) {
				b.WAND.SkipReason = reason
			})
		}
		return nil, false, nil
	}
	if exec := diagnosticsFromContext(ctx); exec != nil {
		exec.setStrategy(strategyBoolOrWAND)
		exec.updateBooleanDiagnostics(func(b *BooleanDiagnostics) {
			b.WAND.Eligible = true
			b.WAND.Used = true
		})
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
	postingsConsidered := 0
	candidateDocs := 0

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
			candidateDocs++
			matchedDocOrd := clauses[0].currentDocOrd()
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

			if _, skip := exclude[matchedDocOrd]; !skip {
				matchedDocID, _ := s.lookupDocID(matchedDocOrd)
				hit := wandHit{id: matchedDocID, ord: matchedDocOrd, accum: accum}
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
					postingsConsidered++
					c.cursor++
				}
			}
			continue
		}

		for i := 0; i <= pivot; i++ {
			c := clauses[i]
			if c.currentSeq() < pivotSeq {
				for c.cursor < len(c.exp.docs) && c.exp.docs[c.cursor].Seq < pivotSeq {
					postingsConsidered++
					c.cursor++
				}
				break
			}
		}
	}

	out := make(map[DocOrd]docAccum, h.Len())
	for _, hit := range *h {
		out[hit.ord] = hit.accum
	}
	if exec := diagnosticsFromContext(ctx); exec != nil {
		exec.updateBooleanDiagnostics(func(b *BooleanDiagnostics) {
			b.WAND.PostingsConsidered = postingsConsidered
			b.WAND.CandidateDocs = candidateDocs
			b.WAND.HeapSize = h.Len()
			b.WAND.FinalTheta = theta
		})
	}
	return out, true, nil
}

type wandClause struct {
	exp    *termExpansion
	ub     float64
	cursor int
}

func (c *wandClause) currentDoc() DocRef    { return c.exp.docs[c.cursor] }
func (c *wandClause) currentSeq() uint32    { return c.exp.docs[c.cursor].Seq }
func (c *wandClause) currentDocOrd() DocOrd { return c.exp.docs[c.cursor].Ord }
func (c *wandClause) exhausted() bool       { return c.cursor >= len(c.exp.docs) }

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
	ds := DocStats{Ord: 0, Length: 1}
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
	ord   DocOrd
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
