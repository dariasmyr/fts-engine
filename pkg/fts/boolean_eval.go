package fts

import (
	"context"
	"sort"
)

func (s *Service) execBoolean(ctx context.Context, q *BooleanQuery, candidateLimit int, scope queryFieldScope) (map[DocID]docAccum, error) {
	if q == nil || len(q.Clauses) == 0 {
		return map[DocID]docAccum{}, nil
	}

	if res, ok, err := s.tryExecBooleanAndFast(ctx, q, scope); err != nil {
		return nil, err
	} else if ok {
		return res, nil
	}

	if res, ok, err := s.tryExecBooleanOrWand(ctx, q, candidateLimit, scope); err != nil {
		return nil, err
	} else if ok {
		return res, nil
	}

	if res, ok, err := s.tryExecBooleanOrFast(ctx, q, scope); err != nil {
		return nil, err
	} else if ok {
		return res, nil
	}

	var musts []map[DocID]docAccum
	var shoulds []map[DocID]docAccum
	exclude := make(map[DocID]struct{})

	for _, clause := range q.Clauses {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if clause.Query == nil {
			continue
		}
		childHits, err := s.executeQuery(ctx, clause.Query, 0, scope)
		if err != nil {
			return nil, err
		}
		switch clause.Occur {
		case Must:
			musts = append(musts, childHits)
		case Should:
			shoulds = append(shoulds, childHits)
		case MustNot:
			for id := range childHits {
				exclude[id] = struct{}{}
			}
		}
	}

	combined := make(map[DocID]docAccum)
	if len(musts) > 0 {
		sort.Slice(musts, func(i, j int) bool { return len(musts[i]) < len(musts[j]) })
		for id, baseHit := range musts[0] {
			if _, skip := exclude[id]; skip {
				continue
			}
			accum := baseHit
			allMustClausesMatch := true
			for _, otherMust := range musts[1:] {
				otherHit, found := otherMust[id]
				if !found {
					allMustClausesMatch = false
					break
				}
				accum = addAccum(accum, otherHit)
			}
			if allMustClausesMatch {
				combined[id] = accum
			}
		}
		for _, shouldHits := range shoulds {
			for id, shouldHit := range shouldHits {
				if existing, ok := combined[id]; ok {
					combined[id] = addAccum(existing, shouldHit)
				}
			}
		}
		return combined, nil
	}

	for _, shouldHits := range shoulds {
		for id, shouldHit := range shouldHits {
			if _, skip := exclude[id]; skip {
				continue
			}
			combined[id] = addAccum(combined[id], shouldHit)
		}
	}

	return combined, nil
}

func termQueryOf(q Query) (TermQuery, bool) {
	switch v := q.(type) {
	case TermQuery:
		return v, true
	case *TermQuery:
		if v == nil {
			return TermQuery{}, false
		}
		return *v, true
	}
	return TermQuery{}, false
}
