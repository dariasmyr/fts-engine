package fts

import (
	"context"
	"fmt"
	"sort"
	"time"
)

func addAccum(a, b docAccum) docAccum {
	return docAccum{
		UniqueMatches: a.UniqueMatches + b.UniqueMatches,
		TotalMatches:  a.TotalMatches + b.TotalMatches,
	}
}

func (s *Service) Search(ctx context.Context, q Query, maxResults int) (*SearchResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if q == nil {
		return &SearchResult{Results: []Result{}, Timings: map[string]string{}}, nil
	}

	start := time.Now()
	timings := make(map[string]string, 2)

	searchStart := time.Now()
	hits, err := s.executeQuery(ctx, q, maxResults)
	if err != nil {
		return nil, err
	}
	timings["search_tokens"] = formatDuration(time.Since(searchStart))

	results := make([]Result, 0, len(hits))
	for id, h := range hits {
		results = append(results, Result{
			ID:            id,
			UniqueMatches: h.UniqueMatches,
			TotalMatches:  h.TotalMatches,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].UniqueMatches != results[j].UniqueMatches {
			return results[i].UniqueMatches > results[j].UniqueMatches
		}
		if results[i].TotalMatches != results[j].TotalMatches {
			return results[i].TotalMatches > results[j].TotalMatches
		}
		return results[i].ID < results[j].ID
	})

	totalFound := len(results)
	if maxResults <= 0 || maxResults > totalFound {
		maxResults = totalFound
	}

	timings["total"] = formatDuration(time.Since(start))

	return &SearchResult{
		Results:           results[:maxResults],
		TotalResultsCount: totalFound,
		Timings:           timings,
	}, nil
}

func (s *Service) executeQuery(ctx context.Context, q Query, topK int) (map[DocID]docAccum, error) {
	switch t := q.(type) {
	case TermQuery:
		return s.execTerm(ctx, t)
	case *TermQuery:
		if t == nil {
			return map[DocID]docAccum{}, nil
		}
		return s.execTerm(ctx, *t)
	case PhraseQuery:
		return s.execPhrase(ctx, t)
	case *PhraseQuery:
		if t == nil {
			return map[DocID]docAccum{}, nil
		}
		return s.execPhrase(ctx, *t)
	case PrefixQuery:
		return s.execPrefix(ctx, t)
	case *PrefixQuery:
		if t == nil {
			return map[DocID]docAccum{}, nil
		}
		return s.execPrefix(ctx, *t)
	case *BooleanQuery:
		return s.execBoolean(ctx, t, topK)
	default:
		return nil, fmt.Errorf("fts: unsupported query type %T", q)
	}
}

func (s *Service) execTerm(ctx context.Context, q TermQuery) (map[DocID]docAccum, error) {
	if q.Term == "" {
		return map[DocID]docAccum{}, nil
	}

	tokens := s.pipeline.Process(q.Term)
	if len(tokens) == 0 {
		return map[DocID]docAccum{}, nil
	}

	plan := make([]tokenGroup, 0, len(tokens))
	totalCap := 0
	fields := s.resolveFields(q.Field)
	for _, token := range tokens {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		keys, err := s.keyGen(token)
		if err != nil {
			return nil, fmt.Errorf("fts: term query: keygen: %w", err)
		}

		group := tokenGroup{expansions: make([][]DocRef, 0, len(fields)*len(keys))}
		for _, field := range fields {
			index, ok := s.lookupIndex(field)
			if !ok {
				continue
			}

			for _, key := range keys {
				if s.filter != nil && !s.filter.Contains([]byte(key)) {
					continue
				}

				docs, err := index.Search(key)
				if err != nil {
					return nil, fmt.Errorf("fts: term query field %q: index search: %w", field, err)
				}
				if len(docs) == 0 {
					continue
				}

				group.expansions = append(group.expansions, docs)
				group.totalDocs += len(docs)
				totalCap += len(docs)
			}
		}

		if len(group.expansions) == 0 {
			continue
		}
		group.single = len(group.expansions) == 1
		plan = append(plan, group)
	}

	hits := make(map[DocID]docAccum, totalCap)
	for _, group := range plan {
		var seenInGroup map[DocID]struct{}
		if !group.single {
			seenInGroup = make(map[DocID]struct{}, group.totalDocs)
		}

		for _, docs := range group.expansions {
			for _, doc := range docs {
				accum := hits[doc.ID]
				if group.single {
					accum.UniqueMatches++
				} else if _, seen := seenInGroup[doc.ID]; !seen {
					accum.UniqueMatches++
					seenInGroup[doc.ID] = struct{}{}
				}
				accum.TotalMatches += int(doc.Count)
				hits[doc.ID] = accum
			}
		}
	}

	return hits, nil
}

func (s *Service) execPhrase(ctx context.Context, q PhraseQuery) (map[DocID]docAccum, error) {
	res, err := s.searchPhraseFields(ctx, s.resolveFields(q.Field), q.Phrase, 0)
	if err != nil {
		return nil, err
	}
	hits := make(map[DocID]docAccum, len(res.Results))
	for _, r := range res.Results {
		hits[r.ID] = docAccum{UniqueMatches: r.UniqueMatches, TotalMatches: r.TotalMatches}
	}
	return hits, nil
}

func (s *Service) execPrefix(ctx context.Context, q PrefixQuery) (map[DocID]docAccum, error) {
	if q.Prefix == "" {
		return map[DocID]docAccum{}, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	hits := make(map[DocID]docAccum)
	seen := make(map[DocID]struct{})
	for _, field := range s.resolveFields(q.Field) {
		index, ok := s.lookupIndex(field)
		if !ok {
			continue
		}
		prefixer, ok := index.(PrefixIndex)
		if !ok {
			continue
		}

		docs, err := prefixer.SearchPrefix(q.Prefix)
		if err != nil {
			return nil, fmt.Errorf("fts: prefix query field %q: %w", field, err)
		}
		for _, doc := range docs {
			accum := hits[doc.ID]
			if _, ok := seen[doc.ID]; !ok {
				accum.UniqueMatches++
				seen[doc.ID] = struct{}{}
			}
			accum.TotalMatches += int(doc.Count)
			hits[doc.ID] = accum
		}
	}
	return hits, nil
}

func (s *Service) execBoolean(ctx context.Context, q *BooleanQuery, topK int) (map[DocID]docAccum, error) {
	if q == nil || len(q.Clauses) == 0 {
		return map[DocID]docAccum{}, nil
	}

	if res, ok, err := s.tryExecBooleanAndFast(ctx, q); err != nil {
		return nil, err
	} else if ok {
		return res, nil
	}

	if res, ok, err := s.tryExecBooleanOrFast(ctx, q); err != nil {
		return nil, err
	} else if ok {
		return res, nil
	}

	_ = topK
	var musts []map[DocID]docAccum
	var shoulds []map[DocID]docAccum
	exclude := make(map[DocID]struct{})

	for _, c := range q.Clauses {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if c.Query == nil {
			continue
		}
		child, err := s.executeQuery(ctx, c.Query, 0)
		if err != nil {
			return nil, err
		}
		switch c.Occur {
		case Must:
			musts = append(musts, child)
		case Should:
			shoulds = append(shoulds, child)
		case MustNot:
			for id := range child {
				exclude[id] = struct{}{}
			}
		}
	}

	combined := make(map[DocID]docAccum)
	if len(musts) > 0 {
		sort.Slice(musts, func(i, j int) bool { return len(musts[i]) < len(musts[j]) })
		for id, h := range musts[0] {
			if _, skip := exclude[id]; skip {
				continue
			}
			accum := h
			ok := true
			for _, other := range musts[1:] {
				oh, found := other[id]
				if !found {
					ok = false
					break
				}
				accum = addAccum(accum, oh)
			}
			if ok {
				combined[id] = accum
			}
		}
		for _, sh := range shoulds {
			for id, h := range sh {
				if existing, ok := combined[id]; ok {
					combined[id] = addAccum(existing, h)
				}
			}
		}
		return combined, nil
	}

	for _, sh := range shoulds {
		for id, h := range sh {
			if _, skip := exclude[id]; skip {
				continue
			}
			combined[id] = addAccum(combined[id], h)
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
