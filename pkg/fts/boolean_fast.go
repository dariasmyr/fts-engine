package fts

import (
	"context"
	"sort"
)

const lookupMapThreshold = 50

type termExpansion struct {
	term  string
	docs  []DocRef
	byDoc map[DocID]uint32
}

func (e *termExpansion) buildMap() {
	if e.byDoc != nil {
		return
	}
	e.byDoc = make(map[DocID]uint32, len(e.docs))
	for _, d := range e.docs {
		e.byDoc[d.ID] = d.Count
	}
}

func (e *termExpansion) lookup(id DocID) (uint32, bool) {
	if e.byDoc != nil {
		tf, ok := e.byDoc[id]
		return tf, ok
	}
	for _, d := range e.docs {
		if d.ID == id {
			return d.Count, true
		}
	}
	return 0, false
}

type fastMust struct {
	expansions []termExpansion
	totalDocs  int
}

func (m *fastMust) contains(id DocID) bool {
	for i := range m.expansions {
		if _, ok := m.expansions[i].lookup(id); ok {
			return true
		}
	}
	return false
}

func (s *Service) tryExecBooleanAndFast(ctx context.Context, q *BooleanQuery) (map[DocID]docAccum, bool, error) {
	var mustTerms []TermQuery
	var shoulds []BoolClause
	var mustNots []BoolClause
	for _, c := range q.Clauses {
		if c.Query == nil {
			continue
		}
		switch c.Occur {
		case Must:
			tq, ok := termQueryOf(c.Query)
			if !ok {
				return nil, false, nil
			}
			mustTerms = append(mustTerms, tq)
		case Should:
			shoulds = append(shoulds, c)
		case MustNot:
			mustNots = append(mustNots, c)
		}
	}
	if len(mustTerms) == 0 {
		return nil, false, nil
	}

	musts := make([]fastMust, 0, len(mustTerms))
	for _, tq := range mustTerms {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		fm, err := s.collectTermPostings(tq)
		if err != nil {
			return nil, false, err
		}
		if fm.totalDocs == 0 {
			return map[DocID]docAccum{}, true, nil
		}
		musts = append(musts, fm)
	}

	exclude := make(map[DocID]struct{})
	for _, c := range mustNots {
		child, err := s.executeQuery(ctx, c.Query, 0)
		if err != nil {
			return nil, false, err
		}
		for id := range child {
			exclude[id] = struct{}{}
		}
	}

	sort.Slice(musts, func(i, j int) bool { return musts[i].totalDocs < musts[j].totalDocs })
	if allSingleExpansion(musts) {
		// Fast path: each MUST clause maps to exactly one postings list, so we can intersect by Seq.
		return s.execBooleanAndSortMerge(musts, shoulds, exclude, ctx)
	}

	// Fallback path: use the smallest MUST group as the candidate driver when clauses expand to multiple lists.
	driver := &musts[0]
	others := musts[1:]
	if driver.totalDocs >= lookupMapThreshold {
		// For larger candidate sets, prebuild lookup maps for the remaining MUST groups.
		for i := range others {
			for j := range others[i].expansions {
				others[i].expansions[j].buildMap()
			}
		}
	}

	// Final AND matches keyed by DocID, with accumulated clause and term-frequency counts.
	combined := make(map[DocID]docAccum, driver.totalDocs)
	for di := range driver.expansions {
		de := &driver.expansions[di]
		for _, d := range de.docs {
			if _, already := combined[d.ID]; already {
				continue
			}
			if _, skip := exclude[d.ID]; skip {
				continue
			}

			// Driver docs are only candidates; keep only docs that also match every other MUST clause.
			survives := true
			for i := range others {
				if !others[i].contains(d.ID) {
					survives = false
					break
				}
			}
			if !survives {
				continue
			}

			// Start with the current driver expansion; other expansions may add more TF for the same clause.
			accum := docAccum{UniqueMatches: 1, TotalMatches: int(d.Count)}
			for dj := range driver.expansions {
				if dj == di {
					continue
				}
				if tf, ok := driver.expansions[dj].lookup(d.ID); ok {
					accum.TotalMatches += int(tf)
				}
			}

			// Count each remaining MUST clause once, but add TF from every matching expansion in that clause.
			for i := range others {
				matchedAny := false
				for ej := range others[i].expansions {
					tf, ok := others[i].expansions[ej].lookup(d.ID)
					if !ok {
						continue
					}
					matchedAny = true
					accum.TotalMatches += int(tf)
				}
				if matchedAny {
					accum.UniqueMatches++
				}
			}

			combined[d.ID] = accum
		}
	}

	// SHOULD clauses only enrich docs that already survived all MUST checks.
	for _, c := range shoulds {
		child, err := s.executeQuery(ctx, c.Query, 0)
		if err != nil {
			return nil, false, err
		}
		for id, h := range child {
			if existing, ok := combined[id]; ok {
				combined[id] = addAccum(existing, h)
			}
		}
	}

	return combined, true, nil
}

func (s *Service) tryExecBooleanOrFast(ctx context.Context, q *BooleanQuery) (map[DocID]docAccum, bool, error) {
	var shouldTerms []TermQuery
	var mustNots []BoolClause
	for _, c := range q.Clauses {
		if c.Query == nil {
			continue
		}
		switch c.Occur {
		case Must:
			return nil, false, nil
		case Should:
			tq, ok := termQueryOf(c.Query)
			if !ok {
				return nil, false, nil
			}
			shouldTerms = append(shouldTerms, tq)
		case MustNot:
			mustNots = append(mustNots, c)
		}
	}
	if len(shouldTerms) == 0 {
		return nil, false, nil
	}

	exclude := make(map[DocID]struct{})
	for _, c := range mustNots {
		child, err := s.executeQuery(ctx, c.Query, 0)
		if err != nil {
			return nil, false, err
		}
		for id := range child {
			exclude[id] = struct{}{}
		}
	}

	plan := make([]fastMust, 0, len(shouldTerms))
	totalCap := 0
	for _, tq := range shouldTerms {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		fm, err := s.collectTermPostings(tq)
		if err != nil {
			return nil, false, err
		}
		if fm.totalDocs == 0 {
			continue
		}
		plan = append(plan, fm)
		totalCap += fm.totalDocs
	}
	if len(plan) == 0 {
		return map[DocID]docAccum{}, true, nil
	}

	combined := make(map[DocID]docAccum, totalCap)
	for _, group := range plan {
		single := len(group.expansions) == 1
		var seenInGroup map[DocID]struct{}
		if !single {
			seenInGroup = make(map[DocID]struct{}, group.totalDocs)
		}

		for _, expansion := range group.expansions {
			for _, d := range expansion.docs {
				if _, skip := exclude[d.ID]; skip {
					continue
				}
				accum := combined[d.ID]
				if single {
					accum.UniqueMatches++
				} else if _, seen := seenInGroup[d.ID]; !seen {
					accum.UniqueMatches++
					seenInGroup[d.ID] = struct{}{}
				}
				accum.TotalMatches += int(d.Count)
				combined[d.ID] = accum
			}
		}
	}

	return combined, true, nil
}

func allSingleExpansion(musts []fastMust) bool {
	for i := range musts {
		if len(musts[i].expansions) != 1 {
			return false
		}
	}
	return true
}

func (s *Service) execBooleanAndSortMerge(musts []fastMust, shoulds []BoolClause, exclude map[DocID]struct{}, ctx context.Context) (map[DocID]docAccum, bool, error) {
	k := len(musts)
	ptrs := make([]int, k)
	exps := make([]*termExpansion, k)
	for i := range musts {
		// allSingleExpansion guarantees exactly one postings list per MUST clause here.
		exps[i] = &musts[i].expansions[0]
		if len(exps[i].docs) == 0 {
			// One empty MUST list makes the whole AND empty.
			return map[DocID]docAccum{}, true, nil
		}
	}

	combined := make(map[DocID]docAccum, len(exps[0].docs))
	currentSeq := exps[0].docs[0].Seq

loop:
	for {
		for i := 0; i < k; i++ {
			docs := exps[i].docs
			// Advance each pointer up to the current candidate Seq.
			for ptrs[i] < len(docs) && docs[ptrs[i]].Seq < currentSeq {
				ptrs[i]++
			}
			if ptrs[i] >= len(docs) {
				break loop
			}
			if docs[ptrs[i]].Seq > currentSeq {
				// This list moved past the candidate, so retry from the larger Seq.
				currentSeq = docs[ptrs[i]].Seq
				i = -1
				continue
			}
		}

		// All MUST lists are aligned on the same Seq, so we found an intersection hit.
		docID := exps[0].docs[ptrs[0]].ID
		if _, skip := exclude[docID]; !skip {
			var accum docAccum
			for i := 0; i < k; i++ {
				d := exps[i].docs[ptrs[i]]
				accum.UniqueMatches++
				accum.TotalMatches += int(d.Count)
			}
			combined[docID] = accum
		}

		// Move every list forward past the matched document and continue the merge walk.
		for i := 0; i < k; i++ {
			ptrs[i]++
			if ptrs[i] >= len(exps[i].docs) {
				break loop
			}
		}
		currentSeq = exps[0].docs[ptrs[0]].Seq
	}

	// SHOULD clauses only enrich docs that already survived all MUST intersections.
	for _, c := range shoulds {
		child, err := s.executeQuery(ctx, c.Query, 0)
		if err != nil {
			return nil, false, err
		}
		for id, h := range child {
			if existing, ok := combined[id]; ok {
				combined[id] = addAccum(existing, h)
			}
		}
	}

	return combined, true, nil
}

func (s *Service) collectTermPostings(q TermQuery) (fastMust, error) {
	var out fastMust
	if q.Field != "" || q.Term == "" {
		return out, nil
	}

	tokens := s.pipeline.Process(q.Term)
	if len(tokens) == 0 {
		return out, nil
	}

	for _, token := range tokens {
		keys, err := s.keyGen(token)
		if err != nil {
			return fastMust{}, err
		}
		for _, key := range keys {
			if s.filter != nil && !s.filter.Contains([]byte(key)) {
				continue
			}
			docs, err := s.index.Search(key)
			if err != nil {
				return fastMust{}, err
			}
			if len(docs) == 0 {
				continue
			}
			out.expansions = append(out.expansions, termExpansion{term: token, docs: docs})
			out.totalDocs += len(docs)
		}
	}
	return out, nil
}
