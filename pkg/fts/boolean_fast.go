package fts

import (
	"context"
	"sort"
)

const lookupMapThreshold = 50

type termExpansion struct {
	field string
	term  string
	df    uint32

	fieldStats FieldStats
	docs       []DocRef
	byDoc      map[DocID]uint32
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

func parseFastAndClauses(q *BooleanQuery) ([]TermQuery, []BoolClause, []BoolClause, bool) {
	var mustTerms []TermQuery
	var shoulds []BoolClause
	var mustNots []BoolClause
	for _, clause := range q.Clauses {
		if clause.Query == nil {
			continue
		}
		switch clause.Occur {
		case Must:
			term, ok := termQueryOf(clause.Query)
			if !ok {
				return nil, nil, nil, false
			}
			mustTerms = append(mustTerms, term)
		case Should:
			shoulds = append(shoulds, clause)
		case MustNot:
			mustNots = append(mustNots, clause)
		}
	}
	if len(mustTerms) == 0 {
		return nil, nil, nil, false
	}
	return mustTerms, shoulds, mustNots, true
}

func parseFastOrClauses(q *BooleanQuery) ([]TermQuery, []BoolClause, bool) {
	var shouldTerms []TermQuery
	var mustNots []BoolClause
	for _, clause := range q.Clauses {
		if clause.Query == nil {
			continue
		}
		switch clause.Occur {
		case Must:
			return nil, nil, false
		case Should:
			term, ok := termQueryOf(clause.Query)
			if !ok {
				return nil, nil, false
			}
			shouldTerms = append(shouldTerms, term)
		case MustNot:
			mustNots = append(mustNots, clause)
		}
	}
	if len(shouldTerms) == 0 {
		return nil, nil, false
	}
	return shouldTerms, mustNots, true
}

func (s *Service) buildExcludeSet(ctx context.Context, clauses []BoolClause, scope queryFieldScope) (map[DocID]struct{}, error) {
	exclude := make(map[DocID]struct{})
	for _, clause := range clauses {
		child, err := s.executeQuery(ctx, clause.Query, 0, scope)
		if err != nil {
			return nil, err
		}
		for id := range child {
			exclude[id] = struct{}{}
		}
	}
	return exclude, nil
}

func (s *Service) collectFastMustGroups(ctx context.Context, terms []TermQuery, scope queryFieldScope) ([]fastMust, bool, error) {
	groups := make([]fastMust, 0, len(terms))
	for _, term := range terms {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		group, err := s.collectTermPostings(ctx, term, scope)
		if err != nil {
			return nil, false, err
		}
		if group.totalDocs == 0 {
			return nil, true, nil
		}
		groups = append(groups, group)
	}
	return groups, false, nil
}

func (s *Service) applyShouldClauseBoosts(ctx context.Context, combined map[DocID]docAccum, shoulds []BoolClause, scope queryFieldScope) error {
	for _, clause := range shoulds {
		child, err := s.executeQuery(ctx, clause.Query, 0, scope)
		if err != nil {
			return err
		}
		for id, hit := range child {
			if existing, ok := combined[id]; ok {
				combined[id] = addAccum(existing, hit)
			}
		}
	}
	return nil
}

func (s *Service) tryExecBooleanAndFast(ctx context.Context, q *BooleanQuery, scope queryFieldScope) (map[DocID]docAccum, bool, error) {
	mustTerms, shoulds, mustNots, ok := parseFastAndClauses(q)
	if !ok {
		return nil, false, nil
	}

	mustGroups, exhausted, err := s.collectFastMustGroups(ctx, mustTerms, scope)
	if err != nil {
		return nil, false, err
	}
	if exhausted {
		return map[DocID]docAccum{}, true, nil
	}

	exclude, err := s.buildExcludeSet(ctx, mustNots, scope)
	if err != nil {
		return nil, false, err
	}

	sort.Slice(mustGroups, func(i, j int) bool { return mustGroups[i].totalDocs < mustGroups[j].totalDocs })
	if allSingleExpansionInSameField(mustGroups) {
		// Fast path: Seq ordinals are only comparable within one field index.
		return s.execBooleanAndSortMerge(mustGroups, shoulds, exclude, ctx, scope)
	}

	// Fallback path: use the smallest MUST group as the candidate driver when clauses expand to multiple lists.
	driverGroup := &mustGroups[0]
	otherGroups := mustGroups[1:]
	if driverGroup.totalDocs >= lookupMapThreshold {
		// For larger candidate sets, prebuild lookup maps for the remaining MUST groups.
		for i := range otherGroups {
			for j := range otherGroups[i].expansions {
				otherGroups[i].expansions[j].buildMap()
			}
		}
	}

	// Final AND matches keyed by DocID, with accumulated clause and term-frequency counts.
	combined := make(map[DocID]docAccum, driverGroup.totalDocs)
	for driverExpansionIdx := range driverGroup.expansions {
		driverExpansion := &driverGroup.expansions[driverExpansionIdx]
		for _, driverDoc := range driverExpansion.docs {
			if _, already := combined[driverDoc.ID]; already {
				continue
			}
			if _, skip := exclude[driverDoc.ID]; skip {
				continue
			}

			// Driver docs are only candidates; keep only docs that also match every other MUST clause.
			survives := true
			for i := range otherGroups {
				if !otherGroups[i].contains(driverDoc.ID) {
					survives = false
					break
				}
			}
			if !survives {
				continue
			}

			// Start with the current driver expansion; other expansions may add more TF for the same clause.
			accum := docAccum{UniqueMatches: 1, TotalMatches: int(driverDoc.Count), Score: s.scoreTermExpansionDoc(*driverExpansion, driverDoc)}
			for siblingExpansionIdx := range driverGroup.expansions {
				if siblingExpansionIdx == driverExpansionIdx {
					continue
				}
				if tf, ok := driverGroup.expansions[siblingExpansionIdx].lookup(driverDoc.ID); ok {
					accum.TotalMatches += int(tf)
					accum.Score += s.scoreTermExpansionTF(driverGroup.expansions[siblingExpansionIdx], driverDoc.ID, tf)
				}
			}

			// Count each remaining MUST clause once, but add TF from every matching expansion in that clause.
			for i := range otherGroups {
				matchedAny := false
				for expansionIdx := range otherGroups[i].expansions {
					tf, ok := otherGroups[i].expansions[expansionIdx].lookup(driverDoc.ID)
					if !ok {
						continue
					}
					matchedAny = true
					accum.TotalMatches += int(tf)
					accum.Score += s.scoreTermExpansionTF(otherGroups[i].expansions[expansionIdx], driverDoc.ID, tf)
				}
				if matchedAny {
					accum.UniqueMatches++
				}
			}

			combined[driverDoc.ID] = accum
		}
	}

	if err := s.applyShouldClauseBoosts(ctx, combined, shoulds, scope); err != nil {
		return nil, false, err
	}

	return combined, true, nil
}

func (s *Service) tryExecBooleanOrFast(ctx context.Context, q *BooleanQuery, scope queryFieldScope) (map[DocID]docAccum, bool, error) {
	shouldTerms, mustNots, ok := parseFastOrClauses(q)
	if !ok {
		return nil, false, nil
	}

	exclude, err := s.buildExcludeSet(ctx, mustNots, scope)
	if err != nil {
		return nil, false, err
	}

	plan := make([]fastMust, 0, len(shouldTerms))
	totalCap := 0
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
		totalCap += group.totalDocs
	}
	if len(plan) == 0 {
		return map[DocID]docAccum{}, true, nil
	}

	// Union all SHOULD matches into one result map keyed by DocID.
	combined := make(map[DocID]docAccum, totalCap)
	for _, shouldGroup := range plan {
		singleExpansion := len(shouldGroup.expansions) == 1
		var seenInGroup map[DocID]struct{}
		if !singleExpansion {
			// Count a logical SHOULD clause once even if it expands to multiple postings lists.
			seenInGroup = make(map[DocID]struct{}, shouldGroup.totalDocs)
		}

		for _, expansion := range shouldGroup.expansions {
			for _, doc := range expansion.docs {
				if _, skip := exclude[doc.ID]; skip {
					continue
				}
				accum := combined[doc.ID]
				if singleExpansion {
					accum.UniqueMatches++
				} else if _, seen := seenInGroup[doc.ID]; !seen {
					accum.UniqueMatches++
					seenInGroup[doc.ID] = struct{}{}
				}
				// TotalMatches still sums TF from every matching expansion.
				accum.TotalMatches += int(doc.Count)
				accum.Score += s.scoreTermExpansionDoc(expansion, doc)
				combined[doc.ID] = accum
			}
		}
	}

	return combined, true, nil
}

func allSingleExpansionInSameField(musts []fastMust) bool {
	field := ""
	for i := range musts {
		if len(musts[i].expansions) != 1 {
			return false
		}
		expField := musts[i].expansions[0].field
		if field == "" {
			field = expField
			continue
		}
		if expField != field {
			return false
		}
	}
	return true
}

func (s *Service) execBooleanAndSortMerge(musts []fastMust, shoulds []BoolClause, exclude map[DocID]struct{}, ctx context.Context, scope queryFieldScope) (map[DocID]docAccum, bool, error) {
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
				accum.Score += s.scoreTermExpansionDoc(*exps[i], d)
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
		// Start from the next Seq in the first list; the other lists will catch up or move the candidate higher.
		currentSeq = exps[0].docs[ptrs[0]].Seq
	}

	if err := s.applyShouldClauseBoosts(ctx, combined, shoulds, scope); err != nil {
		return nil, false, err
	}

	return combined, true, nil
}

func (s *Service) collectTermPostings(ctx context.Context, q TermQuery, scope queryFieldScope) (fastMust, error) {
	var out fastMust
	if q.Term == "" {
		return out, nil
	}

	tokens := s.pipeline.Process(q.Term)
	if len(tokens) == 0 {
		return out, nil
	}

	fields := s.resolveScopedFields(q.Field, scope)
	for _, token := range tokens {
		keys, err := s.keyGen(token)
		if err != nil {
			return fastMust{}, err
		}

		for _, field := range fields {
			index, ok := s.lookupIndex(field)
			if !ok {
				continue
			}

			res, err := s.searchKeysInField(ctx, field, index, token, keys)
			if err != nil {
				return fastMust{}, err
			}
			out.expansions = append(out.expansions, res.expansions...)
			out.totalDocs += res.totalDocs
		}
	}
	return out, nil
}
