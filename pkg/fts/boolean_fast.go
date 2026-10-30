package fts

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"

	"github.com/dariasmyr/fts-engine/v2/pkg/posting"
)

const (
	lookupMapThreshold = 50
	postingGallopRatio = 8
)

type termExpansion struct {
	field string
	term  string
	df    uint32

	fieldStats FieldStats
	docs       []Posting
	byDoc      map[DocOrd]uint32
}

func (e *termExpansion) buildMap() {
	if e.byDoc != nil {
		return
	}
	e.byDoc = make(map[DocOrd]uint32, len(e.docs))
	for _, d := range e.docs {
		e.byDoc[d.Ord] = d.Count
	}
}

func (e *termExpansion) lookup(ord DocOrd) (uint32, bool) {
	if e.byDoc != nil {
		tf, ok := e.byDoc[ord]
		return tf, ok
	}
	for _, d := range e.docs {
		if d.Ord == ord {
			return d.Count, true
		}
	}
	return 0, false
}

type fastMust struct {
	expansions []termExpansion
	totalDocs  int
}

type postingAndTerm struct {
	field string
	term  string
	key   string
}

type frequencyIntersectionTerm struct {
	cursor            posting.FrequencyIterator
	documentFrequency uint32
}

type frequencyIntersectionOpener struct {
	source posting.FrequencyPostingOpener
	terms  map[string]frequencyIntersectionTerm
}

func (o *frequencyIntersectionOpener) OpenOrdinalPostings(
	ctx context.Context,
	key string,
) (posting.OrdinalIterator, uint64, error) {
	cursor, count, err := o.source.OpenFrequencyPostings(ctx, key)
	if err != nil {
		return nil, 0, err
	}
	if count > math.MaxUint32 {
		_ = cursor.Close()
		return nil, 0, fmt.Errorf("fts: posting list exceeds uint32 document frequency")
	}
	o.terms[key] = frequencyIntersectionTerm{
		cursor:            cursor,
		documentFrequency: uint32(count),
	}
	return cursor, count, nil
}

func (m *fastMust) contains(ord DocOrd) bool {
	for i := range m.expansions {
		if _, ok := m.expansions[i].lookup(ord); ok {
			return true
		}
	}
	return false
}

func parseFastAndClauses(q *BooleanQuery) ([]TermQuery, []BoolClause, []BoolClause, bool, string) {
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
				return nil, nil, nil, false, "and_fast_non_term_clause"
			}
			mustTerms = append(mustTerms, term)
		case Should:
			shoulds = append(shoulds, clause)
		case MustNot:
			mustNots = append(mustNots, clause)
		}
	}
	if len(mustTerms) == 0 {
		return nil, nil, nil, false, "and_fast_no_must_terms"
	}
	return mustTerms, shoulds, mustNots, true, ""
}

func parseFastOrClauses(q *BooleanQuery) ([]TermQuery, []BoolClause, bool, string) {
	var shouldTerms []TermQuery
	var mustNots []BoolClause
	for _, clause := range q.Clauses {
		if clause.Query == nil {
			continue
		}
		switch clause.Occur {
		case Must:
			return nil, nil, false, "or_fast_has_must_clause"
		case Should:
			term, ok := termQueryOf(clause.Query)
			if !ok {
				return nil, nil, false, "or_fast_non_term_clause"
			}
			shouldTerms = append(shouldTerms, term)
		case MustNot:
			mustNots = append(mustNots, clause)
		}
	}
	if len(shouldTerms) == 0 {
		return nil, nil, false, "or_fast_no_should_terms"
	}
	return shouldTerms, mustNots, true, ""
}

func (s *Service) buildExcludeSet(ctx context.Context, candidates *candidateSet, clauses []BoolClause, scope queryFieldScope) (map[DocOrd]struct{}, error) {
	if len(clauses) == 0 {
		return nil, nil
	}
	exclude := make(map[DocOrd]struct{})
	for _, clause := range clauses {
		child, err := s.executeQuery(ctx, candidates, clause.Query, 0, scope)
		if err != nil {
			return nil, err
		}
		for id := range child {
			exclude[id] = struct{}{}
		}
	}
	return exclude, nil
}

func (s *Service) collectFastMustGroups(ctx context.Context, candidates *candidateSet, terms []TermQuery, scope queryFieldScope) ([]fastMust, bool, error) {
	groups := make([]fastMust, 0, len(terms))
	for _, term := range terms {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		group, err := s.collectTermPostings(ctx, candidates, term, scope)
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

func (s *Service) applyShouldClauseBoosts(ctx context.Context, candidates *candidateSet, combined map[DocOrd]docAccum, shoulds []BoolClause, scope queryFieldScope) error {
	for _, clause := range shoulds {
		child, err := s.executeQuery(ctx, candidates, clause.Query, 0, scope)
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

func (s *Service) tryExecBooleanAndFast(ctx context.Context, candidates *candidateSet, q *BooleanQuery, scope queryFieldScope) (map[DocOrd]docAccum, bool, error) {
	mustTerms, shoulds, mustNots, ok, reason := parseFastAndClauses(q)
	if !ok {
		if exec := diagnosticsFromContext(ctx); exec != nil {
			exec.recordFastPathSkip(reason)
			exec.updateBooleanDiagnostics(func(b *BooleanDiagnostics) {
				b.AndFast.SkipReason = reason
			})
		}
		return nil, false, nil
	}
	if exec := diagnosticsFromContext(ctx); exec != nil {
		exec.updateBooleanDiagnostics(func(b *BooleanDiagnostics) {
			b.AndFast.Eligible = true
		})
	}

	if result, used, err := s.tryExecPostingAnd(ctx, candidates, mustTerms, shoulds, mustNots, scope); err != nil {
		return nil, false, err
	} else if used {
		return result, true, nil
	}

	mustGroups, exhausted, err := s.collectFastMustGroups(ctx, candidates, mustTerms, scope)
	if err != nil {
		return nil, false, err
	}
	if exhausted {
		return map[DocOrd]docAccum{}, true, nil
	}

	exclude, err := s.buildExcludeSet(ctx, candidates, mustNots, scope)
	if err != nil {
		return nil, false, err
	}

	slices.SortFunc(mustGroups, func(a, b fastMust) int {
		return cmp.Compare(a.totalDocs, b.totalDocs)
	})
	if allSingleExpansionInSameField(mustGroups) {
		// Fast path: document ordinals are only comparable within one field index.
		if exec := diagnosticsFromContext(ctx); exec != nil {
			exec.setStrategy(strategyBoolAndFastSortMerge)
			exec.updateBooleanDiagnostics(func(b *BooleanDiagnostics) {
				b.AndFast.Used = true
				b.AndFast.UsedSortMerge = true
				b.AndFast.DriverGroupSize = mustGroups[0].totalDocs
				b.AndFast.OtherGroupCount = len(mustGroups) - 1
			})
		}
		return s.execBooleanAndSortMerge(mustGroups, shoulds, exclude, ctx, candidates, scope)
	}
	if exec := diagnosticsFromContext(ctx); exec != nil {
		exec.setStrategy(strategyBoolAndFastDriver)
		exec.updateBooleanDiagnostics(func(b *BooleanDiagnostics) {
			b.AndFast.Used = true
			b.AndFast.DriverGroupSize = mustGroups[0].totalDocs
			b.AndFast.OtherGroupCount = len(mustGroups) - 1
		})
	}

	// Fallback path: use the smallest MUST group as the candidate driver when clauses expand to multiple lists.
	driverGroup := &mustGroups[0]
	otherGroups := mustGroups[1:]
	builtLookupMaps := false
	if driverGroup.totalDocs >= lookupMapThreshold {
		// For larger candidate sets, prebuild lookup maps for the remaining MUST groups.
		for i := range otherGroups {
			for j := range otherGroups[i].expansions {
				otherGroups[i].expansions[j].buildMap()
			}
		}
		builtLookupMaps = len(otherGroups) > 0
	}
	if exec := diagnosticsFromContext(ctx); exec != nil {
		exec.updateBooleanDiagnostics(func(b *BooleanDiagnostics) {
			b.AndFast.BuiltLookupMaps = builtLookupMaps
		})
	}

	// Final AND matches keyed by DocID, with accumulated clause and term-frequency counts.
	combined := make(map[DocOrd]docAccum, driverGroup.totalDocs)
	for driverExpansionIdx := range driverGroup.expansions {
		driverExpansion := &driverGroup.expansions[driverExpansionIdx]
		for _, driverDoc := range driverExpansion.docs {
			driverOrd := driverDoc.Ord
			if _, already := combined[driverOrd]; already {
				continue
			}
			if _, skip := exclude[driverOrd]; skip {
				continue
			}

			// Driver docs are only candidates; keep only docs that also match every other MUST clause.
			survives := true
			for i := range otherGroups {
				if !otherGroups[i].contains(driverOrd) {
					survives = false
					break
				}
			}
			if !survives {
				continue
			}

			// Start with the current driver expansion; other expansions may add more TF for the same clause.
			accum := docAccum{UniqueMatches: 1, TotalMatches: int(driverDoc.Count), Score: s.scoreTermExpansionDoc(ctx, *driverExpansion, driverDoc)}
			for siblingExpansionIdx := range driverGroup.expansions {
				if siblingExpansionIdx == driverExpansionIdx {
					continue
				}
				if tf, ok := driverGroup.expansions[siblingExpansionIdx].lookup(driverOrd); ok {
					accum.TotalMatches += int(tf)
					accum.Score += s.scoreTermExpansionTF(ctx, driverGroup.expansions[siblingExpansionIdx], driverOrd, tf)
				}
			}

			// Count each remaining MUST clause once, but add TF from every matching expansion in that clause.
			for i := range otherGroups {
				matchedAny := false
				for expansionIdx := range otherGroups[i].expansions {
					tf, ok := otherGroups[i].expansions[expansionIdx].lookup(driverOrd)
					if !ok {
						continue
					}
					matchedAny = true
					accum.TotalMatches += int(tf)
					accum.Score += s.scoreTermExpansionTF(ctx, otherGroups[i].expansions[expansionIdx], driverOrd, tf)
				}
				if matchedAny {
					accum.UniqueMatches++
				}
			}

			combined[driverOrd] = accum
		}
	}

	if err := s.applyShouldClauseBoosts(ctx, candidates, combined, shoulds, scope); err != nil {
		return nil, false, err
	}
	if exec := diagnosticsFromContext(ctx); exec != nil {
		exec.updateBooleanDiagnostics(func(b *BooleanDiagnostics) {
			b.AndFast.CandidateDocs = len(combined)
		})
	}

	return combined, true, nil
}

func (s *Service) tryExecPostingAnd(
	ctx context.Context,
	candidates *candidateSet,
	mustTerms []TermQuery,
	shoulds []BoolClause,
	mustNots []BoolClause,
	scope queryFieldScope,
) (map[DocOrd]docAccum, bool, error) {
	if candidates.empty() {
		return map[DocOrd]docAccum{}, true, nil
	}

	plan := make([]postingAndTerm, 0, len(mustTerms))
	var field string
	var frequencyOpener posting.FrequencyPostingOpener
	for _, clause := range mustTerms {
		if clause.Term == "" {
			return map[DocOrd]docAccum{}, true, nil
		}
		fields := s.resolveScopedFields(clause.Field, scope)
		groups, err := s.processFields(fields, clause.Term)
		if err != nil {
			return nil, false, err
		}
		// Each group contains fields sharing one analysis pipeline and the tokens
		// produced by that pipeline. For example, title:"Running" may produce
		// fields=["title"] and tokens=["run"]. Direct posting intersection is
		// valid only when the clause resolves to one field, one token and one key.
		if len(groups) != 1 || len(groups[0].fields) != 1 || len(groups[0].tokens) != 1 {
			return nil, false, nil
		}
		group := groups[0]
		clauseField := group.fields[0]
		token := group.tokens[0]
		keys, err := s.keyGen(token)
		if err != nil {
			return nil, false, err
		}
		if len(keys) != 1 {
			return nil, false, nil
		}
		key := keys[0]

		if field != "" && clauseField != field {
			return nil, false, nil
		}
		clauseIndex, ok := s.lookupIndex(clauseField)
		if !ok {
			return map[DocOrd]docAccum{}, true, nil
		}
		clauseOpener, ok := clauseIndex.(posting.FrequencyPostingOpener)
		if !ok {
			return nil, false, nil
		}
		if s.filter != nil && !s.filter.Contains([]byte(key)) {
			return map[DocOrd]docAccum{}, true, nil
		}
		if field == "" {
			field = clauseField
			frequencyOpener = clauseOpener
		}
		plan = append(plan, postingAndTerm{field: clauseField, term: token, key: key})
	}
	if frequencyOpener == nil {
		return nil, false, nil
	}

	keys := make([]string, len(plan))
	for i := range plan {
		keys[i] = plan[i].key
	}
	matches := make([]DocOrd, 0)
	if candidates != nil {
		matches = make([]DocOrd, 0, candidates.cardinality())
	}
	opener := frequencyIntersectionOpener{
		source: frequencyOpener,
		terms:  make(map[string]frequencyIntersectionTerm, len(keys)),
	}
	combined := make(map[DocOrd]docAccum, cap(matches))
	fieldStats := s.fieldStatsFor(field)
	yield := func(ord posting.Ord) bool {
		if s.tombstones != nil && s.tombstones.IsSet(ord) {
			return true
		}
		matches = append(matches, ord)
		var accum docAccum
		for _, item := range plan {
			term := opener.terms[item.key]
			doc := Posting{Ord: ord, Count: term.cursor.Frequency()}
			expansion := termExpansion{
				field:      item.field,
				term:       item.term,
				df:         term.documentFrequency,
				fieldStats: fieldStats,
			}
			accum.UniqueMatches++
			accum.TotalMatches += int(doc.Count)
			accum.Score += s.scoreTermExpansionDoc(ctx, expansion, doc)
		}
		combined[ord] = accum
		return true
	}
	var err error
	if candidates != nil && len(candidates.words) > 0 {
		err = posting.IntersectWithIterator(
			ctx,
			&opener,
			keys,
			posting.NewBitmapIterator(candidates.words),
			uint64(candidates.cardinality()),
			yield,
		)
	} else {
		var candidateOrds []DocOrd
		if candidates != nil {
			candidateOrds = candidates.ords
		}
		err = posting.Intersect(ctx, &opener, keys, candidateOrds, yield)
	}
	if err != nil {
		return nil, false, fmt.Errorf("fts: boolean AND intersection: %w", err)
	}

	if exec := diagnosticsFromContext(ctx); exec != nil {
		exec.setStrategy(strategyBoolAndIntersect)
		exec.addTokens(len(plan))
		exec.addFields(len(plan))
		exec.addKeys(len(keys))
		seen := make(map[string]struct{}, len(keys))
		for _, key := range keys {
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			exec.addIndexLookups(1)
			exec.recordCursorPostingList()
		}
		exec.updateBooleanDiagnostics(func(b *BooleanDiagnostics) {
			b.AndFast.Used = true
			b.AndFast.UsedIntersect = true
			b.AndFast.DriverGroupSize = len(matches)
			b.AndFast.OtherGroupCount = len(plan) - 1
			b.AndFast.CandidateDocs = len(matches)
		})
	}
	if len(matches) == 0 {
		return map[DocOrd]docAccum{}, true, nil
	}

	// Reuse the intersection as the candidate set for optional clauses, limiting
	// their work to documents which already match every MUST term.
	matchedCandidates := candidateSetFromSortedOrds(matches)
	if exec := diagnosticsFromContext(ctx); exec != nil {
		exec.addPostingsRead(len(matches) * len(plan))
	}

	exclude, err := s.buildExcludeSet(ctx, matchedCandidates, mustNots, scope)
	if err != nil {
		return nil, false, err
	}
	for ord := range exclude {
		delete(combined, ord)
	}
	if err := s.applyShouldClauseBoosts(ctx, matchedCandidates, combined, shoulds, scope); err != nil {
		return nil, false, err
	}
	if exec := diagnosticsFromContext(ctx); exec != nil {
		exec.updateBooleanDiagnostics(func(b *BooleanDiagnostics) {
			b.AndFast.CandidateDocs = len(combined)
		})
	}
	return combined, true, nil
}

func (s *Service) tryExecBooleanOrFast(ctx context.Context, candidates *candidateSet, q *BooleanQuery, scope queryFieldScope) (map[DocOrd]docAccum, bool, error) {
	shouldTerms, mustNots, ok, reason := parseFastOrClauses(q)
	if !ok {
		if exec := diagnosticsFromContext(ctx); exec != nil {
			exec.recordFastPathSkip(reason)
		}
		return nil, false, nil
	}
	if exec := diagnosticsFromContext(ctx); exec != nil {
		exec.setStrategy(strategyBoolOrFast)
	}

	exclude, err := s.buildExcludeSet(ctx, candidates, mustNots, scope)
	if err != nil {
		return nil, false, err
	}

	plan := make([]fastMust, 0, len(shouldTerms))
	totalCap := 0
	for _, term := range shouldTerms {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		group, err := s.collectTermPostings(ctx, candidates, term, scope)
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
		return map[DocOrd]docAccum{}, true, nil
	}

	// Union all SHOULD matches into one result map keyed by DocID.
	combined := make(map[DocOrd]docAccum, totalCap)
	for _, shouldGroup := range plan {
		singleExpansion := len(shouldGroup.expansions) == 1
		var seenInGroup map[DocOrd]struct{}
		if !singleExpansion {
			// Count a logical SHOULD clause once even if it expands to multiple postings lists.
			seenInGroup = make(map[DocOrd]struct{}, shouldGroup.totalDocs)
		}

		for _, expansion := range shouldGroup.expansions {
			for _, doc := range expansion.docs {
				ord := doc.Ord
				if _, skip := exclude[ord]; skip {
					continue
				}
				accum := combined[ord]
				if singleExpansion {
					accum.UniqueMatches++
				} else if _, seen := seenInGroup[ord]; !seen {
					accum.UniqueMatches++
					seenInGroup[ord] = struct{}{}
				}
				// TotalMatches still sums TF from every matching expansion.
				accum.TotalMatches += int(doc.Count)
				accum.Score += s.scoreTermExpansionDoc(ctx, expansion, doc)
				combined[ord] = accum
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

func (s *Service) execBooleanAndSortMerge(musts []fastMust, shoulds []BoolClause, exclude map[DocOrd]struct{}, ctx context.Context, candidates *candidateSet, scope queryFieldScope) (map[DocOrd]docAccum, bool, error) {
	listCount := len(musts)
	expansions := make([]*termExpansion, listCount)
	// Mark the common ordinal range across all MUST clauses so candidates
	// outside that range can be skipped.
	var commonStart, commonEnd DocOrd
	var gallopMinLen int
	// MUST lists are length-sorted, so one boundary separates sequential and galloping scans.
	gallopStart := listCount
	for listIndex := range musts {
		// allSingleExpansion guarantees exactly one postings list per MUST clause here.
		expansions[listIndex] = &musts[listIndex].expansions[0]
		if len(expansions[listIndex].docs) == 0 {
			// One empty MUST list makes the whole AND empty.
			return map[DocOrd]docAccum{}, true, nil
		}
		// Mark the first and last ordinal in this MUST list.
		firstOrd := expansions[listIndex].docs[0].Ord
		lastOrd := expansions[listIndex].docs[len(expansions[listIndex].docs)-1].Ord
		if listIndex == 0 {
			// first MUST list sets the initial common range
			commonStart = firstOrd
			commonEnd = lastOrd
			gallopMinLen = len(expansions[listIndex].docs) * postingGallopRatio
			continue
		}
		// subsequent MUST lists narrow the common range
		commonStart = max(commonStart, firstOrd)
		commonEnd = min(commonEnd, lastOrd)
		if gallopStart == listCount && len(expansions[listIndex].docs) >= gallopMinLen {
			gallopStart = listIndex
		}
	}
	// If the common range is empty, there are no possible intersections.
	if commonStart > commonEnd {
		return map[DocOrd]docAccum{}, true, nil
	}

	positions := make([]int, listCount)
	combined := make(map[DocOrd]docAccum, len(expansions[0].docs))
	candidateOrd := commonStart

alignmentLoop:
	for {
		allListsAligned := true
		for listIndex := range listCount {
			postings := expansions[listIndex].docs
			if listIndex >= gallopStart {
				positions[listIndex] = gallopPostingLowerBound(postings, positions[listIndex], candidateOrd)
			} else {
				// Sequential scans are faster while posting-list sizes remain comparable.
				for positions[listIndex] < len(postings) && postings[positions[listIndex]].Ord < candidateOrd {
					positions[listIndex]++
				}
			}
			if positions[listIndex] >= len(postings) {
				break alignmentLoop
			}
			if postings[positions[listIndex]].Ord > candidateOrd {
				// Earlier lists were checked against the old candidate. Restart
				// alignment so every list catches up to the new candidate.
				candidateOrd = postings[positions[listIndex]].Ord
				allListsAligned = false
				break
			}
		}
		if !allListsAligned {
			continue
		}

		// All MUST lists are aligned on the same ordinal, so we found an intersection hit.
		docOrd := expansions[0].docs[positions[0]].Ord
		if _, skip := exclude[docOrd]; !skip {
			var accum docAccum
			for listIndex := range listCount {
				posting := expansions[listIndex].docs[positions[listIndex]]
				accum.UniqueMatches++
				accum.TotalMatches += int(posting.Count)
				accum.Score += s.scoreTermExpansionDoc(ctx, *expansions[listIndex], posting)
			}
			combined[docOrd] = accum
		}

		// Move every list forward past the matched document and continue the merge walk.
		for listIndex := range listCount {
			positions[listIndex]++
			if positions[listIndex] >= len(expansions[listIndex].docs) {
				// One list is exhausted, so there are no more intersections possible.
				break alignmentLoop
			}
		}
		// Start from the next ordinal in the first list; the other lists will catch up or move the candidate higher.
		candidateOrd = expansions[0].docs[positions[0]].Ord
	}

	if err := s.applyShouldClauseBoosts(ctx, candidates, combined, shoulds, scope); err != nil {
		return nil, false, err
	}
	if exec := diagnosticsFromContext(ctx); exec != nil {
		exec.updateBooleanDiagnostics(func(b *BooleanDiagnostics) {
			b.AndFast.CandidateDocs = len(combined)
		})
	}

	return combined, true, nil
}

func gallopPostingLowerBound(postings []Posting, from int, target DocOrd) int {
	n := len(postings)
	if from >= n {
		return n
	}
	if postings[from].Ord >= target {
		return from
	}
	if from+1 >= n {
		return n
	}
	if postings[from+1].Ord >= target {
		return from + 1
	}

	// Find an exponential range that may contain the first ordinal >= target.
	step := 2
	for step < n-from && postings[from+step].Ord < target {
		step *= 2
	}

	// Split step in half to get the lower bound of the range
	left := from + step/2 + 1
	// The upper bound of the range is either the end of the postings or the last step we took.
	right := min(from+step+1, n)

	// Find the lower bound within [left, right) using binary search.
	for left < right {
		mid := left + (right-left)/2
		if postings[mid].Ord < target {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left
}

func (s *Service) collectTermPostings(ctx context.Context, candidates *candidateSet, q TermQuery, scope queryFieldScope) (fastMust, error) {
	var out fastMust
	if q.Term == "" {
		return out, nil
	}

	fields := s.resolveScopedFields(q.Field, scope)
	groups, err := s.processFields(fields, q.Term)
	if err != nil {
		return fastMust{}, err
	}
	if exec := diagnosticsFromContext(ctx); exec != nil {
		exec.addTokens(processedTokenCount(groups))
		exec.addFields(len(fields))
	}
	for _, group := range groups {
		for _, token := range group.tokens {
			keys, err := s.keyGen(token)
			if err != nil {
				return fastMust{}, err
			}
			if exec := diagnosticsFromContext(ctx); exec != nil {
				exec.addKeys(len(keys))
			}

			for _, field := range group.fields {
				index, ok := s.lookupIndex(field)
				if !ok {
					continue
				}

				res, err := s.searchKeysInField(ctx, candidates, field, index, token, keys)
				if err != nil {
					return fastMust{}, err
				}
				out.expansions = append(out.expansions, res.expansions...)
				out.totalDocs += res.totalDocs
			}
		}
	}
	return out, nil
}
