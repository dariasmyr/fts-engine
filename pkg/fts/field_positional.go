package fts

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

type positionalFieldMatchResult struct {
	field        string
	matchesByDoc map[DocOrd]uint32
	err          error
}

func (s *Service) collectPositionalFieldMatches(
	ctx context.Context,
	fields []string,
	searchInField func(PositionalIndex) (map[DocOrd]uint32, error),
) ([]positionalFieldMatchResult, error) {
	resultsByField := make([]positionalFieldMatchResult, 0, len(fields))
	if len(fields) <= 1 {
		for _, field := range fields {
			index, ok := s.lookupIndex(field)
			if !ok {
				continue
			}
			positional, ok := index.(PositionalIndex)
			if !ok {
				continue
			}
			matchesByDoc, err := searchInField(positional)
			if err != nil {
				return nil, err
			}
			if len(matchesByDoc) == 0 {
				continue
			}
			resultsByField = append(resultsByField, positionalFieldMatchResult{field: field, matchesByDoc: matchesByDoc})
		}
		return resultsByField, nil
	}

	results := make(chan positionalFieldMatchResult, len(fields))
	var wg sync.WaitGroup
	for _, field := range fields {
		index, ok := s.lookupIndex(field)
		if !ok {
			continue
		}
		positional, ok := index.(PositionalIndex)
		if !ok {
			continue
		}
		fieldName := field
		positionalIndex := positional

		wg.Go(func() {
			if err := ctx.Err(); err != nil {
				results <- positionalFieldMatchResult{err: err}
				return
			}

			matchesByDoc, err := searchInField(positionalIndex)
			if err != nil {
				results <- positionalFieldMatchResult{err: err}
				return
			}
			if len(matchesByDoc) == 0 {
				return
			}
			results <- positionalFieldMatchResult{field: fieldName, matchesByDoc: matchesByDoc}
		})
	}

	wg.Wait()
	close(results)
	for res := range results {
		if res.err != nil {
			return nil, res.err
		}
		resultsByField = append(resultsByField, res)
	}
	return resultsByField, nil
}

func (s *Service) searchExactPhraseCountsInField(ctx context.Context, positional PositionalIndex, tokens []string) (map[DocOrd]uint32, error) {
	tokenPostings, err := s.searchTokenPostingsInField(ctx, positional, tokens)
	if err != nil || tokenPostings == nil {
		return map[DocOrd]uint32{}, err
	}

	driverIdx := smallestPostingMapIndex(tokenPostings)
	matchesByDoc := make(map[DocOrd]uint32)
	for ord, driverPositions := range tokenPostings[driverIdx] {
		if !docPresentInAllPostings(tokenPostings, ord, driverIdx) {
			continue
		}

		matches := phraseAlign(tokenPostings, ord, driverIdx, driverPositions)
		if matches > 0 {
			matchesByDoc[ord] = matches
		}
	}
	return matchesByDoc, nil
}

func (s *Service) searchNearPhraseCountsInField(ctx context.Context, positional PositionalIndex, tokens []string, maxGap uint32) (map[DocOrd]uint32, error) {
	tokenPostings, err := s.searchTokenPostingsInField(ctx, positional, tokens)
	if err != nil || tokenPostings == nil {
		return map[DocOrd]uint32{}, err
	}

	matchesByDoc := make(map[DocOrd]uint32)
	for ord := range tokenPostings[0] {
		if !docPresentInAllPostings(tokenPostings, ord, 0) {
			continue
		}

		matches := phraseNearAlign(tokenPostings, ord, maxGap)
		if matches > 0 {
			matchesByDoc[ord] = matches
		}
	}
	return matchesByDoc, nil
}

func (s *Service) searchTokenPostingsInField(ctx context.Context, positional PositionalIndex, tokens []string) ([]map[DocOrd][]uint32, error) {
	tokenPostings := make([]map[DocOrd][]uint32, len(tokens))
	for i, token := range tokens {
		merged, err := s.collectPositionalPostingsForToken(ctx, positional, token)
		if err != nil {
			return nil, err
		}
		if len(merged) == 0 {
			return nil, nil
		}
		tokenPostings[i] = merged
	}
	return tokenPostings, nil
}

func (s *Service) collectPositionalPostingsForToken(ctx context.Context, positional PositionalIndex, token string) (map[DocOrd][]uint32, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	exec := diagnosticsFromContext(ctx)
	keys, err := s.keyGen(token)
	if err != nil {
		return nil, fmt.Errorf("fts: positional search: keygen: %w", err)
	}
	if exec != nil {
		exec.addKeys(len(keys))
		exec.addTokens(1)
	}
	var filterChecks, filterRejects, indexLookups, postingsRead int
	defer func() {
		if exec == nil {
			return
		}
		exec.addFilterChecks(filterChecks, filterRejects)
		exec.addIndexLookups(indexLookups)
		exec.addPostingsRead(postingsRead)
	}()

	var merged map[DocOrd][]uint32
	if len(keys) == 1 {
		if s.filter != nil {
			miss := !s.filter.Contains([]byte(keys[0]))
			filterChecks++
			if miss {
				filterRejects++
			}
			if miss {
				return nil, nil
			}
		}

		indexLookups++
		refs, err := positional.SearchPositional(keys[0])
		if err != nil {
			return nil, fmt.Errorf("fts: positional search: index search: %w", err)
		}
		refs = s.normalizePositionalPostings(refs)
		postingsRead += len(refs)
		merged = make(map[DocOrd][]uint32, len(refs))
		for _, r := range refs {
			if len(r.Positions) > 0 {
				ord, _ := s.ordForPositionalPosting(r)
				merged[ord] = r.Positions
			}
		}
		return merged, nil
	}

	merged = make(map[DocOrd][]uint32)
	for _, key := range keys {
		if s.filter != nil {
			miss := !s.filter.Contains([]byte(key))
			filterChecks++
			if miss {
				filterRejects++
			}
			if miss {
				continue
			}
		}

		indexLookups++
		refs, err := positional.SearchPositional(key)
		if err != nil {
			return nil, fmt.Errorf("fts: positional search: index search: %w", err)
		}
		refs = s.normalizePositionalPostings(refs)
		postingsRead += len(refs)
		for _, r := range refs {
			if len(r.Positions) == 0 {
				continue
			}
			ord, _ := s.ordForPositionalPosting(r)
			if existing, ok := merged[ord]; ok {
				merged[ord] = mergeSortedPositions(existing, r.Positions)
			} else {
				merged[ord] = append([]uint32(nil), r.Positions...)
			}
		}
	}

	return merged, nil
}

func smallestPostingMapIndex(tokenPostings []map[DocOrd][]uint32) int {
	driverIdx := 0
	for i := 1; i < len(tokenPostings); i++ {
		if len(tokenPostings[i]) < len(tokenPostings[driverIdx]) {
			driverIdx = i
		}
	}
	return driverIdx
}

func docPresentInAllPostings(tokenPostings []map[DocOrd][]uint32, ord DocOrd, skipIdx int) bool {
	for i := 0; i < len(tokenPostings); i++ {
		if i == skipIdx {
			continue
		}
		if _, ok := tokenPostings[i][ord]; !ok {
			return false
		}
	}
	return true
}

func resultsFromCounts(counts map[DocOrd]uint32, scores map[DocOrd]float64, useScore bool, registry *DocRegistry) ([]Result, int) {
	results := make([]Result, 0, len(counts))
	for ord, cnt := range counts {
		id := registry.Lookup(ord)
		if id == "" {
			continue
		}
		results = append(results, Result{
			ID:            id,
			UniqueMatches: 1,
			TotalMatches:  int(cnt),
			Score:         scores[ord],
		})
	}

	sort.Slice(results, func(i, j int) bool {
		if useScore && results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		if results[i].TotalMatches != results[j].TotalMatches {
			return results[i].TotalMatches > results[j].TotalMatches
		}
		return results[i].ID < results[j].ID
	})

	return results, len(results)
}

func phraseAlign(tokenPostings []map[DocOrd][]uint32, ord DocOrd, driverIdx int, driverPositions []uint32) uint32 {
	n := len(tokenPostings)
	if n == 0 || len(driverPositions) == 0 {
		return 0
	}

	others := make([][]uint32, n)
	// One monotonic pointer (only moves forward) per token postings list; all start at 0.
	ptrs := make([]int, n)
	for i := 0; i < n; i++ {
		if i == driverIdx {
			continue
		}
		others[i] = tokenPostings[i][ord]
		if len(others[i]) == 0 {
			return 0
		}
	}

	var matches uint32
outer:
	for _, p := range driverPositions {
		for i := 0; i < n; i++ {
			if i == driverIdx {
				continue
			}

			var target uint32
			if i < driverIdx {
				// Tokens to the left of the driver must appear delta positions earlier in the document.
				delta := uint32(driverIdx - i)
				if delta > p {
					// p-delta would underflow uint32, so this driver position cannot anchor the full phrase.
					continue outer
				}
				target = p - delta
			} else {
				// Tokens to the right of the driver must appear the same distance later in the document.
				target = p + uint32(i-driverIdx)
			}

			pos := others[i]
			j := ptrs[i]
			// Advance to the first position >= target; exact phrase matching requires landing on target itself.
			for j < len(pos) && pos[j] < target {
				j++
			}
			// Remember how far this token advanced so later driver positions continue from here.
			ptrs[i] = j

			if j >= len(pos) {
				return matches
			}
			if pos[j] != target {
				continue outer
			}
		}
		matches++
	}

	return matches
}

func phraseNearAlign(tokenPostings []map[DocOrd][]uint32, ord DocOrd, maxGap uint32) uint32 {
	if len(tokenPostings) == 0 {
		return 0
	}

	positions := make([][]uint32, len(tokenPostings))
	for i := range tokenPostings {
		positions[i] = tokenPostings[i][ord]
		if len(positions[i]) == 0 {
			return 0
		}
	}

	ptrs := make([]int, len(tokenPostings))
	var matches uint32
outer:
	for _, start := range positions[0] {
		prev := start
		for i := 1; i < len(positions); i++ {
			pos := positions[i]
			j := ptrs[i]
			minPos := prev + 1
			for j < len(pos) && pos[j] < minPos {
				j++
			}
			ptrs[i] = j

			if j >= len(pos) {
				return matches
			}

			maxPos := minPos + maxGap
			if pos[j] > maxPos {
				continue outer
			}

			prev = pos[j]
		}

		matches++
	}

	return matches
}

func mergeSortedPositions(a, b []uint32) []uint32 {
	out := make([]uint32, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			out = append(out, a[i])
			i++
		case a[i] > b[j]:
			out = append(out, b[j])
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	out = append(out, a[i:]...)
	out = append(out, b[j:]...)
	return out
}
