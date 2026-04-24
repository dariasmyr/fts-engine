package fts

import (
	"context"
	"fmt"
	"io"
	"sort"
	"time"
)

type Service struct {
	index    Index
	keyGen   KeyGenerator
	pipeline Pipeline
	filter   Filter
}

func New(index Index, keyGen KeyGenerator, opts ...Option) *Service {
	s := &Service{
		index:    index,
		keyGen:   keyGen,
		pipeline: defaultPipeline{},
	}

	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}

	if s.keyGen == nil {
		s.keyGen = WordKeys
	}

	return s
}

func NewFromReader(r io.Reader, loader IndexLoader, keyGen KeyGenerator, opts ...Option) (*Service, error) {
	if loader == nil {
		return nil, fmt.Errorf("fts: nil index loader")
	}

	index, err := loader(r)
	if err != nil {
		return nil, fmt.Errorf("fts: load index: %w", err)
	}

	return New(index, keyGen, opts...), nil
}

func (s *Service) IndexDocument(ctx context.Context, docID DocID, content string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	positional, hasPositions := s.index.(PositionalIndex)
	tokens := s.pipeline.Process(content)
	for pos, token := range tokens {
		if err := ctx.Err(); err != nil {
			return err
		}

		keys, err := s.keyGen(token)
		if err != nil {
			return fmt.Errorf("fts: index document: keygen: %w", err)
		}

		for _, key := range keys {
			if s.filter != nil {
				if ok := s.filter.Add([]byte(key)); !ok {
					return fmt.Errorf("fts: index document: filter add failed for key %q", key)
				}
			}
			if hasPositions {
				if err := positional.InsertAt(key, docID, uint32(pos)); err != nil {
					return fmt.Errorf("fts: index document: insert: %w", err)
				}
				continue
			}
			if err := s.index.Insert(key, docID); err != nil {
				return fmt.Errorf("fts: index document: insert: %w", err)
			}
		}
	}

	return nil
}

func (s *Service) SearchDocuments(ctx context.Context, query string, maxResults int) (*SearchResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	start := time.Now()
	timings := make(map[string]string, 3)

	preStart := time.Now()
	tokens := s.pipeline.Process(query)
	timings["preprocess"] = formatDuration(time.Since(preStart))

	searchStart := time.Now()
	uniqueMatches := make(map[DocID]int)
	totalMatches := make(map[DocID]int)

	for _, token := range tokens {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		keys, err := s.keyGen(token)
		if err != nil {
			return nil, fmt.Errorf("fts: search: keygen: %w", err)
		}

		for _, key := range keys {
			if s.filter != nil && !s.filter.Contains([]byte(key)) {
				continue
			}

			docs, err := s.index.Search(key)
			if err != nil {
				return nil, fmt.Errorf("fts: search: index search: %w", err)
			}

			for _, doc := range docs {
				uniqueMatches[doc.ID]++
				totalMatches[doc.ID] += int(doc.Count)
			}
		}
	}

	timings["search_tokens"] = formatDuration(time.Since(searchStart))

	results := make([]Result, 0, len(uniqueMatches))
	for id, unique := range uniqueMatches {
		results = append(results, Result{
			ID:            id,
			UniqueMatches: unique,
			TotalMatches:  totalMatches[id],
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

func (s *Service) SearchPhrase(ctx context.Context, phrase string, maxResults int) (*SearchResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	start := time.Now()
	timings := make(map[string]string, 3)

	preStart := time.Now()
	tokens := s.pipeline.Process(phrase)
	timings["preprocess"] = formatDuration(time.Since(preStart))

	if len(tokens) == 0 {
		timings["search_tokens"] = formatDuration(0)
		timings["total"] = formatDuration(time.Since(start))
		return &SearchResult{Results: []Result{}, Timings: timings}, nil
	}
	if len(tokens) == 1 {
		return s.SearchDocuments(ctx, phrase, maxResults)
	}

	positional, ok := s.index.(PositionalIndex)
	if !ok {
		timings["search_tokens"] = formatDuration(0)
		timings["total"] = formatDuration(time.Since(start))
		return &SearchResult{Results: []Result{}, Timings: timings}, nil
	}

	searchStart := time.Now()
	tokenPostings := make([]map[DocID][]uint32, len(tokens))
	for i, token := range tokens {
		merged, err := s.collectPositionalPostings(ctx, positional, token)
		if err != nil {
			return nil, err
		}
		if len(merged) == 0 {
			timings["search_tokens"] = formatDuration(time.Since(searchStart))
			timings["total"] = formatDuration(time.Since(start))
			return &SearchResult{Results: []Result{}, Timings: timings}, nil
		}
		tokenPostings[i] = merged
	}

	driverIdx := 0
	for i := 1; i < len(tokenPostings); i++ {
		if len(tokenPostings[i]) < len(tokenPostings[driverIdx]) {
			driverIdx = i
		}
	}

	phraseCounts := make(map[DocID]uint32)
	for docID, driverPositions := range tokenPostings[driverIdx] {
		missing := false
		for i := 0; i < len(tokenPostings); i++ {
			if i == driverIdx {
				continue
			}
			if _, ok := tokenPostings[i][docID]; !ok {
				missing = true
				break
			}
		}
		if missing {
			continue
		}

		matches := phraseAlign(tokenPostings, docID, driverIdx, driverPositions)
		if matches > 0 {
			phraseCounts[docID] = matches
		}
	}

	timings["search_tokens"] = formatDuration(time.Since(searchStart))

	results := make([]Result, 0, len(phraseCounts))
	for id, cnt := range phraseCounts {
		results = append(results, Result{
			ID:            id,
			UniqueMatches: 1,
			TotalMatches:  int(cnt),
		})
	}

	sort.Slice(results, func(i, j int) bool {
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

func (s *Service) SearchPhraseNear(ctx context.Context, phrase string, distance int, maxResults int) (*SearchResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if distance < 0 {
		return nil, fmt.Errorf("fts: phrase near search: negative distance %d", distance)
	}

	start := time.Now()
	timings := make(map[string]string, 3)

	preStart := time.Now()
	tokens := s.pipeline.Process(phrase)
	timings["preprocess"] = formatDuration(time.Since(preStart))

	if len(tokens) == 0 {
		timings["search_tokens"] = formatDuration(0)
		timings["total"] = formatDuration(time.Since(start))
		return &SearchResult{Results: []Result{}, Timings: timings}, nil
	}
	if len(tokens) == 1 {
		return s.SearchDocuments(ctx, phrase, maxResults)
	}

	positional, ok := s.index.(PositionalIndex)
	if !ok {
		timings["search_tokens"] = formatDuration(0)
		timings["total"] = formatDuration(time.Since(start))
		return &SearchResult{Results: []Result{}, Timings: timings}, nil
	}

	searchStart := time.Now()
	tokenPostings := make([]map[DocID][]uint32, len(tokens))
	for i, token := range tokens {
		merged, err := s.collectPositionalPostings(ctx, positional, token)
		if err != nil {
			return nil, err
		}
		if len(merged) == 0 {
			timings["search_tokens"] = formatDuration(time.Since(searchStart))
			timings["total"] = formatDuration(time.Since(start))
			return &SearchResult{Results: []Result{}, Timings: timings}, nil
		}
		tokenPostings[i] = merged
	}

	driverIdx := 0
	for i := 1; i < len(tokenPostings); i++ {
		if len(tokenPostings[i]) < len(tokenPostings[driverIdx]) {
			driverIdx = i
		}
	}

	phraseCounts := make(map[DocID]uint32)
	for docID := range tokenPostings[driverIdx] {
		missing := false
		for i := 0; i < len(tokenPostings); i++ {
			if i == driverIdx {
				continue
			}
			if _, ok := tokenPostings[i][docID]; !ok {
				missing = true
				break
			}
		}
		if missing {
			continue
		}

		matches := phraseNearAlign(tokenPostings, docID, uint32(distance))
		if matches > 0 {
			phraseCounts[docID] = matches
		}
	}

	timings["search_tokens"] = formatDuration(time.Since(searchStart))

	results := make([]Result, 0, len(phraseCounts))
	for id, cnt := range phraseCounts {
		results = append(results, Result{
			ID:            id,
			UniqueMatches: 1,
			TotalMatches:  int(cnt),
		})
	}

	sort.Slice(results, func(i, j int) bool {
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

func (s *Service) collectPositionalPostings(ctx context.Context, positional PositionalIndex, token string) (map[DocID][]uint32, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	keys, err := s.keyGen(token)
	if err != nil {
		return nil, fmt.Errorf("fts: positional search: keygen: %w", err)
	}

	var merged map[DocID][]uint32
	if len(keys) == 1 {
		if s.filter != nil && !s.filter.Contains([]byte(keys[0])) {
			return nil, nil
		}

		refs, err := positional.SearchPositional(keys[0])
		if err != nil {
			return nil, fmt.Errorf("fts: positional search: index search: %w", err)
		}
		merged = make(map[DocID][]uint32, len(refs))
		for _, r := range refs {
			if len(r.Positions) > 0 {
				merged[r.ID] = r.Positions
			}
		}
		return merged, nil
	}

	merged = make(map[DocID][]uint32)
	for _, key := range keys {
		if s.filter != nil && !s.filter.Contains([]byte(key)) {
			continue
		}

		refs, err := positional.SearchPositional(key)
		if err != nil {
			return nil, fmt.Errorf("fts: positional search: index search: %w", err)
		}
		for _, r := range refs {
			if len(r.Positions) == 0 {
				continue
			}
			if existing, ok := merged[r.ID]; ok {
				merged[r.ID] = mergeSortedPositions(existing, r.Positions)
			} else {
				merged[r.ID] = append([]uint32(nil), r.Positions...)
			}
		}
	}

	return merged, nil
}

func phraseAlign(tokenPostings []map[DocID][]uint32, docID DocID, driverIdx int, driverPositions []uint32) uint32 {
	n := len(tokenPostings)
	if n == 0 || len(driverPositions) == 0 {
		return 0
	}

	others := make([][]uint32, n)
	ptrs := make([]int, n)
	for i := 0; i < n; i++ {
		if i == driverIdx {
			continue
		}
		others[i] = tokenPostings[i][docID]
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
				delta := uint32(driverIdx - i)
				if delta > p {
					continue outer
				}
				target = p - delta
			} else {
				target = p + uint32(i-driverIdx)
			}

			pos := others[i]
			j := ptrs[i]
			for j < len(pos) && pos[j] < target {
				j++
			}
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

func phraseNearAlign(tokenPostings []map[DocID][]uint32, docID DocID, maxGap uint32) uint32 {
	if len(tokenPostings) == 0 {
		return 0
	}

	positions := make([][]uint32, len(tokenPostings))
	for i := range tokenPostings {
		positions[i] = tokenPostings[i][docID]
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

func (s *Service) Analyze() (Stats, bool) {
	analyzer, ok := s.index.(Analyzer)
	if !ok {
		return Stats{}, false
	}
	return analyzer.Analyze(), true
}

func (s *Service) SnapshotComponents() (Index, Filter) {
	if s == nil {
		return nil, nil
	}

	return s.index, s.filter
}

func (s *Service) BuildFilter() error {
	if s == nil || s.filter == nil {
		return nil
	}

	buildable, ok := s.filter.(BuildableFilter)
	if !ok {
		return nil
	}

	if err := buildable.Build(); err != nil {
		return fmt.Errorf("fts: build filter: %w", err)
	}

	return nil
}

func formatDuration(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%dus", d.Microseconds())
	}
	return fmt.Sprintf("%dms", d.Milliseconds())
}
