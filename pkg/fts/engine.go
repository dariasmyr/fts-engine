package fts

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"
)

type docAccum struct {
	UniqueMatches int
	TotalMatches  int
}

type tokenGroup struct {
	expansions [][]DocRef
	totalDocs  int
	single     bool
}

type Service struct {
	index        Index
	indexFactory IndexFactory
	keyGen       KeyGenerator
	pipeline     Pipeline
	filter       Filter

	mu      sync.RWMutex
	indexes map[string]Index
}

func New(index Index, keyGen KeyGenerator, opts ...Option) *Service {
	s := newService(keyGen, opts...)
	s.index = index
	s.indexFactory = func(name string) (Index, error) {
		return nil, fmt.Errorf("fts: field %q is not available (service was built with fts.New; use fts.NewMultiField to index arbitrary fields)", name)
	}
	if index != nil {
		s.indexes[DefaultField] = index
	}
	return s
}

func NewMultiField(factory IndexFactory, keyGen KeyGenerator, opts ...Option) *Service {
	s := newService(keyGen, opts...)
	s.indexFactory = factory
	return s
}

func NewMultiFieldFromIndexes(indexes map[string]Index, keyGen KeyGenerator, opts ...Option) *Service {
	s := newService(keyGen, opts...)
	for name, idx := range indexes {
		s.indexes[name] = idx
	}
	if idx, ok := s.indexes[DefaultField]; ok {
		s.index = idx
	}
	s.indexFactory = func(name string) (Index, error) {
		return nil, fmt.Errorf("fts: field %q was not present in the restored snapshot", name)
	}
	return s
}

func newService(keyGen KeyGenerator, opts ...Option) *Service {
	s := &Service{
		keyGen:   keyGen,
		pipeline: defaultPipeline{},
		indexes:  make(map[string]Index),
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
	if docID == "" {
		return fmt.Errorf("fts: document id is empty")
	}
	return s.indexField(ctx, docID, DefaultField, Field{Value: content})
}

func (s *Service) SearchDocuments(ctx context.Context, query string, maxResults int) (*SearchResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	start := time.Now()
	timings := make(map[string]string, 3)

	preStart := time.Now()
	parsed, err := ParseQuery(query)
	if err != nil {
		return nil, err
	}
	timings["preprocess"] = formatDuration(time.Since(preStart))

	res, err := s.Search(ctx, parsed, maxResults)
	if err != nil {
		return nil, err
	}
	timings["search_tokens"] = res.Timings["search_tokens"]
	timings["total"] = formatDuration(time.Since(start))
	res.Timings = timings
	return res, nil
}

func (s *Service) SearchPhrase(ctx context.Context, phrase string, maxResults int) (*SearchResult, error) {
	return s.searchPhraseFields(ctx, s.fieldNames(), phrase, maxResults)
}

func (s *Service) SearchPhraseNear(ctx context.Context, phrase string, distance int, maxResults int) (*SearchResult, error) {
	return s.searchPhraseNearFields(ctx, s.fieldNames(), phrase, distance, maxResults)
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
	// One monotonic pointer (only moves forward) per token postings list; all start at 0.
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
	s.mu.RLock()
	defer s.mu.RUnlock()

	var combined Stats
	found := false
	for _, idx := range s.indexes {
		analyzer, ok := idx.(Analyzer)
		if !ok {
			continue
		}
		found = true
		combined = mergeStats(combined, analyzer.Analyze())
	}
	return combined, found
}

func (s *Service) SnapshotComponents() (Index, Filter) {
	if s == nil {
		return nil, nil
	}

	s.mu.RLock()
	idx := s.indexes[DefaultField]
	s.mu.RUnlock()
	return idx, s.filter
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
