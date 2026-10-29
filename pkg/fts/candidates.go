package fts

import (
	"context"
	"fmt"
	"math"
	"slices"

	"github.com/dariasmyr/fts-engine/v2/pkg/posting"
)

type candidateSet struct {
	ords  []DocOrd
	byOrd map[DocOrd]struct{}
	words []uint64
}

func (s *Service) newCandidateSet(ctx context.Context, ids []DocID) (*candidateSet, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ids == nil {
		return nil, nil
	}

	candidates := &candidateSet{
		ords: make([]DocOrd, 0, len(ids)),
	}
	for i, id := range ids {
		if i%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		ord, ok := s.registry.Has(id)
		if !ok {
			continue
		}
		if s.tombstones != nil && s.tombstones.IsSet(ord) {
			continue
		}
		candidates.ords = append(candidates.ords, ord)
	}
	slices.Sort(candidates.ords)
	candidates.ords = slices.Compact(candidates.ords)
	totalAssigned := s.registry.TotalAssigned()
	bitsPerWord := 64
	bitmapWords := (totalAssigned + bitsPerWord - 1) / 64
	if bitmapWords <= len(candidates.ords) {
		// Use a bitmap when candidates are dense enough that it requires no
		// more storage than one map entry per candidate.
		candidates.words = make([]uint64, (totalAssigned+63)/64)
		for _, ord := range candidates.ords {
			candidates.words[ord/64] |= uint64(1) << (ord % 64)
		}
	} else {
		// Use a map for sparse candidates to avoid allocating a bitmap spanning
		// every assigned ordinal.
		candidates.byOrd = make(map[DocOrd]struct{}, len(candidates.ords))
		for _, ord := range candidates.ords {
			candidates.byOrd[ord] = struct{}{}
		}
	}
	return candidates, nil
}

func (c *candidateSet) contains(ord DocOrd) bool {
	if c == nil {
		return true
	}
	// use a bitset if it exists, otherwise use the map
	if len(c.words) > 0 {
		wordindex := int(ord / 64)
		return wordindex < len(c.words) && c.words[wordindex]&(uint64(1)<<(ord%64)) != 0
	}
	_, ok := c.byOrd[ord]
	return ok
}

func (c *candidateSet) empty() bool {
	return c != nil && len(c.ords) == 0
}

func (c *candidateSet) filterPostings(docs []Posting) []Posting {
	if c == nil {
		return docs
	}
	filtered := make([]Posting, 0, min(len(docs), len(c.ords)))
	for _, doc := range docs {
		if c.contains(doc.Ord) {
			filtered = append(filtered, doc)
		}
	}
	return filtered
}

func filterCandidateMap[T any](candidates *candidateSet, values map[DocOrd]T) {
	if candidates == nil {
		return
	}
	for ord := range values {
		if !candidates.contains(ord) {
			delete(values, ord)
		}
	}
}

type postingLookup struct {
	docs              []Posting
	documentFrequency uint32
}

func (s *Service) lookupTermPostings(
	ctx context.Context,
	candidates *candidateSet,
	index Index,
	key string,
) (postingLookup, error) {
	cursorIndex, supportsCursor := index.(posting.FrequencyPostingOpener)
	canUseCursor := candidates != nil && supportsCursor && (s.tombstones == nil || !s.tombstones.Any())
	if canUseCursor {
		docs := make([]Posting, 0, len(candidates.ords))
		count, err := posting.FilterPostings(
			ctx,
			cursorIndex,
			key,
			candidates.ords,
			func(value Posting) bool {
				docs = append(docs, value)
				return true
			},
		)
		if exec := diagnosticsFromContext(ctx); exec != nil {
			exec.recordCursorPostingList()
		}
		if err != nil {
			return postingLookup{}, fmt.Errorf("candidate search: %w", err)
		}
		if count > math.MaxUint32 {
			return postingLookup{}, fmt.Errorf("candidate search: fts: posting list exceeds uint32 document frequency")
		}
		return postingLookup{docs: docs, documentFrequency: uint32(count)}, nil
	}

	if exec := diagnosticsFromContext(ctx); exec != nil {
		exec.recordMaterializedPostingList()
	}
	docs, err := index.Search(key)
	if err != nil {
		return postingLookup{}, fmt.Errorf("index search: %w", err)
	}
	if s.tombstones != nil && s.tombstones.Any() {
		liveDocs := make([]Posting, 0, len(docs))
		for _, doc := range docs {
			if !s.tombstones.IsSet(doc.Ord) {
				liveDocs = append(liveDocs, doc)
			}
		}
		docs = liveDocs
	}
	if uint64(len(docs)) > math.MaxUint32 {
		return postingLookup{}, fmt.Errorf("fts: posting list exceeds uint32 document frequency")
	}
	documentFrequency := uint32(len(docs))
	return postingLookup{
		docs:              candidates.filterPostings(docs),
		documentFrequency: documentFrequency,
	}, nil
}
