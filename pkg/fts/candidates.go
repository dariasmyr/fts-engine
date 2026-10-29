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
	count int
}

const candidateMapThreshold = 64

func (s *Service) newCandidateSet(ctx context.Context, ids []DocID) (*candidateSet, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ids == nil {
		return nil, nil
	}

	totalAssigned := s.registry.TotalAssigned()
	bitmapWords := (totalAssigned + 63) / 64
	if bitmapWords > 0 && bitmapWords <= len(ids) {
		candidates := &candidateSet{words: make([]uint64, bitmapWords)}
		for i, id := range ids {
			if i%1024 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			ord, ok := s.registry.Has(id)
			if !ok || s.tombstones != nil && s.tombstones.IsSet(ord) {
				continue
			}
			word := &candidates.words[ord/64]
			mask := uint64(1) << (ord % 64)
			if *word&mask == 0 {
				*word |= mask
				candidates.count++
			}
		}
		if candidates.count >= bitmapWords {
			return candidates, nil
		}

		// A large input can collapse to a sparse set after unknown IDs,
		// tombstones and duplicates are removed. Keep that result as ordinals
		// rather than retaining a mostly empty registry-sized bitmap.
		candidates.ords = ordsFromBitmap(candidates.words, candidates.count)
		candidates.words = nil
		return candidates, nil
	}

	candidates := &candidateSet{ords: make([]DocOrd, 0, len(ids))}
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
	candidates.count = len(candidates.ords)
	return candidates, nil
}

func ordsFromBitmap(words []uint64, count int) []DocOrd {
	ords := make([]DocOrd, 0, count)
	iterator := posting.NewBitmapIterator(words)
	defer iterator.Close()
	for iterator.Next() {
		ords = append(ords, iterator.Ord())
	}
	return ords
}

func candidateSetFromSortedOrds(ords []DocOrd) *candidateSet {
	return &candidateSet{ords: ords, count: len(ords)}
}

func (c *candidateSet) cardinality() int {
	if c == nil {
		return 0
	}
	return c.count
}

func (c *candidateSet) buildMap() {
	if c == nil || c.byOrd != nil || len(c.words) > 0 {
		return
	}
	c.byOrd = make(map[DocOrd]struct{}, len(c.ords))
	for _, ord := range c.ords {
		c.byOrd[ord] = struct{}{}
	}
}

func (c *candidateSet) prepareMembership(checks int) {
	if c == nil || len(c.words) > 0 || len(c.ords) < candidateMapThreshold || checks < candidateMapThreshold {
		return
	}
	c.buildMap()
}

func (c *candidateSet) contains(ord DocOrd) bool {
	if c == nil {
		return true
	}
	// Dense external candidates use a bitmap. Sparse candidates and intersection
	// results retain sorted ordinals until a materialized fallback needs a map.
	if len(c.words) > 0 {
		wordIndex := int(ord / 64)
		return wordIndex < len(c.words) && c.words[wordIndex]&(uint64(1)<<(ord%64)) != 0
	}
	if c.byOrd != nil {
		_, ok := c.byOrd[ord]
		return ok
	}
	_, ok := slices.BinarySearch(c.ords, ord)
	return ok
}

func (c *candidateSet) empty() bool {
	return c != nil && c.cardinality() == 0
}

func (c *candidateSet) filterPostings(docs []Posting) []Posting {
	if c == nil {
		return docs
	}
	c.prepareMembership(len(docs))
	filtered := make([]Posting, 0, min(len(docs), c.cardinality()))
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
	candidates.prepareMembership(len(values))
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
		capacity := candidates.cardinality()
		var docs []Posting
		yield := func(value Posting) bool {
			docs = append(docs, value)
			return true
		}
		var count uint64
		var err error
		if len(candidates.words) > 0 {
			var cursor posting.FrequencyIterator
			cursor, count, err = cursorIndex.OpenFrequencyPostings(ctx, key)
			if err == nil {
				if count < uint64(capacity) {
					capacity = int(count)
				}
				docs = make([]Posting, 0, capacity)
				err = posting.FilterPostingIterator(
					ctx,
					cursor,
					count,
					posting.NewBitmapIterator(candidates.words),
					uint64(candidates.cardinality()),
					yield,
				)
			}
		} else {
			docs = make([]Posting, 0, capacity)
			count, err = posting.FilterPostings(ctx, cursorIndex, key, candidates.ords, yield)
		}
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
