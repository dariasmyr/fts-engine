package segment

import (
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

// Source emits mutable-index terms in a form that can be sealed into a segment.
type Source interface {
	ExportSegmentTerms(func(TermPostings) error) error
}

// BuildFromSource collects exported terms from a mutable index and serializes
// them into a sealed segment.
func BuildFromSource(source Source) ([]byte, error) {
	return buildFromSource(source, nil)
}

// BuildFromSourceWithTombstones skips deleted ords so rebuilt artifacts do not
// copy stale postings forward.
func BuildFromSourceWithTombstones(source Source, tombstoneWords []uint64) ([]byte, error) {
	return buildFromSource(source, fts.RestoreTombstones(tombstoneWords))
}

func buildFromSource(source Source, tombstones *fts.Tombstones) ([]byte, error) {
	if source == nil {
		return nil, fmt.Errorf("segment: nil source")
	}

	terms := make([]TermPostings, 0)
	err := source.ExportSegmentTerms(func(tp TermPostings) error {
		cloned := TermPostings{
			Term:      tp.Term,
			Postings:  append([]fts.Posting(nil), tp.Postings...),
			Positions: clonePositions(tp.Positions),
		}
		if tombstones != nil && tombstones.Any() {
			var ok bool
			cloned, ok = compactTermPostings(cloned, tombstones)
			if !ok {
				return nil
			}
		}
		terms = append(terms, cloned)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return Build(terms)
}

func compactTermPostings(tp TermPostings, tombstones *fts.Tombstones) (TermPostings, bool) {
	if tombstones == nil || !tombstones.Any() {
		return tp, len(tp.Postings) > 0
	}
	postings := make([]fts.Posting, 0, len(tp.Postings))
	var positions [][]uint32
	if len(tp.Positions) > 0 {
		positions = make([][]uint32, 0, len(tp.Positions))
	}
	for i, posting := range tp.Postings {
		if tombstones.IsSet(posting.Ord) {
			continue
		}
		postings = append(postings, posting)
		if len(tp.Positions) > 0 {
			if i < len(tp.Positions) {
				positions = append(positions, tp.Positions[i])
			} else {
				positions = append(positions, nil)
			}
		}
	}
	if len(postings) == 0 {
		return TermPostings{}, false
	}
	tp.Postings = postings
	tp.Positions = positions
	return tp, true
}

func clonePositions(src [][]uint32) [][]uint32 {
	if len(src) == 0 {
		return nil
	}
	out := make([][]uint32, len(src))
	for i := range src {
		out[i] = append([]uint32(nil), src[i]...)
	}
	return out
}
