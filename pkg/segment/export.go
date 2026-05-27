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
	if source == nil {
		return nil, fmt.Errorf("segment: nil source")
	}

	terms := make([]TermPostings, 0)
	err := source.ExportSegmentTerms(func(tp TermPostings) error {
		terms = append(terms, TermPostings{
			Term:      tp.Term,
			Postings:  append([]fts.Posting(nil), tp.Postings...),
			Positions: clonePositions(tp.Positions),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	return Build(terms)
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
