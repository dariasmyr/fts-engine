package ftse

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/dariasmyr/fts-engine/v2/pkg/posting"
)

// BuildFromSource collects exported terms from a mutable index and serializes
// them into a sealed ftse.
func BuildFromSource(source posting.Exporter) ([]byte, error) {
	return buildFromSource(source, nil)
}

// BuildFromSourceWithTombstones skips deleted ords so rebuilt artifacts do not
// copy stale postings forward.
func BuildFromSourceWithTombstones(source posting.Exporter, tombstoneWords []uint64) ([]byte, error) {
	return buildFromSource(source, bitmapExclusions(tombstoneWords).orNil())
}

// WriteFromSourceWithTombstones streams an FTSE v3 segment while omitting
// ordinals selected by the supplied dense tombstone bitmap. On error, w may
// contain a partial ftse.
func WriteFromSourceWithTombstones(
	ctx context.Context,
	w io.Writer,
	source posting.Exporter,
	tombstoneWords []uint64,
) error {
	if source == nil {
		return fmt.Errorf("ftse: nil source")
	}
	return write(ctx, w, source, bitmapExclusions(tombstoneWords).orNil())
}

func buildFromSource(source posting.Exporter, excluded ordinalExclusion) ([]byte, error) {
	if source == nil {
		return nil, fmt.Errorf("ftse: nil source")
	}
	var out bytes.Buffer
	if err := write(context.Background(), &out, source, excluded); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

type ordinalExclusion interface {
	contains(posting.Ord) bool
}

type ordinalSet map[posting.Ord]struct{}

func (s ordinalSet) contains(ord posting.Ord) bool {
	_, ok := s[ord]
	return ok
}

func (s ordinalSet) orNil() ordinalExclusion {
	if len(s) == 0 {
		return nil
	}
	return s
}

type bitmapExclusions []uint64

func (e bitmapExclusions) orNil() ordinalExclusion {
	for _, word := range e {
		if word != 0 {
			return e
		}
	}
	return nil
}

func (e bitmapExclusions) contains(ord posting.Ord) bool {
	word := uint64(ord) / 64
	return word < uint64(len(e)) && e[word]&(uint64(1)<<(ord%64)) != 0
}
