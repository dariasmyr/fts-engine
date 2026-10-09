package semantic

import (
	"fmt"
	"unicode/utf8"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

// ChunkID identifies a source chunk within its document.
type ChunkID string

// Ref identifies the source of an embedding. It does not own source text.
type Ref struct {
	ID        ChunkID
	DocID     fts.DocID
	Field     string
	Ordinal   uint32
	StartByte uint64
	EndByte   uint64
}

// Validate verifies metadata and byte boundaries against the source field.
func (r Ref) Validate(text string) error {
	if r.DocID == "" {
		return ErrInvalidDocID
	}
	if r.ID == "" {
		return ErrInvalidChunkID
	}
	if r.Field == "" {
		return ErrInvalidField
	}
	if !utf8.ValidString(text) {
		return ErrInvalidUTF8
	}
	if r.StartByte > r.EndByte || r.EndByte > uint64(len(text)) {
		return fmt.Errorf("%w: [%d,%d) for %d bytes", ErrInvalidRange, r.StartByte, r.EndByte, len(text))
	}
	if (r.StartByte < uint64(len(text)) && !utf8.RuneStart(text[r.StartByte])) ||
		(r.EndByte < uint64(len(text)) && !utf8.RuneStart(text[r.EndByte])) {
		return fmt.Errorf("%w: range is not aligned to UTF-8 boundaries", ErrInvalidRange)
	}
	return nil
}
