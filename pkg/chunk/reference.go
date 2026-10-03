package chunk

import (
	"fmt"
	"unicode/utf8"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

// ID identifies a chunk within its source document.
type ID string

// Ref identifies the source location and document ownership of a chunk. It is
// metadata associated with an embedding, not the identity of the vector index
// row itself.
type Ref struct {
	ID        ID
	DocID     fts.DocID
	Field     string
	Ordinal   uint32
	StartByte uint64
	EndByte   uint64
}

func (r Ref) Validate(fieldValue string) error {
	if r.DocID == "" {
		return ErrInvalidDocID
	}
	if r.ID == "" {
		return ErrInvalidChunkID
	}
	if r.Field == "" {
		return ErrInvalidField
	}
	if !utf8.ValidString(fieldValue) {
		return ErrInvalidUTF8
	}
	if r.StartByte > r.EndByte || r.EndByte > uint64(len(fieldValue)) {
		return fmt.Errorf("%w: [%d,%d) for %d bytes", ErrInvalidRange, r.StartByte, r.EndByte, len(fieldValue))
	}
	if (r.StartByte < uint64(len(fieldValue)) && !utf8.RuneStart(fieldValue[r.StartByte])) ||
		(r.EndByte < uint64(len(fieldValue)) && !utf8.RuneStart(fieldValue[r.EndByte])) {
		return fmt.Errorf("%w: range is not aligned to UTF-8 boundaries", ErrInvalidRange)
	}
	return nil
}
