// Package chunk defines source references and deterministic text splitters for
// semantic indexing.
package chunk

import (
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

const WholeID ID = "_whole"

var (
	ErrInvalidDocID   = errors.New("chunk: document ID must not be empty")
	ErrInvalidField   = errors.New("chunk: field must not be empty")
	ErrInvalidRange   = errors.New("chunk: invalid byte range")
	ErrInvalidChunkID = errors.New("chunk: chunk ID must not be empty")
	ErrInvalidUTF8    = errors.New("chunk: field value must be valid UTF-8")
)

type ID string

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

type Chunk struct {
	Ref  Ref
	Text string
}

// Whole returns one semantic unit for a complete field value.
func Whole(docID fts.DocID, field, text string) (Chunk, error) {
	if docID == "" {
		return Chunk{}, ErrInvalidDocID
	}
	if field == "" {
		return Chunk{}, ErrInvalidField
	}
	if !utf8.ValidString(text) {
		return Chunk{}, ErrInvalidUTF8
	}
	id := WholeID
	if field != fts.DefaultField {
		id = ID(string(WholeID) + "/" + field)
	}
	return Chunk{Ref: Ref{ID: id, DocID: docID, Field: field, EndByte: uint64(len(text))}, Text: text}, nil
}
