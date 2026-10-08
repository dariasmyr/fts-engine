package chunk

import (
	"fmt"
	"unicode/utf8"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

// Range identifies a half-open byte interval in a source string.
type Range struct {
	StartByte int
	EndByte   int
}

type Chunk struct {
	Ref  Ref
	Text string
}

// Validate checks the reference against its complete source field and requires
// Text to exactly equal the referenced half-open byte range.
func (c Chunk) Validate(fieldValue string) error {
	if err := c.Ref.Validate(fieldValue); err != nil {
		return err
	}
	if c.Ref.StartByte == c.Ref.EndByte || c.Text != fieldValue[int(c.Ref.StartByte):int(c.Ref.EndByte)] {
		return fmt.Errorf("%w: [%d,%d)", ErrTextMismatch, c.Ref.StartByte, c.Ref.EndByte)
	}
	return nil
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
