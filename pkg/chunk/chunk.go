package chunk

import (
	"unicode/utf8"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

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
