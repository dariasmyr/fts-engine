package semantic

import (
	"errors"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
)

func TestRefValidateMetadata(t *testing.T) {
	invalidUTF8 := string([]byte{0xff})
	tests := []struct {
		name string
		ref  Ref
		want error
	}{
		{name: "valid Unicode", ref: Ref{ID: "часть", DocID: "документ", Field: "текст", EndByte: 1}},
		{name: "empty document ID", ref: Ref{ID: "chunk", Field: "body"}, want: ErrInvalidDocID},
		{name: "empty chunk ID", ref: Ref{DocID: "doc", Field: "body"}, want: ErrInvalidChunkID},
		{name: "empty field", ref: Ref{ID: "chunk", DocID: "doc"}, want: ErrInvalidField},
		{name: "invalid document ID UTF-8", ref: Ref{ID: "chunk", DocID: fts.DocID(invalidUTF8), Field: "body"}, want: ErrInvalidUTF8},
		{name: "invalid chunk ID UTF-8", ref: Ref{ID: ChunkID(invalidUTF8), DocID: "doc", Field: "body"}, want: ErrInvalidUTF8},
		{name: "invalid field UTF-8", ref: Ref{ID: "chunk", DocID: "doc", Field: invalidUTF8}, want: ErrInvalidUTF8},
		{name: "reversed range", ref: Ref{ID: "chunk", DocID: "doc", Field: "body", StartByte: 2, EndByte: 1}, want: ErrInvalidRange},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.ref.ValidateMetadata()
			if tt.want == nil && err != nil {
				t.Fatalf("ValidateMetadata() error = %v", err)
			}
			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Fatalf("ValidateMetadata() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestRefValidateChecksSourceAndUTF8Boundaries(t *testing.T) {
	text := "a界b"
	if err := (Ref{ID: "chunk", DocID: "doc", Field: "body", StartByte: 1, EndByte: 4}).Validate(text); err != nil {
		t.Fatalf("Validate() valid Unicode range: %v", err)
	}
	if err := (Ref{ID: "chunk", DocID: "doc", Field: "body", StartByte: 2, EndByte: 4}).Validate(text); !errors.Is(err, ErrInvalidRange) {
		t.Fatalf("Validate() split rune error = %v, want ErrInvalidRange", err)
	}
	if err := (Ref{ID: "chunk", DocID: "doc", Field: "body", EndByte: 5}).Validate("text"); !errors.Is(err, ErrInvalidRange) {
		t.Fatalf("Validate() out-of-bounds error = %v, want ErrInvalidRange", err)
	}
	if err := (Ref{ID: "chunk", DocID: "doc", Field: "body"}).Validate(string([]byte{0xff})); !errors.Is(err, ErrInvalidUTF8) {
		t.Fatalf("Validate() malformed source error = %v, want ErrInvalidUTF8", err)
	}
}
