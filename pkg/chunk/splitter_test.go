package chunk

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

func TestWholeUsesStableFieldAwareID(t *testing.T) {
	whole, err := Whole("doc-1", fts.DefaultField, "hello")
	if err != nil {
		t.Fatal(err)
	}
	if whole.Ref.ID != WholeID || whole.Ref.EndByte != 5 || whole.Text != "hello" {
		t.Fatalf("whole = %+v", whole)
	}
	title, err := Whole("doc-1", "title", "hello")
	if err != nil {
		t.Fatal(err)
	}
	if title.Ref.ID != "_whole/title" {
		t.Fatalf("title ID = %q", title.Ref.ID)
	}
}

func TestSplitterOffsetsOverlapUTF8AndStableIDs(t *testing.T) {
	splitter, err := NewSplitter(Descriptor{ID: "paragraph-v1", TargetBytes: 18, MaxBytes: 28, OverlapBytes: 5})
	if err != nil {
		t.Fatal(err)
	}
	text := "Первый абзац.\n\nSecond paragraph.\n\nТретий."
	first, err := splitter.Split("doc-1", fts.DefaultField, text)
	if err != nil {
		t.Fatal(err)
	}
	second, err := splitter.Split("doc-1", fts.DefaultField, text)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) < 2 || len(first) != len(second) {
		t.Fatalf("chunk counts = %d and %d", len(first), len(second))
	}
	if !strings.HasSuffix(first[0].Text, "\n\n") {
		t.Fatalf("first chunk did not use the nearby paragraph boundary: %q", first[0].Text)
	}
	for i, item := range first {
		if err := item.Ref.Validate(text); err != nil {
			t.Fatalf("chunk %d ref: %v", i, err)
		}
		if item.Text != text[item.Ref.StartByte:item.Ref.EndByte] {
			t.Fatalf("chunk %d text does not match byte range", i)
		}
		if item.Ref.ID != second[i].Ref.ID || item.Ref.StartByte != second[i].Ref.StartByte || item.Ref.EndByte != second[i].Ref.EndByte {
			t.Fatalf("chunk %d is not stable", i)
		}
		if i > 0 && item.Ref.StartByte >= first[i-1].Ref.EndByte {
			t.Fatalf("chunks %d and %d do not overlap", i-1, i)
		}
	}
}

func TestSplitterEmptyShortAndTinyUTF8Window(t *testing.T) {
	splitter, err := NewSplitter(Descriptor{ID: "tiny", TargetBytes: 1, MaxBytes: 4})
	if err != nil {
		t.Fatal(err)
	}
	empty, err := splitter.Split("doc", fts.DefaultField, "")
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty = %v, %v", empty, err)
	}
	chunks, err := splitter.Split("doc", fts.DefaultField, "🙂🙂")
	if err != nil {
		t.Fatal(err)
	}
	texts := make([]string, len(chunks))
	for i, item := range chunks {
		texts[i] = item.Text
		if err := item.Ref.Validate("🙂🙂"); err != nil {
			t.Fatal(err)
		}
	}
	if !slices.Equal(texts, []string{"🙂", "🙂"}) {
		t.Fatalf("texts = %q", texts)
	}
}

func TestUTF8BoundaryDirectionAndProgress(t *testing.T) {
	if got := utf8Boundary("ascii", 2, 0); got != 2 {
		t.Fatalf("ASCII boundary = %d, want 2", got)
	}
	if got := utf8Boundary("a🙂b", 2, 0); got != 1 {
		t.Fatalf("boundary inside a later rune = %d, want backward boundary 1", got)
	}
	if got := utf8Boundary("🙂b", 1, 0); got != len("🙂") {
		t.Fatalf("boundary inside the first rune = %d, want forward boundary %d", got, len("🙂"))
	}
	if got := utf8Boundary("text", len("text"), 0); got != len("text") {
		t.Fatalf("end boundary = %d, want %d", got, len("text"))
	}
}

func TestSplitterRejectsInvalidConfigurationAndInput(t *testing.T) {
	if _, err := NewSplitter(Descriptor{ID: "bad", TargetBytes: 10, MaxBytes: 5}); !errors.Is(err, ErrInvalidSplitConfig) {
		t.Fatalf("config error = %v", err)
	}
	splitter, _ := NewSplitter(Descriptor{ID: "ok", TargetBytes: 10, MaxBytes: 20})
	if _, err := splitter.Split("", fts.DefaultField, "text"); !errors.Is(err, ErrInvalidDocID) {
		t.Fatalf("doc error = %v", err)
	}
	if _, err := splitter.Split("doc", "", "text"); !errors.Is(err, ErrInvalidField) {
		t.Fatalf("field error = %v", err)
	}
	if _, err := splitter.Split("doc", fts.DefaultField, string([]byte{0xff})); !errors.Is(err, ErrInvalidUTF8) {
		t.Fatalf("UTF-8 error = %v", err)
	}
}
