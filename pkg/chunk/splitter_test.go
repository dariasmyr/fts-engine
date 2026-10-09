package chunk

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

func TestWhole(t *testing.T) {
	got, err := Whole("hello")
	if err != nil || got != (Range{0, 5}) {
		t.Fatalf("got %v, %v", got, err)
	}
	if _, err := Whole(""); !errors.Is(err, ErrInvalidRange) {
		t.Fatalf("empty: %v", err)
	}
}

func TestSplitterOffsetsOverlapUTF8AndDeterminism(t *testing.T) {
	splitter, err := NewSplitter(Descriptor{TargetBytes: 18, MaxBytes: 28, OverlapBytes: 5})
	if err != nil {
		t.Fatal(err)
	}
	text := "Первый абзац.\n\nSecond paragraph.\n\nТретий."
	first, err := splitter.Split(text)
	if err != nil {
		t.Fatal(err)
	}
	second, err := splitter.Split(text)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) < 2 || !slices.Equal(first, second) {
		t.Fatalf("ranges: %v, %v", first, second)
	}
	if !strings.HasSuffix(text[first[0].StartByte:first[0].EndByte], "\n\n") {
		t.Fatalf("boundary: %v", first[0])
	}
	for i, r := range first {
		if err := r.Validate(text); err != nil {
			t.Fatalf("range %d: %v", i, err)
		}
		if i > 0 && r.StartByte >= first[i-1].EndByte {
			t.Fatalf("ranges %d and %d do not overlap", i-1, i)
		}
	}
}

func TestSplitterEmptyShortAndTinyUTF8Window(t *testing.T) {
	splitter, err := NewSplitter(Descriptor{TargetBytes: 1, MaxBytes: 4})
	if err != nil {
		t.Fatal(err)
	}
	empty, err := splitter.Split("")
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty = %v, %v", empty, err)
	}
	text := "🙂🙂"
	ranges, err := splitter.Split(text)
	if err != nil {
		t.Fatal(err)
	}
	var parts []string
	for _, r := range ranges {
		if err := r.Validate(text); err != nil {
			t.Fatal(err)
		}
		parts = append(parts, text[r.StartByte:r.EndByte])
	}
	if !slices.Equal(parts, []string{"🙂", "🙂"}) {
		t.Fatalf("parts = %v", parts)
	}
}

func TestUTF8BoundaryDirectionAndProgress(t *testing.T) {
	cases := []struct {
		text                 string
		desired, start, want int
	}{
		{"ascii", 2, 0, 2}, {"a🙂b", 2, 0, 1},
		{"🙂b", 1, 0, len("🙂")}, {"text", 4, 0, 4},
	}
	for _, c := range cases {
		if got := utf8Boundary(c.text, c.desired, c.start); got != c.want {
			t.Errorf("%q: got %d want %d", c.text, got, c.want)
		}
	}
}

func TestSplitterRejectsInvalidConfigurationAndInput(t *testing.T) {
	if _, err := NewSplitter(Descriptor{TargetBytes: 10, MaxBytes: 5}); !errors.Is(err, ErrInvalidSplitConfig) {
		t.Fatal(err)
	}
	splitter, err := NewSplitter(Descriptor{TargetBytes: 10, MaxBytes: 20})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := splitter.Split(string([]byte{0xff})); !errors.Is(err, ErrInvalidUTF8) {
		t.Fatal(err)
	}
}
