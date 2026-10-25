package chunk

import (
	"fmt"
	"unicode/utf8"
)

// Range identifies a half-open UTF-8-aligned byte interval in a source string.
type Range struct {
	StartByte int
	EndByte   int
}

// Validate checks that the range denotes a nonempty UTF-8-aligned substring.
func (r Range) Validate(text string) error {
	if r.StartByte < 0 || r.EndByte > len(text) || r.StartByte >= r.EndByte {
		return fmt.Errorf("%w: [%d,%d) for %d bytes", ErrInvalidRange, r.StartByte, r.EndByte, len(text))
	}
	if !utf8.ValidString(text) {
		return ErrInvalidUTF8
	}
	if !utf8.RuneStart(text[r.StartByte]) || (r.EndByte < len(text) && !utf8.RuneStart(text[r.EndByte])) {
		return fmt.Errorf("%w: range splits UTF-8 rune", ErrInvalidRange)
	}
	return nil
}

// Whole describes a complete nonempty field without retaining its text.
func Whole(text string) (Range, error) {
	if text == "" {
		return Range{}, ErrInvalidRange
	}
	if !utf8.ValidString(text) {
		return Range{}, ErrInvalidUTF8
	}
	return Range{StartByte: 0, EndByte: len(text)}, nil
}
