package chunk

import (
	"strings"
	"unicode/utf8"
)

// Descriptor configures deterministic byte-range splitting.
type Descriptor struct {
	TargetBytes  int
	MaxBytes     int
	OverlapBytes int
}

func (d Descriptor) Validate() error {
	if d.TargetBytes <= 0 || d.MaxBytes < d.TargetBytes || d.OverlapBytes < 0 || d.OverlapBytes >= d.TargetBytes {
		return ErrInvalidSplitConfig
	}
	return nil
}

// Splitter produces half-open, UTF-8-aligned byte ranges.
type Splitter struct {
	descriptor Descriptor
}

func NewSplitter(descriptor Descriptor) (*Splitter, error) {
	if err := descriptor.Validate(); err != nil {
		return nil, err
	}
	return &Splitter{descriptor: descriptor}, nil
}

// Split prefers paragraph boundaries near the target and falls back to
// UTF-8-safe overlapping byte windows. It never retains source text.
func (s *Splitter) Split(text string) ([]Range, error) {
	if text == "" {
		return []Range{}, nil
	}
	if !utf8.ValidString(text) {
		return nil, ErrInvalidUTF8
	}
	if len(text) <= s.descriptor.TargetBytes {
		whole, err := Whole(text)
		if err != nil {
			return nil, err
		}
		return []Range{whole}, nil
	}
	ranges := make([]Range, 0, len(text)/s.descriptor.TargetBytes+1)
	for start := 0; start < len(text); {
		end := s.chooseEnd(text, start)
		if end <= start {
			return nil, ErrInvalidSplitConfig
		}
		ranges = append(ranges, Range{StartByte: start, EndByte: end})
		if end == len(text) {
			break
		}
		next := end - min(s.descriptor.OverlapBytes, end-start-1)
		for next < len(text) && !utf8.RuneStart(text[next]) {
			next++
		}
		start = next
	}
	return ranges, nil
}

// chooseEnd prefers the first paragraph boundary between target and maximum.
// If there is none, it uses the last paragraph boundary before target, then
// falls back to a UTF-8-safe cut near target.
func (s *Splitter) chooseEnd(text string, start int) int {
	target := min(start+s.descriptor.TargetBytes, len(text))
	maximum := min(start+s.descriptor.MaxBytes, len(text))
	if maximum == len(text) {
		return len(text)
	}

	if relative := strings.Index(text[target:maximum], "\n\n"); relative >= 0 {
		return utf8Boundary(text, target+relative+2, start)
	}
	if relative := strings.LastIndex(text[start:target], "\n\n"); relative > 0 {
		return utf8Boundary(text, start+relative+2, start)
	}
	return utf8Boundary(text, target, start)
}

// utf8Boundary adjusts desired to a rune boundary. It normally rounds backward
// to keep the chunk within its target. If that would produce end == start
// because desired falls inside the first rune, it rounds forward to the end of
// that rune so Split can make progress. text is valid UTF-8 and start is already
// a rune boundary.
func utf8Boundary(text string, desired, start int) int {
	if desired >= len(text) {
		return len(text)
	}
	if utf8.RuneStart(text[desired]) {
		return desired
	}

	backward := desired
	for backward > start && !utf8.RuneStart(text[backward]) {
		backward--
	}
	if backward > start {
		return backward
	}

	forward := desired
	for forward < len(text) && !utf8.RuneStart(text[forward]) {
		forward++
	}
	return forward
}
