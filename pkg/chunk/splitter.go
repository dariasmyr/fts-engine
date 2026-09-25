package chunk

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

var ErrInvalidSplitConfig = errors.New("chunk: invalid split configuration")

// Descriptor identifies the chunking policy and its byte-window parameters.
// ID must change when the same input may produce different chunk boundaries or
// IDs.
type Descriptor struct {
	ID           string
	TargetBytes  int
	MaxBytes     int
	OverlapBytes int
}

func (d Descriptor) Validate() error {
	if d.ID == "" || d.TargetBytes <= 0 || d.MaxBytes < d.TargetBytes || d.OverlapBytes < 0 || d.OverlapBytes >= d.TargetBytes {
		return ErrInvalidSplitConfig
	}
	return nil
}

// Splitter produces deterministic ChunkRefs with half-open, UTF-8-aligned byte
// ranges. It prefers paragraph boundaries and otherwise uses overlapping byte
// windows.
type Splitter struct {
	descriptor Descriptor
}

func NewSplitter(descriptor Descriptor) (*Splitter, error) {
	if err := descriptor.Validate(); err != nil {
		return nil, err
	}
	return &Splitter{descriptor: descriptor}, nil
}

func (s *Splitter) Descriptor() Descriptor { return s.descriptor }

// Split prefers paragraph boundaries near the configured target and falls back
// to UTF-8-safe overlapping byte windows.
func (s *Splitter) Split(docID fts.DocID, field, text string) ([]Chunk, error) {
	if docID == "" {
		return nil, ErrInvalidDocID
	}
	if field == "" {
		return nil, ErrInvalidField
	}
	if text == "" {
		return []Chunk{}, nil
	}
	if !utf8.ValidString(text) {
		return nil, ErrInvalidUTF8
	}
	if len(text) <= s.descriptor.TargetBytes {
		whole, err := Whole(docID, field, text)
		if err != nil {
			return nil, err
		}
		return []Chunk{whole}, nil
	}

	// Work directly on the immutable string so splitting does not copy the
	// complete document into a temporary []byte.
	chunks := make([]Chunk, 0, len(text)/s.descriptor.TargetBytes+1)
	for start := 0; start < len(text); {
		end := s.chooseEnd(text, start)
		// A zero-length chunk would keep start unchanged and loop forever.
		// utf8Boundary normally prevents this even when TargetBytes falls inside
		// the first multi-byte rune, but keep the invariant explicit here.
		if end <= start {
			return nil, fmt.Errorf("%w: splitter made no progress at byte %d", ErrInvalidSplitConfig, start)
		}
		ordinal := uint32(len(chunks))
		id := ID(fmt.Sprintf("chunk-%06d", ordinal))
		if field != fts.DefaultField {
			id = ID(fmt.Sprintf("%s/chunk-%06d", field, ordinal))
		}
		chunks = append(chunks, Chunk{
			Ref: Ref{
				ID:        id,
				DocID:     docID,
				Field:     field,
				Ordinal:   ordinal,
				StartByte: uint64(start),
				EndByte:   uint64(end),
			},
			Text: text[start:end],
		})
		if end == len(text) {
			break
		}
		next := end - min(s.descriptor.OverlapBytes, end-start-1)
		for next < len(text) && !utf8.RuneStart(text[next]) {
			next++
		}
		start = next
	}
	return chunks, nil
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
