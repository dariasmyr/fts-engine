package presence

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"math/bits"
	"slices"
	"strings"

	"github.com/dariasmyr/fts-engine/v2/pkg/posting"
)

// BuildFromSource creates an in-memory presence ftse. Frequencies and
// positions are discarded. Postings must be strictly ordinal-sorted.
// Use WriteFromSource to avoid buffering the entire encoded ftse.
func BuildFromSource(source posting.Exporter) ([]byte, error) {
	var buf bytes.Buffer
	if err := WriteFromSource(context.Background(), &buf, source); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// WriteFromSource writes an FPR1 segment without buffering the complete output
// or retaining source postings. It retains the term dictionary, one reusable
// block directory (12 bytes per 128 postings of the largest term), and bounded
// encoding buffers. Source-owned memory and allocations are additional.
// The source is visited once and is not modified. Its term order may be arbitrary;
// keys must be unique and postings strictly increasing. Counts/positions are ignored.
// The caller supplies live postings and persists the matching analyzer identity
// and ordinal-to-ID mapping. Cancellation is checked between terms and blocks.
// On failure the output is partial: write to a temporary file, then sync, close
// and atomically publish it only after success. This function does not close w.
func WriteFromSource(ctx context.Context, w io.Writer, source posting.Exporter) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if source == nil || w == nil {
		return fmt.Errorf("presence: nil source or writer")
	}
	h := crc32.NewIEEE()
	out := bufio.NewWriterSize(io.MultiWriter(w, h), 32<<10)
	var offset uint64
	write := func(p []byte) error {
		n, err := out.Write(p)
		offset += uint64(n)
		return err
	}
	if err := write([]byte(magic)); err != nil {
		return err
	}
	var terms []term
	var directory []byte
	var scratch [blockSize * 5]byte
	err := source.ExportPostings(func(tp posting.TermData) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if tp.Term == "" || len(tp.Postings) == 0 {
			return fmt.Errorf("presence: empty term or postings")
		}
		t := term{key: tp.Term, count: uint64(len(tp.Postings))}
		if t.count > uint64(math.MaxUint32)+1 {
			return fmt.Errorf("presence: too many postings")
		}
		if t.count == 1 {
			t.single = tp.Postings[0].Ord
		} else {
			t.offset = offset
			n := (len(tp.Postings)-1)/blockSize + 1
			if uint64(n)*blockEntrySize > math.MaxInt {
				return fmt.Errorf("presence: block directory too large")
			}
			directory = slices.Grow(directory[:0], n*blockEntrySize)[:n*blockEntrySize]
			end := uint64(len(directory))
			for b := range n {
				if err := ctx.Err(); err != nil {
					return err
				}
				var prev posting.Ord
				length := 0
				start := b * blockSize
				stop := start + min(blockSize, len(tp.Postings)-start)
				for j := start; j < stop; j++ {
					ord := tp.Postings[j].Ord
					if j > 0 && ord <= tp.Postings[j-1].Ord {
						return fmt.Errorf("presence: postings must be strictly increasing")
					}
					length += max(1, (bits.Len32(uint32(ord-prev))+6)/7)
					prev = ord
				}
				if end+uint64(length) > math.MaxUint32 {
					return fmt.Errorf("presence: posting list exceeds 4 GiB")
				}
				entry := directory[b*blockEntrySize:]
				binary.LittleEndian.PutUint32(entry, uint32(prev))
				binary.LittleEndian.PutUint32(entry[4:], uint32(end))
				binary.LittleEndian.PutUint32(entry[8:], uint32(length))
				end += uint64(length)
			}
			if err := write(directory); err != nil {
				return err
			}
			for b := range n {
				if err := ctx.Err(); err != nil {
					return err
				}
				blob := scratch[:0]
				var prev posting.Ord
				start := b * blockSize
				for _, p := range tp.Postings[start : start+min(blockSize, len(tp.Postings)-start)] {
					blob = binary.AppendUvarint(blob, uint64(p.Ord-prev))
					prev = p.Ord
				}
				if err := write(blob); err != nil {
					return err
				}
			}
			t.length = offset - t.offset
		}
		terms = append(terms, t)
		return nil
	})
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	slices.SortFunc(terms, func(a, b term) int { return strings.Compare(a.key, b.key) })
	indexOff := offset
	if err := write(binary.AppendUvarint(scratch[:0], uint64(len(terms)))); err != nil {
		return err
	}
	for i, t := range terms {
		if err := ctx.Err(); err != nil {
			return err
		}
		if i > 0 && terms[i-1].key == t.key {
			return fmt.Errorf("presence: duplicate term %q", t.key)
		}
		if err := write(binary.AppendUvarint(scratch[:0], uint64(len(t.key)))); err != nil {
			return err
		}
		n, err := out.WriteString(t.key)
		offset += uint64(n)
		if err != nil {
			return err
		}
		blob := binary.AppendUvarint(scratch[:0], t.count)
		if t.count == 1 {
			blob = binary.AppendUvarint(blob, uint64(t.single))
		} else {
			blob = binary.AppendUvarint(blob, t.offset)
			blob = binary.AppendUvarint(blob, t.length)
		}
		if err := write(blob); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := out.Flush(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	footer := binary.LittleEndian.AppendUint64(scratch[:0], indexOff)
	footer = binary.LittleEndian.AppendUint64(footer, offset-indexOff)
	footer = binary.LittleEndian.AppendUint32(footer, h.Sum32())
	footer = append(footer, magic...)
	n, err := w.Write(footer)
	if err == nil && n != len(footer) {
		err = io.ErrShortWrite
	}
	return err
}
