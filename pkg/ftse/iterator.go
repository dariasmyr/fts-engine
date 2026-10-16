package ftse

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/dariasmyr/fts-engine/v2/pkg/posting"
)

// OpenOrdinalPostings returns an ordinal-only iterator for an exact term.
// Term frequencies remain stored in the segment but are not exposed through
// this interface because filtering and intersection only require document ordinals.
func (r *Reader) OpenOrdinalPostings(ctx context.Context, key string) (posting.OrdinalIterator, uint64, error) {
	if r.formatVersion == blockedFormatVersion {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		entry, ok := r.findTerm(key)
		if !ok {
			return &blockedCursor{ctx: ctx, done: true, block: -1, pos: -1}, 0, nil
		}
		return r.openBlockedCursor(ctx, entry)
	}
	cursor, count, err := r.openPostingCursor(ctx, key)
	if err != nil {
		return nil, 0, err
	}
	return cursor, count, nil
}

// OpenFrequencyPostings exposes ordinals and term frequencies for candidate-driven
// ranked search. FTSE v2 advances sequentially through its posting stream;
// FTSE v3 uses the block directory persisted with the term.
func (r *Reader) OpenFrequencyPostings(ctx context.Context, key string) (posting.FrequencyIterator, uint64, error) {
	if r.formatVersion == blockedFormatVersion {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		entry, ok := r.findTerm(key)
		if !ok {
			return &blockedCursor{ctx: ctx, done: true, block: -1, pos: -1}, 0, nil
		}
		return r.openBlockedCursor(ctx, entry)
	}
	return r.openPostingCursor(ctx, key)
}

func (r *Reader) openPostingCursor(ctx context.Context, key string) (*postingCursor, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	cursor := &postingCursor{ctx: ctx}
	entry, ok := r.findTerm(key)
	if !ok {
		cursor.done = true
		return cursor, 0, nil
	}
	base := int64(r.postingsBase + entry.postingsOff)
	length := int64(entry.postingsLen)
	cursor.input = bufio.NewReaderSize(io.NewSectionReader(r.readerAt, base, length), 4096)
	count, err := binary.ReadUvarint(cursor.input)
	if err != nil || count == 0 || count > entry.postingsLen/2 {
		return nil, 0, fmt.Errorf("ftse: invalid posting count: %w", errOrInvalid(err))
	}
	cursor.remaining = count
	return cursor, count, nil
}

func errOrInvalid(err error) error {
	if err != nil {
		return err
	}
	return fmt.Errorf("invalid value")
}

// postingCursor is the sequential FTSE v2 read-compatibility cursor. Current
// FTSE writers emit v3, whose posting lists are traversed by blockedCursor.
type postingCursor struct {
	ctx       context.Context
	input     *bufio.Reader
	remaining uint64
	ord       posting.Ord
	count     uint32
	valid     bool
	done      bool
	err       error
}

func (c *postingCursor) Ord() posting.Ord { return c.ord }

func (c *postingCursor) Frequency() uint32 { return c.count }

func (c *postingCursor) Err() error { return c.err }

func (c *postingCursor) Close() error {
	c.done = true
	c.input = nil
	return nil
}

func (c *postingCursor) Next() bool {
	if c.done {
		return false
	}
	if err := c.ctx.Err(); err != nil {
		c.err = err
		c.done = true
		return false
	}
	if c.remaining == 0 {
		c.done = true
		return false
	}
	delta, err := binary.ReadUvarint(c.input)
	if err != nil || delta > uint64(math.MaxUint32-uint32(c.ord)) || c.valid && delta == 0 {
		c.err = fmt.Errorf("ftse: invalid posting ordinal: %w", errOrInvalid(err))
		c.done = true
		return false
	}
	freq, err := binary.ReadUvarint(c.input)
	if err != nil || freq == 0 || freq > math.MaxUint32 {
		c.err = fmt.Errorf("ftse: invalid posting frequency: %w", errOrInvalid(err))
		c.done = true
		return false
	}
	c.ord += posting.Ord(delta)
	c.count = uint32(freq)
	c.valid = true
	c.remaining--
	if c.remaining == 0 {
		_, err := c.input.ReadByte()
		if err != io.EOF {
			if err == nil {
				err = fmt.Errorf("trailing posting data")
			}
			c.err = err
			c.done = true
			return false
		}
	}
	return true
}

func (c *postingCursor) Advance(target posting.Ord) bool {
	if c.done {
		return false
	}
	if err := c.ctx.Err(); err != nil {
		c.err = err
		c.done = true
		return false
	}
	if c.valid && c.ord >= target {
		return true
	}
	for c.Next() {
		if c.ord >= target {
			return true
		}
	}
	return false
}

var (
	_ posting.OrdinalPostingOpener   = (*Reader)(nil)
	_ posting.FrequencyPostingOpener = (*Reader)(nil)
)
