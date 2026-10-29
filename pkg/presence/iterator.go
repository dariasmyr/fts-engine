package presence

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sort"

	"github.com/dariasmyr/fts-engine/v2/pkg/posting"
)

// OpenOrdinalPostings opens an exact-key iterator without I/O. Skip metadata and
// posting blocks are loaded on traversal, so a missing AND term or an empty
// candidate stream avoids reading other lists. Advance skips blocks by maximum
// ordinal; sequential traversal uses bounded read-ahead.
func (r *Reader) OpenOrdinalPostings(ctx context.Context, key string) (posting.OrdinalIterator, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	i := sort.Search(len(r.terms), func(i int) bool { return r.terms[i].key >= key })
	c := &cursor{
		ctx:     ctx,
		metrics: posting.TraversalMetricsFromContext(ctx),
		source:  r.source,
		block:   -1,
		pos:     -1,
	}
	if i == len(r.terms) || r.terms[i].key != key {
		c.done = true
		return c, 0, nil
	}
	c.term = r.terms[i]
	return c, c.term.count, nil
}

func (c *cursor) initDirectory() error {
	if c.dir != nil {
		return nil
	}
	n := int((c.term.count-1)/blockSize + 1)
	dir, err := readAt(c.source, int64(c.term.offset), n*blockEntrySize)
	if err != nil {
		return err
	}
	end := uint64(len(dir))
	var last uint32
	for b := range n {
		if b%1024 == 0 {
			if err := c.ctx.Err(); err != nil {
				return err
			}
		}

		entry := dir[b*blockEntrySize:]
		maxOrd := binary.LittleEndian.Uint32(entry)
		off := uint64(binary.LittleEndian.Uint32(entry[4:]))
		length := uint64(binary.LittleEndian.Uint32(entry[8:]))
		expected := min(uint64(blockSize), c.term.count-uint64(b)*blockSize)
		if b > 0 && maxOrd <= last || off != end || off > c.term.length ||
			length > c.term.length-off || length < expected || length > expected*5 {
			return fmt.Errorf("presence: invalid block directory")
		}
		last = maxOrd
		end = off + length
	}
	if end != c.term.length {
		return fmt.Errorf("presence: trailing posting bytes")
	}
	c.dir = dir
	return nil
}

type cursor struct {
	ctx     context.Context
	metrics *posting.TraversalMetrics
	source  io.ReaderAt
	term    term
	dir     []byte
	readBuf []byte
	readOff uint64
	block   int
	pos     int
	ords    [blockSize]posting.Ord
	scratch [blockSize * 5]byte
	n       int
	current posting.Ord
	valid   bool
	done    bool
	err     error
}

func (c *cursor) Ord() posting.Ord { return c.current }

func (c *cursor) Err() error { return c.err }

func (c *cursor) Close() error {
	c.done = true
	c.dir = nil
	c.readBuf = nil
	c.source = nil
	return nil
}

func (c *cursor) ready() bool {
	if c.done {
		return false
	}
	if err := c.ctx.Err(); err != nil {
		c.err = err
		c.done = true
		return false
	}
	return true
}

func (c *cursor) Next() bool {
	if !c.ready() {
		return false
	}
	if c.term.count == 1 {
		if c.valid {
			c.done = true
			return false
		}
		c.current = c.term.single
		c.valid = true
		return true
	}
	if c.pos+1 >= c.n {
		if !c.load(c.block + 1) {
			return false
		}
	}
	c.pos++
	c.current = c.ords[c.pos]
	c.valid = true
	return true
}

func (c *cursor) Advance(target posting.Ord) bool {
	if !c.ready() {
		return false
	}
	if c.valid && c.current >= target {
		return true
	}
	if c.term.count == 1 {
		if !c.valid && c.term.single >= target {
			return c.Next()
		}
		c.done = true
		return false
	}
	// Most intersections advance only a few ordinals. Avoid searching the skip
	// directory again while the target is still in the decoded block.
	if c.n > 0 && c.ords[c.n-1] >= target {
		start := c.pos + 1
		if c.ords[start] >= target {
			c.pos = start
		} else {
			c.pos = start + sort.Search(c.n-start, func(i int) bool { return c.ords[start+i] >= target })
		}
		c.current = c.ords[c.pos]
		c.valid = true
		return true
	}
	if err := c.initDirectory(); err != nil {
		c.err = err
		c.done = true
		return false
	}
	n := len(c.dir) / blockEntrySize
	b := sort.Search(n, func(i int) bool {
		return binary.LittleEndian.Uint32(c.dir[i*blockEntrySize:]) >= uint32(target)
	})
	if b >= n {
		c.done = true
		return false
	}
	if b != c.block {
		if !c.load(b) {
			return false
		}
	}
	c.pos = sort.Search(c.n, func(i int) bool { return c.ords[i] >= target })
	if c.pos == c.n {
		c.err = fmt.Errorf("presence: block maximum mismatch")
		c.done = true
		return false
	}
	c.current = c.ords[c.pos]
	c.valid = true
	return true
}

func (c *cursor) load(b int) bool {
	if err := c.initDirectory(); err != nil {
		c.err = err
		c.done = true
		return false
	}
	if b >= len(c.dir)/blockEntrySize {
		c.done = true
		return false
	}
	e := c.dir[b*blockEntrySize:]
	off, ln := binary.LittleEndian.Uint32(e[4:]), binary.LittleEndian.Uint32(e[8:])
	var blob []byte
	start, end := uint64(off), uint64(off)+uint64(ln)
	if c.readBuf != nil && start >= c.readOff && end <= c.readOff+uint64(len(c.readBuf)) {
		blob = c.readBuf[start-c.readOff : end-c.readOff]
	} else {
		blob = c.scratch[:int(ln)]
		if c.block >= 0 && b == c.block+1 {
			// Allocate read-ahead only after observing sequential block access.
			// Sparse probes retain the small inline scratch buffer.
			n := int(min(uint64(16<<10), c.term.length-start))
			if cap(c.readBuf) < n {
				c.readBuf = make([]byte, n)
			}
			c.readBuf = c.readBuf[:n]
			c.readOff = start
			blob = c.readBuf
		}
		got, err := c.source.ReadAt(blob, int64(c.term.offset+start))
		if got != len(blob) {
			if err == nil {
				err = io.ErrUnexpectedEOF
			}
			c.err = err
			c.done = true
			return false
		}
		blob = blob[:int(ln)]
	}
	c.n = int(min(uint64(blockSize), c.term.count-uint64(b)*blockSize))
	var prev uint64
	for i := 0; i < c.n; i++ {
		delta, err := takeUvarint(&blob)
		if err != nil || delta > math.MaxUint32-prev || i > 0 && delta == 0 {
			c.err = fmt.Errorf("presence: invalid ordinal delta")
			c.done = true
			return false
		}
		prev += delta
		c.ords[i] = posting.Ord(prev)
	}
	if len(blob) != 0 || uint32(prev) != binary.LittleEndian.Uint32(e) ||
		b > 0 && c.ords[0] <= posting.Ord(binary.LittleEndian.Uint32(c.dir[(b-1)*blockEntrySize:])) {
		c.err = fmt.Errorf("presence: invalid block ordering or length")
		c.done = true
		return false
	}
	c.metrics.RecordDecodedBlock(c.n)
	c.block = b
	c.pos = -1
	return true
}

var _ posting.OrdinalPostingOpener = (*Reader)(nil)
