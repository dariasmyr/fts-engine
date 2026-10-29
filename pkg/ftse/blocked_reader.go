package ftse

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"sort"

	"github.com/dariasmyr/fts-engine/v2/pkg/posting"
)

func (r *Reader) readBlockDirectory(entry *termEntry) ([]blockDirectoryEntry, error) {
	data, err := r.readRange(entry.directoryOff, entry.directoryLen)
	if err != nil {
		return nil, fmt.Errorf("ftse: read v3 directory for term %q: %w", entry.term, err)
	}
	if got := crc32.ChecksumIEEE(data); got != entry.directoryCRC {
		return nil, fmt.Errorf("ftse: v3 directory checksum mismatch for term %q", entry.term)
	}
	count := len(data) / blockDirectoryEntrySize
	blocks := make([]blockDirectoryEntry, count)
	var previousLast posting.Ord
	var postingCount uint64
	dataStart := uint64(blockedHeaderLen)
	termIndex := sort.Search(len(r.terms), func(i int) bool { return r.terms[i].term >= entry.term })
	if termIndex > 0 {
		previous := r.terms[termIndex-1]
		dataStart = previous.directoryOff + previous.directoryLen
	}
	for i := range blocks {
		raw := data[i*blockDirectoryEntrySize:]
		block := blockDirectoryEntry{
			lastOrd:         posting.Ord(binary.LittleEndian.Uint32(raw[0:4])),
			postingCount:    binary.LittleEndian.Uint32(raw[4:8]),
			postingOffset:   binary.LittleEndian.Uint64(raw[8:16]),
			postingLength:   binary.LittleEndian.Uint32(raw[16:20]),
			postingChecksum: binary.LittleEndian.Uint32(raw[20:24]),
			positionsOffset: binary.LittleEndian.Uint64(raw[24:32]),
			positionsLength: binary.LittleEndian.Uint32(raw[32:36]),
			positionsCRC:    binary.LittleEndian.Uint32(raw[36:40]),
		}
		if block.postingCount == 0 || block.postingCount > postingBlockSize ||
			i > 0 && block.lastOrd <= previousLast ||
			!validBlockRange(block.postingOffset, uint64(block.postingLength), entry.directoryOff) ||
			block.postingLength == 0 || block.postingOffset < dataStart {
			return nil, fmt.Errorf("ftse: invalid v3 posting block for term %q", entry.term)
		}
		if entry.hasPositions {
			if block.positionsLength == 0 ||
				!validBlockRange(block.positionsOffset, uint64(block.positionsLength), entry.directoryOff) ||
				block.positionsOffset < block.postingOffset+uint64(block.postingLength) {
				return nil, fmt.Errorf("ftse: invalid v3 position block for term %q", entry.term)
			}
			dataStart = block.positionsOffset + uint64(block.positionsLength)
		} else if block.positionsOffset != 0 || block.positionsLength != 0 || block.positionsCRC != 0 {
			return nil, fmt.Errorf("ftse: unexpected v3 positions for term %q", entry.term)
		} else {
			dataStart = block.postingOffset + uint64(block.postingLength)
		}
		blocks[i] = block
		previousLast = block.lastOrd
		postingCount += uint64(block.postingCount)
	}
	if postingCount != entry.documentFrequency {
		return nil, fmt.Errorf("ftse: v3 document frequency mismatch for term %q", entry.term)
	}
	return blocks, nil
}

func validBlockRange(offset, length, limit uint64) bool {
	return offset >= blockedHeaderLen && offset <= limit && length <= limit-offset
}

func (r *Reader) readPostingBlock(entry *termEntry, block blockDirectoryEntry) ([]posting.Posting, error) {
	data, err := r.readRange(block.postingOffset, uint64(block.postingLength))
	if err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(data) != block.postingChecksum {
		return nil, fmt.Errorf("ftse: v3 posting checksum mismatch for term %q", entry.term)
	}
	values := make([]posting.Posting, 0, block.postingCount)
	pos := 0
	var ord posting.Ord
	for range block.postingCount {
		delta, err := takeUvarint(data, &pos)
		if err != nil || delta > uint64(math.MaxUint32-uint32(ord)) || len(values) > 0 && delta == 0 {
			return nil, fmt.Errorf("ftse: invalid v3 posting ordinal for term %q", entry.term)
		}
		frequency, err := takeUvarint(data, &pos)
		if err != nil || frequency == 0 || frequency > math.MaxUint32 {
			return nil, fmt.Errorf("ftse: invalid v3 posting frequency for term %q", entry.term)
		}
		ord += posting.Ord(delta)
		values = append(values, posting.Posting{Ord: ord, Count: uint32(frequency)})
	}
	if pos != len(data) || values[len(values)-1].Ord != block.lastOrd {
		return nil, fmt.Errorf("ftse: invalid v3 posting block length or maximum for term %q", entry.term)
	}
	return values, nil
}

func (r *Reader) searchBlocked(entry *termEntry) ([]posting.Posting, error) {
	directory, err := r.readBlockDirectory(entry)
	if err != nil {
		return nil, err
	}
	values := make([]posting.Posting, 0, int(entry.documentFrequency))
	for blockIndex, block := range directory {
		decoded, err := r.readPostingBlock(entry, block)
		if err != nil {
			return nil, err
		}
		if err := validateBlockBoundary(entry, directory, blockIndex, decoded); err != nil {
			return nil, err
		}
		values = append(values, decoded...)
	}
	return values, nil
}

func (r *Reader) searchBlockedPositional(entry *termEntry) ([]posting.PositionalPosting, error) {
	directory, err := r.readBlockDirectory(entry)
	if err != nil {
		return nil, err
	}
	out := make([]posting.PositionalPosting, 0, int(entry.documentFrequency))
	for blockIndex, block := range directory {
		values, err := r.readPostingBlock(entry, block)
		if err != nil {
			return nil, err
		}
		if err := validateBlockBoundary(entry, directory, blockIndex, values); err != nil {
			return nil, err
		}
		if !entry.hasPositions {
			for _, value := range values {
				out = append(out, posting.PositionalPosting{Ord: value.Ord})
			}
			continue
		}
		positions, err := r.readPositionBlock(entry, block)
		if err != nil {
			return nil, err
		}
		for i, value := range values {
			out = append(out, posting.PositionalPosting{Ord: value.Ord, Positions: positions[i]})
		}
	}
	return out, nil
}

func (r *Reader) readPositionBlock(entry *termEntry, block blockDirectoryEntry) ([][]uint32, error) {
	data, err := r.readRange(block.positionsOffset, uint64(block.positionsLength))
	if err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(data) != block.positionsCRC {
		return nil, fmt.Errorf("ftse: v3 position checksum mismatch for term %q", entry.term)
	}
	allPositions := make([][]uint32, 0, block.postingCount)
	pos := 0
	for range block.postingCount {
		count, err := takeUvarint(data, &pos)
		if err != nil || count > uint64(len(data)-pos) {
			return nil, fmt.Errorf("ftse: invalid v3 position count for term %q", entry.term)
		}
		positions := make([]uint32, 0, int(count))
		var previous uint32
		for range count {
			delta, err := takeUvarint(data, &pos)
			if err != nil || delta > uint64(math.MaxUint32-previous) {
				return nil, fmt.Errorf("ftse: invalid v3 position delta for term %q", entry.term)
			}
			previous += uint32(delta)
			positions = append(positions, previous)
		}
		allPositions = append(allPositions, positions)
	}
	if pos != len(data) {
		return nil, fmt.Errorf("ftse: trailing v3 position data for term %q", entry.term)
	}
	return allPositions, nil
}

func (r *Reader) verifyBlockedEntry(ctx context.Context, entry *termEntry) error {
	directory, err := r.readBlockDirectory(entry)
	if err != nil {
		return err
	}
	for blockIndex, block := range directory {
		if err := ctx.Err(); err != nil {
			return err
		}
		values, err := r.readPostingBlock(entry, block)
		if err != nil {
			return err
		}
		if err := validateBlockBoundary(entry, directory, blockIndex, values); err != nil {
			return err
		}
		if entry.hasPositions {
			if _, err := r.readPositionBlock(entry, block); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateBlockBoundary(
	entry *termEntry,
	directory []blockDirectoryEntry,
	blockIndex int,
	values []posting.Posting,
) error {
	if blockIndex > 0 && values[0].Ord <= directory[blockIndex-1].lastOrd {
		return fmt.Errorf("ftse: v3 posting blocks overlap for term %q", entry.term)
	}
	return nil
}

type blockedCursor struct {
	ctx       context.Context
	metrics   *posting.TraversalMetrics
	reader    *Reader
	entry     *termEntry
	directory []blockDirectoryEntry
	values    []posting.Posting
	block     int
	pos       int
	valid     bool
	done      bool
	err       error
}

func (c *blockedCursor) Ord() posting.Ord  { return c.values[c.pos].Ord }
func (c *blockedCursor) Frequency() uint32 { return c.values[c.pos].Count }
func (c *blockedCursor) Err() error        { return c.err }
func (c *blockedCursor) Close() error {
	c.done = true
	c.reader, c.entry = nil, nil
	c.directory, c.values = nil, nil
	return nil
}

func (c *blockedCursor) ready() bool {
	if c.done {
		return false
	}
	if err := c.ctx.Err(); err != nil {
		c.err, c.done = err, true
		return false
	}
	if c.directory == nil {
		directory, err := c.reader.readBlockDirectory(c.entry)
		if err != nil {
			c.err, c.done = err, true
			return false
		}
		c.directory = directory
	}
	return true
}

func (c *blockedCursor) Next() bool {
	if !c.ready() {
		return false
	}
	if c.valid && c.pos+1 < len(c.values) {
		c.pos++
		return true
	}
	if !c.loadBlock(c.block + 1) {
		return false
	}
	c.pos, c.valid = 0, true
	return true
}

func (c *blockedCursor) Advance(target posting.Ord) bool {
	if !c.ready() {
		return false
	}
	if c.valid && c.Ord() >= target {
		return true
	}
	// If target still fits in the decoded block, find it without touching the
	// block directory or reading more data. When valid, the current ordinal was
	// checked above and is below target, so searching starts at the next item.
	if len(c.values) > 0 && c.values[len(c.values)-1].Ord >= target {
		start := c.pos + 1
		c.pos = start + sort.Search(len(c.values)-start, func(i int) bool {
			return c.values[start+i].Ord >= target
		})
		c.valid = true
		return true
	}
	// The target is beyond the decoded block. Directory entries contain each
	// block's maximum ordinal, so skip directly to the first remaining block
	// that may contain an ordinal >= target.
	blockStart := c.block + 1
	block := blockStart + sort.Search(len(c.directory)-blockStart, func(i int) bool {
		return c.directory[blockStart+i].lastOrd >= target
	})
	if block == len(c.directory) {
		c.done = true
		return false
	}
	if block != c.block && !c.loadBlock(block) {
		return false
	}
	// lastOrd identifies the target block, not the exact posting. Locate the
	// first ordinal in that block which satisfies Advance's >= contract.
	c.pos = sort.Search(len(c.values), func(i int) bool { return c.values[i].Ord >= target })
	if c.pos == len(c.values) {
		c.err = fmt.Errorf("ftse: v3 block maximum mismatch")
		c.done = true
		return false
	}
	c.valid = true
	return true
}

func (c *blockedCursor) loadBlock(block int) bool {
	if block >= len(c.directory) {
		c.done = true
		return false
	}
	values, err := c.reader.readPostingBlock(c.entry, c.directory[block])
	if err != nil {
		c.err, c.done = err, true
		return false
	}
	if err := validateBlockBoundary(c.entry, c.directory, block, values); err != nil {
		c.err, c.done = err, true
		return false
	}
	c.metrics.RecordDecodedBlock(len(values))
	c.values = values
	c.block = block
	c.pos = -1
	c.valid = false
	return true
}

func (r *Reader) openBlockedCursor(ctx context.Context, entry *termEntry) (*blockedCursor, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	return &blockedCursor{
		ctx:     ctx,
		metrics: posting.TraversalMetricsFromContext(ctx),
		reader:  r,
		entry:   entry,
		block:   -1,
		pos:     -1,
	}, entry.documentFrequency, nil
}

var (
	_ posting.OrdinalIterator   = (*blockedCursor)(nil)
	_ posting.FrequencyIterator = (*blockedCursor)(nil)
	_ io.Closer                 = (*blockedCursor)(nil)
)
