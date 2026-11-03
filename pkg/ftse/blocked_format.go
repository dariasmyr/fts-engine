package ftse

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"

	"github.com/dariasmyr/fts-engine/v2/pkg/posting"
)

const (
	postingBlockSize        = 128
	blockDirectoryEntrySize = 40
	termHasPositions        = 1 << 0
)

type blockDirectoryEntry struct {
	lastOrd         posting.Ord
	postingCount    uint32
	postingOffset   uint64
	postingLength   uint32
	postingChecksum uint32
	positionsOffset uint64
	positionsLength uint32
	positionsCRC    uint32
}

type encodedTerm struct {
	term         string
	df           uint64
	hasPositions bool
	directoryOff uint64
	directoryLen uint64
	directoryCRC uint32
}

// BuildOptions configures FTSE v3 serialization.
type BuildOptions struct {
	// ExcludedOrdinals are omitted from every term. The slice is borrowed for
	// the duration of Write and may be in any order.
	ExcludedOrdinals []posting.Ord
}

// Write serializes source as FTSE v3 without buffering the complete artifact.
// Source-owned slices are only read during the callback. If Write returns an
// error, w may contain a partial segment and must not be published.
func Write(ctx context.Context, w io.Writer, source posting.Exporter, options BuildOptions) error {
	excluded := make(ordinalSet, len(options.ExcludedOrdinals))
	for _, ord := range options.ExcludedOrdinals {
		excluded[ord] = struct{}{}
	}
	return write(ctx, w, source, excluded.orNil())
}

func write(ctx context.Context, w io.Writer, source posting.Exporter, excluded ordinalExclusion) error {
	if ctx == nil {
		return fmt.Errorf("ftse: nil context")
	}
	if w == nil {
		return fmt.Errorf("ftse: nil writer")
	}
	if source == nil {
		return fmt.Errorf("ftse: nil source")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	out := &countingWriter{writer: w}
	if err := writeHeader(out); err != nil {
		return err
	}

	terms := make([]encodedTerm, 0)
	var previousTerm string
	havePrevious := false
	err := source.ExportPostings(func(term posting.TermData) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if havePrevious && term.Term <= previousTerm {
			return fmt.Errorf("ftse: exporter terms must be strictly increasing: %q after %q", term.Term, previousTerm)
		}
		previousTerm, havePrevious = term.Term, true
		if err := validateTermData(term); err != nil {
			return err
		}
		filtered := filterTerm(term, excluded)
		if len(filtered.Postings) == 0 {
			return nil
		}
		encoded, err := writeTermBlocks(ctx, out, filtered)
		if err != nil {
			return err
		}
		terms = append(terms, encoded)
		return nil
	})
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	indexOff := out.offset
	dictionary := encodeDictionary(terms)
	if err := out.write(dictionary); err != nil {
		return fmt.Errorf("ftse: write dictionary: %w", err)
	}
	if err := writeFooter(out, indexOff, uint64(len(dictionary)), crc32.ChecksumIEEE(dictionary)); err != nil {
		return err
	}
	return nil
}

type countingWriter struct {
	writer io.Writer
	offset uint64
}

func (w *countingWriter) Write(data []byte) (int, error) {
	n, err := w.writer.Write(data)
	w.offset += uint64(n)
	return n, err
}

func (w *countingWriter) write(data []byte) error {
	n, err := w.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return nil
}

func writeHeader(out *countingWriter) error {
	var header [blockedHeaderLen]byte
	copy(header[:4], magic)
	binary.LittleEndian.PutUint16(header[4:6], blockedFormatVersion)
	binary.LittleEndian.PutUint16(header[8:10], postingBlockSize)
	if err := out.write(header[:]); err != nil {
		return fmt.Errorf("ftse: write header: %w", err)
	}
	return nil
}

func writeTermBlocks(ctx context.Context, out *countingWriter, term posting.TermData) (encodedTerm, error) {
	hasPositions := anyPositions(term.Positions)
	blocks := make([]blockDirectoryEntry, 0, (len(term.Postings)-1)/postingBlockSize+1)
	for start := 0; start < len(term.Postings); start += postingBlockSize {
		if err := ctx.Err(); err != nil {
			return encodedTerm{}, err
		}
		stop := min(start+postingBlockSize, len(term.Postings))
		postingBlob := encodePostingBlock(term.Postings[start:stop])
		if uint64(len(postingBlob)) > math.MaxUint32 {
			return encodedTerm{}, fmt.Errorf("ftse: posting block too large for term %q", term.Term)
		}
		block := blockDirectoryEntry{
			lastOrd:         term.Postings[stop-1].Ord,
			postingCount:    uint32(stop - start),
			postingOffset:   out.offset,
			postingLength:   uint32(len(postingBlob)),
			postingChecksum: crc32.ChecksumIEEE(postingBlob),
		}
		if err := out.write(postingBlob); err != nil {
			return encodedTerm{}, fmt.Errorf("ftse: write posting block for term %q: %w", term.Term, err)
		}
		if hasPositions {
			positionBlob := encodePositionBlock(term.Positions, start, stop)
			if uint64(len(positionBlob)) > math.MaxUint32 {
				return encodedTerm{}, fmt.Errorf("ftse: position block too large for term %q", term.Term)
			}
			block.positionsOffset = out.offset
			block.positionsLength = uint32(len(positionBlob))
			block.positionsCRC = crc32.ChecksumIEEE(positionBlob)
			if err := out.write(positionBlob); err != nil {
				return encodedTerm{}, fmt.Errorf("ftse: write position block for term %q: %w", term.Term, err)
			}
		}
		blocks = append(blocks, block)
	}

	directoryOff := out.offset
	directoryHash := crc32.NewIEEE()
	for _, block := range blocks {
		data := encodeBlockDirectoryEntry(block)
		if err := out.write(data[:]); err != nil {
			return encodedTerm{}, fmt.Errorf("ftse: write directory for term %q: %w", term.Term, err)
		}
		_, _ = directoryHash.Write(data[:])
	}
	return encodedTerm{
		term:         term.Term,
		df:           uint64(len(term.Postings)),
		hasPositions: hasPositions,
		directoryOff: directoryOff,
		directoryLen: out.offset - directoryOff,
		directoryCRC: directoryHash.Sum32(),
	}, nil
}

func encodeDictionary(terms []encodedTerm) []byte {
	data := make([]byte, 0, len(terms)*24)
	data = binary.AppendUvarint(data, uint64(len(terms)))
	for _, term := range terms {
		data = binary.AppendUvarint(data, uint64(len(term.term)))
		data = append(data, term.term...)
		data = binary.AppendUvarint(data, term.df)
		data = binary.AppendUvarint(data, term.directoryOff)
		data = binary.AppendUvarint(data, term.directoryLen)
		data = binary.LittleEndian.AppendUint32(data, term.directoryCRC)
		if term.hasPositions {
			data = append(data, termHasPositions)
		} else {
			data = append(data, 0)
		}
	}
	return data
}

func writeFooter(out *countingWriter, indexOff, indexLen uint64, dictionaryCRC uint32) error {
	var footer [footerLen]byte
	binary.LittleEndian.PutUint64(footer[0:8], indexOff)
	binary.LittleEndian.PutUint64(footer[8:16], indexLen)
	binary.LittleEndian.PutUint32(footer[16:20], dictionaryCRC)
	copy(footer[24:28], magic)
	binary.LittleEndian.PutUint16(footer[28:30], blockedFormatVersion)
	if err := out.write(footer[:]); err != nil {
		return fmt.Errorf("ftse: write footer: %w", err)
	}
	return nil
}

func filterTerm(term posting.TermData, excluded ordinalExclusion) posting.TermData {
	if excluded == nil {
		return term
	}
	postings := make([]posting.Posting, 0, len(term.Postings))
	var positions [][]uint32
	if len(term.Positions) > 0 {
		positions = make([][]uint32, 0, len(term.Positions))
	}
	for i, value := range term.Postings {
		if excluded.contains(value.Ord) {
			continue
		}
		postings = append(postings, value)
		if positions != nil {
			if i < len(term.Positions) {
				positions = append(positions, term.Positions[i])
			} else {
				positions = append(positions, nil)
			}
		}
	}
	term.Postings = postings
	term.Positions = positions
	return term
}

func validateTermData(term posting.TermData) error {
	if term.Term == "" {
		return fmt.Errorf("ftse: empty term")
	}
	if len(term.Postings) == 0 {
		return fmt.Errorf("ftse: term %q has no postings", term.Term)
	}
	if !ordsAreSorted(term.Postings) {
		return fmt.Errorf("ftse: term %q postings not strictly sorted by Ord", term.Term)
	}
	for _, value := range term.Postings {
		if value.Count == 0 {
			return fmt.Errorf("ftse: term %q has a zero posting count", term.Term)
		}
	}
	if len(term.Positions) > len(term.Postings) {
		return fmt.Errorf("ftse: term %q has more position lists than postings", term.Term)
	}
	for _, positions := range term.Positions {
		if !positionsAreSorted(positions) {
			return fmt.Errorf("ftse: term %q positions not sorted", term.Term)
		}
	}
	return nil
}

func encodePostingBlock(values []posting.Posting) []byte {
	blob := make([]byte, 0, len(values)*4)
	var previous posting.Ord
	for _, value := range values {
		blob = binary.AppendUvarint(blob, uint64(value.Ord-previous))
		blob = binary.AppendUvarint(blob, uint64(value.Count))
		previous = value.Ord
	}
	return blob
}

func encodePositionBlock(positions [][]uint32, start, stop int) []byte {
	blob := make([]byte, 0, (stop-start)*2)
	for i := start; i < stop; i++ {
		var values []uint32
		if i < len(positions) {
			values = positions[i]
		}
		blob = binary.AppendUvarint(blob, uint64(len(values)))
		var previous uint32
		for _, position := range values {
			blob = binary.AppendUvarint(blob, uint64(position-previous))
			previous = position
		}
	}
	return blob
}

func encodeBlockDirectoryEntry(block blockDirectoryEntry) [blockDirectoryEntrySize]byte {
	var data [blockDirectoryEntrySize]byte
	binary.LittleEndian.PutUint32(data[0:4], uint32(block.lastOrd))
	binary.LittleEndian.PutUint32(data[4:8], block.postingCount)
	binary.LittleEndian.PutUint64(data[8:16], block.postingOffset)
	binary.LittleEndian.PutUint32(data[16:20], block.postingLength)
	binary.LittleEndian.PutUint32(data[20:24], block.postingChecksum)
	binary.LittleEndian.PutUint64(data[24:32], block.positionsOffset)
	binary.LittleEndian.PutUint32(data[32:36], block.positionsLength)
	binary.LittleEndian.PutUint32(data[36:40], block.positionsCRC)
	return data
}
