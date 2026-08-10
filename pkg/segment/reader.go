package segment

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sort"
	"strings"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

// Reader queries a sealed segment using an in-memory term table. The segment
// body can be backed by bytes, mmap, or ReaderAt file access.
type Reader struct {
	bytes    []byte
	readerAt io.ReaderAt
	size     uint64
	terms    []termEntry

	postingsBase  uint64
	positionsBase uint64
	indexOff      uint64
}

type termEntry struct {
	term         string
	postingsOff  uint64
	postingsLen  uint64
	positionsOff uint64
	positionsLen uint64
	hasPositions bool
}

// MappedReader is a Reader backed by a memory-mapped file.
type MappedReader struct {
	*Reader
	closer func() error
}

func (m *MappedReader) Close() error {
	if m == nil || m.closer == nil {
		return nil
	}
	return m.closer()
}

// FileReader is a Reader backed by pread-style file access.
type FileReader struct {
	*Reader
	file *os.File
}

func (f *FileReader) Close() error {
	if f == nil || f.file == nil {
		return nil
	}
	err := f.file.Close()
	f.file = nil
	return err
}

// OpenFile opens a segment file using mmap on supported platforms.
func OpenFile(path string) (*MappedReader, error) {
	data, closer, err := openMmap(path)
	if err != nil {
		return nil, err
	}
	r, err := Open(data)
	if err != nil {
		_ = closer()
		return nil, err
	}
	return &MappedReader{Reader: r, closer: closer}, nil
}

// OpenFileReader opens a segment without reading or mapping the complete file.
func OpenFileReader(path string) (*FileReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("segment: open %q: %w", path, err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("segment: stat %q: %w", path, err)
	}
	r, err := OpenReaderAt(file, info.Size())
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("segment: open %q: %w", path, err)
	}
	return &FileReader{Reader: r, file: file}, nil
}

// Open parses a segment from bytes. The byte slice is referenced, not copied.
func Open(data []byte) (*Reader, error) {
	r, err := openReaderAt(bytes.NewReader(data), int64(len(data)), data)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// OpenReaderAt parses metadata from a random-access source and keeps only the
// term lookup table resident. Searches read the matching posting ranges.
func OpenReaderAt(source io.ReaderAt, size int64) (*Reader, error) {
	return openReaderAt(source, size, nil)
}

func openReaderAt(source io.ReaderAt, size int64, data []byte) (*Reader, error) {
	if source == nil {
		return nil, fmt.Errorf("segment: nil reader")
	}
	if size < headerLen+legacyFooterLen {
		return nil, fmt.Errorf("segment: too short (%d bytes)", size)
	}

	header, err := readAt(source, 0, headerLen)
	if err != nil {
		return nil, fmt.Errorf("segment: read header: %w", err)
	}
	if string(header[:4]) != magic {
		return nil, fmt.Errorf("segment: bad header magic")
	}
	formatVersion := binary.LittleEndian.Uint16(header[4:6])
	var formatFooterLen int
	switch formatVersion {
	case legacyVersion:
		formatFooterLen = legacyFooterLen
	case version:
		formatFooterLen = footerLen
	default:
		return nil, fmt.Errorf("segment: unsupported version %d", formatVersion)
	}
	if size < int64(headerLen+formatFooterLen) {
		return nil, fmt.Errorf("segment: too short (%d bytes)", size)
	}

	footerOff := size - int64(formatFooterLen)
	footer, err := readAt(source, footerOff, formatFooterLen)
	if err != nil {
		return nil, fmt.Errorf("segment: read footer: %w", err)
	}
	magicOff, versionOff := 16, 20
	if formatVersion == version {
		magicOff, versionOff = 24, 28
	}
	if string(footer[magicOff:magicOff+4]) != magic {
		return nil, fmt.Errorf("segment: bad footer magic")
	}
	if footerVersion := binary.LittleEndian.Uint16(footer[versionOff : versionOff+2]); footerVersion != formatVersion {
		return nil, fmt.Errorf("segment: footer version %d does not match header version %d", footerVersion, formatVersion)
	}

	indexOff := binary.LittleEndian.Uint64(footer[0:8])
	indexLen := binary.LittleEndian.Uint64(footer[8:16])
	contentLen := uint64(footerOff)
	if indexOff < headerLen || indexOff > contentLen || indexLen > contentLen-indexOff || indexOff+indexLen != contentLen {
		return nil, fmt.Errorf("segment: index range out of bounds")
	}
	if indexLen > uint64(math.MaxInt) {
		return nil, fmt.Errorf("segment: index is too large")
	}
	if formatVersion == version {
		wantChecksum := binary.LittleEndian.Uint32(footer[16:20])
		hash := crc32.NewIEEE()
		if _, err := io.Copy(hash, io.NewSectionReader(source, 0, footerOff)); err != nil {
			return nil, fmt.Errorf("segment: checksum read: %w", err)
		}
		if got := hash.Sum32(); got != wantChecksum {
			return nil, fmt.Errorf("segment: checksum mismatch: got %08x, want %08x", got, wantChecksum)
		}
	}

	indexBytes, err := readAt(source, int64(indexOff), int(indexLen))
	if err != nil {
		return nil, fmt.Errorf("segment: read index: %w", err)
	}
	terms, postingsEnd, err := parseTermIndex(indexBytes)
	if err != nil {
		return nil, err
	}
	positionsBase := uint64(headerLen) + postingsEnd
	if positionsBase > indexOff {
		return nil, fmt.Errorf("segment: postings range overlaps term index")
	}
	for _, term := range terms {
		if term.postingsOff > postingsEnd || term.postingsLen > postingsEnd-term.postingsOff {
			return nil, fmt.Errorf("segment: postings range out of bounds for term %q", term.term)
		}
		positionsSize := indexOff - positionsBase
		if term.positionsOff > positionsSize || term.positionsLen > positionsSize-term.positionsOff {
			return nil, fmt.Errorf("segment: positions range out of bounds for term %q", term.term)
		}
		if !term.hasPositions && (term.positionsOff != 0 || term.positionsLen != 0) {
			return nil, fmt.Errorf("segment: unexpected positions range for term %q", term.term)
		}
	}

	return &Reader{
		bytes:         data,
		readerAt:      source,
		size:          uint64(size),
		terms:         terms,
		postingsBase:  headerLen,
		positionsBase: positionsBase,
		indexOff:      indexOff,
	}, nil
}

func parseTermIndex(data []byte) ([]termEntry, uint64, error) {
	pos := 0
	take := func() (uint64, error) {
		if pos >= len(data) {
			return 0, io.ErrUnexpectedEOF
		}
		value, n := binary.Uvarint(data[pos:])
		if n <= 0 {
			return 0, fmt.Errorf("invalid varint")
		}
		pos += n
		return value, nil
	}
	count, err := take()
	if err != nil || count > uint64(len(data)) {
		return nil, 0, fmt.Errorf("segment: bad term count")
	}
	terms := make([]termEntry, 0, int(count))
	var postingsEnd uint64
	for range count {
		termLen, err := take()
		if err != nil || termLen == 0 || termLen > uint64(len(data)-pos) {
			return nil, 0, fmt.Errorf("segment: bad term length")
		}
		term := string(data[pos : pos+int(termLen)])
		pos += int(termLen)
		if len(terms) > 0 && term <= terms[len(terms)-1].term {
			return nil, 0, fmt.Errorf("segment: terms are not strictly sorted")
		}
		postingsOff, err1 := take()
		postingsLen, err2 := take()
		positionsOff, err3 := take()
		positionsLen, err4 := take()
		if err1 != nil || err2 != nil || err3 != nil || err4 != nil || postingsLen == 0 || pos >= len(data) {
			return nil, 0, fmt.Errorf("segment: bad entry for term %q", term)
		}
		hasPositions := data[pos] != 0
		pos++
		if postingsOff > math.MaxUint64-postingsLen {
			return nil, 0, fmt.Errorf("segment: postings range overflow for term %q", term)
		}
		if end := postingsOff + postingsLen; end > postingsEnd {
			postingsEnd = end
		}
		terms = append(terms, termEntry{
			term: term, postingsOff: postingsOff, postingsLen: postingsLen,
			positionsOff: positionsOff, positionsLen: positionsLen, hasPositions: hasPositions,
		})
	}
	if pos != len(data) {
		return nil, 0, fmt.Errorf("segment: trailing term index data")
	}
	return terms, postingsEnd, nil
}

func readAt(source io.ReaderAt, off int64, length int) ([]byte, error) {
	if off < 0 || length < 0 {
		return nil, fmt.Errorf("invalid read range")
	}
	data := make([]byte, length)
	if length == 0 {
		return data, nil
	}
	n, err := source.ReadAt(data, off)
	if n != length {
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return data, nil
}

// Bytes returns the underlying bytes for byte-backed and mmap readers. It
// returns nil for ReaderAt-backed files.
func (r *Reader) Bytes() []byte { return r.bytes }

func (r *Reader) TermCount() int { return len(r.terms) }

func (r *Reader) findTerm(term string) (termEntry, bool) {
	i := sort.Search(len(r.terms), func(i int) bool { return r.terms[i].term >= term })
	if i < len(r.terms) && r.terms[i].term == term {
		return r.terms[i], true
	}
	return termEntry{}, false
}

func (r *Reader) readRange(off, length uint64) ([]byte, error) {
	if off > r.indexOff || length > r.indexOff-off || length > uint64(math.MaxInt) {
		return nil, fmt.Errorf("segment: read range out of bounds")
	}
	if r.bytes != nil {
		return r.bytes[int(off):int(off+length)], nil
	}
	return readAt(r.readerAt, int64(off), int(length))
}

func (r *Reader) Insert(key string, ord fts.DocOrd) error {
	return fmt.Errorf("segment: read-only (Insert called on sealed segment)")
}

func (r *Reader) InsertAt(key string, position uint32, ord fts.DocOrd) error {
	return fmt.Errorf("segment: read-only (InsertAt called on sealed segment)")
}

func (r *Reader) Search(key string) ([]fts.Posting, error) {
	e, ok := r.findTerm(key)
	if !ok {
		return nil, nil
	}
	blob, err := r.readRange(r.postingsBase+e.postingsOff, e.postingsLen)
	if err != nil {
		return nil, fmt.Errorf("segment: read postings for term %q: %w", key, err)
	}
	return decodePostings(blob)
}

func (r *Reader) SearchPositional(key string) ([]fts.PositionalPosting, error) {
	e, ok := r.findTerm(key)
	if !ok {
		return nil, nil
	}
	postingBlob, err := r.readRange(r.postingsBase+e.postingsOff, e.postingsLen)
	if err != nil {
		return nil, fmt.Errorf("segment: read postings for term %q: %w", key, err)
	}
	postings, err := decodePostings(postingBlob)
	if err != nil {
		return nil, fmt.Errorf("segment: decode postings for term %q: %w", key, err)
	}
	out := make([]fts.PositionalPosting, len(postings))
	if !e.hasPositions {
		for i, posting := range postings {
			out[i] = fts.PositionalPosting{Ord: posting.Ord}
		}
		return out, nil
	}
	posBuf, err := r.readRange(r.positionsBase+e.positionsOff, e.positionsLen)
	if err != nil {
		return nil, fmt.Errorf("segment: read positions for term %q: %w", key, err)
	}
	cur := 0
	for i, posting := range postings {
		count, err := takeUvarint(posBuf, &cur)
		if err != nil || count > uint64(len(posBuf)-cur) {
			return nil, fmt.Errorf("segment: bad position count for term %q", key)
		}
		positions := make([]uint32, 0, int(count))
		var previous uint32
		for range count {
			delta, err := takeUvarint(posBuf, &cur)
			if err != nil || delta > uint64(math.MaxUint32-previous) {
				return nil, fmt.Errorf("segment: bad position delta for term %q", key)
			}
			previous += uint32(delta)
			positions = append(positions, previous)
		}
		out[i] = fts.PositionalPosting{Ord: posting.Ord, Positions: positions}
	}
	if cur != len(posBuf) {
		return nil, fmt.Errorf("segment: trailing position data for term %q", key)
	}
	return out, nil
}

func (r *Reader) SearchPrefix(prefix string) ([]fts.Posting, error) {
	if prefix == "" {
		return nil, nil
	}
	start := sort.Search(len(r.terms), func(i int) bool { return r.terms[i].term >= prefix })
	aggregated := make(map[fts.DocOrd]uint32)
	for i := start; i < len(r.terms); i++ {
		entry := r.terms[i]
		if !strings.HasPrefix(entry.term, prefix) {
			break
		}
		blob, err := r.readRange(r.postingsBase+entry.postingsOff, entry.postingsLen)
		if err != nil {
			return nil, fmt.Errorf("segment: read postings for term %q: %w", entry.term, err)
		}
		postings, err := decodePostings(blob)
		if err != nil {
			return nil, fmt.Errorf("segment: decode postings for term %q: %w", entry.term, err)
		}
		for _, posting := range postings {
			if posting.Count > math.MaxUint32-aggregated[posting.Ord] {
				return nil, fmt.Errorf("segment: posting count overflow for prefix %q", prefix)
			}
			aggregated[posting.Ord] += posting.Count
		}
	}
	out := make([]fts.Posting, 0, len(aggregated))
	for ord, count := range aggregated {
		out = append(out, fts.Posting{Ord: ord, Seq: uint32(ord), Count: count})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ord < out[j].Ord })
	return out, nil
}

func decodePostings(blob []byte) ([]fts.Posting, error) {
	pos := 0
	count, err := takeUvarint(blob, &pos)
	if err != nil || count == 0 || count > uint64(len(blob)) {
		return nil, fmt.Errorf("invalid posting count")
	}
	out := make([]fts.Posting, 0, int(count))
	var ord fts.DocOrd
	for range count {
		delta, err := takeUvarint(blob, &pos)
		if err != nil || delta > uint64(math.MaxUint32-uint32(ord)) {
			return nil, fmt.Errorf("invalid posting ordinal")
		}
		termFrequency, err := takeUvarint(blob, &pos)
		if err != nil || termFrequency == 0 || termFrequency > math.MaxUint32 {
			return nil, fmt.Errorf("invalid posting count value")
		}
		ord += fts.DocOrd(delta)
		out = append(out, fts.Posting{Ord: ord, Seq: uint32(ord), Count: uint32(termFrequency)})
	}
	if pos != len(blob) {
		return nil, fmt.Errorf("trailing posting data")
	}
	return out, nil
}

func takeUvarint(data []byte, pos *int) (uint64, error) {
	if *pos >= len(data) {
		return 0, io.ErrUnexpectedEOF
	}
	value, n := binary.Uvarint(data[*pos:])
	if n <= 0 {
		return 0, fmt.Errorf("invalid varint")
	}
	*pos += n
	return value, nil
}

var (
	_ fts.Index           = (*Reader)(nil)
	_ fts.PositionalIndex = (*Reader)(nil)
	_ fts.PrefixIndex     = (*Reader)(nil)
)
