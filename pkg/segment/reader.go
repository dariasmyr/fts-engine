package segment

import (
	"encoding/binary"
	"fmt"
	"sort"
	"strings"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

// Reader queries a sealed segment by holding the file bytes and an in-memory
// term lookup table. It is read-only.
type Reader struct {
	bytes []byte
	terms []termEntry

	postingsBase  uint64
	positionsBase uint64
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

// Open parses a segment from bytes. The byte slice is referenced, not copied.
func Open(data []byte) (*Reader, error) {
	if len(data) < headerLen+footerLen {
		return nil, fmt.Errorf("segment: too short (%d bytes)", len(data))
	}
	if string(data[:4]) != magic {
		return nil, fmt.Errorf("segment: bad header magic")
	}
	if v := binary.LittleEndian.Uint16(data[4:6]); v != version {
		return nil, fmt.Errorf("segment: unsupported version %d", v)
	}

	footer := data[len(data)-footerLen:]
	if string(footer[16:20]) != magic {
		return nil, fmt.Errorf("segment: bad footer magic")
	}
	if v := binary.LittleEndian.Uint16(footer[20:22]); v != version {
		return nil, fmt.Errorf("segment: unsupported footer version %d", v)
	}
	indexOff := binary.LittleEndian.Uint64(footer[0:8])
	indexLen := binary.LittleEndian.Uint64(footer[8:16])
	if indexOff+indexLen > uint64(len(data)-footerLen) {
		return nil, fmt.Errorf("segment: index range out of bounds")
	}

	idx := data[indexOff : indexOff+indexLen]
	count, n := binary.Uvarint(idx)
	if n <= 0 {
		return nil, fmt.Errorf("segment: bad term count")
	}
	pos := n
	terms := make([]termEntry, 0, count)
	for range count {
		termLen, m := binary.Uvarint(idx[pos:])
		if m <= 0 {
			return nil, fmt.Errorf("segment: bad termLen")
		}
		pos += m
		if pos+int(termLen) > len(idx) {
			return nil, fmt.Errorf("segment: term bytes overflow")
		}
		term := string(idx[pos : pos+int(termLen)])
		pos += int(termLen)

		postingsOff, m1 := binary.Uvarint(idx[pos:])
		pos += m1
		postingsLen, m2 := binary.Uvarint(idx[pos:])
		pos += m2
		positionsOff, m3 := binary.Uvarint(idx[pos:])
		pos += m3
		positionsLen, m4 := binary.Uvarint(idx[pos:])
		pos += m4
		if m1 <= 0 || m2 <= 0 || m3 <= 0 || m4 <= 0 || pos >= len(idx) {
			return nil, fmt.Errorf("segment: bad term entry")
		}
		hasPositions := idx[pos] != 0
		pos++

		terms = append(terms, termEntry{
			term:         term,
			postingsOff:  postingsOff,
			postingsLen:  postingsLen,
			positionsOff: positionsOff,
			positionsLen: positionsLen,
			hasPositions: hasPositions,
		})
	}

	r := &Reader{
		bytes:        data,
		terms:        terms,
		postingsBase: uint64(headerLen),
	}
	var postingsEnd uint64
	for _, t := range terms {
		end := t.postingsOff + t.postingsLen
		if end > postingsEnd {
			postingsEnd = end
		}
	}
	r.positionsBase = uint64(headerLen) + postingsEnd
	return r, nil
}

func (r *Reader) Bytes() []byte { return r.bytes }

func (r *Reader) TermCount() int { return len(r.terms) }

func (r *Reader) findTerm(term string) (termEntry, bool) {
	i := sort.Search(len(r.terms), func(i int) bool { return r.terms[i].term >= term })
	if i < len(r.terms) && r.terms[i].term == term {
		return r.terms[i], true
	}
	return termEntry{}, false
}

func (r *Reader) Insert(key string, id fts.DocID, ord ...fts.DocOrd) error {
	return fmt.Errorf("segment: read-only (Insert called on sealed segment)")
}

func (r *Reader) InsertAt(key string, id fts.DocID, position uint32, ord ...fts.DocOrd) error {
	return fmt.Errorf("segment: read-only (InsertAt called on sealed segment)")
}

func (r *Reader) Search(key string) ([]fts.Posting, error) {
	e, ok := r.findTerm(key)
	if !ok {
		return nil, nil
	}
	return decodePostings(r.bytes[r.postingsBase+e.postingsOff : r.postingsBase+e.postingsOff+e.postingsLen]), nil
}

func (r *Reader) SearchPositional(key string) ([]fts.PositionalPosting, error) {
	e, ok := r.findTerm(key)
	if !ok {
		return nil, nil
	}
	postings := decodePostings(r.bytes[r.postingsBase+e.postingsOff : r.postingsBase+e.postingsOff+e.postingsLen])
	out := make([]fts.PositionalPosting, len(postings))
	if !e.hasPositions {
		for i, p := range postings {
			out[i] = fts.PositionalPosting{Ord: p.Ord}
		}
		return out, nil
	}
	posBuf := r.bytes[r.positionsBase+e.positionsOff : r.positionsBase+e.positionsOff+e.positionsLen]
	cur := 0
	for i, p := range postings {
		count, n := binary.Uvarint(posBuf[cur:])
		if n <= 0 {
			return nil, fmt.Errorf("segment: bad position count for term %q", key)
		}
		cur += n
		positions := make([]uint32, 0, count)
		var prev uint32
		for range count {
			delta, m := binary.Uvarint(posBuf[cur:])
			if m <= 0 {
				return nil, fmt.Errorf("segment: bad position delta for term %q", key)
			}
			cur += m
			prev += uint32(delta)
			positions = append(positions, prev)
		}
		out[i] = fts.PositionalPosting{Ord: p.Ord, Positions: positions}
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
		term := r.terms[i].term
		if !strings.HasPrefix(term, prefix) {
			break
		}
		postings := decodePostings(r.bytes[r.postingsBase+r.terms[i].postingsOff : r.postingsBase+r.terms[i].postingsOff+r.terms[i].postingsLen])
		for _, p := range postings {
			aggregated[p.Ord] += p.Count
		}
	}
	out := make([]fts.Posting, 0, len(aggregated))
	for ord, count := range aggregated {
		out = append(out, fts.Posting{Ord: ord, Seq: uint32(ord), Count: count})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ord < out[j].Ord })
	return out, nil
}

func decodePostings(blob []byte) []fts.Posting {
	if len(blob) == 0 {
		return nil
	}
	count, n := binary.Uvarint(blob)
	if n <= 0 || count == 0 {
		return nil
	}
	pos := n
	out := make([]fts.Posting, 0, count)
	var ord fts.DocOrd
	for range count {
		delta, m := binary.Uvarint(blob[pos:])
		if m <= 0 {
			return out
		}
		pos += m
		tf, m2 := binary.Uvarint(blob[pos:])
		if m2 <= 0 {
			return out
		}
		pos += m2
		ord += fts.DocOrd(delta)
		out = append(out, fts.Posting{Ord: ord, Seq: uint32(ord), Count: uint32(tf)})
	}
	return out
}

var (
	_ fts.Index           = (*Reader)(nil)
	_ fts.PositionalIndex = (*Reader)(nil)
	_ fts.PrefixIndex     = (*Reader)(nil)
)
