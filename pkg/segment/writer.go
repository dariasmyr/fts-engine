package segment

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

// TermPostings is the writer's input for one term: ord-sorted postings plus
// an optional positions list per posting (parallel slice; nil entries are
// allowed when a posting has no positions).
type TermPostings struct {
	Term      string
	Postings  []fts.Posting
	Positions [][]uint32
}

// Build serializes a list of TermPostings into a sealed-segment byte blob.
// Terms can be supplied in any order; the writer sorts them. Postings within
// each term must already be sorted by Ord.
func Build(terms []TermPostings) ([]byte, error) {
	for i := range terms {
		if !ordsAreSorted(terms[i].Postings) {
			return nil, fmt.Errorf("segment: term %q postings not sorted by Ord", terms[i].Term)
		}
	}

	sorted := make([]TermPostings, len(terms))
	copy(sorted, terms)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Term < sorted[j].Term })

	var buf bytes.Buffer
	writeHeader(&buf)

	type entry struct {
		term         string
		postingsOff  uint64
		postingsLen  uint64
		positionsOff uint64
		positionsLen uint64
		hasPositions bool
	}
	entries := make([]entry, 0, len(sorted))

	postingsStart := uint64(buf.Len())
	for _, tp := range sorted {
		off := uint64(buf.Len()) - postingsStart
		writePostings(&buf, tp.Postings)
		end := uint64(buf.Len()) - postingsStart
		entries = append(entries, entry{
			term:         tp.Term,
			postingsOff:  off,
			postingsLen:  end - off,
			hasPositions: anyPositions(tp.Positions),
		})
	}
	positionsStart := uint64(buf.Len())
	for i, tp := range sorted {
		if !entries[i].hasPositions {
			continue
		}
		off := uint64(buf.Len()) - positionsStart
		writePositions(&buf, tp.Postings, tp.Positions)
		end := uint64(buf.Len()) - positionsStart
		entries[i].positionsOff = off
		entries[i].positionsLen = end - off
	}

	indexOff := uint64(buf.Len())
	var idx bytes.Buffer
	tmp := make([]byte, 0, 64)
	tmp = binary.AppendUvarint(tmp, uint64(len(entries)))
	idx.Write(tmp)
	for _, e := range entries {
		tmp = tmp[:0]
		tmp = binary.AppendUvarint(tmp, uint64(len(e.term)))
		idx.Write(tmp)
		idx.WriteString(e.term)

		tmp = tmp[:0]
		tmp = binary.AppendUvarint(tmp, e.postingsOff)
		tmp = binary.AppendUvarint(tmp, e.postingsLen)
		tmp = binary.AppendUvarint(tmp, e.positionsOff)
		tmp = binary.AppendUvarint(tmp, e.positionsLen)
		if e.hasPositions {
			tmp = append(tmp, 1)
		} else {
			tmp = append(tmp, 0)
		}
		idx.Write(tmp)
	}
	indexBytes := idx.Bytes()
	buf.Write(indexBytes)

	writeFooter(&buf, indexOff, uint64(len(indexBytes)))
	return buf.Bytes(), nil
}

func writeHeader(buf *bytes.Buffer) {
	var hdr [headerLen]byte
	copy(hdr[0:4], magic)
	binary.LittleEndian.PutUint16(hdr[4:6], version)
	binary.LittleEndian.PutUint16(hdr[6:8], 0)
	buf.Write(hdr[:])
}

func writeFooter(buf *bytes.Buffer, indexOff, indexLen uint64) {
	var ftr [footerLen]byte
	binary.LittleEndian.PutUint64(ftr[0:8], indexOff)
	binary.LittleEndian.PutUint64(ftr[8:16], indexLen)
	copy(ftr[16:20], magic)
	binary.LittleEndian.PutUint16(ftr[20:22], version)
	binary.LittleEndian.PutUint16(ftr[22:24], 0)
	buf.Write(ftr[:])
}

func writePostings(buf *bytes.Buffer, postings []fts.Posting) {
	tmp := make([]byte, 0, len(postings)*4+5)
	tmp = binary.AppendUvarint(tmp, uint64(len(postings)))
	var prev fts.DocOrd
	for _, p := range postings {
		tmp = binary.AppendUvarint(tmp, uint64(p.Ord-prev))
		tmp = binary.AppendUvarint(tmp, uint64(p.Count))
		prev = p.Ord
	}
	buf.Write(tmp)
}

func writePositions(buf *bytes.Buffer, postings []fts.Posting, positions [][]uint32) {
	tmp := make([]byte, 0, 64)
	for i := range postings {
		var pos []uint32
		if i < len(positions) {
			pos = positions[i]
		}
		tmp = tmp[:0]
		tmp = binary.AppendUvarint(tmp, uint64(len(pos)))
		var prev uint32
		for _, p := range pos {
			tmp = binary.AppendUvarint(tmp, uint64(p-prev))
			prev = p
		}
		buf.Write(tmp)
	}
}

func ordsAreSorted(postings []fts.Posting) bool {
	for i := 1; i < len(postings); i++ {
		if postings[i].Ord < postings[i-1].Ord {
			return false
		}
	}
	return true
}

func anyPositions(positions [][]uint32) bool {
	for _, p := range positions {
		if len(p) > 0 {
			return true
		}
	}
	return false
}
