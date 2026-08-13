// Package flat provides a pointer-light mutable index optimized for building
// immutable segments with high term cardinality.
package flat

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"slices"
	"sort"
	"sync"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/segment"
)

const (
	snapshotMagicV1  = "FLT1"
	snapshotMagic    = "FLT2"
	maxSnapshotBytes = 512 << 20
	maxTokenBytes    = 1 << 20
	maxTerms         = 32 << 20
)

// Index stores token bytes in one arena and hash-chain heads in a pointer-free
// map. The first posting is inline because observability datasets commonly have
// a large majority of terms with document frequency one. Positional metadata is
// kept in a separate lazy parallel table so plain high-cardinality entries stay
// compact.
type Index struct {
	mu        sync.RWMutex
	arena     []byte
	entries   []entry
	byHash    map[uint64]int32
	termOrder []int32
	positions [][][]uint32
}

type entry struct {
	first    fts.Posting
	rest     []fts.Posting
	tokenOff uint32
	tokenLen uint32
	next     int32
}

func New() *Index { return &Index{byHash: make(map[uint64]int32)} }

func tokenHash(token string) uint64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(token); i++ {
		h ^= uint64(token[i])
		h *= 1099511628211
	}
	return h
}

func (idx *Index) tokenBytes(entryIndex int32) []byte {
	e := &idx.entries[entryIndex]
	return idx.arena[e.tokenOff : e.tokenOff+e.tokenLen]
}

func (idx *Index) find(token string, hash uint64) int32 {
	head, ok := idx.byHash[hash]
	if !ok {
		return -1
	}
	for i := head; i >= 0; i = idx.entries[i].next {
		if tokenEqual(idx.tokenBytes(i), token) {
			return i
		}
	}
	return -1
}

func tokenEqual(tokenBytes []byte, token string) bool {
	if len(tokenBytes) != len(token) {
		return false
	}
	for i := range tokenBytes {
		if tokenBytes[i] != token[i] {
			return false
		}
	}
	return true
}

func tokenHasPrefix(tokenBytes []byte, prefix string) bool {
	if len(tokenBytes) < len(prefix) {
		return false
	}
	for i := range prefix {
		if tokenBytes[i] != prefix[i] {
			return false
		}
	}
	return true
}

func compareTokenString(tokenBytes []byte, token string) int {
	limit := min(len(tokenBytes), len(token))
	for i := range limit {
		if tokenBytes[i] < token[i] {
			return -1
		}
		if tokenBytes[i] > token[i] {
			return 1
		}
	}
	if len(tokenBytes) < len(token) {
		return -1
	}
	if len(tokenBytes) > len(token) {
		return 1
	}
	return 0
}

// Insert adds one term occurrence for ord.
func (idx *Index) Insert(token string, ord fts.DocOrd) error {
	return idx.insert(token, ord, false, 0)
}

// InsertAt adds one term occurrence and records its position within the field.
func (idx *Index) InsertAt(token string, position uint32, ord fts.DocOrd) error {
	return idx.insert(token, ord, true, position)
}

func (idx *Index) insert(token string, ord fts.DocOrd, hasPosition bool, position uint32) error {
	if token == "" {
		return errors.New("flat: empty token")
	}
	if len(token) > maxTokenBytes {
		return fmt.Errorf("flat: token is too large: %d bytes", len(token))
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.byHash == nil {
		idx.byHash = make(map[uint64]int32)
	}
	hash := tokenHash(token)
	entryIndex := idx.find(token, hash)
	if entryIndex < 0 {
		if len(idx.entries) >= math.MaxInt32 {
			return errors.New("flat: term count exceeds int32 address space")
		}
		if uint64(len(idx.arena))+uint64(len(token)) > math.MaxUint32 {
			return errors.New("flat: token arena exceeds uint32 address space")
		}
		off := uint32(len(idx.arena))
		idx.arena = append(idx.arena, token...)
		head, ok := idx.byHash[hash]
		if !ok {
			head = -1
		}
		idx.entries = append(idx.entries, entry{
			first:    fts.Posting{Ord: ord, Count: 1, Seq: uint32(ord)},
			tokenOff: off, tokenLen: uint32(len(token)), next: head,
		})
		if hasPosition {
			idx.growPositions(int32(len(idx.entries)-1), 1)
			idx.positions[len(idx.entries)-1][0] = []uint32{position}
		}
		idx.byHash[hash] = int32(len(idx.entries) - 1)
		return nil
	}
	return idx.addPosting(entryIndex, ord, hasPosition, position)
}

func (idx *Index) addPosting(entryIndex int32, ord fts.DocOrd, hasPosition bool, position uint32) error {
	e := &idx.entries[entryIndex]
	total := len(e.rest) + 1
	last := &e.first
	if len(e.rest) > 0 {
		last = &e.rest[len(e.rest)-1]
	}
	if last.Ord == ord {
		return idx.incrementPosting(entryIndex, e, total-1, last, hasPosition, position)
	}
	if ord > last.Ord {
		e.rest = append(e.rest, fts.Posting{Ord: ord, Count: 1, Seq: uint32(ord)})
		if len(idx.entryPositions(entryIndex)) > 0 || hasPosition {
			idx.growPositions(entryIndex, total)
			var positions []uint32
			if hasPosition {
				positions = []uint32{position}
			}
			idx.positions[entryIndex] = append(idx.positions[entryIndex], positions)
		}
		return nil
	}

	postingIndex := sort.Search(total, func(i int) bool {
		return idx.postingAt(e, i).Ord >= ord
	})
	if postingIndex < total {
		posting := idx.postingAt(e, postingIndex)
		if posting.Ord == ord {
			return idx.incrementPosting(entryIndex, e, postingIndex, posting, hasPosition, position)
		}
	}

	posting := fts.Posting{Ord: ord, Count: 1, Seq: uint32(ord)}
	if postingIndex == 0 {
		e.rest = slices.Insert(e.rest, 0, e.first)
		e.first = posting
	} else {
		e.rest = slices.Insert(e.rest, postingIndex-1, posting)
	}
	if len(idx.entryPositions(entryIndex)) > 0 || hasPosition {
		idx.growPositions(entryIndex, total)
		var positions []uint32
		if hasPosition {
			positions = []uint32{position}
		}
		idx.positions[entryIndex] = slices.Insert(idx.positions[entryIndex], postingIndex, positions)
	}
	return nil
}

func (idx *Index) postingAt(e *entry, postingIndex int) *fts.Posting {
	if postingIndex == 0 {
		return &e.first
	}
	return &e.rest[postingIndex-1]
}

func (idx *Index) incrementPosting(entryIndex int32, e *entry, postingIndex int, posting *fts.Posting, hasPosition bool, position uint32) error {
	if posting.Count == math.MaxUint32 {
		return errors.New("flat: posting count overflow")
	}
	posting.Count++
	if hasPosition {
		idx.growPositions(entryIndex, len(e.rest)+1)
		positions := idx.positions[entryIndex][postingIndex]
		idx.positions[entryIndex][postingIndex] = insertPosition(positions, position)
	}
	return nil
}

func (idx *Index) entryPositions(entryIndex int32) [][]uint32 {
	if int(entryIndex) >= len(idx.positions) {
		return nil
	}
	return idx.positions[entryIndex]
}

func (idx *Index) growPositions(entryIndex int32, want int) {
	outerWant := int(entryIndex) + 1
	if len(idx.positions) < outerWant {
		idx.positions = append(idx.positions, make([][][]uint32, outerWant-len(idx.positions))...)
	}
	if len(idx.positions[entryIndex]) < want {
		idx.positions[entryIndex] = append(idx.positions[entryIndex], make([][]uint32, want-len(idx.positions[entryIndex]))...)
	}
}

func insertPosition(positions []uint32, position uint32) []uint32 {
	if len(positions) == 0 || positions[len(positions)-1] <= position {
		return append(positions, position)
	}
	i := sort.Search(len(positions), func(i int) bool { return positions[i] > position })
	return slices.Insert(positions, i, position)
}

func (idx *Index) postings(e *entry) []fts.Posting {
	out := make([]fts.Posting, 0, len(e.rest)+1)
	out = append(out, e.first)
	out = append(out, e.rest...)
	return out
}

// Search returns ord-sorted exact postings.
func (idx *Index) Search(token string) ([]fts.Posting, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.byHash == nil || token == "" {
		return nil, nil
	}
	entryIndex := idx.find(token, tokenHash(token))
	if entryIndex < 0 {
		return nil, nil
	}
	return idx.postings(&idx.entries[entryIndex]), nil
}

// SearchPositional returns exact-match documents with read-only position slices.
func (idx *Index) SearchPositional(token string) ([]fts.PositionalPosting, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.byHash == nil || token == "" {
		return nil, nil
	}
	entryIndex := idx.find(token, tokenHash(token))
	if entryIndex < 0 {
		return nil, nil
	}
	e := &idx.entries[entryIndex]
	positions := idx.entryPositions(entryIndex)
	out := make([]fts.PositionalPosting, len(e.rest)+1)
	for i := range out {
		out[i].Ord = idx.postingAt(e, i).Ord
		if i < len(positions) {
			out[i].Positions = positions[i]
		}
	}
	return out, nil
}

// SearchPrefix merges postings for all terms with the supplied prefix.
func (idx *Index) SearchPrefix(prefix string) ([]fts.Posting, error) {
	idx.lockTermOrder()
	defer idx.mu.RUnlock()

	start := sort.Search(len(idx.termOrder), func(i int) bool {
		return compareTokenString(idx.tokenBytes(idx.termOrder[i]), prefix) >= 0
	})
	merged := make(map[fts.DocOrd]fts.Posting)
	for _, entryIndex := range idx.termOrder[start:] {
		if !tokenHasPrefix(idx.tokenBytes(entryIndex), prefix) {
			break
		}
		e := &idx.entries[entryIndex]
		for postingIndex := 0; postingIndex <= len(e.rest); postingIndex++ {
			posting := *idx.postingAt(e, postingIndex)
			if current, ok := merged[posting.Ord]; ok {
				current.Count += posting.Count
				merged[posting.Ord] = current
			} else {
				merged[posting.Ord] = posting
			}
		}
	}
	out := make([]fts.Posting, 0, len(merged))
	for _, posting := range merged {
		out = append(out, posting)
	}
	slices.SortFunc(out, func(a, b fts.Posting) int {
		return cmp.Compare(a.Seq, b.Seq)
	})
	return out, nil
}

// lockTermOrder acquires a read lock after lazily building a compact
// lexicographic view. The final operation is always protected by a read lock,
// including the first call that had to rebuild the view.
func (idx *Index) lockTermOrder() {
	for {
		idx.mu.RLock()
		if len(idx.termOrder) == len(idx.entries) {
			return
		}
		idx.mu.RUnlock()

		idx.mu.Lock()
		if len(idx.termOrder) != len(idx.entries) {
			idx.termOrder = make([]int32, len(idx.entries))
			for i := range idx.termOrder {
				idx.termOrder[i] = int32(i)
			}
			slices.SortFunc(idx.termOrder, func(a, b int32) int {
				return bytes.Compare(idx.tokenBytes(a), idx.tokenBytes(b))
			})
		}
		idx.mu.Unlock()
	}
}

// ExportSegmentTerms implements segment.Source.
func (idx *Index) ExportSegmentTerms(yield func(segment.TermPostings) error) error {
	if yield == nil {
		return errors.New("flat: nil segment callback")
	}
	idx.lockTermOrder()
	defer idx.mu.RUnlock()
	for _, entryIndex := range idx.termOrder {
		e := &idx.entries[entryIndex]
		if err := yield(segment.TermPostings{
			Term:      string(idx.tokenBytes(entryIndex)),
			Postings:  idx.postings(e),
			Positions: clonePositions(idx.entryPositions(entryIndex)),
		}); err != nil {
			return err
		}
	}
	return nil
}

// Serialize writes a versioned, checksummed mutable snapshot.
func (idx *Index) Serialize(w io.Writer) error {
	if w == nil {
		return errors.New("flat: serialize: nil writer")
	}
	idx.lockTermOrder()
	defer idx.mu.RUnlock()
	payload := make([]byte, 0, len(idx.arena)+len(idx.entries)*16)
	payload = append(payload, snapshotMagic...)
	payload = binary.AppendUvarint(payload, uint64(len(idx.termOrder)))
	for _, entryIndex := range idx.termOrder {
		e := &idx.entries[entryIndex]
		entryPositions := idx.entryPositions(entryIndex)
		token := idx.tokenBytes(entryIndex)
		postings := idx.postings(e)
		payload = binary.AppendUvarint(payload, uint64(len(token)))
		payload = append(payload, token...)
		payload = binary.AppendUvarint(payload, uint64(len(postings)))
		var previous fts.DocOrd
		for postingIndex, posting := range postings {
			payload = binary.AppendUvarint(payload, uint64(posting.Ord-previous))
			payload = binary.AppendUvarint(payload, uint64(posting.Count))
			var positions []uint32
			if postingIndex < len(entryPositions) {
				positions = entryPositions[postingIndex]
			}
			payload = binary.AppendUvarint(payload, uint64(len(positions)))
			for _, position := range positions {
				payload = binary.AppendUvarint(payload, uint64(position))
			}
			previous = posting.Ord
		}
	}
	payload = binary.LittleEndian.AppendUint32(payload, crc32.ChecksumIEEE(payload))
	if len(payload) > maxSnapshotBytes {
		return errors.New("flat: serialize: snapshot exceeds size limit")
	}
	n, err := w.Write(payload)
	if err != nil {
		return fmt.Errorf("flat: serialize: %w", err)
	}
	if n != len(payload) {
		return fmt.Errorf("flat: serialize: %w", io.ErrShortWrite)
	}
	return nil
}

// Load restores a mutable flat index snapshot.
func Load(r io.Reader) (fts.Index, error) {
	if r == nil {
		return nil, errors.New("flat: load: nil reader")
	}
	payload, err := io.ReadAll(io.LimitReader(r, maxSnapshotBytes+1))
	if err != nil {
		return nil, fmt.Errorf("flat: load: read: %w", err)
	}
	if len(payload) > maxSnapshotBytes {
		return nil, errors.New("flat: load: snapshot exceeds size limit")
	}
	if len(payload) < 9 {
		return nil, errors.New("flat: load: bad magic")
	}
	magic := string(payload[:4])
	if magic != snapshotMagic && magic != snapshotMagicV1 {
		return nil, errors.New("flat: load: bad magic")
	}
	hasPositions := magic == snapshotMagic
	body, sum := payload[:len(payload)-4], payload[len(payload)-4:]
	if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(sum) {
		return nil, errors.New("flat: load: checksum mismatch")
	}
	data := body[4:]
	take := func() (uint64, error) {
		value, n := binary.Uvarint(data)
		if n <= 0 {
			return 0, errors.New("invalid varint")
		}
		data = data[n:]
		return value, nil
	}
	termCount, err := take()
	if err != nil || termCount > maxTerms {
		return nil, errors.New("flat: load: invalid term count")
	}
	idx := New()
	for range termCount {
		tokenLen, err := take()
		if err != nil || tokenLen == 0 || tokenLen > maxTokenBytes || tokenLen > uint64(len(data)) {
			return nil, errors.New("flat: load: invalid token length")
		}
		token := string(data[:tokenLen])
		data = data[tokenLen:]
		postingCount, err := take()
		if err != nil || postingCount == 0 || postingCount > uint64(len(data)) {
			return nil, errors.New("flat: load: invalid posting count")
		}
		postings := make([]fts.Posting, 0, postingCount)
		var positions [][]uint32
		if hasPositions {
			positions = make([][]uint32, postingCount)
		}
		var ord fts.DocOrd
		for postingIndex := uint64(0); postingIndex < postingCount; postingIndex++ {
			delta, deltaErr := take()
			count, countErr := take()
			if deltaErr != nil || countErr != nil || (postingIndex > 0 && delta == 0) ||
				delta > uint64(math.MaxUint32-uint32(ord)) || count == 0 || count > math.MaxUint32 {
				return nil, errors.New("flat: load: invalid posting")
			}
			ord += fts.DocOrd(delta)
			postings = append(postings, fts.Posting{Ord: ord, Count: uint32(count), Seq: uint32(ord)})
			if hasPositions {
				positionCount, positionCountErr := take()
				if positionCountErr != nil || positionCount > uint64(len(data)) {
					return nil, errors.New("flat: load: invalid positions")
				}
				for range positionCount {
					position, positionErr := take()
					if positionErr != nil || position > math.MaxUint32 {
						return nil, errors.New("flat: load: invalid position")
					}
					current := positions[postingIndex]
					if len(current) > 0 && uint32(position) < current[len(current)-1] {
						return nil, errors.New("flat: load: unsorted positions")
					}
					positions[postingIndex] = append(current, uint32(position))
				}
			}
		}
		if !positionsPresent(positions) {
			positions = nil
		}
		if err := idx.addLoaded(token, postings, positions); err != nil {
			return nil, err
		}
	}
	if len(data) != 0 {
		return nil, errors.New("flat: load: trailing data")
	}
	return idx, nil
}

func positionsPresent(positions [][]uint32) bool {
	for i := range positions {
		if len(positions[i]) > 0 {
			return true
		}
	}
	return false
}

func clonePositions(positions [][]uint32) [][]uint32 {
	if len(positions) == 0 {
		return nil
	}
	out := make([][]uint32, len(positions))
	for i := range positions {
		out[i] = append([]uint32(nil), positions[i]...)
	}
	return out
}

func (idx *Index) addLoaded(token string, postings []fts.Posting, positions [][]uint32) error {
	if len(postings) == 0 || len(token) > maxTokenBytes || len(idx.entries) >= math.MaxInt32 ||
		uint64(len(idx.arena))+uint64(len(token)) > math.MaxUint32 || len(positions) > len(postings) {
		return errors.New("flat: load: invalid term")
	}
	hash := tokenHash(token)
	if idx.find(token, hash) >= 0 {
		return errors.New("flat: load: duplicate term")
	}
	off := uint32(len(idx.arena))
	idx.arena = append(idx.arena, token...)
	head, ok := idx.byHash[hash]
	if !ok {
		head = -1
	}
	idx.entries = append(idx.entries, entry{
		first: postings[0], rest: append([]fts.Posting(nil), postings[1:]...),
		tokenOff: off, tokenLen: uint32(len(token)), next: head,
	})
	if len(positions) > 0 {
		entryIndex := int32(len(idx.entries) - 1)
		idx.growPositions(entryIndex, len(positions))
		idx.positions[entryIndex] = clonePositions(positions)
	}
	idx.byHash[hash] = int32(len(idx.entries) - 1)
	return nil
}

var (
	_ fts.Index           = (*Index)(nil)
	_ fts.PrefixIndex     = (*Index)(nil)
	_ fts.PositionalIndex = (*Index)(nil)
	_ fts.Serializable    = (*Index)(nil)
	_ segment.Source      = (*Index)(nil)
)
