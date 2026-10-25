package semantic

import (
	"math"
	"sort"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

type workingState struct {
	revision Revision
	pending  pendingBatch

	documents map[fts.DocID]documentVectors
	locations map[VectorID]vectorLocation

	liveVectorCount      int
	maxAllocatedVectorID VectorID
	nextComponentID      SegmentID
}

type pendingBatch struct {
	additions []pendingVector
	removals  []VectorID
}

func (p pendingBatch) empty() bool {
	return len(p.additions) == 0 && len(p.removals) == 0
}

func (p *pendingBatch) reset() {
	p.additions = nil
	p.removals = nil
}

type pendingVector struct {
	row    VectorRow
	vector []float32
}

type vectorLocation struct {
	component SegmentID
	ordinal   vector.Ordinal
}

// documentVectors is the contiguous VectorID range owned by the current
// document version. IDs remain stable across compaction; ordinals do not.
type documentVectors struct {
	first VectorID
	count int
}

func (v documentVectors) vectorID(index int) VectorID {
	return v.first + VectorID(index)
}

func (s *workingState) addDocument(docID fts.DocID, encoded []EncodedChunk, limits Limits) error {
	if _, exists := s.documents[docID]; exists {
		return ErrDocumentExists
	}
	return s.replaceVersion(docID, encoded, documentVectors{}, limits)
}

func (s *workingState) replaceDocument(docID fts.DocID, encoded []EncodedChunk, limits Limits) error {
	old, exists := s.documents[docID]
	if !exists {
		return ErrDocumentNotFound
	}
	return s.replaceVersion(docID, encoded, old, limits)
}

func (s *workingState) deleteDocument(docID fts.DocID) error {
	current, exists := s.documents[docID]
	if !exists {
		return ErrDocumentNotFound
	}
	if s.revision == Revision(math.MaxUint64) {
		return ErrRevisionExhausted
	}

	publishedIDs, err := s.supersede(current)
	if err != nil {
		return err
	}
	delete(s.documents, docID)
	s.liveVectorCount -= current.count
	s.pending.removals = append(s.pending.removals, publishedIDs...)
	s.revision++
	return nil
}

func (s *workingState) replaceVersion(docID fts.DocID, encoded []EncodedChunk, old documentVectors, limits Limits) error {
	nextLiveCount := s.liveVectorCount - old.count + len(encoded)
	if nextLiveCount > limits.MaxLiveVectors {
		return ErrCapacityExceeded
	}
	if s.revision == Revision(math.MaxUint64) {
		return ErrRevisionExhausted
	}

	version, err := s.allocateDocumentVectors(len(encoded))
	if err != nil {
		return err
	}
	publishedOldIDs, err := s.supersede(old)
	if err != nil {
		return err
	}

	for n, item := range encoded {
		id := version.vectorID(n)
		s.pending.additions = append(s.pending.additions, pendingVector{
			row:    VectorRow{VectorID: id, Chunk: item.Ref},
			vector: item.Vector,
		})
	}

	s.documents[docID] = version
	s.pending.removals = append(s.pending.removals, publishedOldIDs...)
	s.liveVectorCount = nextLiveCount
	s.maxAllocatedVectorID = version.vectorID(version.count - 1)
	s.revision++
	return nil
}

// supersede removes an unpublished document version from pending additions.
// For a committed version it returns IDs that must be marked stale on Flush.
func (s *workingState) supersede(version documentVectors) ([]VectorID, error) {
	if version.count == 0 {
		return nil, nil
	}

	_, published := s.locations[version.first]
	for n := 1; n < version.count; n++ {
		_, currentPublished := s.locations[version.vectorID(n)]
		if currentPublished != published {
			return nil, ErrInternalState
		}
	}
	if published {
		ids := make([]VectorID, version.count)
		for n := range ids {
			ids[n] = version.vectorID(n)
		}
		return ids, nil
	}

	additions := s.pending.additions
	start := sort.Search(len(additions), func(n int) bool {
		return additions[n].row.VectorID >= version.first
	})
	end := start + version.count
	if end > len(additions) {
		return nil, ErrInternalState
	}
	for n := range version.count {
		if additions[start+n].row.VectorID != version.vectorID(n) {
			return nil, ErrInternalState
		}
	}
	copy(additions[start:], additions[end:])
	clear(additions[len(additions)-version.count:])
	s.pending.additions = additions[:len(additions)-version.count]
	return nil, nil
}

func (s *workingState) allocateDocumentVectors(count int) (documentVectors, error) {
	if count <= 0 || uint64(count) > math.MaxUint64-uint64(s.maxAllocatedVectorID) {
		return documentVectors{}, ErrVectorIDExhausted
	}
	first := s.maxAllocatedVectorID + 1
	if first == 0 {
		return documentVectors{}, ErrVectorIDExhausted
	}
	return documentVectors{first: first, count: count}, nil
}
