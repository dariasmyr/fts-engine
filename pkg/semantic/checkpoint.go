package semantic

import (
	"context"
	"fmt"
	"slices"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	vectorflat "github.com/dariasmyr/fts-engine/pkg/vector/flat"
)

// Checkpoint is one coherent, immutable semantic generation input.
type Checkpoint struct {
	Space    SpaceDescriptor
	Chunking ChunkingDescriptor
	// MaxAllocatedVectorID preserves the monotonic allocator across generations.
	MaxAllocatedVectorID VectorID
	Segment              *vectorflat.Reader
	VectorIDs            []VectorID
	Live                 vector.BitSet
	Documents            []DocumentRecord
	Refs                 []RefRecord
	// DuplicateStatistics is nil when optional duplicate collection is disabled.
	DuplicateStatistics     *DuplicateStatistics
	MaxK                    int
	MaxChunkCandidates      int
	MaxChunksPerDocumentHit int
}

func (s *Service) Checkpoint() (Checkpoint, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	segment := s.head.Freeze()
	documents := make([]DocumentRecord, 0, len(s.currentByDoc))
	for docID, ids := range s.currentByDoc {
		documents = append(documents, DocumentRecord{DocID: docID, VectorIDs: append([]VectorID(nil), ids...)})
	}
	slices.SortFunc(documents, func(a, b DocumentRecord) int {
		if a.DocID < b.DocID {
			return -1
		}
		if a.DocID > b.DocID {
			return 1
		}
		return 0
	})
	refs := make([]RefRecord, 0, len(s.vectorIDs))
	for _, id := range s.vectorIDs {
		ref, ok := s.refByVector[id]
		if !ok {
			return Checkpoint{}, ErrInternalState
		}
		refs = append(refs, RefRecord{VectorID: id, Ref: ref})
	}
	var duplicateStats *DuplicateStatistics
	if s.config.CollectDuplicateStatistics {
		stats := duplicateStatistics(segment.ExactDuplicateStats(spaceNamespace(s.config.Space)))
		duplicateStats = &stats
	}
	checkpoint := Checkpoint{
		Space:                   s.config.Space,
		Chunking:                s.config.Chunking,
		MaxAllocatedVectorID:    s.maxAllocatedVectorID,
		Segment:                 segment,
		VectorIDs:               append([]VectorID(nil), s.vectorIDs...),
		Live:                    s.live,
		Documents:               documents,
		Refs:                    refs,
		DuplicateStatistics:     duplicateStats,
		MaxK:                    s.config.MaxK,
		MaxChunkCandidates:      s.config.MaxChunkCandidates,
		MaxChunksPerDocumentHit: s.config.MaxChunksPerDocumentHit,
	}
	if err := checkpoint.Validate(); err != nil {
		return Checkpoint{}, err
	}
	return checkpoint, nil
}

// BuildLiveOnly returns a new checkpoint rebuilt from live vectors. It does not
// change the source checkpoint. Stable VectorIDs and MaxAllocatedVectorID are
// preserved while local ordinals become dense.
func (c Checkpoint) BuildLiveOnly(ctx context.Context) (Checkpoint, error) {
	if ctx == nil {
		return Checkpoint{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return Checkpoint{}, err
	}
	if err := c.Validate(); err != nil {
		return Checkpoint{}, err
	}
	if c.Live.AllowedOrdinalCount() == c.Segment.Len() {
		if err := ctx.Err(); err != nil {
			return Checkpoint{}, err
		}
		return cloneCheckpoint(c), nil
	}

	segment, err := c.Segment.Compact(ctx, c.Live)
	if err != nil {
		return Checkpoint{}, err
	}
	refsByID := make(map[VectorID]chunk.Ref, len(c.Refs))
	for _, record := range c.Refs {
		refsByID[record.VectorID] = record.Ref
	}
	vectorIDs := make([]VectorID, 0, c.Live.AllowedOrdinalCount())
	refs := make([]RefRecord, 0, c.Live.AllowedOrdinalCount())
	for ordinal, id := range c.VectorIDs {
		if !c.Live.Allows(vector.Ordinal(ordinal)) {
			continue
		}
		vectorIDs = append(vectorIDs, id)
		refs = append(refs, RefRecord{VectorID: id, Ref: refsByID[id]})
	}
	compacted := Checkpoint{
		Space: c.Space, Chunking: c.Chunking, MaxAllocatedVectorID: c.MaxAllocatedVectorID,
		Segment: segment, VectorIDs: vectorIDs, Live: vector.NewFullBitSet(uint32(len(vectorIDs))),
		Documents: cloneDocumentRecords(c.Documents), Refs: refs,
		MaxK: c.MaxK, MaxChunkCandidates: c.MaxChunkCandidates,
		MaxChunksPerDocumentHit: c.MaxChunksPerDocumentHit,
	}
	if c.DuplicateStatistics != nil {
		stats := duplicateStatistics(segment.ExactDuplicateStats(spaceNamespace(c.Space)))
		compacted.DuplicateStatistics = &stats
	}
	if err := compacted.Validate(); err != nil {
		return Checkpoint{}, err
	}
	return compacted, nil
}

func (c Checkpoint) Validate() error {
	if c.Segment == nil || c.Space.ID == "" || c.Chunking.ID == "" || c.Space.VectorFormatVersion == 0 ||
		c.MaxK <= 0 || c.MaxChunkCandidates < c.MaxK || c.MaxChunksPerDocumentHit <= 0 ||
		c.Segment.MaxK() < max(c.MaxK, c.MaxChunkCandidates) {
		return ErrInvalidCheckpoint
	}
	space, err := vector.NewSpace(c.Space.Dimensions, c.Space.Metric)
	if err != nil || space.Normalization() != c.Space.Normalization || c.Segment.Dimensions() != c.Space.Dimensions ||
		c.Segment.Metric() != c.Space.Metric || c.Segment.Normalization() != c.Space.Normalization ||
		c.Segment.Len() != len(c.VectorIDs) || c.Live.TotalOrdinalCount() != uint32(c.Segment.Len()) {
		return ErrInvalidCheckpoint
	}
	refs := make(map[VectorID]RefRecord, len(c.Refs))
	ordinalByID := make(map[VectorID]vector.Ordinal, len(c.VectorIDs))
	var maxID VectorID
	for ordinal, id := range c.VectorIDs {
		if id == 0 {
			return ErrInvalidCheckpoint
		}
		if _, duplicate := ordinalByID[id]; duplicate {
			return ErrInvalidCheckpoint
		}
		ordinalByID[id] = vector.Ordinal(ordinal)
		maxID = max(maxID, id)
	}
	for _, record := range c.Refs {
		if record.VectorID == 0 || record.Ref.ID == "" || record.Ref.DocID == "" || record.Ref.Field == "" || record.Ref.StartByte > record.Ref.EndByte {
			return ErrInvalidCheckpoint
		}
		if _, exists := ordinalByID[record.VectorID]; !exists {
			return ErrInvalidCheckpoint
		}
		if _, duplicate := refs[record.VectorID]; duplicate {
			return ErrInvalidCheckpoint
		}
		refs[record.VectorID] = record
	}
	if len(refs) != len(c.VectorIDs) || c.MaxAllocatedVectorID < maxID {
		return ErrInvalidCheckpoint
	}
	documents := make(map[fts.DocID]struct{}, len(c.Documents))
	current := make(map[VectorID]struct{}, c.Live.AllowedOrdinalCount())
	for _, document := range c.Documents {
		if document.DocID == "" || len(document.VectorIDs) == 0 {
			return ErrInvalidCheckpoint
		}
		if _, duplicate := documents[document.DocID]; duplicate {
			return ErrInvalidCheckpoint
		}
		documents[document.DocID] = struct{}{}
		chunkIDs := make(map[string]struct{}, len(document.VectorIDs))
		for _, id := range document.VectorIDs {
			ordinal, exists := ordinalByID[id]
			record, hasRef := refs[id]
			if !exists || !hasRef || !c.Live.Allows(ordinal) || record.Ref.DocID != document.DocID {
				return ErrInvalidCheckpoint
			}
			if _, duplicate := current[id]; duplicate {
				return ErrInvalidCheckpoint
			}
			current[id] = struct{}{}
			chunkID := string(record.Ref.ID)
			if _, duplicate := chunkIDs[chunkID]; duplicate {
				return ErrInvalidCheckpoint
			}
			chunkIDs[chunkID] = struct{}{}
		}
	}
	if len(current) != c.Live.AllowedOrdinalCount() {
		return ErrInvalidCheckpoint
	}
	if c.DuplicateStatistics != nil {
		wantDuplicates := duplicateStatistics(c.Segment.ExactDuplicateStats(spaceNamespace(c.Space)))
		if *c.DuplicateStatistics != wantDuplicates {
			return fmt.Errorf("%w: duplicate statistics mismatch", ErrInvalidCheckpoint)
		}
	}
	return nil
}

func duplicateStatistics(stats vectorflat.DuplicateStats) DuplicateStatistics {
	return DuplicateStatistics{
		VectorRows: stats.VectorRows, UniqueVectors: stats.UniqueVectors, DuplicateRows: stats.DuplicateRows,
		DuplicateGroups: stats.DuplicateGroups, MaxFanOut: stats.MaxFanOut,
	}
}

func spaceNamespace(space SpaceDescriptor) string {
	return fmt.Sprintf("%s/%d/%d/%d/%d", space.ID, space.Dimensions, space.Metric, space.Normalization, space.VectorFormatVersion)
}
