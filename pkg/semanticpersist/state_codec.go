package semanticpersist

import (
	"math"
	"slices"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

const (
	stateMagic   = "SSTA"
	stateVersion = uint16(1)
)

func encodeState(checkpoint semantic.Checkpoint, limits Limits) ([]byte, fileReference, error) {
	if err := checkpoint.Validate(); err != nil {
		return nil, fileReference{}, err
	}
	if len(checkpoint.Documents) > limits.MaxDocuments || len(checkpoint.Refs) > limits.MaxVectors {
		return nil, fileReference{}, ErrLimitExceeded
	}
	if checkpoint.MaxK > limits.MaxK || checkpoint.MaxChunkCandidates > limits.MaxVectors ||
		uint64(checkpoint.MaxK) > math.MaxUint32 || uint64(checkpoint.MaxChunkCandidates) > math.MaxUint32 ||
		uint64(checkpoint.MaxChunksPerDocumentHit) > math.MaxUint32 {
		return nil, fileReference{}, ErrLimitExceeded
	}
	documents := cloneAndSortDocuments(checkpoint.Documents)
	refs := append([]semantic.RefRecord(nil), checkpoint.Refs...)
	slices.SortFunc(refs, func(a, b semantic.RefRecord) int {
		if a.VectorID < b.VectorID {
			return -1
		}
		if a.VectorID > b.VectorID {
			return 1
		}
		return 0
	})
	e := newEncoder(stateMagic, stateVersion, limits.MaxFileBytes)
	e.u32(uint32(checkpoint.Space.Dimensions))
	e.u8(uint8(checkpoint.Space.Metric))
	e.u8(uint8(checkpoint.Space.Normalization))
	e.u16(0)
	e.u32(checkpoint.Space.VectorFormatVersion)
	e.string(checkpoint.Space.ID, limits.MaxStringBytes)
	e.string(checkpoint.Chunking.ID, limits.MaxStringBytes)
	e.u64(uint64(checkpoint.HighWatermark))
	e.u32(uint32(checkpoint.MaxK))
	e.u32(uint32(checkpoint.MaxChunkCandidates))
	e.u32(uint32(checkpoint.MaxChunksPerDocumentHit))
	e.u32(uint32(len(refs)))
	for _, record := range refs {
		e.u64(uint64(record.VectorID))
		encodeRef(e, record.Ref, limits)
	}
	e.u32(uint32(len(documents)))
	for _, document := range documents {
		e.string(string(document.DocID), limits.MaxStringBytes)
		if len(document.VectorIDs) > limits.MaxChunksPerDocument {
			return nil, fileReference{}, ErrLimitExceeded
		}
		e.u32(uint32(len(document.VectorIDs)))
		for _, id := range document.VectorIDs {
			e.u64(uint64(id))
		}
	}
	return e.finish()
}

func decodeState(data []byte, limits Limits) (decodedState, error) {
	d, err := newDecoder(data, stateMagic, stateVersion, limits)
	if err != nil {
		return decodedState{}, err
	}
	dimensions := int(d.u32())
	metric := vector.Metric(d.u8())
	normalization := vector.Normalization(d.u8())
	if d.u16() != 0 {
		return decodedState{}, ErrCorrupt
	}
	formatVersion := d.u32()
	spaceID := d.string()
	chunkingID := d.string()
	highWatermark := semantic.VectorID(d.u64())
	maxK := int(d.u32())
	maxCandidates := int(d.u32())
	maxChunksHit := int(d.u32())
	if dimensions <= 0 || dimensions > limits.MaxDimensions || maxK <= 0 || maxK > limits.MaxK || maxCandidates < maxK || maxChunksHit <= 0 || formatVersion == 0 || spaceID == "" || chunkingID == "" {
		return decodedState{}, ErrCorrupt
	}
	space, err := vector.NewSpace(dimensions, metric)
	if err != nil || space.Normalization() != normalization {
		return decodedState{}, ErrCorrupt
	}
	refCount := int(d.u32())
	if refCount < 0 || refCount > limits.MaxVectors || refCount > d.remaining()/40 {
		return decodedState{}, ErrLimitExceeded
	}
	refs := make([]semantic.RefRecord, refCount)
	for i := range refs {
		refs[i] = semantic.RefRecord{VectorID: semantic.VectorID(d.u64()), Ref: decodeRef(d)}
		if d.err != nil {
			return decodedState{}, d.err
		}
		if i > 0 && refs[i-1].VectorID >= refs[i].VectorID {
			return decodedState{}, ErrCorrupt
		}
	}
	documentCount := int(d.u32())
	if documentCount < 0 || documentCount > limits.MaxDocuments || documentCount > d.remaining()/8 {
		return decodedState{}, ErrLimitExceeded
	}
	documents := make([]semantic.DocumentRecord, documentCount)
	for i := range documents {
		docID := fts.DocID(d.string())
		count := int(d.u32())
		if count < 0 || count > limits.MaxChunksPerDocument || count > d.remaining()/8 {
			return decodedState{}, ErrLimitExceeded
		}
		ids := make([]semantic.VectorID, count)
		for j := range ids {
			ids[j] = semantic.VectorID(d.u64())
			if j > 0 && ids[j-1] >= ids[j] {
				return decodedState{}, ErrCorrupt
			}
		}
		documents[i] = semantic.DocumentRecord{DocID: docID, VectorIDs: ids}
		if i > 0 && compareDocIDs(documents[i-1].DocID, docID) >= 0 {
			return decodedState{}, ErrCorrupt
		}
	}
	if err := d.done(); err != nil {
		return decodedState{}, err
	}
	return decodedState{
		Space:    semantic.SpaceDescriptor{ID: spaceID, Dimensions: dimensions, Metric: metric, Normalization: normalization, VectorFormatVersion: formatVersion},
		Chunking: semantic.ChunkingDescriptor{ID: chunkingID}, HighWatermark: highWatermark,
		Documents: documents, Refs: refs, MaxK: maxK, MaxChunkCandidates: maxCandidates, MaxChunksPerDocumentHit: maxChunksHit,
	}, nil
}

func encodeRef(e *encoder, ref chunk.Ref, limits Limits) {
	e.string(string(ref.ID), limits.MaxStringBytes)
	e.string(string(ref.DocID), limits.MaxStringBytes)
	e.string(ref.Field, limits.MaxStringBytes)
	e.u32(ref.Ordinal)
	e.u64(ref.StartByte)
	e.u64(ref.EndByte)
}

func decodeRef(d *decoder) chunk.Ref {
	return chunk.Ref{
		ID: chunk.ID(d.string()), DocID: fts.DocID(d.string()), Field: d.string(), Ordinal: d.u32(), StartByte: d.u64(), EndByte: d.u64(),
	}
}

func cloneAndSortDocuments(input []semantic.DocumentRecord) []semantic.DocumentRecord {
	documents := make([]semantic.DocumentRecord, len(input))
	for i, document := range input {
		ids := append([]semantic.VectorID(nil), document.VectorIDs...)
		slices.Sort(ids)
		documents[i] = semantic.DocumentRecord{DocID: document.DocID, VectorIDs: ids}
	}
	slices.SortFunc(documents, func(a, b semantic.DocumentRecord) int {
		return compareDocIDs(a.DocID, b.DocID)
	})
	return documents
}

func compareDocIDs(a, b fts.DocID) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
