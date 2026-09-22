package semanticpersist

import (
	"math"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

const (
	stateMagic   = "SSTA"
	stateVersion = uint16(6)
)

func encodeState(sealed SealedSegment, limits Limits) ([]byte, fileReference, error) {
	if err := validateSealedSegment(sealed, limits); err != nil {
		return nil, fileReference{}, err
	}
	metadata := sealed.Segment.Metadata()
	rows := sealed.Segment.Rows()
	if len(rows) > limits.MaxVectors {
		return nil, fileReference{}, ErrLimitExceeded
	}
	if sealed.MaxK > limits.MaxK || sealed.MaxChunkCandidates > limits.MaxVectors ||
		uint64(sealed.MaxK) > math.MaxUint32 || uint64(sealed.MaxChunkCandidates) > math.MaxUint32 ||
		uint64(sealed.MaxChunksPerDocumentHit) > math.MaxUint32 {
		return nil, fileReference{}, ErrLimitExceeded
	}
	e := newEncoder(stateMagic, stateVersion, limits.MaxFileBytes)
	space, err := vector.NewSpace(metadata.Embedding.Vector.Dimensions, metadata.Embedding.Vector.Metric)
	if err != nil {
		return nil, fileReference{}, err
	}
	e.u32(uint32(metadata.Embedding.Vector.Dimensions))
	e.u8(uint8(metadata.Embedding.Vector.Metric))
	e.u8(uint8(space.Normalization()))
	e.u16(0)
	e.u32(metadata.Embedding.Vector.VectorFormatVersion)
	e.string(metadata.Embedding.ProviderID, limits.MaxStringBytes)
	e.string(metadata.Embedding.ModelID, limits.MaxStringBytes)
	e.string(metadata.Embedding.ModelVersion, limits.MaxStringBytes)
	e.string(metadata.Embedding.PipelineFingerprint, limits.MaxStringBytes)
	e.string(metadata.Chunking.ID, limits.MaxStringBytes)
	e.u32(metadata.Chunking.Version)
	e.string(metadata.Chunking.Fingerprint, limits.MaxStringBytes)
	e.u64(uint64(sealed.MaxAllocatedVectorID))
	e.u32(uint32(sealed.MaxK))
	e.u32(uint32(sealed.MaxChunkCandidates))
	e.u32(uint32(sealed.MaxChunksPerDocumentHit))
	e.u64(uint64(sealed.Segment.ComponentID()))
	e.u32(uint32(len(rows)))
	for _, record := range rows {
		e.u64(uint64(record.VectorID))
		encodeRef(e, record.Chunk, limits)
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
	providerID := d.string()
	modelID := d.string()
	modelVersion := d.string()
	pipelineFingerprint := d.string()
	chunkingID := d.string()
	chunkingVersion := d.u32()
	chunkingFingerprint := d.string()
	maxAllocatedVectorID := semantic.VectorID(d.u64())
	maxK := int(d.u32())
	maxCandidates := int(d.u32())
	maxChunksHit := int(d.u32())
	componentID := semantic.ComponentID(d.u64())
	if dimensions <= 0 || dimensions > limits.MaxDimensions || maxK <= 0 || maxK > limits.MaxK || maxCandidates < maxK || maxChunksHit <= 0 || formatVersion == 0 || providerID == "" || modelID == "" || modelVersion == "" || pipelineFingerprint == "" || chunkingID == "" || componentID == 0 {
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
	rows := make([]semantic.VectorRow, refCount)
	documentChunks := make(map[fts.DocID]int)
	for i := range rows {
		rows[i] = semantic.VectorRow{VectorID: semantic.VectorID(d.u64()), Chunk: decodeRef(d)}
		if d.err != nil {
			return decodedState{}, d.err
		}
		if i > 0 && rows[i-1].VectorID >= rows[i].VectorID {
			return decodedState{}, ErrCorrupt
		}
		docID := rows[i].Chunk.DocID
		if _, exists := documentChunks[docID]; !exists && len(documentChunks) >= limits.MaxDocuments {
			return decodedState{}, ErrLimitExceeded
		}
		documentChunks[docID]++
		if documentChunks[docID] > limits.MaxChunksPerDocument {
			return decodedState{}, ErrLimitExceeded
		}
	}
	if err := d.done(); err != nil {
		return decodedState{}, err
	}
	return decodedState{
		Embedding: semantic.EmbeddingDescriptor{ProviderID: providerID, ModelID: modelID, ModelVersion: modelVersion, PipelineFingerprint: pipelineFingerprint, Vector: semantic.VectorSpec{Dimensions: dimensions, Metric: metric, VectorFormatVersion: formatVersion}},
		Chunking:  semantic.ChunkingDescriptor{ID: chunkingID, Version: chunkingVersion, Fingerprint: chunkingFingerprint}, MaxAllocatedVectorID: maxAllocatedVectorID,
		Rows: rows, ComponentID: componentID, MaxK: maxK, MaxChunkCandidates: maxCandidates, MaxChunksPerDocumentHit: maxChunksHit,
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
