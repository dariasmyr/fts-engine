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
	stateVersion = uint16(3)
)

func encodeState(snapshot semantic.Snapshot, limits Limits) ([]byte, fileReference, error) {
	if err := snapshot.Validate(); err != nil {
		return nil, fileReference{}, err
	}
	if len(snapshot.Rows) > limits.MaxVectors {
		return nil, fileReference{}, ErrLimitExceeded
	}
	if snapshot.MaxK > limits.MaxK || snapshot.MaxChunkCandidates > limits.MaxVectors ||
		uint64(snapshot.MaxK) > math.MaxUint32 || uint64(snapshot.MaxChunkCandidates) > math.MaxUint32 ||
		uint64(snapshot.MaxChunksPerDocumentHit) > math.MaxUint32 {
		return nil, fileReference{}, ErrLimitExceeded
	}
	e := newEncoder(stateMagic, stateVersion, limits.MaxFileBytes)
	e.u32(uint32(snapshot.Space.Dimensions))
	e.u8(uint8(snapshot.Space.Metric))
	e.u8(uint8(snapshot.Space.Normalization))
	e.u16(0)
	e.u32(snapshot.Space.VectorFormatVersion)
	e.string(snapshot.Space.ID, limits.MaxStringBytes)
	e.string(snapshot.Space.ModelVersion, limits.MaxStringBytes)
	e.string(snapshot.Space.Fingerprint, limits.MaxStringBytes)
	e.string(snapshot.Chunking.ID, limits.MaxStringBytes)
	e.u32(snapshot.Chunking.Version)
	e.string(snapshot.Chunking.Fingerprint, limits.MaxStringBytes)
	e.u64(uint64(snapshot.MaxAllocatedVectorID))
	e.u32(uint32(snapshot.MaxK))
	e.u32(uint32(snapshot.MaxChunkCandidates))
	e.u32(uint32(snapshot.MaxChunksPerDocumentHit))
	e.u32(uint32(len(snapshot.Rows)))
	for _, record := range snapshot.Rows {
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
	spaceID := d.string()
	modelVersion := d.string()
	spaceFingerprint := d.string()
	chunkingID := d.string()
	chunkingVersion := d.u32()
	chunkingFingerprint := d.string()
	maxAllocatedVectorID := semantic.VectorID(d.u64())
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
		Space:    semantic.SpaceDescriptor{ID: spaceID, ModelVersion: modelVersion, Fingerprint: spaceFingerprint, Dimensions: dimensions, Metric: metric, Normalization: normalization, VectorFormatVersion: formatVersion},
		Chunking: semantic.ChunkingDescriptor{ID: chunkingID, Version: chunkingVersion, Fingerprint: chunkingFingerprint}, MaxAllocatedVectorID: maxAllocatedVectorID,
		Rows: rows, MaxK: maxK, MaxChunkCandidates: maxCandidates, MaxChunksPerDocumentHit: maxChunksHit,
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
