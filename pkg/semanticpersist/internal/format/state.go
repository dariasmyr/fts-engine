package format

import (
	"math"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

const stateVersion = uint16(6)

const minimumRowBytes = 32

// State is the decoded SSTA payload.
type State struct {
	Embedding               semantic.EmbeddingDescriptor
	Chunking                semantic.ChunkingDescriptor
	MaxAllocatedVectorID    semantic.VectorID
	ComponentID             semantic.ComponentID
	Rows                    []semantic.VectorRow
	MaxK                    int
	MaxChunkCandidates      int
	MaxChunksPerDocumentHit int
}

func EncodeState(value State, limits Limits) ([]byte, FileReference, error) {
	if len(value.Rows) > limits.MaxVectors || value.MaxK <= 0 || value.MaxK > limits.MaxK || value.MaxChunkCandidates < value.MaxK || value.MaxChunkCandidates > limits.MaxVectors || value.MaxChunksPerDocumentHit <= 0 ||
		uint64(value.MaxK) > math.MaxUint32 || uint64(value.MaxChunkCandidates) > math.MaxUint32 || uint64(value.MaxChunksPerDocumentHit) > math.MaxUint32 {
		return nil, FileReference{}, ErrLimitExceeded
	}
	if len(value.Rows) > 0 && value.MaxAllocatedVectorID < value.Rows[len(value.Rows)-1].VectorID {
		return nil, FileReference{}, ErrCorrupt
	}
	calculator, err := value.Embedding.Calculator()
	if err != nil {
		return nil, FileReference{}, err
	}
	e := newEncoder("SSTA", stateVersion, limits)
	e.u32(uint32(value.Embedding.Dimensions))
	e.u8(uint8(value.Embedding.Metric))
	e.u8(uint8(calculator.Normalization()))
	e.u16(0)
	e.u32(value.Embedding.VectorFormatVersion)
	e.string(value.Embedding.ProviderID, limits.MaxStringBytes)
	e.string(value.Embedding.ModelID, limits.MaxStringBytes)
	e.string(value.Embedding.ModelVersion, limits.MaxStringBytes)
	e.string(value.Embedding.PipelineFingerprint, limits.MaxStringBytes)
	e.string(value.Chunking.ID, limits.MaxStringBytes)
	e.u32(value.Chunking.Version)
	e.string(value.Chunking.Fingerprint, limits.MaxStringBytes)
	e.u64(uint64(value.MaxAllocatedVectorID))
	e.u32(uint32(value.MaxK))
	e.u32(uint32(value.MaxChunkCandidates))
	e.u32(uint32(value.MaxChunksPerDocumentHit))
	e.u64(uint64(value.ComponentID))
	e.u32(uint32(len(value.Rows)))
	for _, row := range value.Rows {
		e.u64(uint64(row.VectorID))
		encodeRef(e, row.Chunk, limits.MaxStringBytes)
	}
	return e.finish()
}

func DecodeState(data []byte, limits Limits) (State, error) {
	d, err := newDecoder(data, "SSTA", stateVersion, limits)
	if err != nil {
		return State{}, err
	}
	dimensions := int(d.u32())
	metric := vector.Metric(d.u8())
	normalization := vector.Normalization(d.u8())
	if d.u16() != 0 {
		return State{}, ErrCorrupt
	}
	formatVersion := d.u32()
	providerID, modelID, modelVersion, pipelineFingerprint := d.string(), d.string(), d.string(), d.string()
	chunkingID, chunkingVersion, chunkingFingerprint := d.string(), d.u32(), d.string()
	value := State{MaxAllocatedVectorID: semantic.VectorID(d.u64()), MaxK: int(d.u32()), MaxChunkCandidates: int(d.u32()), MaxChunksPerDocumentHit: int(d.u32()), ComponentID: semantic.ComponentID(d.u64())}
	if dimensions <= 0 || dimensions > limits.MaxDimensions || value.MaxK <= 0 || value.MaxK > limits.MaxK || value.MaxChunkCandidates < value.MaxK || value.MaxChunksPerDocumentHit <= 0 || formatVersion == 0 || providerID == "" || modelID == "" || modelVersion == "" || pipelineFingerprint == "" || chunkingID == "" || value.ComponentID == 0 {
		return State{}, ErrCorrupt
	}
	calculator, err := vector.NewCalculator(dimensions, metric)
	if err != nil || calculator.Normalization() != normalization {
		return State{}, ErrCorrupt
	}
	refCount := int(d.u32())
	if refCount > limits.MaxVectors || refCount > d.remaining()/minimumRowBytes {
		return State{}, ErrLimitExceeded
	}
	value.Rows = make([]semantic.VectorRow, refCount)
	documentChunks := make(map[fts.DocID]int)
	for i := range value.Rows {
		value.Rows[i] = semantic.VectorRow{VectorID: semantic.VectorID(d.u64()), Chunk: decodeRef(d)}
		if err := d.err(); err != nil {
			return State{}, err
		}
		if i > 0 && value.Rows[i-1].VectorID >= value.Rows[i].VectorID {
			return State{}, ErrCorrupt
		}
		docID := value.Rows[i].Chunk.DocID
		if _, exists := documentChunks[docID]; !exists && len(documentChunks) >= limits.MaxDocuments {
			return State{}, ErrLimitExceeded
		}
		documentChunks[docID]++
		if documentChunks[docID] > limits.MaxChunksPerDocument {
			return State{}, ErrLimitExceeded
		}
	}
	if len(value.Rows) > 0 && value.MaxAllocatedVectorID < value.Rows[len(value.Rows)-1].VectorID {
		return State{}, ErrCorrupt
	}
	if err := d.done(); err != nil {
		return State{}, err
	}
	value.Embedding = semantic.EmbeddingDescriptor{ProviderID: providerID, ModelID: modelID, ModelVersion: modelVersion, PipelineFingerprint: pipelineFingerprint, Dimensions: dimensions, Metric: metric, VectorFormatVersion: formatVersion}
	value.Chunking = semantic.ChunkingDescriptor{ID: chunkingID, Version: chunkingVersion, Fingerprint: chunkingFingerprint}
	return value, nil
}

func encodeRef(e *encoder, ref chunk.Ref, maxString int) {
	e.string(string(ref.ID), maxString)
	e.string(string(ref.DocID), maxString)
	e.string(ref.Field, maxString)
	e.u32(ref.Ordinal)
	e.u64(ref.StartByte)
	e.u64(ref.EndByte)
}

func decodeRef(d *decoder) chunk.Ref {
	return chunk.Ref{ID: chunk.ID(d.string()), DocID: fts.DocID(d.string()), Field: d.string(), Ordinal: d.u32(), StartByte: d.u64(), EndByte: d.u64()}
}
