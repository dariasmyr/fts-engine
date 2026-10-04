package format

import (
	"math"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

const stateVersion = uint16(8)
const minimumRowBytes = 32

type StateSegment struct {
	ComponentID   uint64
	Rows          []semantic.VectorRow
	LivenessWords []uint64
}

// State is the decoded SSTA payload required to resume semantic writes.
type State struct {
	Config               semantic.Config
	Revision             uint64
	MaxAllocatedVectorID uint64
	NextComponentID      uint64
	Segments             []StateSegment
}

func EncodeState(value State, limits Limits) ([]byte, FileReference, error) {
	if err := validateState(value, limits); err != nil {
		return nil, FileReference{}, err
	}
	c := value.Config
	calculator, _ := c.Embedding.Calculator()
	e := newEncoder("SSTA", stateVersion, limits)
	e.u32(uint32(c.Embedding.Dimensions))
	e.u8(uint8(c.Embedding.Metric))
	e.u8(uint8(calculator.Normalization()))
	e.u16(0)
	e.u32(c.Embedding.VectorFormatVersion)
	e.string(c.Embedding.ProviderID, limits.MaxStringBytes)
	e.string(c.Embedding.ModelID, limits.MaxStringBytes)
	e.string(c.Embedding.ModelVersion, limits.MaxStringBytes)
	e.string(c.Embedding.PipelineFingerprint, limits.MaxStringBytes)
	e.string(c.Chunking.ID, limits.MaxStringBytes)
	e.u32(c.Chunking.Version)
	e.string(c.Chunking.Fingerprint, limits.MaxStringBytes)
	encodeConfig(e, c)
	e.u64(value.Revision)
	e.u64(uint64(value.MaxAllocatedVectorID))
	e.u64(uint64(value.NextComponentID))
	e.u32(uint32(len(value.Segments)))
	for _, segment := range value.Segments {
		e.u64(uint64(segment.ComponentID))
		e.u32(uint32(len(segment.Rows)))
		e.u32(uint32(len(segment.LivenessWords)))
		for _, word := range segment.LivenessWords {
			e.u64(word)
		}
		for _, row := range segment.Rows {
			e.u64(uint64(row.VectorID))
			encodeRef(e, row.Chunk, limits.MaxStringBytes)
		}
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
	embedding := semantic.EmbeddingDescriptor{Dimensions: dimensions, Metric: metric, VectorFormatVersion: formatVersion}
	embedding.ProviderID, embedding.ModelID, embedding.ModelVersion, embedding.PipelineFingerprint = d.string(), d.string(), d.string(), d.string()
	chunking := semantic.ChunkingDescriptor{ID: d.string(), Version: d.u32(), Fingerprint: d.string()}
	config := decodeConfig(d, embedding, chunking)
	value := State{Config: config, Revision: d.u64(), MaxAllocatedVectorID: d.u64(), NextComponentID: d.u64()}
	countValue := uint64(d.u32())
	if countValue > uint64(limits.MaxVectors) || countValue > uint64(d.remaining()/16) {
		return State{}, ErrLimitExceeded
	}
	count := int(countValue)
	value.Segments = make([]StateSegment, count)
	totalRows := 0
	for i := range value.Segments {
		segment := StateSegment{ComponentID: d.u64()}
		rowCountValue, wordCountValue := uint64(d.u32()), uint64(d.u32())
		if rowCountValue > uint64(limits.MaxVectors-totalRows) || wordCountValue != (rowCountValue+63)/64 || wordCountValue > uint64(d.remaining()/8) {
			return State{}, ErrLimitExceeded
		}
		rowCount, wordCount := int(rowCountValue), int(wordCountValue)
		segment.LivenessWords = make([]uint64, wordCount)
		for j := range segment.LivenessWords {
			segment.LivenessWords[j] = d.u64()
		}
		if rowCount > d.remaining()/minimumRowBytes {
			return State{}, ErrLimitExceeded
		}
		segment.Rows = make([]semantic.VectorRow, rowCount)
		for j := range segment.Rows {
			segment.Rows[j] = semantic.VectorRow{VectorID: d.u64(), Chunk: decodeRef(d)}
			if err := d.err(); err != nil {
				return State{}, err
			}
		}
		totalRows += rowCount
		value.Segments[i] = segment
	}
	if err := d.done(); err != nil {
		return State{}, err
	}
	calculator, err := vector.NewCalculator(dimensions, metric)
	if err != nil || calculator.Normalization() != normalization {
		return State{}, ErrCorrupt
	}
	if err := validateState(value, limits); err != nil {
		return State{}, err
	}
	return value, nil
}

func encodeConfig(e *encoder, c semantic.Config) {
	values := []int{
		c.Limits.MaxLiveVectors,
		c.Limits.MaxChunksPerDocument,
		c.Limits.MaxDocumentsPerSearch,
		c.Limits.MaxChunkCandidates,
		c.Limits.MaxChunksPerDocumentHit,
		c.HNSW.MaxNeighbors,
		c.HNSW.EfConstruction,
		c.HNSW.DefaultEfSearch,
		c.HNSW.MaxEfSearch,
		c.HNSW.DefaultVisitLimit,
		c.HNSW.MaxVisitLimit,
	}
	for _, value := range values {
		e.u32(uint32(value))
	}
	e.u64(c.HNSW.Seed)
}

func decodeConfig(d *decoder, embedding semantic.EmbeddingDescriptor, chunking semantic.ChunkingDescriptor) semantic.Config {
	c := semantic.Config{Embedding: embedding, Chunking: chunking}
	c.Limits = semantic.Limits{
		MaxLiveVectors:          int(d.u32()),
		MaxChunksPerDocument:    int(d.u32()),
		MaxDocumentsPerSearch:   int(d.u32()),
		MaxChunkCandidates:      int(d.u32()),
		MaxChunksPerDocumentHit: int(d.u32()),
	}
	c.HNSW = semantic.HNSWTuning{
		MaxNeighbors:      int(d.u32()),
		EfConstruction:    int(d.u32()),
		DefaultEfSearch:   int(d.u32()),
		MaxEfSearch:       int(d.u32()),
		DefaultVisitLimit: int(d.u32()),
		MaxVisitLimit:     int(d.u32()),
		Seed:              d.u64(),
	}
	return c
}

func validateState(value State, limits Limits) error {
	if value.Config.Validate() != nil || value.NextComponentID <= 0 || len(value.Segments) > limits.MaxVectors {
		return ErrCorrupt
	}
	c := value.Config
	ints := []int{
		c.Limits.MaxLiveVectors,
		c.Limits.MaxChunksPerDocument,
		c.Limits.MaxDocumentsPerSearch,
		c.Limits.MaxChunkCandidates,
		c.Limits.MaxChunksPerDocumentHit,
		c.HNSW.MaxNeighbors,
		c.HNSW.EfConstruction,
		c.HNSW.DefaultEfSearch,
		c.HNSW.MaxEfSearch,
		c.HNSW.DefaultVisitLimit,
		c.HNSW.MaxVisitLimit,
	}
	for _, v := range ints {
		if v <= 0 {
			return ErrCorrupt
		}
		if uint64(v) > math.MaxUint32 {
			return ErrLimitExceeded
		}
	}
	vectorBytes := uint64(c.Limits.MaxLiveVectors) * uint64(c.Embedding.Dimensions) * 4
	if c.Embedding.Dimensions > limits.MaxDimensions || c.Limits.MaxLiveVectors > limits.MaxVectors ||
		c.Limits.MaxChunksPerDocument > limits.MaxChunksPerDocument ||
		c.Limits.MaxDocumentsPerSearch > limits.MaxK || c.Limits.MaxChunkCandidates > limits.MaxK ||
		vectorBytes > limits.MaxVectorBytes || c.HNSW.MaxEfSearch > limits.MaxEfSearch || c.HNSW.MaxVisitLimit > limits.MaxVisitLimit {
		return ErrLimitExceeded
	}
	totalRows := 0
	documents := make(map[fts.DocID]struct{})
	for _, segment := range value.Segments {
		if segment.ComponentID == 0 || len(segment.LivenessWords) != (len(segment.Rows)+63)/64 || len(segment.Rows) > limits.MaxVectors-totalRows {
			return ErrLimitExceeded
		}
		if len(segment.LivenessWords) > 0 && len(segment.Rows)%64 != 0 && segment.LivenessWords[len(segment.LivenessWords)-1]>>uint(len(segment.Rows)%64) != 0 {
			return ErrCorrupt
		}
		for i, row := range segment.Rows {
			if row.VectorID == 0 || i > 0 && segment.Rows[i-1].VectorID >= row.VectorID || row.Chunk.ID == "" || row.Chunk.DocID == "" || row.Chunk.Field == "" || row.Chunk.StartByte > row.Chunk.EndByte || value.MaxAllocatedVectorID < row.VectorID {
				return ErrCorrupt
			}
			if _, exists := documents[row.Chunk.DocID]; !exists {
				if len(documents) >= limits.MaxDocuments {
					return ErrLimitExceeded
				}
				documents[row.Chunk.DocID] = struct{}{}
			}
		}
		totalRows += len(segment.Rows)
	}
	return nil
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
