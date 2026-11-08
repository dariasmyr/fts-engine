package generationformat

import (
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// SSTA wire-format version.
const stateVersion = uint16(11)

// Conservative decoder bound before reading a variable-size row.
const minimumRowBytes = wireUint64Size + 3*wireStringLengthPrefixSize + wireUint32Size + 2*wireUint64Size

const (
	// Seven index limits and six HNSW integer settings.
	stateConfigUint32FieldCount = 13
	// Revision, max allocated vector ID, and next segment ID.
	stateUint64MetadataFieldCount = 3
	stateIndexIDSize              = 16

	// Compatibility version, dimensions, metric, normalization, reserved flags,
	// and vector format version.
	stateEmbeddingFixedSize = 2*wireUint32Size + 2*wireUint8Size + wireUint16Size + wireUint32Size
	// Chunking version; schema strings are dynamic.
	stateChunkingFixedSize = wireUint32Size
	// Integer config fields and HNSW seed.
	stateConfigFixedSize = stateConfigUint32FieldCount*wireUint32Size + wireUint64Size
	// Revision and allocation metadata.
	stateUint64MetadataSize = stateUint64MetadataFieldCount * wireUint64Size
	// File-level fixed fields, segment count, and CRC32.
	stateFixedEncodedSize = wireHeaderSize + stateIndexIDSize + stateEmbeddingFixedSize + stateChunkingFixedSize + stateConfigFixedSize + stateUint64MetadataSize + wireUint32Size + wireChecksumSize

	// Component ID, row count, and liveness-word count.
	stateSegmentFixedSize = wireUint64Size + 2*wireUint32Size
	// Vector ID, ordinal, and chunk byte range.
	stateRowFixedSize = wireUint64Size + wireUint32Size + 2*wireUint64Size
)

type SegmentState struct {
	ID            semantic.SegmentID
	Rows          []semantic.VectorRow
	LivenessWords []uint64
}

// IndexState is the decoded SSTA payload required to resume semantic writes.
type IndexState struct {
	ID                   semantic.IndexID
	Config               semantic.IndexConfig
	Revision             semantic.Revision
	MaxAllocatedVectorID semantic.VectorID
	NextSegmentID        semantic.SegmentID
	Segments             []SegmentState
}

func EncodeState(value IndexState, limits StateLimits) ([]byte, FileRef, error) {
	if err := limits.validate(); err != nil {
		return nil, FileRef{}, err
	}
	expectedSize, err := validateStateAndEncodedSize(value, limits)
	if err != nil {
		return nil, FileRef{}, err
	}
	c := value.Config
	space, _ := c.Schema.Embedding.VectorSpace()
	e := newEncoder("SSTA", stateVersion, expectedSize, limits.FileLimits)
	e.writeBytes(value.ID[:])
	e.writeUint32(c.Schema.Embedding.CompatibilityVersion)
	e.writeUint32(uint32(c.Schema.Embedding.Dimensions))
	e.writeUint8(uint8(c.Schema.Embedding.Metric))
	e.writeUint8(uint8(space.Normalization()))
	e.writeUint16(0)
	e.writeUint32(c.Schema.Embedding.VectorFormatVersion)
	e.writeString(c.Schema.Embedding.ModelID, limits.MaxStringBytes)
	e.writeString(c.Schema.Embedding.ModelRevision, limits.MaxStringBytes)
	e.writeString(c.Schema.Embedding.PipelineFingerprint, limits.MaxStringBytes)
	e.writeString(c.Schema.Chunking.ID, limits.MaxStringBytes)
	e.writeUint32(c.Schema.Chunking.Version)
	e.writeString(c.Schema.Chunking.Fingerprint, limits.MaxStringBytes)
	encodeConfig(e, c)
	e.writeUint64(uint64(value.Revision))
	e.writeUint64(uint64(value.MaxAllocatedVectorID))
	e.writeUint64(uint64(value.NextSegmentID))
	e.writeUint32(uint32(len(value.Segments)))
	for _, segment := range value.Segments {
		e.writeUint64(uint64(segment.ID))
		e.writeUint32(uint32(len(segment.Rows)))
		e.writeUint32(uint32(len(segment.LivenessWords)))
		for _, word := range segment.LivenessWords {
			e.writeUint64(word)
		}
		for _, row := range segment.Rows {
			e.writeUint64(uint64(row.VectorID))
			encodeRef(e, row.Chunk, limits.MaxStringBytes)
		}
	}
	return e.finish()
}

func DecodeState(data []byte, limits StateLimits) (IndexState, error) {
	if err := limits.validate(); err != nil {
		return IndexState{}, err
	}
	d, err := newDecoder(data, "SSTA", stateVersion, limits.FileLimits)
	if err != nil {
		return IndexState{}, err
	}
	value := IndexState{}
	copy(value.ID[:], d.readBytes(stateIndexIDSize))
	compatibilityVersion := d.readUint32()
	if compatibilityVersion != semantic.CompatibilityFormatVersion {
		return IndexState{}, ErrUnsupportedVersion
	}
	dimensions := int(d.readUint32())
	metric := vector.Metric(d.readUint8())
	normalization := vector.Normalization(d.readUint8())
	if d.readUint16() != 0 {
		return IndexState{}, ErrCorrupt
	}
	formatVersion := d.readUint32()
	embedding := semantic.EmbeddingSchema{
		CompatibilityVersion: compatibilityVersion,
		Dimensions:           dimensions,
		Metric:               metric,
		VectorFormatVersion:  formatVersion,
	}
	embedding.ModelID, embedding.ModelRevision, embedding.PipelineFingerprint = d.readString(), d.readString(), d.readString()
	chunking := semantic.ChunkingSchema{ID: d.readString(), Version: d.readUint32(), Fingerprint: d.readString()}
	config := decodeConfig(d, embedding, chunking)
	value.Config = config
	value.Revision = semantic.Revision(d.readUint64())
	value.MaxAllocatedVectorID = semantic.VectorID(d.readUint64())
	value.NextSegmentID = semantic.SegmentID(d.readUint64())
	countValue := uint64(d.readUint32())
	if countValue > uint64(limits.MaxSegments) || countValue > uint64(d.remaining()/16) {
		return IndexState{}, ErrLimitExceeded
	}
	count := int(countValue)
	value.Segments = make([]SegmentState, count)
	totalRows := 0
	for i := range value.Segments {
		segment := SegmentState{ID: semantic.SegmentID(d.readUint64())}
		rowCountValue, wordCountValue := uint64(d.readUint32()), uint64(d.readUint32())
		if rowCountValue > uint64(limits.MaxVectors-totalRows) || wordCountValue != (rowCountValue+63)/64 || wordCountValue > uint64(d.remaining()/8) {
			return IndexState{}, ErrLimitExceeded
		}
		rowCount, wordCount := int(rowCountValue), int(wordCountValue)
		segment.LivenessWords = make([]uint64, wordCount)
		for j := range segment.LivenessWords {
			segment.LivenessWords[j] = d.readUint64()
		}
		if rowCount > d.remaining()/minimumRowBytes {
			return IndexState{}, ErrLimitExceeded
		}
		segment.Rows = make([]semantic.VectorRow, rowCount)
		for j := range segment.Rows {
			segment.Rows[j] = semantic.VectorRow{VectorID: semantic.VectorID(d.readUint64()), Chunk: decodeRef(d)}
			if err := d.err(); err != nil {
				return IndexState{}, err
			}
		}
		totalRows += rowCount
		value.Segments[i] = segment
	}
	if err := d.done(); err != nil {
		return IndexState{}, err
	}
	space, err := vector.NewSpace(dimensions, metric)
	if err != nil || space.Normalization() != normalization {
		return IndexState{}, ErrCorrupt
	}
	if _, err := validateStateAndEncodedSize(value, limits); err != nil {
		return IndexState{}, err
	}
	return value, nil
}

func encodeConfig(e *encoder, c semantic.IndexConfig) {
	values := []int{
		c.Limits.MaxLiveVectors,
		c.Limits.MaxStaleVectors,
		c.Limits.MaxSegments,
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
		e.writeUint32(uint32(value))
	}
	e.writeUint64(c.HNSW.Seed)
}

func decodeConfig(d *decoder, embedding semantic.EmbeddingSchema, chunking semantic.ChunkingSchema) semantic.IndexConfig {
	schema := semantic.Schema{Embedding: embedding, Chunking: chunking}
	c := semantic.IndexConfig{Schema: schema}
	c.Limits = semantic.Limits{
		MaxLiveVectors:          int(d.readUint32()),
		MaxStaleVectors:         int(d.readUint32()),
		MaxSegments:             int(d.readUint32()),
		MaxChunksPerDocument:    int(d.readUint32()),
		MaxDocumentsPerSearch:   int(d.readUint32()),
		MaxChunkCandidates:      int(d.readUint32()),
		MaxChunksPerDocumentHit: int(d.readUint32()),
	}
	c.HNSW = semantic.HNSWTuning{
		MaxNeighbors:      int(d.readUint32()),
		EfConstruction:    int(d.readUint32()),
		DefaultEfSearch:   int(d.readUint32()),
		MaxEfSearch:       int(d.readUint32()),
		DefaultVisitLimit: int(d.readUint32()),
		MaxVisitLimit:     int(d.readUint32()),
		Seed:              d.readUint64(),
	}
	return c
}

func encodeRef(e *encoder, ref semantic.Ref, maxString int) {
	e.writeString(string(ref.ID), maxString)
	e.writeString(string(ref.DocID), maxString)
	e.writeString(ref.Field, maxString)
	e.writeUint32(ref.Ordinal)
	e.writeUint64(ref.StartByte)
	e.writeUint64(ref.EndByte)
}

func decodeRef(d *decoder) semantic.Ref {
	return semantic.Ref{ID: semantic.ChunkID(d.readString()), DocID: fts.DocID(d.readString()), Field: d.readString(), Ordinal: d.readUint32(), StartByte: d.readUint64(), EndByte: d.readUint64()}
}
