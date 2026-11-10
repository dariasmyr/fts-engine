package generationformat

import (
	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// SSTA wire-format version. Semantic persistence predates production use, so
// old semantic state versions are intentionally not migrated.
const stateVersion = uint16(14)

const minimumRowBytes = wireUint64Size + 3*wireStringLengthPrefixSize + wireUint32Size + 2*wireUint64Size

const (
	stateConfigUint32FieldCount   = 13
	stateUint64MetadataFieldCount = 3
	stateIndexIDSize              = 16

	// Dimensions, context size, metric, chunking kind, reserved flags, and
	// built-in chunking values.
	stateSemanticFixedSize  = 5*wireUint32Size + 2*wireUint8Size + wireUint16Size
	stateConfigFixedSize    = stateConfigUint32FieldCount*wireUint32Size + wireUint64Size
	stateUint64MetadataSize = stateUint64MetadataFieldCount * wireUint64Size
	stateFixedEncodedSize   = wireHeaderSize + stateIndexIDSize + stateSemanticFixedSize + stateConfigFixedSize + stateUint64MetadataSize + wireUint32Size + wireChecksumSize

	stateSegmentFixedSize = wireUint64Size + 2*wireUint32Size
	stateRowFixedSize     = wireUint64Size + wireUint32Size + 2*wireUint64Size
)

type SegmentState struct {
	ID            semantic.SegmentID
	Rows          []semantic.VectorRow
	LivenessWords []uint64
}

type IndexState struct {
	ID                   semantic.IndexID
	Config               semantic.SemanticConfig
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
	e := newEncoder("SSTA", stateVersion, expectedSize, limits.FileLimits)
	e.writeBytes(value.ID[:])
	encodeSemanticConfig(e, value.Config, limits.MaxStringBytes)
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
	value.Config = decodeSemanticConfig(d)
	value.Revision = semantic.Revision(d.readUint64())
	value.MaxAllocatedVectorID = semantic.VectorID(d.readUint64())
	value.NextSegmentID = semantic.SegmentID(d.readUint64())
	countValue := uint64(d.readUint32())
	if countValue > uint64(limits.MaxSegments) || countValue > uint64(d.remaining()/16) {
		return IndexState{}, ErrLimitExceeded
	}
	value.Segments = make([]SegmentState, int(countValue))
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
	if _, err := validateStateAndEncodedSize(value, limits); err != nil {
		return IndexState{}, err
	}
	return value, nil
}

func encodeSemanticConfig(e *encoder, c semantic.SemanticConfig, maxString int) {
	e.writeUint32(uint32(c.Model.Dimensions))
	e.writeUint32(uint32(c.Model.ContextTokens))
	e.writeUint8(uint8(c.Index.Metric))
	e.writeUint8(uint8(c.Chunking.Kind))
	e.writeUint16(0)
	e.writeUint32(uint32(c.Chunking.Builtin.TargetBytes))
	e.writeUint32(uint32(c.Chunking.Builtin.MaxBytes))
	e.writeUint32(uint32(c.Chunking.Builtin.OverlapBytes))
	e.writeString(c.Model.ID, maxString)
	e.writeString(c.Model.Revision, maxString)
	e.writeString(c.Model.DocumentPrefix, maxString)
	e.writeString(c.Model.QueryPrefix, maxString)
	encodeIndexConfig(e, c.Index)
}

func decodeSemanticConfig(d *decoder) semantic.SemanticConfig {
	c := semantic.SemanticConfig{}
	c.Model.Dimensions = int(d.readUint32())
	c.Model.ContextTokens = int(d.readUint32())
	c.Index.Metric = vector.Metric(d.readUint8())
	c.Chunking.Kind = semantic.ChunkingKind(d.readUint8())
	if d.readUint16() != 0 {
		d.decodeErr = ErrCorrupt
	}
	c.Chunking.Builtin = chunk.Config{
		TargetBytes:  int(d.readUint32()),
		MaxBytes:     int(d.readUint32()),
		OverlapBytes: int(d.readUint32()),
	}
	c.Model.ID = d.readString()
	c.Model.Revision = d.readString()
	c.Model.DocumentPrefix = d.readString()
	c.Model.QueryPrefix = d.readString()
	c.Index = decodeIndexConfig(d, c.Index.Metric)
	return c
}

func encodeIndexConfig(e *encoder, c semantic.IndexConfig) {
	values := []int{
		c.Limits.MaxLiveVectors,
		c.Limits.MaxStaleVectors,
		c.Limits.MaxSegments,
		c.Limits.MaxChunksPerDocument,
		c.Search.MaxResults,
		c.Search.MaxCandidateChunks,
		c.Search.MaxChunksPerResult,
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

func decodeIndexConfig(d *decoder, metric vector.Metric) semantic.IndexConfig {
	c := semantic.IndexConfig{Metric: metric}
	c.Limits = semantic.LimitsConfig{
		MaxLiveVectors:       int(d.readUint32()),
		MaxStaleVectors:      int(d.readUint32()),
		MaxSegments:          int(d.readUint32()),
		MaxChunksPerDocument: int(d.readUint32()),
	}
	c.Search = semantic.SearchConfig{
		MaxResults:         int(d.readUint32()),
		MaxCandidateChunks: int(d.readUint32()),
		MaxChunksPerResult: int(d.readUint32()),
	}
	c.HNSW = semantic.HNSWConfig{
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
