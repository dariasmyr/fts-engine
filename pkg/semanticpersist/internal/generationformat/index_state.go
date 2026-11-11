package generationformat

import (
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"
)

// SSTA wire-format version. Semantic persistence predates production use, so
// old semantic state versions are intentionally not migrated.
const stateVersion = uint16(16)

const minimumRowBytes = wireUint64Size + 3*wireStringLengthPrefixSize + wireUint32Size + 2*wireUint64Size

const (
	stateConfigUint32FieldCount   = 15
	stateUint64MetadataFieldCount = 3
	stateIndexIDSize              = 16

	stateConfigFixedSize    = stateConfigUint32FieldCount*wireUint32Size + wireUint64Size + 2*wireUint8Size + wireUint16Size
	stateUint64MetadataSize = stateUint64MetadataFieldCount * wireUint64Size
	stateFixedEncodedSize   = wireHeaderSize + stateIndexIDSize + stateConfigFixedSize + stateUint64MetadataSize + wireUint32Size + wireChecksumSize

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
	Config               semantic.IndexConfig
	Revision             semantic.Revision
	MaxAllocatedVectorID semantic.VectorID
	NextSegmentID        semantic.SegmentID
	Segments             []SegmentState
}

func EncodeState(value IndexState, limits Config) ([]byte, FileRef, error) {
	if err := limits.validateState(); err != nil {
		return nil, FileRef{}, err
	}
	expectedSize, err := validateStateAndEncodedSize(value, limits)
	if err != nil {
		return nil, FileRef{}, err
	}
	e := newEncoder("SSTA", stateVersion, expectedSize, limits)
	e.writeBytes(value.ID[:])
	encodeIndexConfig(e, value.Config)
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

func DecodeState(data []byte, limits Config) (IndexState, error) {
	if err := limits.validateState(); err != nil {
		return IndexState{}, err
	}
	d, err := newDecoder(data, "SSTA", stateVersion, limits)
	if err != nil {
		return IndexState{}, err
	}
	value := IndexState{}
	copy(value.ID[:], d.readBytes(stateIndexIDSize))
	value.Config = decodeIndexConfig(d)
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

func encodeIndexConfig(e *encoder, c semantic.IndexConfig) {
	e.writeUint32(uint32(c.Dimensions))
	e.writeUint8(uint8(c.Metric))
	e.writeUint8(0)
	e.writeUint16(0)
	values := []int{
		c.Limits.MaxLiveVectors,
		c.Limits.MaxStaleVectors,
		c.Limits.MaxSegments,
		c.Limits.MaxChunksPerDocument,
		c.Search.MaxResults,
		c.Search.MaxCandidateChunks,
		c.Search.MaxChunksPerResult,
		c.Search.MaxQueryChunks,
		c.HNSW.Build.MaxNeighbors,
		c.HNSW.Build.EfConstruction,
		c.HNSW.Search.EfSearch,
		c.Search.MaxEfSearch,
		c.HNSW.Search.VisitLimit,
		c.Search.MaxVisitLimit,
	}
	for _, value := range values {
		e.writeUint32(uint32(value))
	}
	e.writeUint64(c.HNSW.Build.Seed)
}

func decodeIndexConfig(d *decoder) semantic.IndexConfig {
	c := semantic.IndexConfig{
		Dimensions: int(d.readUint32()),
		Metric:     vector.Metric(d.readUint8()),
	}
	if d.readUint8() != 0 || d.readUint16() != 0 {
		d.decodeErr = ErrCorrupt
	}
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
		MaxQueryChunks:     int(d.readUint32()),
	}
	c.HNSW = hnsw.Config{
		Build: hnsw.BuildConfig{
			MaxNeighbors:   int(d.readUint32()),
			EfConstruction: int(d.readUint32()),
		},
		Search: hnsw.SearchConfig{EfSearch: int(d.readUint32())},
	}
	c.Search.MaxEfSearch = int(d.readUint32())
	c.HNSW.Search.VisitLimit = int(d.readUint32())
	c.Search.MaxVisitLimit = int(d.readUint32())
	c.HNSW.Build.Seed = d.readUint64()
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
