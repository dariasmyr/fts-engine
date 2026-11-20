package generationformat

// SSTA wire-format version. Semantic persistence predates production use, so
// old semantic state versions are intentionally not migrated.
const stateVersion = uint16(18)

const minimumRowBytes = wireUint64Size + 3*wireStringLengthPrefixSize + wireUint32Size + 2*wireUint64Size

const (
	stateConfigUint32FieldCount   = 15
	stateUint64MetadataFieldCount = 3
	stateIndexIDSize              = 16

	stateConfigFixedSize    = stateConfigUint32FieldCount*wireUint32Size + wireUint64Size + 2*wireUint8Size + wireUint16Size
	statePipelineFlagSize   = wireUint32Size
	statePipelineFixedSize  = 7*wireUint32Size + 2*wireUint64Size
	stateUint64MetadataSize = stateUint64MetadataFieldCount * wireUint64Size
	stateFixedEncodedSize   = wireHeaderSize + stateIndexIDSize + stateConfigFixedSize + statePipelineFlagSize + stateUint64MetadataSize + wireUint32Size + wireChecksumSize

	stateSegmentFixedSize = wireUint64Size + 2*wireUint32Size
	stateRowFixedSize     = wireUint64Size + wireUint32Size + 2*wireUint64Size
)

// IndexConfig is the format-owned representation of the persisted index
// contract. Its fields intentionally use only wire-layer value types.
type IndexConfig struct {
	Dimensions  int
	Metric      uint8
	Build       BuildConfig
	Operational OperationalLimits
}

type BuildConfig struct {
	MaxNeighbors   int
	EfConstruction int
	Seed           uint64
}

type OperationalLimits struct {
	Capacity       CapacityLimits
	Ingestion      IngestionLimits
	Search         SearchLimits
	SearchDefaults SearchConfig
}

type CapacityLimits struct {
	MaxLiveVectors  int
	MaxStaleVectors int
	MaxSegments     int
}

type IngestionLimits struct {
	MaxChunksPerDocument int
}

type SearchLimits struct {
	MaxResults         int
	MaxCandidateChunks int
	MaxChunksPerResult int
	MaxQueryChunks     int
	MaxEfSearch        int
	MaxVisitLimit      int
}

type SearchConfig struct {
	EfSearch   int
	VisitLimit int
}

type PipelineConfig struct {
	Model                   Model
	Chunking                ChunkingConfig
	EncodingBehaviorVersion uint32
	Operational             PipelineOperationalLimits
}

type Model struct {
	ID             string
	Revision       string
	Dimensions     int
	ContextTokens  int
	DocumentPrefix string
	QueryPrefix    string
}

type ChunkingConfig struct {
	TargetBytes  int
	MaxBytes     int
	OverlapBytes int
}

type PipelineOperationalLimits struct {
	Encoding EncodingLimits
}

type EncodingLimits struct {
	MaxFields         int
	MaxSourceBytes    uint64
	MaxEmbeddingBytes uint64
}

type Ref struct {
	ID        string
	DocID     string
	Field     string
	Ordinal   uint32
	StartByte uint64
	EndByte   uint64
}

type VectorRow struct {
	VectorID uint64
	Chunk    Ref
}

type SegmentState struct {
	ID            uint64
	Rows          []VectorRow
	LivenessWords []uint64
}

type IndexState struct {
	ID                   [stateIndexIDSize]byte
	Config               IndexConfig
	Pipeline             *PipelineConfig
	Revision             uint64
	MaxAllocatedVectorID uint64
	NextSegmentID        uint64
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
	encodePipelineConfig(e, value.Pipeline, limits.MaxStringBytes)
	e.writeUint64(value.Revision)
	e.writeUint64(value.MaxAllocatedVectorID)
	e.writeUint64(value.NextSegmentID)
	e.writeUint32(uint32(len(value.Segments)))
	for _, segment := range value.Segments {
		e.writeUint64(segment.ID)
		e.writeUint32(uint32(len(segment.Rows)))
		e.writeUint32(uint32(len(segment.LivenessWords)))
		for _, word := range segment.LivenessWords {
			e.writeUint64(word)
		}
		for _, row := range segment.Rows {
			e.writeUint64(row.VectorID)
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
	value.Pipeline = decodePipelineConfig(d)
	value.Revision = d.readUint64()
	value.MaxAllocatedVectorID = d.readUint64()
	value.NextSegmentID = d.readUint64()
	countValue := uint64(d.readUint32())
	if countValue > uint64(limits.MaxSegments) || countValue > uint64(d.remaining()/16) {
		return IndexState{}, ErrLimitExceeded
	}
	value.Segments = make([]SegmentState, int(countValue))
	totalRows := 0
	for i := range value.Segments {
		segment := SegmentState{ID: d.readUint64()}
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
		segment.Rows = make([]VectorRow, rowCount)
		for j := range segment.Rows {
			segment.Rows[j] = VectorRow{VectorID: d.readUint64(), Chunk: decodeRef(d)}
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

func encodeIndexConfig(e *encoder, c IndexConfig) {
	e.writeUint32(uint32(c.Dimensions))
	e.writeUint8(c.Metric)
	e.writeUint8(0)
	e.writeUint16(0)
	values := []int{
		c.Operational.Capacity.MaxLiveVectors,
		c.Operational.Capacity.MaxStaleVectors,
		c.Operational.Capacity.MaxSegments,
		c.Operational.Ingestion.MaxChunksPerDocument,
		c.Operational.Search.MaxResults,
		c.Operational.Search.MaxCandidateChunks,
		c.Operational.Search.MaxChunksPerResult,
		c.Operational.Search.MaxQueryChunks,
		c.Build.MaxNeighbors,
		c.Build.EfConstruction,
		c.Operational.SearchDefaults.EfSearch,
		c.Operational.Search.MaxEfSearch,
		c.Operational.SearchDefaults.VisitLimit,
		c.Operational.Search.MaxVisitLimit,
	}
	for _, value := range values {
		e.writeUint32(uint32(value))
	}
	e.writeUint64(c.Build.Seed)
}

func decodeIndexConfig(d *decoder) IndexConfig {
	c := IndexConfig{Dimensions: int(d.readUint32()), Metric: d.readUint8()}
	if d.readUint8() != 0 || d.readUint16() != 0 {
		d.decodeErr = ErrCorrupt
	}
	c.Operational.Capacity = CapacityLimits{
		MaxLiveVectors: int(d.readUint32()), MaxStaleVectors: int(d.readUint32()), MaxSegments: int(d.readUint32()),
	}
	c.Operational.Ingestion.MaxChunksPerDocument = int(d.readUint32())
	c.Operational.Search = SearchLimits{
		MaxResults: int(d.readUint32()), MaxCandidateChunks: int(d.readUint32()),
		MaxChunksPerResult: int(d.readUint32()), MaxQueryChunks: int(d.readUint32()),
	}
	c.Build = BuildConfig{MaxNeighbors: int(d.readUint32()), EfConstruction: int(d.readUint32())}
	c.Operational.SearchDefaults.EfSearch = int(d.readUint32())
	c.Operational.Search.MaxEfSearch = int(d.readUint32())
	c.Operational.SearchDefaults.VisitLimit = int(d.readUint32())
	c.Operational.Search.MaxVisitLimit = int(d.readUint32())
	c.Build.Seed = d.readUint64()
	return c
}

func encodePipelineConfig(e *encoder, c *PipelineConfig, maxString int) {
	if c == nil {
		e.writeUint32(0)
		return
	}
	e.writeUint32(1)
	e.writeUint32(uint32(c.Model.Dimensions))
	e.writeUint32(uint32(c.Model.ContextTokens))
	e.writeUint32(uint32(c.Chunking.TargetBytes))
	e.writeUint32(uint32(c.Chunking.MaxBytes))
	e.writeUint32(uint32(c.Chunking.OverlapBytes))
	e.writeUint32(uint32(c.Operational.Encoding.MaxFields))
	e.writeUint32(c.EncodingBehaviorVersion)
	e.writeUint64(c.Operational.Encoding.MaxSourceBytes)
	e.writeUint64(c.Operational.Encoding.MaxEmbeddingBytes)
	e.writeString(c.Model.ID, maxString)
	e.writeString(c.Model.Revision, maxString)
	e.writeString(c.Model.DocumentPrefix, maxString)
	e.writeString(c.Model.QueryPrefix, maxString)
}

func decodePipelineConfig(d *decoder) *PipelineConfig {
	present := d.readUint32()
	if present == 0 {
		return nil
	}
	if present != 1 {
		d.decodeErr = ErrCorrupt
		return nil
	}
	c := &PipelineConfig{}
	c.Model.Dimensions = int(d.readUint32())
	c.Model.ContextTokens = int(d.readUint32())
	c.Chunking.TargetBytes = int(d.readUint32())
	c.Chunking.MaxBytes = int(d.readUint32())
	c.Chunking.OverlapBytes = int(d.readUint32())
	c.Operational.Encoding.MaxFields = int(d.readUint32())
	c.EncodingBehaviorVersion = d.readUint32()
	c.Operational.Encoding.MaxSourceBytes = d.readUint64()
	c.Operational.Encoding.MaxEmbeddingBytes = d.readUint64()
	c.Model.ID = d.readString()
	c.Model.Revision = d.readString()
	c.Model.DocumentPrefix = d.readString()
	c.Model.QueryPrefix = d.readString()
	return c
}

func encodeRef(e *encoder, ref Ref, maxString int) {
	e.writeString(ref.ID, maxString)
	e.writeString(ref.DocID, maxString)
	e.writeString(ref.Field, maxString)
	e.writeUint32(ref.Ordinal)
	e.writeUint64(ref.StartByte)
	e.writeUint64(ref.EndByte)
}

func decodeRef(d *decoder) Ref {
	return Ref{ID: d.readString(), DocID: d.readString(), Field: d.readString(), Ordinal: d.readUint32(), StartByte: d.readUint64(), EndByte: d.readUint64()}
}
