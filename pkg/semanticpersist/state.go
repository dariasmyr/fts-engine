package semanticpersist

import (
	"math"
	"unicode/utf8"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// SSTA wire-format version.
const stateVersion = uint16(8)

// Conservative decoder bound before reading a variable-size row.
const minimumRowBytes = 32

const (
	// Five service limits and six HNSW integer settings.
	stateConfigUint32FieldCount = 11
	// Revision, max allocated vector ID, and next component ID.
	stateUint64MetadataFieldCount = 3

	// Dimensions, metric, normalization, reserved flags, and vector format version.
	stateEmbeddingFixedSize = wireUint32Size + 2*wireUint8Size + wireUint16Size + wireUint32Size
	// Chunking version; descriptor strings are dynamic.
	stateChunkingFixedSize = wireUint32Size
	// Integer config fields and HNSW seed.
	stateConfigFixedSize = stateConfigUint32FieldCount*wireUint32Size + wireUint64Size
	// Revision and allocation metadata.
	stateUint64MetadataSize = stateUint64MetadataFieldCount * wireUint64Size
	// File-level fixed fields, segment count, and CRC32.
	stateFixedEncodedSize = wireHeaderSize + stateEmbeddingFixedSize + stateChunkingFixedSize + stateConfigFixedSize + stateUint64MetadataSize + wireUint32Size + wireChecksumSize

	// Component ID, row count, and liveness-word count.
	stateSegmentFixedSize = wireUint64Size + 2*wireUint32Size
	// Vector ID, ordinal, and chunk byte range.
	stateRowFixedSize = wireUint64Size + wireUint32Size + 2*wireUint64Size
)

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
	expectedSize, err := validateStateAndSize(value, limits)
	if err != nil {
		return nil, FileReference{}, err
	}
	c := value.Config
	calculator, _ := c.Embedding.Calculator()
	e := newEncoder("SSTA", stateVersion, expectedSize, limits)
	e.writeUint32(uint32(c.Embedding.Dimensions))
	e.writeUint8(uint8(c.Embedding.Metric))
	e.writeUint8(uint8(calculator.Normalization()))
	e.writeUint16(0)
	e.writeUint32(c.Embedding.VectorFormatVersion)
	e.writeString(c.Embedding.ProviderID, limits.MaxStringBytes)
	e.writeString(c.Embedding.ModelID, limits.MaxStringBytes)
	e.writeString(c.Embedding.ModelVersion, limits.MaxStringBytes)
	e.writeString(c.Embedding.PipelineFingerprint, limits.MaxStringBytes)
	e.writeString(c.Chunking.ID, limits.MaxStringBytes)
	e.writeUint32(c.Chunking.Version)
	e.writeString(c.Chunking.Fingerprint, limits.MaxStringBytes)
	encodeConfig(e, c)
	e.writeUint64(value.Revision)
	e.writeUint64(uint64(value.MaxAllocatedVectorID))
	e.writeUint64(uint64(value.NextComponentID))
	e.writeUint32(uint32(len(value.Segments)))
	for _, segment := range value.Segments {
		e.writeUint64(uint64(segment.ComponentID))
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

func DecodeState(data []byte, limits Limits) (State, error) {
	d, err := newDecoder(data, "SSTA", stateVersion, limits)
	if err != nil {
		return State{}, err
	}
	dimensions := int(d.readUint32())
	metric := vector.Metric(d.readUint8())
	normalization := vector.Normalization(d.readUint8())
	if d.readUint16() != 0 {
		return State{}, ErrCorrupt
	}
	formatVersion := d.readUint32()
	embedding := semantic.EmbeddingDescriptor{Dimensions: dimensions, Metric: metric, VectorFormatVersion: formatVersion}
	embedding.ProviderID, embedding.ModelID, embedding.ModelVersion, embedding.PipelineFingerprint = d.readString(), d.readString(), d.readString(), d.readString()
	chunking := semantic.ChunkingDescriptor{ID: d.readString(), Version: d.readUint32(), Fingerprint: d.readString()}
	config := decodeConfig(d, embedding, chunking)
	value := State{Config: config, Revision: d.readUint64(), MaxAllocatedVectorID: d.readUint64(), NextComponentID: d.readUint64()}
	countValue := uint64(d.readUint32())
	if countValue > uint64(limits.MaxVectors) || countValue > uint64(d.remaining()/16) {
		return State{}, ErrLimitExceeded
	}
	count := int(countValue)
	value.Segments = make([]StateSegment, count)
	totalRows := 0
	for i := range value.Segments {
		segment := StateSegment{ComponentID: d.readUint64()}
		rowCountValue, wordCountValue := uint64(d.readUint32()), uint64(d.readUint32())
		if rowCountValue > uint64(limits.MaxVectors-totalRows) || wordCountValue != (rowCountValue+63)/64 || wordCountValue > uint64(d.remaining()/8) {
			return State{}, ErrLimitExceeded
		}
		rowCount, wordCount := int(rowCountValue), int(wordCountValue)
		segment.LivenessWords = make([]uint64, wordCount)
		for j := range segment.LivenessWords {
			segment.LivenessWords[j] = d.readUint64()
		}
		if rowCount > d.remaining()/minimumRowBytes {
			return State{}, ErrLimitExceeded
		}
		segment.Rows = make([]semantic.VectorRow, rowCount)
		for j := range segment.Rows {
			segment.Rows[j] = semantic.VectorRow{VectorID: d.readUint64(), Chunk: decodeRef(d)}
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
	if _, err := validateStateAndSize(value, limits); err != nil {
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
		e.writeUint32(uint32(value))
	}
	e.writeUint64(c.HNSW.Seed)
}

func decodeConfig(d *decoder, embedding semantic.EmbeddingDescriptor, chunking semantic.ChunkingDescriptor) semantic.Config {
	c := semantic.Config{Embedding: embedding, Chunking: chunking}
	c.Limits = semantic.Limits{
		MaxLiveVectors:          int(d.readUint32()),
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

func validateStateAndSize(value State, limits Limits) (uint64, error) {
	if err := value.Config.Validate(); err != nil {
		return 0, codecErrorf(ErrCorrupt, "state config is invalid: %v", err)
	}
	if value.NextComponentID == 0 {
		return 0, codecErrorf(ErrCorrupt, "state next component ID is zero")
	}
	if len(value.Segments) > limits.MaxVectors {
		return 0, codecErrorf(ErrLimitExceeded, "state segment count %d exceeds limit %d", len(value.Segments), limits.MaxVectors)
	}
	c := value.Config
	size := uint64(stateFixedEncodedSize)
	for _, descriptor := range []string{c.Embedding.ProviderID, c.Embedding.ModelID, c.Embedding.ModelVersion, c.Embedding.PipelineFingerprint, c.Chunking.ID, c.Chunking.Fingerprint} {
		if !utf8.ValidString(descriptor) || len(descriptor) > limits.MaxStringBytes || !addEncodedStringSize(&size, descriptor) {
			return 0, ErrLimitExceeded
		}
	}
	integerFields := []struct {
		name  string
		value int
	}{
		{name: "max live vectors", value: c.Limits.MaxLiveVectors},
		{name: "max chunks per document", value: c.Limits.MaxChunksPerDocument},
		{name: "max documents per search", value: c.Limits.MaxDocumentsPerSearch},
		{name: "max chunk candidates", value: c.Limits.MaxChunkCandidates},
		{name: "max chunks per document hit", value: c.Limits.MaxChunksPerDocumentHit},
		{name: "HNSW max neighbors", value: c.HNSW.MaxNeighbors},
		{name: "HNSW ef construction", value: c.HNSW.EfConstruction},
		{name: "HNSW default ef search", value: c.HNSW.DefaultEfSearch},
		{name: "HNSW max ef search", value: c.HNSW.MaxEfSearch},
		{name: "HNSW default visit limit", value: c.HNSW.DefaultVisitLimit},
		{name: "HNSW max visit limit", value: c.HNSW.MaxVisitLimit},
	}
	for _, field := range integerFields {
		if field.value <= 0 {
			return 0, codecErrorf(ErrCorrupt, "state config %s must be positive, got %d", field.name, field.value)
		}
		if uint64(field.value) > math.MaxUint32 {
			return 0, codecErrorf(ErrLimitExceeded, "state config %s value %d exceeds uint32", field.name, field.value)
		}
	}
	if c.Embedding.Dimensions > limits.MaxDimensions {
		return 0, codecErrorf(ErrLimitExceeded, "state dimensions %d exceed limit %d", c.Embedding.Dimensions, limits.MaxDimensions)
	}
	if c.Limits.MaxLiveVectors > limits.MaxVectors {
		return 0, codecErrorf(ErrLimitExceeded, "state max live vectors %d exceed limit %d", c.Limits.MaxLiveVectors, limits.MaxVectors)
	}
	if c.Limits.MaxChunksPerDocument > limits.MaxChunksPerDocument {
		return 0, codecErrorf(ErrLimitExceeded, "state max chunks per document %d exceed limit %d", c.Limits.MaxChunksPerDocument, limits.MaxChunksPerDocument)
	}
	if c.Limits.MaxDocumentsPerSearch > limits.MaxK {
		return 0, codecErrorf(ErrLimitExceeded, "state max documents per search %d exceed limit %d", c.Limits.MaxDocumentsPerSearch, limits.MaxK)
	}
	if c.Limits.MaxChunkCandidates > limits.MaxK {
		return 0, codecErrorf(ErrLimitExceeded, "state max chunk candidates %d exceed limit %d", c.Limits.MaxChunkCandidates, limits.MaxK)
	}
	dimensions := uint64(c.Embedding.Dimensions)
	maxLiveVectors := uint64(c.Limits.MaxLiveVectors)
	if dimensions > math.MaxUint64/4 || maxLiveVectors > math.MaxUint64/(dimensions*4) {
		return 0, codecErrorf(ErrLimitExceeded, "state maximum vector bytes overflow uint64")
	}
	vectorBytes := maxLiveVectors * dimensions * 4
	if vectorBytes > limits.MaxVectorBytes {
		return 0, codecErrorf(ErrLimitExceeded, "state maximum vector bytes %d exceed limit %d", vectorBytes, limits.MaxVectorBytes)
	}
	if c.HNSW.MaxEfSearch > limits.MaxEfSearch {
		return 0, codecErrorf(ErrLimitExceeded, "state HNSW max ef search %d exceeds limit %d", c.HNSW.MaxEfSearch, limits.MaxEfSearch)
	}
	if c.HNSW.MaxVisitLimit > limits.MaxVisitLimit {
		return 0, codecErrorf(ErrLimitExceeded, "state HNSW max visit limit %d exceeds limit %d", c.HNSW.MaxVisitLimit, limits.MaxVisitLimit)
	}
	totalRows := 0
	documents := make(map[fts.DocID]struct{})
	for segmentIndex, segment := range value.Segments {
		documentChunks := make(map[fts.DocID]int)
		if !addEncodedSize(&size, stateSegmentFixedSize) || uint64(len(segment.LivenessWords)) > math.MaxUint64/wireUint64Size || !addEncodedSize(&size, uint64(len(segment.LivenessWords))*wireUint64Size) {
			return 0, ErrLimitExceeded
		}
		if segment.ComponentID == 0 {
			return 0, codecErrorf(ErrCorrupt, "state segment %d has zero component ID", segmentIndex)
		}
		expectedWordCount := len(segment.Rows) / 64
		if len(segment.Rows)%64 != 0 {
			expectedWordCount++
		}
		if len(segment.LivenessWords) != expectedWordCount {
			return 0, codecErrorf(ErrCorrupt, "state segment %d has %d liveness words, want %d for %d rows", segmentIndex, len(segment.LivenessWords), expectedWordCount, len(segment.Rows))
		}
		if len(segment.Rows) > limits.MaxVectors-totalRows {
			return 0, codecErrorf(ErrLimitExceeded, "state segment %d raises total row count above limit %d", segmentIndex, limits.MaxVectors)
		}
		if len(segment.LivenessWords) > 0 && len(segment.Rows)%64 != 0 && segment.LivenessWords[len(segment.LivenessWords)-1]>>uint(len(segment.Rows)%64) != 0 {
			return 0, codecErrorf(ErrCorrupt, "state segment %d has non-zero liveness bits beyond row count %d", segmentIndex, len(segment.Rows))
		}
		for rowIndex, row := range segment.Rows {
			if !addEncodedSize(&size, stateRowFixedSize) {
				return 0, ErrLimitExceeded
			}
			for _, text := range []string{string(row.Chunk.ID), string(row.Chunk.DocID), row.Chunk.Field} {
				if !utf8.ValidString(text) || len(text) > limits.MaxStringBytes || !addEncodedStringSize(&size, text) {
					return 0, ErrLimitExceeded
				}
			}
			if row.VectorID == 0 {
				return 0, codecErrorf(ErrCorrupt, "state segment %d row %d has zero vector ID", segmentIndex, rowIndex)
			}
			if rowIndex > 0 && segment.Rows[rowIndex-1].VectorID >= row.VectorID {
				return 0, codecErrorf(ErrCorrupt, "state segment %d row %d vector ID %d is not greater than previous ID %d", segmentIndex, rowIndex, row.VectorID, segment.Rows[rowIndex-1].VectorID)
			}
			if row.Chunk.ID == "" {
				return 0, codecErrorf(ErrCorrupt, "state segment %d row %d has empty chunk ID", segmentIndex, rowIndex)
			}
			if row.Chunk.DocID == "" {
				return 0, codecErrorf(ErrCorrupt, "state segment %d row %d has empty document ID", segmentIndex, rowIndex)
			}
			if row.Chunk.Field == "" {
				return 0, codecErrorf(ErrCorrupt, "state segment %d row %d has empty field", segmentIndex, rowIndex)
			}
			if row.Chunk.StartByte > row.Chunk.EndByte {
				return 0, codecErrorf(ErrCorrupt, "state segment %d row %d byte range [%d,%d) is invalid", segmentIndex, rowIndex, row.Chunk.StartByte, row.Chunk.EndByte)
			}
			if value.MaxAllocatedVectorID < row.VectorID {
				return 0, codecErrorf(ErrCorrupt, "state segment %d row %d vector ID %d exceeds max allocated ID %d", segmentIndex, rowIndex, row.VectorID, value.MaxAllocatedVectorID)
			}
			documentChunks[row.Chunk.DocID]++
			if documentChunks[row.Chunk.DocID] > limits.MaxChunksPerDocument {
				return 0, codecErrorf(ErrLimitExceeded, "state segment %d document %q exceeds chunk limit %d", segmentIndex, row.Chunk.DocID, limits.MaxChunksPerDocument)
			}
			if _, exists := documents[row.Chunk.DocID]; !exists {
				if len(documents) >= limits.MaxDocuments {
					return 0, codecErrorf(ErrLimitExceeded, "state document count exceeds limit %d at segment %d row %d", limits.MaxDocuments, segmentIndex, rowIndex)
				}
				documents[row.Chunk.DocID] = struct{}{}
			}
		}
		totalRows += len(segment.Rows)
	}
	if size > limits.MaxFileBytes {
		return 0, ErrLimitExceeded
	}
	return size, nil
}

func encodeRef(e *encoder, ref chunk.Ref, maxString int) {
	e.writeString(string(ref.ID), maxString)
	e.writeString(string(ref.DocID), maxString)
	e.writeString(ref.Field, maxString)
	e.writeUint32(ref.Ordinal)
	e.writeUint64(ref.StartByte)
	e.writeUint64(ref.EndByte)
}

func decodeRef(d *decoder) chunk.Ref {
	return chunk.Ref{ID: chunk.ID(d.readString()), DocID: fts.DocID(d.readString()), Field: d.readString(), Ordinal: d.readUint32(), StartByte: d.readUint64(), EndByte: d.readUint64()}
}

type committedSegment struct {
	data          semantic.SegmentData
	livenessWords []uint64
}

type committedState struct {
	config               semantic.Config
	revision             uint64
	maxAllocatedVectorID uint64
	nextComponentID      uint64
	segments             []committedSegment
	stateSegments        []StateSegment
	encodedState         []byte
	stateReference       fileReference
}

func captureCommittedState(state *semantic.CommittedState, limits Limits) (committedState, error) {
	segments := state.Segments()
	captured := committedState{
		config:               state.Config(),
		revision:             state.Revision(),
		maxAllocatedVectorID: state.MaxAllocatedVectorID(),
		nextComponentID:      state.NextComponentID(),
		segments:             make([]committedSegment, len(segments)),
		stateSegments:        make([]StateSegment, len(segments)),
	}
	for i, segment := range segments {
		data := segment.Data()
		livenessWords := segment.LivenessWords()
		captured.segments[i] = committedSegment{data: data, livenessWords: livenessWords}
		captured.stateSegments[i] = StateSegment{ComponentID: data.ComponentID, Rows: data.Rows, LivenessWords: livenessWords}
	}
	value := State{Config: captured.config, Revision: captured.revision, MaxAllocatedVectorID: captured.maxAllocatedVectorID, NextComponentID: captured.nextComponentID, Segments: captured.stateSegments}
	data, ref, err := EncodeState(value, codecLimits(limits))
	if err != nil {
		return committedState{}, mapCodecError(err)
	}
	captured.encodedState = data
	captured.stateReference = persistReference(ref)
	return captured, nil
}

func encodeState(state *semantic.CommittedState, limits Limits) ([]byte, fileReference, error) {
	captured, err := captureCommittedState(state, limits)
	if err != nil {
		return nil, fileReference{}, err
	}
	return captured.encodedState, captured.stateReference, nil
}

func decodeState(data []byte, limits Limits) (decodedState, error) {
	value, err := DecodeState(data, codecLimits(limits))
	if err != nil {
		return decodedState{}, mapCodecError(err)
	}
	segments := make([]decodedStateSegment, len(value.Segments))
	for i, segment := range value.Segments {
		segments[i] = decodedStateSegment{ComponentID: segment.ComponentID, Rows: segment.Rows, LivenessWords: segment.LivenessWords}
	}
	return decodedState{Config: value.Config, Revision: value.Revision, MaxAllocatedVectorID: value.MaxAllocatedVectorID, NextComponentID: value.NextComponentID, Segments: segments}, nil
}
