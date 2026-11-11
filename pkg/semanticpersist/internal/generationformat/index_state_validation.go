package generationformat

import (
	"math"
	"math/bits"
	"unicode/utf8"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
)

func validateStateAndEncodedSize(value IndexState, limits Config) (uint64, error) {
	if err := value.Config.Validate(); err != nil {
		return 0, codecErrorf(ErrCorrupt, "state config is invalid: %v", err)
	}
	if value.ID == (semantic.IndexID{}) {
		return 0, codecErrorf(ErrCorrupt, "state index ID is zero")
	}
	if value.NextSegmentID == 0 {
		return 0, codecErrorf(ErrCorrupt, "state next segment ID is zero")
	}
	if len(value.Segments) > limits.MaxSegments {
		return 0, codecErrorf(ErrLimitExceeded, "state segment count %d exceeds limit %d", len(value.Segments), limits.MaxSegments)
	}
	c := value.Config
	if len(value.Segments) > c.Limits.MaxSegments {
		return 0, codecErrorf(ErrLimitExceeded, "state segment count %d exceeds configured limit %d", len(value.Segments), c.Limits.MaxSegments)
	}
	encodedSize := uint64(stateFixedEncodedSize)
	integerFields := []struct {
		name  string
		value int
	}{
		{name: "dimensions", value: c.Dimensions},
		{name: "max live vectors", value: c.Limits.MaxLiveVectors},
		{name: "max stale vectors", value: c.Limits.MaxStaleVectors},
		{name: "max segments", value: c.Limits.MaxSegments},
		{name: "max chunks per document", value: c.Limits.MaxChunksPerDocument},
		{name: "max documents per search", value: c.Search.MaxResults},
		{name: "max chunk candidates", value: c.Search.MaxCandidateChunks},
		{name: "max chunks per document hit", value: c.Search.MaxChunksPerResult},
		{name: "max query chunks", value: c.Search.MaxQueryChunks},
		{name: "HNSW max neighbors", value: c.HNSW.Build.MaxNeighbors},
		{name: "HNSW ef construction", value: c.HNSW.Build.EfConstruction},
		{name: "HNSW default ef search", value: c.HNSW.Search.EfSearch},
		{name: "HNSW max ef search", value: c.Search.MaxEfSearch},
		{name: "HNSW default visit limit", value: c.HNSW.Search.VisitLimit},
		{name: "HNSW max visit limit", value: c.Search.MaxVisitLimit},
	}
	for _, field := range integerFields {
		if field.value <= 0 {
			return 0, codecErrorf(ErrCorrupt, "state config %s must be positive, got %d", field.name, field.value)
		}
		if uint64(field.value) > math.MaxUint32 {
			return 0, codecErrorf(ErrLimitExceeded, "state config %s value %d exceeds uint32", field.name, field.value)
		}
	}
	if value.ServiceConfig != nil {
		service := *value.ServiceConfig
		if service.Index != c || service.Validate() != nil {
			return 0, codecErrorf(ErrCorrupt, "state service config is invalid")
		}
		serviceIntegers := []struct {
			name  string
			value int
		}{
			{name: "model dimensions", value: service.Model.Dimensions},
			{name: "model context tokens", value: service.Model.ContextTokens},
			{name: "chunk target bytes", value: service.Chunking.TargetBytes},
			{name: "chunk max bytes", value: service.Chunking.MaxBytes},
			{name: "maximum encoding fields", value: service.Encoding.MaxFields},
		}
		for _, field := range serviceIntegers {
			if field.value <= 0 {
				return 0, codecErrorf(ErrCorrupt, "state service config %s must be positive, got %d", field.name, field.value)
			}
			if uint64(field.value) > math.MaxUint32 {
				return 0, codecErrorf(ErrLimitExceeded, "state service config %s value %d exceeds uint32", field.name, field.value)
			}
		}
		if service.Chunking.OverlapBytes < 0 || uint64(service.Chunking.OverlapBytes) > math.MaxUint32 ||
			service.Encoding.MaxSourceBytes == 0 || service.Encoding.MaxEmbeddingBytes == 0 {
			return 0, codecErrorf(ErrCorrupt, "state service config limits are invalid")
		}
		if !accumulateEncodedSize(&encodedSize, stateServiceFixedSize) {
			return 0, ErrLimitExceeded
		}
		for _, text := range []string{
			service.Model.ID,
			service.Model.Revision,
			service.Model.DocumentPrefix,
			service.Model.QueryPrefix,
		} {
			if !utf8.ValidString(text) || len(text) > limits.MaxStringBytes ||
				!accumulateEncodedStringSize(&encodedSize, text) {
				return 0, ErrLimitExceeded
			}
		}
	}
	if c.Dimensions > limits.MaxDimensions {
		return 0, codecErrorf(ErrLimitExceeded, "state dimensions %d exceed limit %d", c.Dimensions, limits.MaxDimensions)
	}
	if c.Limits.MaxLiveVectors > limits.MaxVectors {
		return 0, codecErrorf(ErrLimitExceeded, "state max live vectors %d exceed limit %d", c.Limits.MaxLiveVectors, limits.MaxVectors)
	}
	if c.Limits.MaxStaleVectors > limits.MaxVectors-c.Limits.MaxLiveVectors {
		return 0, codecErrorf(ErrLimitExceeded, "state physical vector limit exceeds %d", limits.MaxVectors)
	}
	if c.Limits.MaxSegments > limits.MaxSegments {
		return 0, codecErrorf(ErrLimitExceeded, "state max segments %d exceed limit %d", c.Limits.MaxSegments, limits.MaxSegments)
	}
	if c.Limits.MaxChunksPerDocument > limits.MaxChunksPerDocument {
		return 0, codecErrorf(ErrLimitExceeded, "state max chunks per document %d exceed limit %d", c.Limits.MaxChunksPerDocument, limits.MaxChunksPerDocument)
	}
	if c.Search.MaxQueryChunks > limits.MaxQueryChunks {
		return 0, codecErrorf(ErrLimitExceeded, "state max query chunks %d exceed limit %d", c.Search.MaxQueryChunks, limits.MaxQueryChunks)
	}
	if c.Search.MaxResults > limits.MaxK {
		return 0, codecErrorf(ErrLimitExceeded, "state max documents per search %d exceed limit %d", c.Search.MaxResults, limits.MaxK)
	}
	if c.Search.MaxCandidateChunks > limits.MaxK {
		return 0, codecErrorf(ErrLimitExceeded, "state max chunk candidates %d exceed limit %d", c.Search.MaxCandidateChunks, limits.MaxK)
	}
	dimensions := uint64(c.Dimensions)
	maxPhysicalVectors := uint64(c.Limits.MaxLiveVectors + c.Limits.MaxStaleVectors)
	if dimensions > math.MaxUint64/4 || maxPhysicalVectors > math.MaxUint64/(dimensions*4) {
		return 0, codecErrorf(ErrLimitExceeded, "state maximum vector bytes overflow uint64")
	}
	vectorBytes := maxPhysicalVectors * dimensions * 4
	if vectorBytes > limits.MaxVectorBytes {
		return 0, codecErrorf(ErrLimitExceeded, "state maximum vector bytes %d exceed limit %d", vectorBytes, limits.MaxVectorBytes)
	}
	if c.Search.MaxEfSearch > limits.MaxEfSearch {
		return 0, codecErrorf(ErrLimitExceeded, "state HNSW max ef search %d exceeds limit %d", c.Search.MaxEfSearch, limits.MaxEfSearch)
	}
	if c.Search.MaxVisitLimit > limits.MaxVisitLimit {
		return 0, codecErrorf(ErrLimitExceeded, "state HNSW max visit limit %d exceeds limit %d", c.Search.MaxVisitLimit, limits.MaxVisitLimit)
	}
	totalRows := 0
	liveRows := 0
	documents := make(map[fts.DocID]struct{})
	for segmentIndex, segment := range value.Segments {
		documentChunks := make(map[fts.DocID]int)
		if !accumulateEncodedSize(&encodedSize, stateSegmentFixedSize) || uint64(len(segment.LivenessWords)) > math.MaxUint64/wireUint64Size || !accumulateEncodedSize(&encodedSize, uint64(len(segment.LivenessWords))*wireUint64Size) {
			return 0, ErrLimitExceeded
		}
		if segment.ID == 0 {
			return 0, codecErrorf(ErrCorrupt, "state segment %d has zero ID", segmentIndex)
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
		for _, word := range segment.LivenessWords {
			liveRows += bits.OnesCount64(word)
		}
		for rowIndex, row := range segment.Rows {
			if !accumulateEncodedSize(&encodedSize, stateRowFixedSize) {
				return 0, ErrLimitExceeded
			}
			for _, text := range []string{string(row.Chunk.ID), string(row.Chunk.DocID), row.Chunk.Field} {
				if !utf8.ValidString(text) || len(text) > limits.MaxStringBytes || !accumulateEncodedStringSize(&encodedSize, text) {
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
	if liveRows > c.Limits.MaxLiveVectors || totalRows-liveRows > c.Limits.MaxStaleVectors {
		return 0, codecErrorf(ErrLimitExceeded, "state lifecycle vector bounds exceeded")
	}
	if encodedSize > limits.MaxFileBytes {
		return 0, ErrLimitExceeded
	}
	return encodedSize, nil
}
