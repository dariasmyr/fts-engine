package semanticpersist

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic/service"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/generationformat"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"
)

type segmentSource struct {
	state semantic.SegmentState
}

type persistenceState struct {
	state    generationformat.IndexState
	segments []segmentSource
}

func buildPersistenceState(
	ctx context.Context,
	committed *semantic.CommittedIndexState,
	pipeline *service.PipelineConfig,
) (persistenceState, error) {
	if committed == nil {
		return persistenceState{}, ErrCorrupt
	}
	segments := committed.Segments
	result := persistenceState{
		state: generationformat.IndexState{
			ID:                   [16]byte(committed.ID),
			Config:               indexConfigToFormat(committed.Config),
			Revision:             uint64(committed.Revision),
			MaxAllocatedVectorID: uint64(committed.MaxAllocatedVectorID),
			NextSegmentID:        uint64(committed.NextSegmentID),
			Segments:             make([]generationformat.SegmentState, len(segments)),
		},
		segments: make([]segmentSource, len(segments)),
	}
	if pipeline != nil {
		config := pipelineConfigToFormat(*pipeline)
		result.state.Pipeline = &config
	}
	for i, segment := range segments {
		if err := ctx.Err(); err != nil {
			return persistenceState{}, err
		}
		result.segments[i] = segmentSource{state: segment}
		result.state.Segments[i] = segmentStateToFormat(segment)
	}
	if err := validateFormatState(result.state); err != nil {
		return persistenceState{}, mapFormatError(err)
	}
	return result, nil
}

func restoreIndex(
	ctx context.Context,
	persisted persistedGeneration,
	store segmentStore,
) (*semantic.Index, error) {
	config, err := indexConfigFromFormat(persisted.state.Config)
	if err != nil {
		return nil, mapFormatError(err)
	}
	segments := make([]semantic.SegmentState, len(persisted.state.Segments))

	for i, stateSegment := range persisted.state.Segments {
		ref := persisted.manifest.Segments[i]
		metadata := segmentStateFromFormat(stateSegment)

		data, err := store.open(ctx, ref, config, metadata)
		if err != nil {
			return nil, err
		}

		data.LivenessWords = append([]uint64(nil), metadata.LivenessWords...)
		segments[i] = data
	}

	index, err := semantic.Restore(
		ctx,
		semantic.CommittedIndexState{
			ID:                   semantic.IndexID(persisted.state.ID),
			Config:               config,
			Revision:             semantic.Revision(persisted.state.Revision),
			MaxAllocatedVectorID: semantic.VectorID(persisted.state.MaxAllocatedVectorID),
			NextSegmentID:        semantic.SegmentID(persisted.state.NextSegmentID),
			Segments:             segments,
		},
	)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, ErrCorrupt
	}
	return index, nil
}

func validateFormatState(state generationformat.IndexState) error {
	config, err := indexConfigFromFormat(state.Config)
	if err != nil {
		return err
	}
	if state.Pipeline != nil {
		if _, err := pipelineConfigFromFormat(*state.Pipeline, config); err != nil {
			return err
		}
	}
	return nil
}

func indexConfigToFormat(config semantic.IndexConfig) generationformat.IndexConfig {
	return generationformat.IndexConfig{
		Dimensions: config.Dimensions,
		Metric:     uint8(config.Metric),
		Build: generationformat.BuildConfig{
			MaxNeighbors: config.Build.MaxNeighbors, EfConstruction: config.Build.EfConstruction, Seed: config.Build.Seed,
		},
		Operational: generationformat.OperationalLimits{
			Capacity: generationformat.CapacityLimits{
				MaxLiveVectors: config.Operational.Capacity.MaxLiveVectors, MaxStaleVectors: config.Operational.Capacity.MaxStaleVectors,
				MaxSegments: config.Operational.Capacity.MaxSegments,
			},
			Ingestion: generationformat.IngestionLimits{MaxChunksPerDocument: config.Operational.Ingestion.MaxChunksPerDocument},
			Search: generationformat.SearchLimits{
				MaxResults: config.Operational.Search.MaxResults, MaxCandidateChunks: config.Operational.Search.MaxCandidateChunks,
				MaxChunksPerResult: config.Operational.Search.MaxChunksPerResult, MaxQueryChunks: config.Operational.Search.MaxQueryChunks,
				MaxEfSearch: config.Operational.Search.MaxEfSearch, MaxVisitLimit: config.Operational.Search.MaxVisitLimit,
			},
			SearchDefaults: generationformat.SearchConfig{
				EfSearch: config.Operational.SearchDefaults.EfSearch, VisitLimit: config.Operational.SearchDefaults.VisitLimit,
			},
		},
	}
}

func indexConfigFromFormat(config generationformat.IndexConfig) (semantic.IndexConfig, error) {
	result := semantic.IndexConfig{
		Dimensions: config.Dimensions,
		Metric:     vector.Metric(config.Metric),
		Build: hnsw.BuildConfig{
			MaxNeighbors: config.Build.MaxNeighbors, EfConstruction: config.Build.EfConstruction, Seed: config.Build.Seed,
		},
		Operational: semantic.OperationalLimits{
			Capacity: semantic.CapacityLimits{
				MaxLiveVectors: config.Operational.Capacity.MaxLiveVectors, MaxStaleVectors: config.Operational.Capacity.MaxStaleVectors,
				MaxSegments: config.Operational.Capacity.MaxSegments,
			},
			Ingestion: semantic.IngestionLimits{MaxChunksPerDocument: config.Operational.Ingestion.MaxChunksPerDocument},
			Search: semantic.SearchLimits{
				MaxResults: config.Operational.Search.MaxResults, MaxCandidateChunks: config.Operational.Search.MaxCandidateChunks,
				MaxChunksPerResult: config.Operational.Search.MaxChunksPerResult, MaxQueryChunks: config.Operational.Search.MaxQueryChunks,
				MaxEfSearch: config.Operational.Search.MaxEfSearch, MaxVisitLimit: config.Operational.Search.MaxVisitLimit,
			},
			SearchDefaults: hnsw.SearchConfig{
				EfSearch: config.Operational.SearchDefaults.EfSearch, VisitLimit: config.Operational.SearchDefaults.VisitLimit,
			},
		},
	}
	if err := result.ValidateCanonical(); err != nil {
		return semantic.IndexConfig{}, generationformat.ErrCorrupt
	}
	return result, nil
}

func pipelineConfigToFormat(config service.PipelineConfig) generationformat.PipelineConfig {
	return generationformat.PipelineConfig{
		Model: generationformat.Model{
			ID: config.Model.ID, Revision: config.Model.Revision, Dimensions: config.Model.Dimensions,
			ContextTokens: config.Model.ContextTokens, DocumentPrefix: config.Model.DocumentPrefix, QueryPrefix: config.Model.QueryPrefix,
		},
		Chunking: generationformat.ChunkingConfig{
			TargetBytes: config.Chunking.TargetBytes, MaxBytes: config.Chunking.MaxBytes, OverlapBytes: config.Chunking.OverlapBytes,
		},
		EncodingBehaviorVersion: config.EncodingBehaviorVersion,
		Operational: generationformat.PipelineOperationalLimits{Encoding: generationformat.EncodingLimits{
			MaxFields: config.Operational.Encoding.MaxFields, MaxSourceBytes: config.Operational.Encoding.MaxSourceBytes,
			MaxEmbeddingBytes: config.Operational.Encoding.MaxEmbeddingBytes,
		}},
	}
}

func pipelineConfigFromFormat(config generationformat.PipelineConfig, index semantic.IndexConfig) (service.PipelineConfig, error) {
	if config.EncodingBehaviorVersion != service.CurrentEncodingBehaviorVersion {
		return service.PipelineConfig{}, generationformat.ErrUnsupportedVersion
	}
	result := service.PipelineConfig{
		Model: embedding.Model{
			ID: config.Model.ID, Revision: config.Model.Revision, Dimensions: config.Model.Dimensions,
			ContextTokens: config.Model.ContextTokens, DocumentPrefix: config.Model.DocumentPrefix, QueryPrefix: config.Model.QueryPrefix,
		},
		Chunking: chunk.Config{
			TargetBytes: config.Chunking.TargetBytes, MaxBytes: config.Chunking.MaxBytes, OverlapBytes: config.Chunking.OverlapBytes,
		},
		EncodingBehaviorVersion: config.EncodingBehaviorVersion,
		Operational: service.PipelineOperationalLimits{Encoding: service.EncodingLimits{
			MaxFields: config.Operational.Encoding.MaxFields, MaxSourceBytes: config.Operational.Encoding.MaxSourceBytes,
			MaxEmbeddingBytes: config.Operational.Encoding.MaxEmbeddingBytes,
		}},
	}
	if err := result.ValidateCanonical(index); err != nil {
		return service.PipelineConfig{}, generationformat.ErrCorrupt
	}
	return result, nil
}

func optionalPipelineFromFormat(
	config *generationformat.PipelineConfig,
	index semantic.IndexConfig,
) (*service.PipelineConfig, error) {
	if config == nil {
		return nil, nil
	}
	result, err := pipelineConfigFromFormat(*config, index)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func segmentStateToFormat(state semantic.SegmentState) generationformat.SegmentState {
	rows := make([]generationformat.VectorRow, len(state.Rows))
	for i, row := range state.Rows {
		rows[i] = generationformat.VectorRow{
			VectorID: uint64(row.VectorID),
			Chunk: generationformat.Ref{
				ID: string(row.Chunk.ID), DocID: string(row.Chunk.DocID), Field: row.Chunk.Field,
				Ordinal: row.Chunk.Ordinal, StartByte: row.Chunk.StartByte, EndByte: row.Chunk.EndByte,
			},
		}
	}
	return generationformat.SegmentState{
		ID: uint64(state.ID), Rows: rows, LivenessWords: append([]uint64(nil), state.LivenessWords...),
	}
}

func segmentStateFromFormat(state generationformat.SegmentState) semantic.SegmentState {
	rows := make([]semantic.VectorRow, len(state.Rows))
	for i, row := range state.Rows {
		rows[i] = semantic.VectorRow{
			VectorID: semantic.VectorID(row.VectorID),
			Chunk: semantic.Ref{
				ID: semantic.ChunkID(row.Chunk.ID), DocID: fts.DocID(row.Chunk.DocID), Field: row.Chunk.Field,
				Ordinal: row.Chunk.Ordinal, StartByte: row.Chunk.StartByte, EndByte: row.Chunk.EndByte,
			},
		}
	}
	return semantic.SegmentState{
		ID: semantic.SegmentID(state.ID), Rows: rows, LivenessWords: append([]uint64(nil), state.LivenessWords...),
	}
}
