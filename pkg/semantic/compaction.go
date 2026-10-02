package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
	"github.com/dariasmyr/fts-engine/pkg/vectorstore"
)

// buildCompactedSegment materializes live rows from an immutable read view and
// builds the replacement HNSW segment without touching mutable service state.
func buildCompactedSegment(ctx context.Context, view *ReadView, componentID uint64, config Config) (*segment, []VectorRow, error) {
	flatVectors, liveRows, err := materializeLiveRows(ctx, view)
	if err != nil {
		return nil, nil, err
	}
	calculator, err := config.Embedding.Calculator()
	if err != nil {
		return nil, nil, err
	}
	source, err := vectorstore.NewPreparedMemoryVectorStore(calculator, flatVectors)
	if err != nil {
		return nil, nil, err
	}
	segment, err := buildSegment(
		ctx,
		componentID,
		PipelineDescriptor{
			Embedding: config.Embedding,
			Chunking:  config.Chunking,
		},
		source,
		liveRows,
		hnsw.BuildOptions{
			Build:  config.HNSWBuild,
			Search: config.HNSWSearch,
		})

	if err != nil {
		return nil, nil, err
	}
	return segment, liveRows, nil
}

func materializeLiveRows(ctx context.Context, published *ReadView) ([]float32, []VectorRow, error) {
	// flatVectors is a flat slice of all live vectors in the published segments, concatenated in order.
	// liveRows is a slice of all live vector ids in the published segments, concatenated in order.
	// The two slices are aligned by ordinal: flatVectors[dim*i:dim*(i+1)] is the vector for liveRows[i].
	var flatVectors []float32
	var liveRows []VectorRow
	for _, item := range published.segments {
		dim := item.segment.dimensions()

		for ordinal, row := range item.segment.rows {
			if ordinal%64 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, nil, err
				}
			}
			if !item.filter.Allows(vector.Ordinal(ordinal)) {
				continue
			}

			start := len(flatVectors)
			flatVectors = append(flatVectors, make([]float32, dim)...)
			dst := flatVectors[start:]
			if err := item.segment.vectorStore().ReadVectorInto(ctx, vector.Ordinal(ordinal), dst); err != nil {
				return nil, nil, err
			}
			liveRows = append(liveRows, row)
		}
	}
	return flatVectors, liveRows, nil
}
