package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

// buildCompactedSegment materializes live rows from an immutable read view and
// builds the replacement HNSW segment without touching mutable service state.
func buildCompactedSegment(ctx context.Context, view *ReadView, componentID ComponentID, config Config) (*Segment, []VectorRow, error) {
	values, rows, err := materializeLiveRows(ctx, view)
	if err != nil {
		return nil, nil, err
	}
	source, err := newInMemoryVectorSourceFromConfig(config, values)
	if err != nil {
		return nil, nil, err
	}
	segment, err := BuildSegment(ctx, componentID, SegmentMetadata{Space: config.Space, Chunking: config.Chunking}, source, rows, hnsw.BuildOptions{
		BuildConfig:  withBuildCapacity(config.HNSWBuild, len(rows)),
		SearchConfig: config.HNSWSearch,
	})
	if err != nil {
		return nil, nil, err
	}
	return segment, rows, nil
}

func materializeLiveRows(ctx context.Context, published *ReadView) ([][]float32, []VectorRow, error) {
	var values [][]float32
	var rows []VectorRow
	for _, item := range published.segments {
		for ordinal, row := range item.segment.Rows() {
			if !item.filter.Allows(vector.Ordinal(ordinal)) {
				continue
			}
			value := make([]float32, item.segment.Dimensions())
			if err := item.segment.Vectors().ReadVectorInto(ctx, vector.Ordinal(ordinal), value); err != nil {
				return nil, nil, err
			}
			values = append(values, value)
			rows = append(rows, row)
		}
	}
	return values, rows, nil
}
