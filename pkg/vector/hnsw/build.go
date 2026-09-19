package hnsw

import (
	"context"
	"fmt"
	"math"
	"reflect"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// BuildIndexReader constructs and freezes one HNSW reader from prepared source rows in
// stable ordinal order 0..source.Len()-1.
func BuildIndexReader(ctx context.Context, source vector.PreparedVectorSource, options BuildOptions) (*Reader, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if source == nil || isNilPreparedVectorSource(source) {
		return nil, fmt.Errorf("%w: nil source", ErrBuildSourceMismatch)
	}
	total := source.Len()
	reportBuildProgress(options.Progress, BuildPhasePreflight, 0, total)
	builder, err := NewBuilder(options.BuildConfig, options.SearchConfig, total)
	if err != nil {
		return nil, err
	}
	dimensions := source.Dimensions()
	metric := source.Metric()
	normalization := source.Normalization()
	if dimensions != builder.space.Dimensions() || metric != builder.space.Metric() || normalization != builder.space.Normalization() {
		return nil, fmt.Errorf("%w: source dimensions=%d metric=%s normalization=%s, build dimensions=%d metric=%s normalization=%s",
			ErrBuildSourceMismatch, dimensions, metric, normalization,
			builder.space.Dimensions(), builder.space.Metric(), builder.space.Normalization())
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	builder.source = source

	reportBuildProgress(options.Progress, BuildPhaseVectors, 0, total)
	var scratch []float32
	if total > 0 {
		scratch = make([]float32, builder.space.Dimensions())
	}
	for row := range total {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ordinal := vector.Ordinal(row)
		for component := range scratch {
			scratch[component] = float32(math.NaN())
		}
		if err := source.ReadVectorInto(ctx, ordinal, scratch); err != nil {
			return nil, fmt.Errorf("vector/hnsw: read source row %d: %w", ordinal, err)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, err := builder.addPrepared(ordinal, scratch); err != nil {
			return nil, fmt.Errorf("vector/hnsw: add source row %d: %w", ordinal, err)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		reportBuildProgress(options.Progress, BuildPhaseVectors, row+1, total)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	reportBuildProgress(options.Progress, BuildPhaseFreeze, total, total)
	reader, err := builder.Freeze()
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reportBuildProgress(options.Progress, BuildPhaseComplete, total, total)
	return reader, nil
}

func isNilPreparedVectorSource(source vector.PreparedVectorSource) bool {
	value := reflect.ValueOf(source)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func reportBuildProgress(report func(BuildProgress), phase BuildPhase, completed, total int) {
	if report != nil {
		report(BuildProgress{Phase: phase, Completed: completed, Total: total})
	}
}
