package hnsw

import (
	"context"
	"fmt"
	"math"
	"reflect"

	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vectorstore"
)

// Build constructs and freezes one HNSW index from prepared source rows in
// stable ordinal order 0..source.Len()-1.
func Build(ctx context.Context, source vectorstore.PreparedVectorStore, options BuildOptions) (*Index, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if source == nil || isNilPreparedVectorStore(source) {
		return nil, fmt.Errorf("%w: nil source", ErrBuildSourceMismatch)
	}
	total := source.Len()
	reportBuildProgress(options.Progress, BuildPhasePreflight, 0, total)
	builder, err := newBuilder(options.Build, options.Search, total)
	if err != nil {
		return nil, err
	}
	dimensions := source.Dimensions()
	metric := source.Metric()
	normalization := source.Normalization()
	if dimensions != builder.calculator.Dimensions() || metric != builder.calculator.Metric() || normalization != builder.calculator.Normalization() {
		return nil, fmt.Errorf("%w: source dimensions=%d metric=%s normalization=%s, build dimensions=%d metric=%s normalization=%s",
			ErrBuildSourceMismatch, dimensions, metric, normalization,
			builder.calculator.Dimensions(), builder.calculator.Metric(), builder.calculator.Normalization())
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	builder.source = source

	reportBuildProgress(options.Progress, BuildPhaseVectors, 0, total)
	var scratch []float32
	if total > 0 {
		scratch = make([]float32, builder.calculator.Dimensions())
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
	index, err := builder.Freeze()
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reportBuildProgress(options.Progress, BuildPhaseComplete, total, total)
	return index, nil
}

func isNilPreparedVectorStore(source vectorstore.PreparedVectorStore) bool {
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
