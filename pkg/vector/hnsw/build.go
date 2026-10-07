package hnsw

import (
	"context"
	"fmt"
	"math"
	"reflect"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// Build constructs and freezes one HNSW index from prepared source rows in
// stable ordinal order 0..source.Len()-1.
func Build(ctx context.Context, source vector.PreparedVectorStore, options BuildOptions) (*Index, error) {
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
	calculator, err := vector.NewCalculator(source.Dimensions(), source.Metric())
	if err != nil {
		return nil, err
	}
	if calculator.Normalization() != source.Normalization() {
		return nil, fmt.Errorf("%w: source normalization=%s, calculator normalization=%s",
			ErrBuildSourceMismatch, source.Normalization(), calculator.Normalization())
	}
	components, err := options.Limits.validateVectorAllocation(total, calculator.Dimensions())
	if err != nil {
		return nil, err
	}
	if err := options.Build.validate(); err != nil {
		return nil, err
	}
	if err := options.Search.validate(); err != nil {
		return nil, err
	}
	builder := newBuilder(options.Build, calculator, total, components)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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
		if err := builder.add(scratch); err != nil {
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
	index, err := builder.freeze(source, options.Search)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reportBuildProgress(options.Progress, BuildPhaseComplete, total, total)
	return index, nil
}

func isNilPreparedVectorStore(source vector.PreparedVectorStore) bool {
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
