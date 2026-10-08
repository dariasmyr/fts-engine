package semanticpersist

import (
	"math"

	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/semanticpersist/internal/semanticformat"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

func validateSegmentData(data semantic.SegmentData, config semantic.Config, limits Limits) error {
	semanticLimits := config.Limits
	if data.Vectors == nil || data.Index == nil {
		return ErrLimitExceeded
	}
	dimensions := data.Index.Dimensions()
	length := data.Index.Len()
	topology := hnsw.Snapshot(data.Index)
	if dimensions <= 0 || dimensions > limits.MaxDimensions || length < 0 || length > limits.MaxVectors ||
		semanticLimits.MaxLiveVectors <= 0 || semanticLimits.MaxStaleVectors < 0 ||
		semanticLimits.MaxLiveVectors > limits.MaxVectors-semanticLimits.MaxStaleVectors ||
		semanticLimits.MaxSegments <= 0 || semanticLimits.MaxSegments > limits.MaxVectors ||
		semanticLimits.MaxDocumentsPerSearch <= 0 || semanticLimits.MaxDocumentsPerSearch > limits.MaxK ||
		semanticLimits.MaxChunkCandidates < semanticLimits.MaxDocumentsPerSearch || semanticLimits.MaxChunkCandidates > limits.MaxK ||
		semanticLimits.MaxChunksPerDocumentHit <= 0 || semanticLimits.MaxChunksPerDocumentHit > limits.MaxChunksPerDocument {
		return ErrLimitExceeded
	}
	components, ok := checkedMultiply64(uint64(length), uint64(dimensions))
	if !ok {
		return ErrLimitExceeded
	}
	vectorBytes, ok := checkedMultiply64(components, 4)
	if !ok || vectorBytes > limits.MaxVectorBytes || limits.MaxFileBytes < 44 || vectorBytes > limits.MaxFileBytes-44 {
		return ErrLimitExceeded
	}
	directedLinks, ok := checkedAdd64(uint64(len(topology.Level0Links)), uint64(len(topology.UpperLinks)))
	if !ok || config.HNSW.MaxEfSearch > limits.MaxEfSearch || config.HNSW.MaxVisitLimit > limits.MaxVisitLimit ||
		topology.MaxNeighbors != config.HNSW.MaxNeighbors || topology.LevelZeroMaxNeighbors != config.HNSW.MaxNeighbors*2 ||
		topology.EfConstruction != config.HNSW.EfConstruction || topology.Seed != config.HNSW.Seed ||
		directedLinks > limits.MaxGraphLinks {
		return ErrLimitExceeded
	}
	graphBytes, err := semanticformat.EncodedSize(graphFromSnapshot(topology, semanticformat.FileRef{Size: 1, SHA256: [32]byte{1}}))
	if err != nil || graphBytes > min(limits.MaxFileBytes, limits.MaxGraphBytes) {
		return ErrLimitExceeded
	}
	return nil
}

func validateOpenReferences(manifestBytes uint64, value semanticformat.GenerationManifest, limits Limits) error {
	if value.State.Size > limits.MaxFileBytes {
		return ErrLimitExceeded
	}
	estimate := manifestBytes
	components := []struct{ size, multiplier uint64 }{{value.State.Size, 16}}
	for _, segment := range value.Segments {
		if segment.Vectors.Size > min(limits.MaxFileBytes, limits.MaxVectorBytes+128) || segment.Graph.Size > min(limits.MaxFileBytes, limits.MaxGraphBytes) {
			return ErrLimitExceeded
		}
		components = append(components, struct{ size, multiplier uint64 }{segment.Vectors.Size, 4}, struct{ size, multiplier uint64 }{segment.Graph.Size, 8})
	}
	for _, component := range components {
		weighted, ok := checkedMultiply64(component.size, component.multiplier)
		if !ok {
			return ErrLimitExceeded
		}
		estimate, ok = checkedAdd64(estimate, weighted)
		if !ok {
			return ErrLimitExceeded
		}
	}
	scratchBytes, ok := checkedMultiply64(uint64(limits.MaxDimensions), 8)
	if !ok {
		return ErrLimitExceeded
	}
	estimate, ok = checkedAdd64(estimate, scratchBytes)
	if !ok || estimate > limits.MaxOpenBytes {
		return ErrLimitExceeded
	}
	return nil
}

func checkedMultiply64(a, b uint64) (uint64, bool) {
	if a != 0 && b > math.MaxUint64/a {
		return 0, false
	}
	return a * b, true
}
func checkedAdd64(a, b uint64) (uint64, bool) {
	if b > math.MaxUint64-a {
		return 0, false
	}
	return a + b, true
}
