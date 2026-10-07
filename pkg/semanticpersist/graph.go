package semanticpersist

import (
	"context"
	"errors"
	"io"
	"math"

	"github.com/dariasmyr/fts-engine/internal/hnswmodel"
	"github.com/dariasmyr/fts-engine/pkg/semanticpersist/internal/semanticformat"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

const maxPersistedHNSWLevel = 63

func writeGraphFile(
	ctx context.Context,
	writer io.Writer,
	index *hnsw.Index,
	vectors semanticformat.FileRef,
) (semanticformat.Metadata, error) {
	if ctx == nil {
		return semanticformat.Metadata{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return semanticformat.Metadata{}, err
	}
	if writer == nil || index == nil || !validGraphReference(vectors) {
		return semanticformat.Metadata{}, ErrCorrupt
	}

	metadata, err := semanticformat.Encode(
		contextWriter{ctx: ctx, writer: writer},
		graphFromSnapshot(hnsw.Snapshot(index), vectors),
	)
	if err != nil {
		return semanticformat.Metadata{}, mapGraphFormatError(err)
	}
	return metadata, nil
}

func openGraphFile(
	ctx context.Context,
	data []byte,
	vectors vector.PreparedVectorStore,
	vectorRef semanticformat.FileRef,
	search hnsw.SearchConfig,
	limits Limits,
) (*hnsw.Index, semanticformat.Metadata, error) {
	if ctx == nil {
		return nil, semanticformat.Metadata{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, semanticformat.Metadata{}, err
	}
	if vectors == nil || !validGraphReference(vectorRef) {
		return nil, semanticformat.Metadata{}, ErrCorrupt
	}

	graph, metadata, err := semanticformat.DecodeBytesContext(ctx, data, semanticformat.Limits{
		MaxGraphBytes: min(limits.MaxFileBytes, limits.MaxGraphBytes),
		MaxVectors:    uint64(limits.MaxVectors),
		MaxLinks:      limits.MaxGraphLinks,
		MaxDimensions: uint64(limits.MaxDimensions),
		MaxLevel:      maxPersistedHNSWLevel,
	})
	if err != nil {
		return nil, semanticformat.Metadata{}, mapGraphFormatError(err)
	}
	if graph.Vectors.Size != vectorRef.Size || graph.Vectors.SHA256 != vectorRef.SHA256 {
		return nil, semanticformat.Metadata{}, ErrCorrupt
	}

	snapshot, err := validateDecodedGraph(ctx, graph, vectors, limits)
	if err != nil {
		return nil, semanticformat.Metadata{}, err
	}
	index, err := hnsw.Restore(snapshot, vectors, search)
	if err != nil {
		return nil, semanticformat.Metadata{}, ErrCorrupt
	}
	return index, metadata, nil
}

func graphFromSnapshot(snapshot hnswmodel.Snapshot, vectors semanticformat.FileRef) semanticformat.Graph {
	maxLevel := uint8(0xff)
	if snapshot.HasEntry {
		maxLevel = snapshot.Levels[snapshot.Entry]
	}
	return semanticformat.Graph{
		Dimensions:    uint32(snapshot.Dimensions),
		Metric:        uint8(snapshot.Metric),
		Normalization: uint8(snapshot.Normalization),
		HasEntry:      snapshot.HasEntry,
		Entry:         snapshot.Entry,
		MaxLevel:      maxLevel,
		Build: semanticformat.BuildInfo{
			BuildVersion:          snapshot.BuildVersion,
			LevelGeneratorVersion: snapshot.LevelGeneratorVersion,
			MaxNeighbors:          uint32(snapshot.MaxNeighbors),
			LevelZeroMaxNeighbors: uint32(snapshot.LevelZeroMaxNeighbors),
			EfConstruction:        uint32(snapshot.EfConstruction),
			Seed:                  snapshot.Seed,
		},
		Vectors: semanticformat.Reference{
			Size:   vectors.Size,
			SHA256: vectors.SHA256,
		},
		Levels:           snapshot.Levels,
		Level0Offsets:    snapshot.Level0Offsets,
		Level0Links:      snapshot.Level0Links,
		UpperNodeOffsets: snapshot.UpperNodeOffsets,
		UpperLinkOffsets: snapshot.UpperLinkOffsets,
		UpperLinks:       snapshot.UpperLinks,
	}
}

func validateDecodedGraph(
	ctx context.Context,
	graph semanticformat.Graph,
	vectors vector.PreparedVectorStore,
	limits Limits,
) (hnswmodel.Snapshot, error) {
	calculator, err := vector.NewCalculator(int(graph.Dimensions), vector.Metric(graph.Metric))
	if err != nil || calculator.Normalization() != vector.Normalization(graph.Normalization) {
		return hnswmodel.Snapshot{}, ErrCorrupt
	}
	nodes := len(graph.Levels)
	if vectors.Len() != nodes || vectors.Dimensions() != calculator.Dimensions() ||
		vectors.Metric() != calculator.Metric() || vectors.Normalization() != calculator.Normalization() {
		return hnswmodel.Snapshot{}, ErrCorrupt
	}
	components, ok := checkedMultiply64(uint64(nodes), uint64(graph.Dimensions))
	if !ok {
		return hnswmodel.Snapshot{}, ErrLimitExceeded
	}
	vectorBytes, ok := checkedMultiply64(components, 4)
	if !ok || vectorBytes > limits.MaxVectorBytes {
		return hnswmodel.Snapshot{}, ErrLimitExceeded
	}
	if uint64(graph.Build.MaxNeighbors) > uint64(math.MaxInt) ||
		uint64(graph.Build.LevelZeroMaxNeighbors) > uint64(math.MaxInt) ||
		uint64(graph.Build.EfConstruction) > uint64(math.MaxInt) {
		return hnswmodel.Snapshot{}, ErrLimitExceeded
	}
	if graph.Build.BuildVersion == 0 || graph.Build.LevelGeneratorVersion == 0 ||
		graph.Build.MaxNeighbors < 2 || graph.Build.LevelZeroMaxNeighbors != graph.Build.MaxNeighbors*2 ||
		graph.Build.EfConstruction < graph.Build.MaxNeighbors {
		return hnswmodel.Snapshot{}, ErrCorrupt
	}
	if err := validateDecodedTopology(ctx, graph); err != nil {
		return hnswmodel.Snapshot{}, err
	}

	return hnswmodel.Snapshot{
		Dimensions:            int(graph.Dimensions),
		Metric:                vector.Metric(graph.Metric),
		Normalization:         vector.Normalization(graph.Normalization),
		BuildVersion:          graph.Build.BuildVersion,
		LevelGeneratorVersion: graph.Build.LevelGeneratorVersion,
		MaxNeighbors:          int(graph.Build.MaxNeighbors),
		LevelZeroMaxNeighbors: int(graph.Build.LevelZeroMaxNeighbors),
		EfConstruction:        int(graph.Build.EfConstruction),
		Seed:                  graph.Build.Seed,
		Entry:                 graph.Entry,
		HasEntry:              graph.HasEntry,
		Levels:                graph.Levels,
		Level0Offsets:         graph.Level0Offsets,
		Level0Links:           graph.Level0Links,
		UpperNodeOffsets:      graph.UpperNodeOffsets,
		UpperLinkOffsets:      graph.UpperLinkOffsets,
		UpperLinks:            graph.UpperLinks,
	}, nil
}

func validateDecodedTopology(ctx context.Context, graph semanticformat.Graph) error {
	nodes := len(graph.Levels)
	if len(graph.Level0Offsets) != nodes+1 || len(graph.UpperNodeOffsets) != nodes+1 ||
		len(graph.UpperLinkOffsets) == 0 {
		return ErrCorrupt
	}
	if nodes == 0 {
		if graph.HasEntry || graph.Entry != 0 || graph.MaxLevel != 0xff ||
			!validOffsets(ctx, graph.Level0Offsets, 0) || !validOffsets(ctx, graph.UpperNodeOffsets, 0) ||
			!validOffsets(ctx, graph.UpperLinkOffsets, 0) {
			return ErrCorrupt
		}
		return nil
	}
	if !graph.HasEntry || uint64(graph.Entry) >= uint64(nodes) {
		return ErrCorrupt
	}

	placements := uint64(0)
	maxLevel := uint8(0)
	for node, level := range graph.Levels {
		if node&0x3fff == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if level > maxPersistedHNSWLevel || uint64(graph.UpperNodeOffsets[node]) != placements {
			return ErrCorrupt
		}
		placements += uint64(level)
		if placements >= math.MaxUint32 {
			return ErrCorrupt
		}
		maxLevel = max(maxLevel, level)
	}
	if uint64(graph.UpperNodeOffsets[nodes]) != placements ||
		placements != uint64(len(graph.UpperLinkOffsets)-1) ||
		graph.Levels[graph.Entry] != maxLevel || graph.MaxLevel != maxLevel ||
		!validOffsets(ctx, graph.Level0Offsets, len(graph.Level0Links)) ||
		!validOffsets(ctx, graph.UpperLinkOffsets, len(graph.UpperLinks)) {
		if err := ctx.Err(); err != nil {
			return err
		}
		return ErrCorrupt
	}

	marks := make([]uint64, nodes)
	var epoch uint64
	for node, levelCount := range graph.Levels {
		for level := 0; level <= int(levelCount); level++ {
			neighbors := graphNeighbors(graph, node, level)
			limit := graph.Build.MaxNeighbors
			if level == 0 {
				limit = graph.Build.LevelZeroMaxNeighbors
			}
			if uint64(len(neighbors)) > uint64(limit) {
				return ErrCorrupt
			}
			epoch++
			for _, neighbor := range neighbors {
				if uint64(neighbor) >= uint64(nodes) || int(neighbor) == node ||
					int(graph.Levels[neighbor]) < level || marks[neighbor] == epoch {
					return ErrCorrupt
				}
				marks[neighbor] = epoch
			}
		}
	}
	return ctx.Err()
}

func graphNeighbors(graph semanticformat.Graph, node, level int) []uint32 {
	if level == 0 {
		return graph.Level0Links[graph.Level0Offsets[node]:graph.Level0Offsets[node+1]]
	}
	placement := graph.UpperNodeOffsets[node] + uint32(level-1)
	return graph.UpperLinks[graph.UpperLinkOffsets[placement]:graph.UpperLinkOffsets[placement+1]]
}

func validOffsets(ctx context.Context, offsets []uint32, values int) bool {
	if len(offsets) == 0 || offsets[0] != 0 || uint64(offsets[len(offsets)-1]) != uint64(values) {
		return false
	}
	for i := 1; i < len(offsets); i++ {
		if i&0x3fff == 0 && ctx.Err() != nil {
			return false
		}
		if offsets[i] < offsets[i-1] || uint64(offsets[i]) > uint64(values) {
			return false
		}
	}
	return true
}

func validGraphReference(reference semanticformat.FileRef) bool {
	return reference.Size != 0 && reference.SHA256 != [32]byte{}
}

func mapGraphFormatError(err error) error {
	switch {
	case errors.Is(err, semanticformat.ErrUnsupportedVersion):
		return ErrUnsupportedVersion
	case errors.Is(err, semanticformat.ErrLimitExceeded):
		return ErrLimitExceeded
	case errors.Is(err, semanticformat.ErrCorrupt), errors.Is(err, semanticformat.ErrCorruptData):
		return ErrCorrupt
	default:
		return err
	}
}

type contextWriter struct {
	ctx    context.Context
	writer io.Writer
}

func (w contextWriter) Write(data []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return w.writer.Write(data)
}
