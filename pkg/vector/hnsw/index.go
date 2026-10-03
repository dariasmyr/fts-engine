package hnsw

import (
	"context"
	"math"
	"reflect"

	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vectorstore"
)

type topology struct {
	calculator   vector.Calculator
	searchConfig SearchConfig
	buildInfo    BuildInfo
	stats        GraphStats
	validated    bool

	nodeToVector []vector.Ordinal
	levels       []uint8
	entry        nodeOrdinal
	hasEntry     bool

	level0Offsets   []uint32
	level0Neighbors []nodeOrdinal

	upperNodeOffsets []uint32
	upperLinkOffsets []uint32
	upperNeighbors   []nodeOrdinal
}

// Index is an immutable HNSW topology paired with authoritative vector storage.
// The vector storage is retained for search and is never mutated by Index.
type Index struct {
	topology   topology
	vectors    vectorstore.PreparedVectorStore
	workspaces *searchWorkspacePool
}

// newHNSWIndex binds an immutable topology to authoritative prepared vector
// storage. The storage must have the same ordinal layout and vector metadata as
// the topology.
func newIndex(vectors vectorstore.PreparedVectorStore, topology topology) (*Index, error) {
	if vectors == nil || isNilPreparedVectorStore(vectors) || !topology.validated {
		return nil, ErrBuildSourceMismatch
	}
	if vectors.Len() != len(topology.nodeToVector) || vectors.Dimensions() != topology.calculator.Dimensions() ||
		vectors.Metric() != topology.calculator.Metric() || vectors.Normalization() != topology.calculator.Normalization() {
		return nil, ErrBuildSourceMismatch
	}
	return &Index{topology: topology, vectors: vectors, workspaces: newSearchWorkspacePool()}, nil
}

func newIndexFromGraph(calculator vector.Calculator, searchConfig SearchConfig, buildInfo BuildInfo, graph graphData, source vectorstore.PreparedVectorStore) (*Index, error) {
	if err := searchConfig.validate(); err != nil {
		return nil, err
	}
	stats, err := validateGraphData(calculator, buildInfo, graph)
	if err != nil {
		return nil, err
	}
	topology := &topology{
		calculator: calculator, searchConfig: searchConfig, buildInfo: buildInfo, stats: cloneGraphStats(stats),
		levels:       make([]uint8, len(graph.nodes)),
		nodeToVector: make([]vector.Ordinal, len(graph.nodes)), entry: graph.entry, hasEntry: graph.hasEntry,
		level0Offsets: make([]uint32, len(graph.nodes)+1), upperNodeOffsets: make([]uint32, len(graph.nodes)+1),
	}

	// Count first so every packed section is allocated once and all uint32
	// offsets are proven representable before conversion.
	var level0Count uint64
	var upperPlacementCount uint64
	var upperNeighborCount uint64
	for nodeOrdinal, node := range graph.nodes {
		topology.levels[nodeOrdinal] = node.level
		topology.nodeToVector[nodeOrdinal] = node.vectorOrdinal
		topology.level0Offsets[nodeOrdinal] = uint32(level0Count)
		level0Count += uint64(len(node.links[0]))
		topology.upperNodeOffsets[nodeOrdinal] = uint32(upperPlacementCount)
		upperPlacementCount += uint64(node.level)
		for level := 1; level <= int(node.level); level++ {
			upperNeighborCount += uint64(len(node.links[level]))
		}
		if level0Count >= math.MaxUint32 || upperPlacementCount >= math.MaxUint32 || upperNeighborCount >= math.MaxUint32 {
			return nil, errInvalidGraph
		}
	}
	topology.level0Offsets[len(graph.nodes)] = uint32(level0Count)
	topology.upperNodeOffsets[len(graph.nodes)] = uint32(upperPlacementCount)
	topology.level0Neighbors = make([]nodeOrdinal, 0, int(level0Count))
	topology.upperLinkOffsets = make([]uint32, int(upperPlacementCount)+1)
	topology.upperNeighbors = make([]nodeOrdinal, 0, int(upperNeighborCount))

	// Level 0 has one adjacency placement per node. Upper levels are sparse, so
	// upperNodeOffsets maps a node to its consecutive levels 1..nodeLevel(node).
	placement := 0
	for _, node := range graph.nodes {
		topology.level0Neighbors = append(topology.level0Neighbors, node.links[0]...)
		for level := 1; level <= int(node.level); level++ {
			topology.upperLinkOffsets[placement] = uint32(len(topology.upperNeighbors))
			topology.upperNeighbors = append(topology.upperNeighbors, node.links[level]...)
			placement++
		}
	}
	topology.upperLinkOffsets[placement] = uint32(len(topology.upperNeighbors))
	topology.validated = true
	return newIndex(source, *topology)
}

func (r *Index) Search(ctx context.Context, query []float32, k int, options vector.SearchOptions) (vector.SearchResult, error) {
	return search(ctx, r, query, k, options)
}

func (r *Index) Len() int {
	if r == nil {
		return 0
	}
	return len(r.topology.nodeToVector)
}

func (r *Index) Dimensions() int {
	if r == nil {
		return 0
	}
	return r.topology.calculator.Dimensions()
}

func (r *Index) Metric() vector.Metric {
	if r == nil {
		return 0
	}
	return r.topology.calculator.Metric()
}

// ValidateSource verifies that source is the prepared vector store bound to the
// index, or contains exactly the same prepared rows.
func (r *Index) ValidateSource(ctx context.Context, source vectorstore.PreparedVectorStore) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil || source == nil || isNilPreparedVectorStore(source) ||
		source.Len() != r.Len() || source.Dimensions() != r.Dimensions() ||
		source.Metric() != r.Metric() || source.Normalization() != r.topology.calculator.Normalization() {
		return ErrBuildSourceMismatch
	}
	left, right := reflect.ValueOf(r.vectors), reflect.ValueOf(source)
	if left.Type() == right.Type() && left.Kind() == reflect.Pointer && left.Pointer() == right.Pointer() {
		return nil
	}
	bound := make([]float32, r.Dimensions())
	candidate := make([]float32, r.Dimensions())
	for row := range r.Len() {
		if err := ctx.Err(); err != nil {
			return err
		}
		ordinal := vector.Ordinal(row)
		if err := r.vectors.ReadVectorInto(ctx, ordinal, bound); err != nil {
			return err
		}
		if err := source.ReadVectorInto(ctx, ordinal, candidate); err != nil {
			return err
		}
		for component := range bound {
			if math.Float32bits(bound[component]) != math.Float32bits(candidate[component]) {
				return ErrBuildSourceMismatch
			}
		}
	}
	return nil
}

func (r *Index) storageStats() StorageStats {
	if r == nil {
		return StorageStats{}
	}
	stats := StorageStats{
		VectorRows:        r.Len(),
		GraphNodes:        r.Len(),
		DirectedLinks:     len(r.topology.level0Neighbors) + len(r.topology.upperNeighbors),
		VectorBytes:       uint64(r.Len()) * uint64(r.Dimensions()) * 4,
		LinkBytes:         uint64(len(r.topology.level0Neighbors)+len(r.topology.upperNeighbors)) * 4,
		NodeMetadataBytes: uint64(len(r.topology.nodeToVector))*4 + uint64(len(r.topology.levels)),
		OffsetBytes:       uint64(len(r.topology.level0Offsets)+len(r.topology.upperNodeOffsets)+len(r.topology.upperLinkOffsets)) * 4,
	}
	for _, count := range r.topology.stats.LevelNodeCounts {
		stats.LevelPlacements += count
	}
	stats.TotalBytes = stats.VectorBytes + stats.NodeMetadataBytes + stats.OffsetBytes + stats.LinkBytes
	return stats
}

// Report returns the index build provenance, search limits, graph health, and
// logical storage cardinalities as one diagnostic snapshot.
func (r *Index) Report() Report {
	if r == nil {
		return Report{}
	}
	return Report{
		Build:   r.topology.buildInfo,
		Search:  r.topology.searchConfig,
		Graph:   cloneGraphStats(r.topology.stats),
		Storage: r.storageStats(),
	}
}

func (r *Index) entryPoint() (nodeOrdinal, int, bool) {
	if r == nil || !r.topology.hasEntry {
		return 0, 0, false
	}
	return r.topology.entry, int(r.topology.levels[r.topology.entry]), true
}

func (r *Index) nodeLevel(node nodeOrdinal) (uint8, bool) {
	if r == nil || uint64(node) >= uint64(len(r.topology.levels)) {
		return 0, false
	}
	return r.topology.levels[node], true
}

func (r *Index) neighbors(node nodeOrdinal, level int) ([]nodeOrdinal, bool) {
	neighbors, ok := r.neighborView(node, level)
	return append([]nodeOrdinal(nil), neighbors...), ok
}

func (r *Index) vectorByNode(node nodeOrdinal) ([]float32, vector.Ordinal, bool) {
	if r == nil || uint64(node) >= uint64(len(r.topology.nodeToVector)) {
		return nil, 0, false
	}
	ordinal := r.topology.nodeToVector[node]
	value := make([]float32, r.Dimensions())
	if err := r.vectors.ReadVectorInto(context.Background(), ordinal, value); err != nil {
		return nil, 0, false
	}
	return value, ordinal, true
}

func (r *Index) neighborView(node nodeOrdinal, level int) ([]nodeOrdinal, bool) {
	if r == nil || level < 0 || uint64(node) >= uint64(len(r.topology.levels)) || level > int(r.topology.levels[node]) {
		return nil, false
	}
	if level == 0 {
		start := r.topology.level0Offsets[node]
		end := r.topology.level0Offsets[int(node)+1]
		return r.topology.level0Neighbors[start:end], true
	}
	// Upper placements are stored consecutively per node; level 1 is the first.
	placement := r.topology.upperNodeOffsets[node] + uint32(level-1)
	start := r.topology.upperLinkOffsets[placement]
	end := r.topology.upperLinkOffsets[placement+1]
	return r.topology.upperNeighbors[start:end], true
}

func cloneGraphStats(stats GraphStats) GraphStats {
	stats.LevelNodeCounts = append([]int(nil), stats.LevelNodeCounts...)
	stats.LevelLinkCounts = append([]int(nil), stats.LevelLinkCounts...)
	return stats
}
