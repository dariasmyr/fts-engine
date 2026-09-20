package hnsw

import (
	"context"
	"math"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

type topology struct {
	space        vector.Space
	searchConfig SearchConfig
	buildInfo    BuildInfo
	stats        GraphStats
	validated    bool

	nodeToVector []vector.Ordinal
	levels       []uint8
	entry        NodeOrdinal
	hasEntry     bool

	level0Offsets   []uint32
	level0Neighbors []NodeOrdinal

	upperNodeOffsets []uint32
	upperLinkOffsets []uint32
	upperNeighbors   []NodeOrdinal
}

// Searcher is an immutable HNSW search index paired with an authoritative
// vector source. The source is never mutated by Searcher.
type Searcher struct {
	topology topology
	vectors  vector.PreparedVectorSource
}

// newSearcher binds an immutable topology to an authoritative prepared vector
// source. The source must have the same ordinal layout and vector metadata as
// the topology.
func newSearcher(source vector.PreparedVectorSource, topology topology) (*Searcher, error) {
	if source == nil || isNilPreparedVectorSource(source) || !topology.validated {
		return nil, ErrBuildSourceMismatch
	}
	if source.Len() != len(topology.nodeToVector) || source.Dimensions() != topology.space.Dimensions() ||
		source.Metric() != topology.space.Metric() || source.Normalization() != topology.space.Normalization() {
		return nil, ErrBuildSourceMismatch
	}
	return &Searcher{topology: topology, vectors: source}, nil
}

func newSearcherFromGraph(space vector.Space, searchConfig SearchConfig, buildInfo BuildInfo, graph graphData, sourceOverride ...vector.PreparedVectorSource) (*Searcher, error) {
	if err := searchConfig.validate(); err != nil {
		return nil, err
	}
	stats, err := validateGraphData(space, buildInfo, graph)
	if err != nil {
		return nil, err
	}
	topology := &topology{
		space: space, searchConfig: searchConfig, buildInfo: buildInfo, stats: cloneGraphStats(stats),
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
			return nil, ErrInvalidGraph
		}
	}
	topology.level0Offsets[len(graph.nodes)] = uint32(level0Count)
	topology.upperNodeOffsets[len(graph.nodes)] = uint32(upperPlacementCount)
	topology.level0Neighbors = make([]NodeOrdinal, 0, int(level0Count))
	topology.upperLinkOffsets = make([]uint32, int(upperPlacementCount)+1)
	topology.upperNeighbors = make([]NodeOrdinal, 0, int(upperNeighborCount))

	// Level 0 has one adjacency placement per node. Upper levels are sparse, so
	// upperNodeOffsets maps a node to its consecutive levels 1..NodeLevel(node).
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
	if len(sourceOverride) > 0 && sourceOverride[0] != nil && !isNilPreparedVectorSource(sourceOverride[0]) {
		return newSearcher(sourceOverride[0], *topology)
	}
	source, err := vector.NewPreparedMemorySource(space, graph.values)
	if err != nil {
		return nil, err
	}
	return &Searcher{topology: *topology, vectors: source}, nil
}

func (r *Searcher) Search(ctx context.Context, query []float32, k int, options vector.SearchOptions) (vector.SearchResult, error) {
	return search(ctx, r, query, k, options)
}

func (r *Searcher) Len() int {
	if r == nil {
		return 0
	}
	return len(r.topology.nodeToVector)
}

func (r *Searcher) NodeCount() int { return r.Len() }

func (r *Searcher) Dimensions() int {
	if r == nil {
		return 0
	}
	return r.topology.space.Dimensions()
}

func (r *Searcher) Metric() vector.Metric {
	if r == nil {
		return 0
	}
	return r.topology.space.Metric()
}

func (r *Searcher) Normalization() vector.Normalization {
	if r == nil {
		return 0
	}
	return r.topology.space.Normalization()
}

func (r *Searcher) MaxK() int {
	if r == nil {
		return 0
	}
	return r.topology.searchConfig.MaxK
}

// SearchConfig returns the immutable request limits stored with the reader.
func (r *Searcher) SearchConfig() SearchConfig {
	if r == nil {
		return SearchConfig{}
	}
	return r.topology.searchConfig
}

// StorageStats reports cardinalities and logical bytes of packed reader slices.
func (r *Searcher) StorageStats() StorageStats {
	if r == nil {
		return StorageStats{}
	}
	stats := StorageStats{
		VectorRows:        r.Len(),
		GraphNodes:        r.NodeCount(),
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

func (r *Searcher) BuildInfo() BuildInfo {
	if r == nil {
		return BuildInfo{}
	}
	return r.topology.buildInfo
}

func (r *Searcher) GraphStats() GraphStats {
	if r == nil {
		return GraphStats{}
	}
	return cloneGraphStats(r.topology.stats)
}

func (r *Searcher) EntryPoint() (NodeOrdinal, int, bool) {
	if r == nil || !r.topology.hasEntry {
		return 0, 0, false
	}
	return r.topology.entry, int(r.topology.levels[r.topology.entry]), true
}

func (r *Searcher) NodeLevel(node NodeOrdinal) (uint8, bool) {
	if r == nil || uint64(node) >= uint64(len(r.topology.levels)) {
		return 0, false
	}
	return r.topology.levels[node], true
}

// Neighbors returns a copy of one directed adjacency list. A node with an empty
// list still exists at level when level <= NodeLevel(node).
func (r *Searcher) Neighbors(node NodeOrdinal, level int) ([]NodeOrdinal, bool) {
	neighbors, ok := r.neighborView(node, level)
	return append([]NodeOrdinal(nil), neighbors...), ok
}

// VectorSource returns the authoritative vector storage paired with the
// index. The source is immutable and is never mutated by Searcher.
func (r *Searcher) VectorSource() vector.PreparedVectorSource {
	if r == nil {
		return nil
	}
	return r.vectors
}

func (r *Searcher) vectorByNode(node NodeOrdinal) ([]float32, vector.Ordinal, bool) {
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

func (r *Searcher) neighborView(node NodeOrdinal, level int) ([]NodeOrdinal, bool) {
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
