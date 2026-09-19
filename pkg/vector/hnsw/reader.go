package hnsw

import (
	"context"
	"fmt"
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

// Reader is an immutable HNSW topology paired with an authoritative vector
// source. The source is never mutated by Reader.
type Reader struct {
	topology
	source vector.PreparedVectorSource
}

// newReader binds an immutable topology to an authoritative prepared vector
// source. The source must have the same ordinal layout and vector metadata as
// the topology.
func newReader(source vector.PreparedVectorSource, topology *topology) (*Reader, error) {
	if source == nil || isNilPreparedVectorSource(source) || topology == nil || !topology.validated {
		return nil, ErrBuildSourceMismatch
	}
	if source.Len() != len(topology.nodeToVector) || source.Dimensions() != topology.space.Dimensions() ||
		source.Metric() != topology.space.Metric() || source.Normalization() != topology.space.Normalization() {
		return nil, ErrBuildSourceMismatch
	}
	return &Reader{topology: *topology, source: source}, nil
}

func newReaderFromGraph(space vector.Space, searchConfig SearchConfig, buildInfo BuildInfo, graph graphData, sourceOverride ...vector.PreparedVectorSource) (*Reader, error) {
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
		return newReader(sourceOverride[0], topology)
	}
	return &Reader{topology: *topology, source: &matrixSource{space: space, values: append([]float32(nil), graph.values...)}}, nil
}

func (r *Reader) Search(ctx context.Context, query []float32, k int, options vector.SearchOptions) (vector.SearchResult, error) {
	return search(ctx, r, query, k, options)
}

func (r *Reader) Len() int {
	if r == nil {
		return 0
	}
	return len(r.nodeToVector)
}

func (r *Reader) NodeCount() int { return r.Len() }

func (r *Reader) Dimensions() int {
	if r == nil {
		return 0
	}
	return r.space.Dimensions()
}

func (r *Reader) Metric() vector.Metric {
	if r == nil {
		return 0
	}
	return r.space.Metric()
}

func (r *Reader) Normalization() vector.Normalization {
	if r == nil {
		return 0
	}
	return r.space.Normalization()
}

func (r *Reader) MaxK() int {
	if r == nil {
		return 0
	}
	return r.searchConfig.MaxK
}

// SearchConfig returns the immutable request limits stored with the reader.
func (r *Reader) SearchConfig() SearchConfig {
	if r == nil {
		return SearchConfig{}
	}
	return r.searchConfig
}

// StorageStats reports cardinalities and logical bytes of packed reader slices.
func (r *Reader) StorageStats() StorageStats {
	if r == nil {
		return StorageStats{}
	}
	stats := StorageStats{
		VectorRows:        r.Len(),
		GraphNodes:        r.NodeCount(),
		DirectedLinks:     len(r.level0Neighbors) + len(r.upperNeighbors),
		VectorBytes:       uint64(r.Len()) * uint64(r.Dimensions()) * 4,
		LinkBytes:         uint64(len(r.level0Neighbors)+len(r.upperNeighbors)) * 4,
		NodeMetadataBytes: uint64(len(r.nodeToVector))*4 + uint64(len(r.levels)),
		OffsetBytes:       uint64(len(r.level0Offsets)+len(r.upperNodeOffsets)+len(r.upperLinkOffsets)) * 4,
	}
	for _, count := range r.stats.LevelNodeCounts {
		stats.LevelPlacements += count
	}
	stats.TotalBytes = stats.VectorBytes + stats.NodeMetadataBytes + stats.OffsetBytes + stats.LinkBytes
	return stats
}

func (r *Reader) BuildInfo() BuildInfo {
	if r == nil {
		return BuildInfo{}
	}
	return r.buildInfo
}

func (r *Reader) GraphStats() GraphStats {
	if r == nil {
		return GraphStats{}
	}
	return cloneGraphStats(r.stats)
}

func (r *Reader) EntryPoint() (NodeOrdinal, int, bool) {
	if r == nil || !r.hasEntry {
		return 0, 0, false
	}
	return r.entry, int(r.levels[r.entry]), true
}

func (r *Reader) NodeLevel(node NodeOrdinal) (uint8, bool) {
	if r == nil || uint64(node) >= uint64(len(r.levels)) {
		return 0, false
	}
	return r.levels[node], true
}

// Neighbors returns a copy of one directed adjacency list. A node with an empty
// list still exists at level when level <= NodeLevel(node).
func (r *Reader) Neighbors(node NodeOrdinal, level int) ([]NodeOrdinal, bool) {
	neighbors, ok := r.neighborView(node, level)
	return append([]NodeOrdinal(nil), neighbors...), ok
}

// Vector returns a copy of one prepared vector row.
func (r *Reader) Vector(ordinal vector.Ordinal) ([]float32, bool) {
	if r == nil || uint64(ordinal) >= uint64(r.Len()) {
		return nil, false
	}
	value := make([]float32, r.Dimensions())
	if err := r.ReadVectorInto(context.Background(), ordinal, value); err != nil {
		return nil, false
	}
	return value, true
}

// ReadVectorInto copies one prepared vector row into dst.
func (r *Reader) ReadVectorInto(ctx context.Context, ordinal vector.Ordinal, dst []float32) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil {
		return ErrInvalidGraph
	}
	if len(dst) != r.Dimensions() {
		return fmt.Errorf("%w: got %d, want %d", vector.ErrDimensionMismatch, len(dst), r.Dimensions())
	}
	if uint64(ordinal) >= uint64(r.Len()) {
		return fmt.Errorf("%w: %d", vector.ErrOrdinalOutOfRange, ordinal)
	}
	if r.source == nil {
		return ErrInvalidGraph
	}
	return r.source.ReadVectorInto(ctx, ordinal, dst)
}

func (r *Reader) vectorByNode(node NodeOrdinal) ([]float32, vector.Ordinal, bool) {
	if r == nil || uint64(node) >= uint64(len(r.nodeToVector)) {
		return nil, 0, false
	}
	ordinal := r.nodeToVector[node]
	value, ok := r.Vector(ordinal)
	return value, ordinal, ok
}

type matrixSource struct {
	space  vector.Space
	values []float32
}

func (s *matrixSource) Len() int                            { return len(s.values) / s.space.Dimensions() }
func (s *matrixSource) Dimensions() int                     { return s.space.Dimensions() }
func (s *matrixSource) Metric() vector.Metric               { return s.space.Metric() }
func (s *matrixSource) Normalization() vector.Normalization { return s.space.Normalization() }
func (s *matrixSource) ReadVectorInto(ctx context.Context, ordinal vector.Ordinal, dst []float32) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(dst) != s.Dimensions() {
		return fmt.Errorf("%w: got %d, want %d", vector.ErrDimensionMismatch, len(dst), s.Dimensions())
	}
	if uint64(ordinal) >= uint64(s.Len()) {
		return fmt.Errorf("%w: %d", vector.ErrOrdinalOutOfRange, ordinal)
	}
	start := int(ordinal) * s.Dimensions()
	copy(dst, s.values[start:start+s.Dimensions()])
	return nil
}

func (r *Reader) neighborView(node NodeOrdinal, level int) ([]NodeOrdinal, bool) {
	if r == nil || level < 0 || uint64(node) >= uint64(len(r.levels)) || level > int(r.levels[node]) {
		return nil, false
	}
	if level == 0 {
		start := r.level0Offsets[node]
		end := r.level0Offsets[int(node)+1]
		return r.level0Neighbors[start:end], true
	}
	// Upper placements are stored consecutively per node; level 1 is the first.
	placement := r.upperNodeOffsets[node] + uint32(level-1)
	start := r.upperLinkOffsets[placement]
	end := r.upperLinkOffsets[placement+1]
	return r.upperNeighbors[start:end], true
}

func cloneGraphStats(stats GraphStats) GraphStats {
	stats.LevelNodeCounts = append([]int(nil), stats.LevelNodeCounts...)
	stats.LevelLinkCounts = append([]int(nil), stats.LevelLinkCounts...)
	return stats
}
