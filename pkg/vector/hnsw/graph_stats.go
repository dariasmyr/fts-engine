package hnsw

// BuildInfo records graph-construction provenance. Exact links, not provenance,
// remain authoritative after freezing or persistence.
type BuildInfo struct {
	BuildVersion          uint32
	LevelGeneratorVersion uint32
	MaxNeighbors          int
	LevelZeroMaxNeighbors int
	EfConstruction        int
	Seed                  uint64
}

// GraphStats reports structural cardinalities and directed level-0 reachability.
type GraphStats struct {
	NodeCount        int
	VectorCount      int
	MaxLevel         int
	LevelNodeCounts  []int
	LevelLinkCounts  []int
	ReachableNodes   int
	UnreachableNodes int
	ZeroDegreeNodes  int
}

// StorageStats reports logical in-memory cardinalities and packed byte counts.
type StorageStats struct {
	VectorRows        int
	GraphNodes        int
	LevelPlacements   int
	DirectedLinks     int
	VectorBytes       uint64
	NodeMetadataBytes uint64
	OffsetBytes       uint64
	LinkBytes         uint64
	TotalBytes        uint64
}

// Report is one immutable snapshot of index configuration and diagnostics.
type Report struct {
	Build   BuildInfo
	Search  SearchConfig
	Graph   GraphStats
	Storage StorageStats
}

func (i BuildInfo) neighborLimit(level int) int {
	if level == 0 {
		return i.LevelZeroMaxNeighbors
	}
	return i.MaxNeighbors
}
