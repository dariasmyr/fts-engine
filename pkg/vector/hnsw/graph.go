package hnsw

type buildNeighbor struct {
	node nodeOrdinal
	// distance is measured from the mutable node that owns this link.
	distance float64
}

type mutableNode struct {
	level uint8
	links [][]buildNeighbor
	// verifiedLinkBits is a build-only bit set aligned with links at every level.
	// It is deliberately omitted when graphData is packed into topology.
	verifiedLinkBits []uint64
}

type graphData struct {
	nodes    []mutableNode
	entry    nodeOrdinal
	hasEntry bool
}
