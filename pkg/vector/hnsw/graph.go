package hnsw

type mutableNode struct {
	level uint8
	links [][]nodeOrdinal
	// verifiedLinkBits is a build-only bit set aligned with links at every level.
	// It is deliberately omitted when graphData is packed into topology.
	verifiedLinkBits []uint64
}

type graphData struct {
	values   []float32
	nodes    []mutableNode
	entry    nodeOrdinal
	hasEntry bool
}
