package hnsw

type mutableNode struct {
	level uint8
	links [][]nodeOrdinal
}

type graphData struct {
	values   []float32
	nodes    []mutableNode
	entry    nodeOrdinal
	hasEntry bool
}
