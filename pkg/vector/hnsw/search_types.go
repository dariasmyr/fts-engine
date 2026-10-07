package hnsw

type searchCandidate struct {
	node     nodeOrdinal
	distance float64
	accepted bool
}
