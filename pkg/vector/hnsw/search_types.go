package hnsw

import "github.com/dariasmyr/fts-engine/pkg/vector"

type searchCandidate struct {
	node          NodeOrdinal
	vectorOrdinal vector.Ordinal
	distance      float64
	accepted      bool
}
