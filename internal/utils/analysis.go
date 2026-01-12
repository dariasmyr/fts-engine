package utils

import "runtime"

type TrieStats struct {
	Nodes               int
	Leaves              int
	MaxDepth            int
	AvgDepth            float64
	TotalDocs           int
	AvgChildrenPerLevel []float64 // average (not nil for trigram) children count per level
	TotalChildren       int
}

func MeasureMemory(build func()) runtime.MemStats {
	runtime.GC()
	runtime.GC()

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	build()

	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&after)

	after.HeapAlloc -= before.HeapAlloc
	after.TotalAlloc -= before.TotalAlloc
	after.HeapObjects -= before.HeapObjects

	return after
}
