package fts

type Stats struct {
	Nodes               int
	Leaves              int
	MaxDepth            int
	AvgDepth            float64
	TotalDocs           int
	AvgChildrenPerLevel []float64
	TotalChildren       int
}
