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

func mergeStats(a, b Stats) Stats {
	out := Stats{
		Nodes:         a.Nodes + b.Nodes,
		Leaves:        a.Leaves + b.Leaves,
		TotalDocs:     a.TotalDocs + b.TotalDocs,
		TotalChildren: a.TotalChildren + b.TotalChildren,
	}

	if a.MaxDepth > b.MaxDepth {
		out.MaxDepth = a.MaxDepth
	} else {
		out.MaxDepth = b.MaxDepth
	}

	total := a.Nodes + b.Nodes
	if total > 0 {
		out.AvgDepth = (a.AvgDepth*float64(a.Nodes) + b.AvgDepth*float64(b.Nodes)) / float64(total)
	}

	if a.Nodes >= b.Nodes {
		out.AvgChildrenPerLevel = a.AvgChildrenPerLevel
	} else {
		out.AvgChildrenPerLevel = b.AvgChildrenPerLevel
	}

	return out
}
