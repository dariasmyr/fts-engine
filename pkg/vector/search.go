package vector

// Hit is one vector-search result. Hits are ordered by ascending Distance and
// then ascending Ordinal.
type Hit struct {
	Ordinal  Ordinal
	Distance float64
}

// ResultFilter is an immutable snapshot controlling which local ordinals may
// appear in search results.
type ResultFilter interface {
	Allows(Ordinal) bool
	AllowedOrdinalCount() int
	TotalOrdinalCount() uint32
}
