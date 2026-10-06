package hnsw

// nodeOrdinal names a dense graph-local node number. The alias lets packed
// topology share the wire format's uint32 arrays without conversions.
type nodeOrdinal = uint32
