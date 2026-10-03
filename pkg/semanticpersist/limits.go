package semanticpersist

type DurabilityMode uint8

const (
	// DurabilitySynchronous fsyncs published files and affected directories.
	DurabilitySynchronous DurabilityMode = iota + 1
	// DurabilityAsynchronous preserves atomic visibility but does not promise
	// that the latest generation survives sudden power loss.
	DurabilityAsynchronous
)

type Limits struct {
	MaxFileBytes         uint64
	MaxVectorBytes       uint64
	MaxGraphBytes        uint64
	MaxGraphLinks        uint64
	MaxOpenBytes         uint64
	MaxEfSearch          int
	MaxVisitLimit        int
	MaxDimensions        int
	MaxVectors           int
	MaxDocuments         int
	MaxStringBytes       int
	MaxChunksPerDocument int
	MaxK                 int
}

func DefaultLimits() Limits {
	return Limits{
		MaxFileBytes: 512 << 20, MaxVectorBytes: 512 << 20, MaxGraphBytes: 512 << 20, MaxGraphLinks: 100_000_000, MaxOpenBytes: 1 << 30,
		MaxEfSearch: 1_000_000, MaxVisitLimit: 10_000_000, MaxDimensions: 65_536,
		MaxVectors: 10_000_000, MaxDocuments: 10_000_000, MaxStringBytes: 1 << 20,
		MaxChunksPerDocument: 1_000_000, MaxK: 1_000_000,
	}
}
