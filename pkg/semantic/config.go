package semantic

import "github.com/dariasmyr/fts-engine/pkg/vector/hnsw"

type Config struct {
	Embedding EmbeddingDescriptor
	Chunking  ChunkingDescriptor
	// MaxVectors limits the number of live vectors accepted by the service.
	// Replaced and deleted physical rows remain until Compact.
	MaxVectors              int
	MaxChunksPerDocument    int
	MaxK                    int
	MaxChunkCandidates      int
	MaxChunksPerDocumentHit int
	InitialVectorCapacity   int
	HNSWBuild               hnsw.BuildConfig
	HNSWSearch              hnsw.SearchConfig
	// InitialMaxAllocatedVectorID seeds the allocator; the first new vector gets
	// the following ID. Use it when continuing an existing ID namespace.
	InitialMaxAllocatedVectorID VectorID
}

func (c Config) Validate() error {
	if !c.Embedding.IsValid() || !c.Chunking.IsValid() ||
		c.MaxVectors <= 0 || c.MaxChunksPerDocument <= 0 || c.MaxK <= 0 ||
		c.MaxChunkCandidates < c.MaxK || c.MaxChunkCandidates > c.MaxVectors ||
		c.MaxChunksPerDocument > c.MaxVectors || c.MaxChunksPerDocumentHit <= 0 ||
		c.MaxChunksPerDocumentHit > c.MaxChunksPerDocument || c.InitialVectorCapacity < 0 ||
		c.InitialVectorCapacity > c.MaxVectors {
		return ErrInvalidConfig
	}
	return nil
}

// SearchPolicy contains the immutable limits required to search and group a
// read view. It is separate from request-local SearchOptions.
type SearchPolicy struct {
	MaxK                    int
	MaxChunkCandidates      int
	MaxChunksPerDocumentHit int
}

func (p SearchPolicy) Validate() error {
	if p.MaxK <= 0 || p.MaxChunkCandidates < p.MaxK || p.MaxChunksPerDocumentHit <= 0 {
		return ErrInvalidConfig
	}
	return nil
}
