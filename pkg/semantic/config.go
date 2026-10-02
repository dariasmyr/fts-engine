package semantic

import (
	"math"

	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

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
	InitialMaxAllocatedVectorID uint64
}

func (c Config) Validate() error {
	_, err := c.normalized()
	return err
}

func (c Config) normalized() (Config, error) {
	if !c.Embedding.IsValid() || !c.Chunking.IsValid() ||
		c.MaxVectors <= 0 || c.MaxChunksPerDocument <= 0 || c.MaxK <= 0 ||
		c.MaxChunkCandidates < c.MaxK || c.MaxChunkCandidates > c.MaxVectors ||
		c.MaxChunksPerDocument > c.MaxVectors || c.MaxChunksPerDocumentHit <= 0 ||
		c.MaxChunksPerDocumentHit > c.MaxChunksPerDocument || c.InitialVectorCapacity < 0 ||
		c.InitialVectorCapacity > c.MaxVectors {
		return Config{}, ErrInvalidConfig
	}
	c.HNSWBuild = normalizeBuildConfig(c.HNSWBuild, c.Embedding, c.MaxVectors)
	c.HNSWSearch = normalizeSearchConfig(c.HNSWSearch, c.MaxK, c.MaxChunkCandidates, c.MaxVectors)
	if c.HNSWBuild.Dimensions != c.Embedding.Dimensions || c.HNSWBuild.Metric != c.Embedding.Metric ||
		c.HNSWBuild.MaxVectors < c.MaxVectors || c.HNSWSearch.MaxK < c.MaxChunkCandidates ||
		c.HNSWSearch.MaxEfSearch < c.MaxChunkCandidates {
		return Config{}, ErrInvalidConfig
	}
	dimensions := uint64(c.Embedding.Dimensions)
	if dimensions > math.MaxUint64/4 || uint64(c.MaxVectors) > math.MaxUint64/(dimensions*4) ||
		c.HNSWBuild.MaxVectorBytes < uint64(c.MaxVectors)*dimensions*4 {
		return Config{}, ErrInvalidConfig
	}
	if err := (hnsw.BuildOptions{Build: c.HNSWBuild, Search: c.HNSWSearch}).Validate(c.MaxVectors); err != nil {
		return Config{}, ErrInvalidConfig
	}
	return c, nil
}

func normalizeBuildConfig(config hnsw.BuildConfig, embedding EmbeddingDescriptor, maxVectors int) hnsw.BuildConfig {
	if config.Dimensions == 0 {
		config.Dimensions = embedding.Dimensions
	}
	if config.Metric == 0 {
		config.Metric = embedding.Metric
	}
	if config.MaxVectors == 0 {
		config.MaxVectors = maxVectors
	}
	dimensions := uint64(embedding.Dimensions)
	if config.MaxVectorBytes == 0 && embedding.Dimensions > 0 && dimensions <= math.MaxUint64/4 && uint64(maxVectors) <= math.MaxUint64/(dimensions*4) {
		config.MaxVectorBytes = uint64(maxVectors) * uint64(embedding.Dimensions) * 4
	}
	if config.MaxNeighbors == 0 {
		config.MaxNeighbors = 16
	}
	if config.EfConstruction == 0 {
		config.EfConstruction = max(64, config.MaxNeighbors)
	}
	return config
}

func normalizeSearchConfig(config hnsw.SearchConfig, maxK, maxCandidates, maxVectors int) hnsw.SearchConfig {
	if config.DefaultEfSearch == 0 {
		config.DefaultEfSearch = max(maxK, min(maxCandidates, 64))
	}
	if config.MaxEfSearch == 0 {
		config.MaxEfSearch = max(maxCandidates, config.DefaultEfSearch)
	}
	if config.DefaultVisitLimit == 0 {
		config.DefaultVisitLimit = maxVectors
	}
	if config.MaxVisitLimit == 0 {
		config.MaxVisitLimit = maxVectors
	}
	if config.MaxK == 0 {
		config.MaxK = maxCandidates
	}
	return config
}

// searchPolicy contains the immutable limits required to search and group a
// read view. It is separate from request-local SearchOptions.
type searchPolicy struct {
	MaxK                    int
	MaxChunkCandidates      int
	MaxChunksPerDocumentHit int
}

func (p searchPolicy) validate() error {
	if p.MaxK <= 0 || p.MaxChunkCandidates < p.MaxK || p.MaxChunksPerDocumentHit <= 0 {
		return ErrInvalidConfig
	}
	return nil
}
