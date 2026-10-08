package semantic

import (
	"math"

	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

const (
	maxHNSWNeighbors      = 1024
	maxHNSWEfConstruction = 1_000_000
)

// Config defines the semantic pipeline, service limits, and optional HNSW
// tuning. New derives vector-space and allocation settings from these values.
type Config struct {
	// Schema is the canonical compatibility contract of the semantic index.
	Schema Schema

	// Embedding and Chunking are retained for source compatibility. normalized
	// canonicalizes them with Schema; new code should populate Schema instead.
	Embedding EmbeddingDescriptor
	Chunking  ChunkingDescriptor

	Limits Limits
	HNSW   HNSWTuning
}

// Limits bounds semantic document lifecycle and result grouping.
type Limits struct {
	// MaxLiveVectors bounds visible vectors. Stale physical rows remain until
	// compaction.
	MaxLiveVectors int
	// MaxStaleVectors bounds superseded physical rows between compactions. Zero
	// defaults to MaxLiveVectors.
	MaxStaleVectors int
	// MaxSegments bounds committed immutable segments. Zero defaults to 16.
	MaxSegments int
	// MaxChunksPerDocument bounds one encoded document mutation or query.
	MaxChunksPerDocument int
	// MaxDocumentsPerSearch bounds the public document result count.
	MaxDocumentsPerSearch int
	// MaxChunkCandidates bounds chunk hits collected before document grouping.
	MaxChunkCandidates int
	// MaxChunksPerDocumentHit bounds explanatory chunks attached to one result.
	MaxChunksPerDocumentHit int
}

// HNSWTuning controls graph quality and request work. Zero fields select
// defaults derived from Limits and the embedding descriptor.
type HNSWTuning struct {
	// MaxNeighbors and EfConstruction control graph build quality and cost.
	MaxNeighbors   int
	EfConstruction int
	// Seed makes graph construction deterministic for a fixed insertion order.
	Seed uint64
	// DefaultEfSearch and MaxEfSearch control candidate breadth per request.
	DefaultEfSearch int
	MaxEfSearch     int
	// DefaultVisitLimit and MaxVisitLimit bound scored graph nodes per request.
	DefaultVisitLimit int
	MaxVisitLimit     int
}

func (c Config) Validate() error {
	_, err := c.normalized()
	return err
}

func (c Config) normalized() (Config, error) {
	schema, err := c.resolvedSchema()
	if err != nil {
		return Config{}, err
	}

	c.Schema = schema
	c.Embedding = schema.Embedding
	c.Chunking = schema.Chunking

	limits := c.Limits
	if limits.MaxStaleVectors == 0 {
		limits.MaxStaleVectors = limits.MaxLiveVectors
	}
	if limits.MaxSegments == 0 {
		limits.MaxSegments = 16
	}
	c.Limits = limits
	if !schema.IsValid() ||
		limits.MaxLiveVectors <= 0 ||
		limits.MaxStaleVectors < 0 ||
		limits.MaxSegments <= 0 ||
		limits.MaxChunksPerDocument <= 0 ||
		limits.MaxDocumentsPerSearch <= 0 ||
		limits.MaxChunkCandidates < limits.MaxDocumentsPerSearch ||
		limits.MaxChunkCandidates > limits.MaxLiveVectors ||
		limits.MaxChunksPerDocument > limits.MaxLiveVectors ||
		limits.MaxChunksPerDocumentHit <= 0 ||
		limits.MaxChunksPerDocumentHit > limits.MaxChunksPerDocument {
		return Config{}, ErrInvalidConfig
	}

	c.HNSW = normalizeHNSWTuning(c.HNSW, limits)
	if c.HNSW.MaxEfSearch <= 0 || c.HNSW.MaxVisitLimit <= 0 ||
		c.HNSW.DefaultEfSearch > c.HNSW.MaxEfSearch ||
		c.HNSW.DefaultVisitLimit > c.HNSW.MaxVisitLimit {
		return Config{}, ErrInvalidConfig
	}

	buildConfig, ok := c.hnswBuildConfig()
	if !ok {
		return Config{}, ErrInvalidConfig
	}

	searchConfig, ok := c.hnswSearchConfig()
	if !ok {
		return Config{}, ErrInvalidConfig
	}

	if err := validateHNSWBuildConfig(buildConfig); err != nil {
		return Config{}, ErrInvalidConfig
	}

	if err := validateHNSWSearchConfig(searchConfig); err != nil {
		return Config{}, ErrInvalidConfig
	}

	maxInt := int(^uint(0) >> 1)
	if limits.MaxLiveVectors > maxInt-limits.MaxStaleVectors ||
		!validVectorCapacity(c.Schema.Embedding.Dimensions, limits.MaxLiveVectors+limits.MaxStaleVectors) {
		return Config{}, ErrInvalidConfig
	}

	return c, nil
}

func normalizeHNSWTuning(tuning HNSWTuning, limits Limits) HNSWTuning {
	if tuning.MaxNeighbors == 0 {
		tuning.MaxNeighbors = 16
	}
	if tuning.EfConstruction == 0 {
		tuning.EfConstruction = max(64, tuning.MaxNeighbors)
	}
	if tuning.DefaultEfSearch == 0 {
		tuning.DefaultEfSearch = max(limits.MaxDocumentsPerSearch, min(limits.MaxChunkCandidates, 64))
		if tuning.MaxEfSearch > 0 {
			tuning.DefaultEfSearch = min(tuning.DefaultEfSearch, tuning.MaxEfSearch)
		}
	}
	if tuning.MaxEfSearch == 0 {
		tuning.MaxEfSearch = max(limits.MaxChunkCandidates, tuning.DefaultEfSearch)
	}
	if tuning.DefaultVisitLimit == 0 {
		tuning.DefaultVisitLimit = limits.MaxLiveVectors
		if tuning.MaxVisitLimit > 0 {
			tuning.DefaultVisitLimit = min(tuning.DefaultVisitLimit, tuning.MaxVisitLimit)
		}
	}
	if tuning.MaxVisitLimit == 0 {
		tuning.MaxVisitLimit = limits.MaxLiveVectors
	}
	return tuning
}

func (c Config) hnswBuildConfig() (hnsw.BuildConfig, bool) {
	if c.HNSW.MaxNeighbors <= 0 ||
		c.HNSW.EfConstruction <= 0 {
		return hnsw.BuildConfig{}, false
	}

	return hnsw.BuildConfig{
		MaxNeighbors:   c.HNSW.MaxNeighbors,
		EfConstruction: c.HNSW.EfConstruction,
		Seed:           c.HNSW.Seed,
	}, true
}

func (c Config) hnswSearchConfig() (hnsw.SearchConfig, bool) {
	if c.HNSW.DefaultEfSearch <= 0 ||
		c.HNSW.DefaultVisitLimit <= 0 {
		return hnsw.SearchConfig{}, false
	}

	return hnsw.SearchConfig{
		EfSearch:   c.HNSW.DefaultEfSearch,
		VisitLimit: c.HNSW.DefaultVisitLimit,
	}, true
}

func validateHNSWBuildConfig(c hnsw.BuildConfig) error {
	if c.MaxNeighbors < 2 || c.MaxNeighbors > maxHNSWNeighbors {
		return ErrInvalidConfig
	}

	if c.EfConstruction < c.MaxNeighbors || c.EfConstruction > maxHNSWEfConstruction {
		return ErrInvalidConfig
	}

	return nil
}

func validateHNSWSearchConfig(c hnsw.SearchConfig) error {
	if c.EfSearch <= 0 || c.VisitLimit <= 0 {
		return ErrInvalidConfig
	}

	return nil
}

func validVectorCapacity(dimensions, maxVectors int) bool {
	if dimensions <= 0 || maxVectors <= 0 {
		return false
	}

	d := uint64(dimensions)
	n := uint64(maxVectors)

	if d > math.MaxUint64/4 {
		return false
	}

	return n <= math.MaxUint64/(d*4)
}

func (c Config) schema() Schema {
	return c.Schema
}

func (c Config) resolvedSchema() (Schema, error) {
	legacy := Schema{Embedding: c.Embedding, Chunking: c.Chunking}
	hasSchema := c.Schema.Embedding != (EmbeddingDescriptor{}) || c.Schema.Chunking != (ChunkingDescriptor{})
	hasLegacy := c.Embedding != (EmbeddingDescriptor{}) || c.Chunking != (ChunkingDescriptor{})

	switch {
	case hasSchema && hasLegacy:
		if c.Schema != legacy {
			return Schema{}, ErrInvalidConfig
		}
		return c.Schema, nil
	case hasSchema:
		return c.Schema, nil
	case hasLegacy:
		return legacy, nil
	default:
		return Schema{}, ErrInvalidConfig
	}
}

func (c Config) searchPolicy() searchPolicy {
	return searchPolicy{
		MaxDocumentsPerSearch:   c.Limits.MaxDocumentsPerSearch,
		MaxChunkCandidates:      c.Limits.MaxChunkCandidates,
		MaxChunksPerDocumentHit: c.Limits.MaxChunksPerDocumentHit,
		MaxQueryChunks:          c.Limits.MaxChunksPerDocument,
		MaxEfSearch:             c.HNSW.MaxEfSearch,
		MaxVisitLimit:           c.HNSW.MaxVisitLimit,
	}
}

// searchPolicy contains the immutable limits required to search and group a
// read view. It is separate from request-local SearchOptions.
type searchPolicy struct {
	MaxDocumentsPerSearch   int
	MaxChunkCandidates      int
	MaxChunksPerDocumentHit int
	MaxQueryChunks          int
	MaxEfSearch             int
	MaxVisitLimit           int
}

func (p searchPolicy) validate() error {
	if p.MaxDocumentsPerSearch <= 0 || p.MaxChunkCandidates < p.MaxDocumentsPerSearch ||
		p.MaxChunksPerDocumentHit <= 0 || p.MaxQueryChunks <= 0 ||
		p.MaxEfSearch <= 0 || p.MaxVisitLimit <= 0 {
		return ErrInvalidConfig
	}
	return nil
}
