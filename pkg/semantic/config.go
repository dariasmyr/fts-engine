package semantic

import (
	"math"

	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
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
	if !schema.IsValid() ||
		limits.MaxLiveVectors <= 0 || limits.MaxChunksPerDocument <= 0 ||
		limits.MaxDocumentsPerSearch <= 0 || limits.MaxChunkCandidates < limits.MaxDocumentsPerSearch ||
		limits.MaxChunkCandidates > limits.MaxLiveVectors || limits.MaxChunksPerDocument > limits.MaxLiveVectors ||
		limits.MaxChunksPerDocumentHit <= 0 || limits.MaxChunksPerDocumentHit > limits.MaxChunksPerDocument {
		return Config{}, ErrInvalidConfig
	}

	c.HNSW = normalizeHNSWTuning(c.HNSW, limits)
	options, ok := c.hnswOptions()
	if !ok {
		return Config{}, ErrInvalidConfig
	}
	if err := options.Validate(limits.MaxLiveVectors); err != nil {
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
	}
	if tuning.MaxEfSearch == 0 {
		tuning.MaxEfSearch = max(limits.MaxChunkCandidates, tuning.DefaultEfSearch)
	}
	if tuning.DefaultVisitLimit == 0 {
		tuning.DefaultVisitLimit = limits.MaxLiveVectors
	}
	if tuning.MaxVisitLimit == 0 {
		tuning.MaxVisitLimit = limits.MaxLiveVectors
	}
	return tuning
}

func (c Config) hnswOptions() (hnsw.BuildOptions, bool) {
	dimensions := uint64(c.Schema.Embedding.Dimensions)
	maxVectors := uint64(c.Limits.MaxLiveVectors)
	if dimensions == 0 || dimensions > math.MaxUint64/4 || maxVectors > math.MaxUint64/(dimensions*4) {
		return hnsw.BuildOptions{}, false
	}
	return hnsw.BuildOptions{
		Build: hnsw.BuildConfig{
			Dimensions:     c.Schema.Embedding.Dimensions,
			Metric:         c.Schema.Embedding.Metric,
			MaxVectors:     c.Limits.MaxLiveVectors,
			MaxVectorBytes: maxVectors * dimensions * 4,
			MaxNeighbors:   c.HNSW.MaxNeighbors,
			EfConstruction: c.HNSW.EfConstruction,
			Seed:           c.HNSW.Seed,
		},
		Search: hnsw.SearchConfig{
			DefaultEfSearch:   c.HNSW.DefaultEfSearch,
			MaxEfSearch:       c.HNSW.MaxEfSearch,
			DefaultVisitLimit: c.HNSW.DefaultVisitLimit,
			MaxVisitLimit:     c.HNSW.MaxVisitLimit,
			MaxK:              c.Limits.MaxChunkCandidates,
		},
	}, true
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
	}
}

// searchPolicy contains the immutable limits required to search and group a
// read view. It is separate from request-local SearchOptions.
type searchPolicy struct {
	MaxDocumentsPerSearch   int
	MaxChunkCandidates      int
	MaxChunksPerDocumentHit int
	MaxQueryChunks          int
}

func (p searchPolicy) validate() error {
	if p.MaxDocumentsPerSearch <= 0 || p.MaxChunkCandidates < p.MaxDocumentsPerSearch ||
		p.MaxChunksPerDocumentHit <= 0 || p.MaxQueryChunks <= 0 {
		return ErrInvalidConfig
	}
	return nil
}
