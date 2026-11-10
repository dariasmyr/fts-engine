package semantic

import (
	"math"

	"github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"
)

const (
	maxHNSWNeighbors      = 1024
	maxHNSWEfConstruction = 1_000_000
)

// IndexConfig configures the low-level vector index. NewIndex derives
// vector-space and allocation settings from these values.
type IndexConfig struct {
	// Schema is the canonical compatibility contract of the semantic index.
	Schema Schema

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
// defaults derived from Limits and the embedding schema.
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

func (c IndexConfig) Validate() error {
	_, err := c.normalized()
	return err
}

func (c IndexConfig) normalized() (IndexConfig, error) {
	limits := c.Limits
	if limits.MaxStaleVectors == 0 {
		limits.MaxStaleVectors = limits.MaxLiveVectors
	}
	if limits.MaxSegments == 0 {
		limits.MaxSegments = 16
	}
	c.Limits = limits
	if !c.Schema.IsValid() ||
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
		return IndexConfig{}, ErrInvalidConfig
	}
	c.HNSW = normalizeHNSWTuning(c.HNSW, limits)
	if c.HNSW.MaxEfSearch <= 0 || c.HNSW.MaxVisitLimit <= 0 ||
		c.HNSW.DefaultEfSearch > c.HNSW.MaxEfSearch ||
		c.HNSW.DefaultVisitLimit > c.HNSW.MaxVisitLimit {
		return IndexConfig{}, ErrInvalidConfig
	}

	buildConfig, ok := c.hnswBuildConfig()
	if !ok {
		return IndexConfig{}, ErrInvalidConfig
	}

	searchConfig, ok := c.hnswSearchConfig()
	if !ok {
		return IndexConfig{}, ErrInvalidConfig
	}

	if err := validateHNSWBuildConfig(buildConfig); err != nil {
		return IndexConfig{}, ErrInvalidConfig
	}

	if err := validateHNSWSearchConfig(searchConfig); err != nil {
		return IndexConfig{}, ErrInvalidConfig
	}

	maxInt := int(^uint(0) >> 1)
	if limits.MaxLiveVectors > maxInt-limits.MaxStaleVectors ||
		!validVectorCapacity(c.Schema.Embedding.Dimensions, limits.MaxLiveVectors+limits.MaxStaleVectors) {
		return IndexConfig{}, ErrInvalidConfig
	}
	if err := c.runtimeLimits().validate(); err != nil {
		return IndexConfig{}, err
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

func (c IndexConfig) hnswBuildConfig() (hnsw.BuildConfig, bool) {
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

func (c IndexConfig) hnswSearchConfig() (hnsw.SearchConfig, bool) {
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

type capacityLimits struct {
	maxLiveVectors  int
	maxStaleVectors int
}

type lifecycleLimits struct {
	maxSegments int
}

type ingestionLimits struct {
	maxChunksPerDocument int
}

// searchBudgetLimits contains immutable service-wide ceilings. SearchOptions
// supplies request-local values within these bounds.
type searchBudgetLimits struct {
	maxChunkCandidates int
	maxQueryChunks     int
	maxEfSearch        int
	maxVisitLimit      int
}

type resultShapeLimits struct {
	maxDocumentsPerSearch   int
	maxChunksPerDocumentHit int
}

type indexLimits struct {
	capacity  capacityLimits
	lifecycle lifecycleLimits
	ingestion ingestionLimits
	search    searchBudgetLimits
	results   resultShapeLimits
}

func (c IndexConfig) runtimeLimits() indexLimits {
	return indexLimits{
		capacity: capacityLimits{
			maxLiveVectors:  c.Limits.MaxLiveVectors,
			maxStaleVectors: c.Limits.MaxStaleVectors,
		},
		lifecycle: lifecycleLimits{maxSegments: c.Limits.MaxSegments},
		ingestion: ingestionLimits{maxChunksPerDocument: c.Limits.MaxChunksPerDocument},
		search: searchBudgetLimits{
			maxChunkCandidates: c.Limits.MaxChunkCandidates,
			maxQueryChunks:     c.Limits.MaxChunksPerDocument,
			maxEfSearch:        c.HNSW.MaxEfSearch,
			maxVisitLimit:      c.HNSW.MaxVisitLimit,
		},
		results: resultShapeLimits{
			maxDocumentsPerSearch:   c.Limits.MaxDocumentsPerSearch,
			maxChunksPerDocumentHit: c.Limits.MaxChunksPerDocumentHit,
		},
	}
}

func (l indexLimits) validate() error {
	if l.capacity.maxLiveVectors <= 0 || l.capacity.maxStaleVectors < 0 ||
		l.lifecycle.maxSegments <= 0 || l.ingestion.maxChunksPerDocument <= 0 ||
		l.search.maxChunkCandidates < l.results.maxDocumentsPerSearch ||
		l.search.maxQueryChunks <= 0 || l.search.maxEfSearch <= 0 || l.search.maxVisitLimit <= 0 ||
		l.results.maxDocumentsPerSearch <= 0 || l.results.maxChunksPerDocumentHit <= 0 {
		return ErrInvalidConfig
	}
	return nil
}
