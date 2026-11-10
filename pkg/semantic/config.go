package semantic

import (
	"math"

	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"
)

const (
	defaultMaxChunksPerDocument    = 64
	defaultMaxLiveVectors          = 1_000_000
	defaultMaxSearchResults        = 20
	defaultMaxChunkCandidates      = 200
	defaultMaxChunksPerDocumentHit = 3
)

// LongQueryStrategy controls text queries larger than QueryConfig.MaxBytes.
type LongQueryStrategy uint8

const (
	LongQueryStrategyUnknown LongQueryStrategy = iota
	LongQueryReject
	LongQueryTruncate
	LongQueryChunk
)

// QueryConfig controls one semantic query. Zero values select safe service
// defaults: the model context limit, rejection of long input, and configured
// HNSW/search budgets.
type QueryConfig struct {
	MaxBytes        int
	LongQuery       LongQueryStrategy
	EfSearch        int
	VisitLimit      int
	CandidateChunks int
}

// Validate checks request-local query values independently of service ceilings.
func (c QueryConfig) Validate() error {
	if c.MaxBytes < 0 || uint64(c.MaxBytes) > math.MaxUint32 ||
		c.LongQuery > LongQueryChunk || c.EfSearch < 0 || c.VisitLimit < 0 ||
		c.CandidateChunks < 0 {
		return ErrInvalidQueryConfig
	}
	return nil
}

// SearchConfig defines static ceilings for request-local search options and
// result shape. Every field is required.
type SearchConfig struct {
	MaxResults         int
	MaxCandidateChunks int
	MaxChunksPerResult int
}

// Validate checks service-wide search ceilings.
func (c SearchConfig) Validate() error {
	if c.MaxResults <= 0 || c.MaxCandidateChunks < c.MaxResults || c.MaxChunksPerResult <= 0 {
		return ErrInvalidConfig
	}
	return nil
}

// ChunkingKind identifies how document fields are split into chunks.
type ChunkingKind uint8

const (
	ChunkingKindUnknown ChunkingKind = iota
	ChunkingKindByteSplitter
	ChunkingKindCustom
)

// ChunkingConfig describes document splitting. Builtin configures the standard
// splitter. A custom splitter is supplied at construction time and cannot be
// reconstructed from persisted data.
type ChunkingConfig struct {
	Kind    ChunkingKind
	Builtin chunk.Config
}

// Validate checks the document splitting configuration.
func (c ChunkingConfig) Validate() error {
	switch c.Kind {
	case ChunkingKindByteSplitter:
		if c.Builtin.TargetBytes < 0 || c.Builtin.MaxBytes < 0 || c.Builtin.OverlapBytes < 0 ||
			uint64(c.Builtin.TargetBytes) > math.MaxUint32 ||
			uint64(c.Builtin.MaxBytes) > math.MaxUint32 ||
			uint64(c.Builtin.OverlapBytes) > math.MaxUint32 || c.Builtin.Validate() != nil {
			return ErrInvalidConfig
		}
		return nil
	case ChunkingKindCustom:
		if c.Builtin != (chunk.Config{}) {
			return ErrInvalidConfig
		}
		return nil
	default:
		return ErrInvalidConfig
	}
}

// ModelConfig contains the static embedding model contract required to encode
// inputs and validate vector dimensions.
type ModelConfig struct {
	ID             string
	Revision       string
	Dimensions     int
	ContextTokens  int
	DocumentPrefix string
	QueryPrefix    string
}

// Validate checks the model contract.
func (c ModelConfig) Validate() error {
	if err := c.model().Validate(); err != nil {
		return ErrInvalidConfig
	}
	return nil
}

func newModelConfig(model embedding.Model) ModelConfig {
	return ModelConfig{
		ID:             model.ID,
		Revision:       model.Revision,
		Dimensions:     model.Dimensions,
		ContextTokens:  model.ContextTokens,
		DocumentPrefix: model.DocumentPrefix,
		QueryPrefix:    model.QueryPrefix,
	}
}

func (c ModelConfig) model() embedding.Model {
	return embedding.Model{
		ID:             c.ID,
		Revision:       c.Revision,
		Dimensions:     c.Dimensions,
		ContextTokens:  c.ContextTokens,
		DocumentPrefix: c.DocumentPrefix,
		QueryPrefix:    c.QueryPrefix,
	}
}

// SemanticConfig is the single static configuration document for a semantic
// service and its persisted index.
type SemanticConfig struct {
	Model    ModelConfig
	Index    IndexConfig
	Chunking ChunkingConfig
}

// DefaultSemanticConfig returns the validated effective defaults for model.
func DefaultSemanticConfig(model embedding.Model) (SemanticConfig, error) {
	if err := model.Validate(); err != nil {
		return SemanticConfig{}, err
	}
	maxChunkBytes := min(model.ContextTokens, 4096)
	targetBytes := max(1, maxChunkBytes*3/4)
	overlapBytes := min(targetBytes-1, maxChunkBytes/8)
	config := SemanticConfig{
		Model: newModelConfig(model),
		Index: IndexConfig{
			Metric: vector.MetricCosine,
			Limits: LimitsConfig{
				MaxLiveVectors:       defaultMaxLiveVectors,
				MaxChunksPerDocument: defaultMaxChunksPerDocument,
			},
			Search: SearchConfig{
				MaxResults:         defaultMaxSearchResults,
				MaxCandidateChunks: defaultMaxChunkCandidates,
				MaxChunksPerResult: defaultMaxChunksPerDocumentHit,
			},
		},
		Chunking: ChunkingConfig{
			Kind: ChunkingKindByteSplitter,
			Builtin: chunk.Config{
				TargetBytes: targetBytes, MaxBytes: maxChunkBytes, OverlapBytes: overlapBytes,
			},
		},
	}
	return config.normalized()
}

// Validate checks the complete static configuration.
func (c SemanticConfig) Validate() error {
	_, err := c.normalized()
	return err
}

func (c SemanticConfig) normalized() (SemanticConfig, error) {
	if err := c.Model.Validate(); err != nil {
		return SemanticConfig{}, err
	}
	if !c.Index.Metric.Valid() {
		return SemanticConfig{}, ErrInvalidConfig
	}
	if err := c.Chunking.Validate(); err != nil {
		return SemanticConfig{}, err
	}
	indexConfig, err := c.Index.normalized(c.Model.Dimensions)
	if err != nil {
		return SemanticConfig{}, err
	}
	c.Index = indexConfig
	return c, nil
}

// IndexConfig configures vector indexing, static search ceilings, and HNSW.
type IndexConfig struct {
	Metric vector.Metric
	Limits LimitsConfig
	Search SearchConfig
	HNSW   HNSWConfig
}

// LimitsConfig bounds semantic document lifecycle and ingestion.
type LimitsConfig struct {
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
}

// Validate checks lifecycle and ingestion limits. Zero stale-vector and
// segment limits select defaults during SemanticConfig normalization.
func (c LimitsConfig) Validate() error {
	if c.MaxLiveVectors <= 0 || c.MaxStaleVectors < 0 || c.MaxSegments < 0 ||
		c.MaxChunksPerDocument <= 0 || c.MaxChunksPerDocument > c.MaxLiveVectors {
		return ErrInvalidConfig
	}
	return nil
}

// HNSWConfig controls graph quality and request work. Zero fields select
// defaults derived from LimitsConfig and SearchConfig.
type HNSWConfig struct {
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

// Validate checks explicitly configured HNSW values. Zero fields select
// defaults during SemanticConfig normalization.
func (c HNSWConfig) Validate() error {
	if c.MaxNeighbors < 0 || c.EfConstruction < 0 || c.DefaultEfSearch < 0 ||
		c.MaxEfSearch < 0 || c.DefaultVisitLimit < 0 || c.MaxVisitLimit < 0 {
		return ErrInvalidConfig
	}
	if c.DefaultEfSearch > 0 && c.MaxEfSearch > 0 && c.DefaultEfSearch > c.MaxEfSearch {
		return ErrInvalidConfig
	}
	if c.DefaultVisitLimit > 0 && c.MaxVisitLimit > 0 && c.DefaultVisitLimit > c.MaxVisitLimit {
		return ErrInvalidConfig
	}
	return nil
}

func (c IndexConfig) Validate(dimensions int) error {
	_, err := c.normalized(dimensions)
	return err
}

func (c IndexConfig) normalized(dimensions int) (IndexConfig, error) {
	if err := c.Limits.Validate(); err != nil {
		return IndexConfig{}, err
	}
	if err := c.HNSW.Validate(); err != nil {
		return IndexConfig{}, err
	}
	limits := c.Limits
	if limits.MaxStaleVectors == 0 {
		limits.MaxStaleVectors = limits.MaxLiveVectors
	}
	if limits.MaxSegments == 0 {
		limits.MaxSegments = 16
	}
	c.Limits = limits
	if dimensions <= 0 || !c.Metric.Valid() ||
		limits.MaxLiveVectors <= 0 ||
		limits.MaxStaleVectors < 0 ||
		limits.MaxSegments <= 0 ||
		limits.MaxChunksPerDocument <= 0 ||
		limits.MaxChunksPerDocument > limits.MaxLiveVectors ||
		c.Search.Validate() != nil ||
		c.Search.MaxCandidateChunks > limits.MaxLiveVectors ||
		c.Search.MaxChunksPerResult > limits.MaxChunksPerDocument {
		return IndexConfig{}, ErrInvalidConfig
	}
	c.HNSW = normalizeHNSWConfig(c.HNSW, limits, c.Search)
	if c.HNSW.MaxEfSearch <= 0 || c.HNSW.MaxVisitLimit <= 0 ||
		c.HNSW.DefaultEfSearch > c.HNSW.MaxEfSearch ||
		c.HNSW.DefaultVisitLimit > c.HNSW.MaxVisitLimit {
		return IndexConfig{}, ErrInvalidConfig
	}

	if err := c.hnswBuildConfig().Validate(); err != nil {
		return IndexConfig{}, ErrInvalidConfig
	}

	if err := c.hnswSearchConfig().Validate(); err != nil {
		return IndexConfig{}, ErrInvalidConfig
	}

	maxInt := int(^uint(0) >> 1)
	if limits.MaxLiveVectors > maxInt-limits.MaxStaleVectors ||
		!validVectorCapacity(dimensions, limits.MaxLiveVectors+limits.MaxStaleVectors) {
		return IndexConfig{}, ErrInvalidConfig
	}
	if err := c.runtimeLimits().validate(); err != nil {
		return IndexConfig{}, err
	}

	return c, nil
}

func normalizeHNSWConfig(tuning HNSWConfig, limits LimitsConfig, search SearchConfig) HNSWConfig {
	if tuning.MaxNeighbors == 0 {
		tuning.MaxNeighbors = 16
	}
	if tuning.EfConstruction == 0 {
		tuning.EfConstruction = max(64, tuning.MaxNeighbors)
	}
	if tuning.DefaultEfSearch == 0 {
		tuning.DefaultEfSearch = max(search.MaxResults, min(search.MaxCandidateChunks, 64))
		if tuning.MaxEfSearch > 0 {
			tuning.DefaultEfSearch = min(tuning.DefaultEfSearch, tuning.MaxEfSearch)
		}
	}
	if tuning.MaxEfSearch == 0 {
		tuning.MaxEfSearch = max(search.MaxCandidateChunks, tuning.DefaultEfSearch)
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

func (c IndexConfig) hnswBuildConfig() hnsw.BuildConfig {
	return hnsw.BuildConfig{
		MaxNeighbors:   c.HNSW.MaxNeighbors,
		EfConstruction: c.HNSW.EfConstruction,
		Seed:           c.HNSW.Seed,
	}
}

func (c IndexConfig) hnswSearchConfig() hnsw.SearchConfig {
	return hnsw.SearchConfig{
		EfSearch:   c.HNSW.DefaultEfSearch,
		VisitLimit: c.HNSW.DefaultVisitLimit,
	}
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

// searchBudgetLimits contains immutable service-wide ceilings. QueryConfig
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
			maxChunkCandidates: c.Search.MaxCandidateChunks,
			maxQueryChunks:     c.Limits.MaxChunksPerDocument,
			maxEfSearch:        c.HNSW.MaxEfSearch,
			maxVisitLimit:      c.HNSW.MaxVisitLimit,
		},
		results: resultShapeLimits{
			maxDocumentsPerSearch:   c.Search.MaxResults,
			maxChunksPerDocumentHit: c.Search.MaxChunksPerResult,
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
