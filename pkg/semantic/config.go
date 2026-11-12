package semantic

import (
	"math"

	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"
)

const (
	// CurrentEncodingBehaviorVersion identifies the built-in field ordering,
	// chunk identity, prefix, and embedding assembly semantics.
	CurrentEncodingBehaviorVersion = uint32(1)

	defaultMaxChunksPerDocument    = 64
	defaultMaxLiveVectors          = 1_000_000
	defaultMaxSearchResults        = 20
	defaultMaxChunkCandidates      = 200
	defaultMaxChunksPerDocumentHit = 3
	defaultMaxFields               = 16
	defaultMaxSourceBytes          = uint64(1 << 20)
)

// QueryConfig controls request-local semantic search budgets. Text queries are
// split with the same chunk splitter used to encode indexed documents.
type QueryConfig struct {
	EfSearch        int
	VisitLimit      int
	CandidateChunks int
}

// Validate checks request-local query values independently of service ceilings.
func (c QueryConfig) Validate() error {
	if c.CandidateChunks < 0 {
		return ErrInvalidQueryConfig
	}
	if err := (hnsw.QueryConfig{
		EfSearch:   c.EfSearch,
		VisitLimit: c.VisitLimit,
	}).Validate(); err != nil {
		return ErrInvalidQueryConfig
	}
	return nil
}

// SearchConfig defines static ceilings for request-local search options and
// result shape. Zero query/HNSW ceilings are derived during normalization.
type SearchConfig struct {
	MaxResults         int
	MaxCandidateChunks int
	MaxChunksPerResult int
	MaxQueryChunks     int
	MaxEfSearch        int
	MaxVisitLimit      int
}

// Validate checks service-wide search ceilings.
func (c SearchConfig) Validate() error {
	if c.MaxResults <= 0 || c.MaxCandidateChunks < c.MaxResults || c.MaxChunksPerResult <= 0 ||
		c.MaxQueryChunks < 0 || c.MaxEfSearch < 0 || c.MaxVisitLimit < 0 {
		return ErrInvalidConfig
	}
	return nil
}

// EncodingLimits bounds source text and embedding material created by the
// built-in model pipeline. Zero fields select defaults during normalization.
type EncodingLimits struct {
	// MaxFields bounds fields in one document. It does not apply to queries.
	MaxFields int
	// MaxSourceBytes bounds combined document text or one query.
	MaxSourceBytes uint64
	// MaxEmbeddingBytes bounds one document or query embedding batch. It may
	// impose a lower effective chunk count than the document/query chunk limit.
	MaxEmbeddingBytes uint64
}

// Validate checks encoding limits after applying defaults for the supplied
// vector dimensions and maximum document/query chunk count.
func (c EncodingLimits) Validate(dimensions, maxChunks int) error {
	_, err := c.normalized(dimensions, maxChunks)
	return err
}

func (c EncodingLimits) normalized(dimensions, maxChunks int) (EncodingLimits, error) {
	if c.MaxFields < 0 || dimensions <= 0 || maxChunks <= 0 || uint64(dimensions) > math.MaxUint64/4 {
		return EncodingLimits{}, ErrInvalidConfig
	}
	bytesPerVector := uint64(dimensions) * 4
	if uint64(maxChunks) > math.MaxUint64/bytesPerVector {
		return EncodingLimits{}, ErrInvalidConfig
	}
	if c.MaxFields == 0 {
		c.MaxFields = defaultMaxFields
	}
	if c.MaxSourceBytes == 0 {
		c.MaxSourceBytes = defaultMaxSourceBytes
	}
	if c.MaxEmbeddingBytes == 0 {
		c.MaxEmbeddingBytes = uint64(maxChunks) * bytesPerVector
	}
	if c.MaxEmbeddingBytes < bytesPerVector {
		return EncodingLimits{}, ErrInvalidConfig
	}
	return c, nil
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

// ServiceConfig configures the built-in model-backed document and query
// encoding pipeline together with its vector index.
type ServiceConfig struct {
	Model                   ModelConfig
	Index                   IndexConfig
	Chunking                chunk.Config
	Encoding                EncodingLimits
	EncodingBehaviorVersion uint32
}

// DefaultServiceConfig returns the validated effective defaults for model.
func DefaultServiceConfig(model embedding.Model) (ServiceConfig, error) {
	if err := model.Validate(); err != nil {
		return ServiceConfig{}, err
	}
	maxChunkBytes := min(model.ContextTokens, 4096)
	targetBytes := max(1, maxChunkBytes*3/4)
	overlapBytes := min(targetBytes-1, maxChunkBytes/8)
	config := ServiceConfig{
		Model:                   newModelConfig(model),
		EncodingBehaviorVersion: CurrentEncodingBehaviorVersion,
		Index: IndexConfig{
			Dimensions: model.Dimensions,
			Metric:     vector.MetricCosine,
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
		Chunking: chunk.Config{
			TargetBytes: targetBytes, MaxBytes: maxChunkBytes, OverlapBytes: overlapBytes,
		},
	}
	return config.normalized()
}

// Validate checks the complete static configuration.
func (c ServiceConfig) Validate() error {
	_, err := c.normalized()
	return err
}

func (c ServiceConfig) normalized() (ServiceConfig, error) {
	if c.EncodingBehaviorVersion == 0 {
		c.EncodingBehaviorVersion = CurrentEncodingBehaviorVersion
	}
	if c.EncodingBehaviorVersion != CurrentEncodingBehaviorVersion {
		return ServiceConfig{}, ErrUnsupportedEncodingBehavior
	}
	if err := c.Model.Validate(); err != nil {
		return ServiceConfig{}, err
	}
	if c.Model.Dimensions != c.Index.Dimensions {
		return ServiceConfig{}, ErrInvalidConfig
	}
	if c.Chunking.TargetBytes < 0 || c.Chunking.MaxBytes < 0 || c.Chunking.OverlapBytes < 0 ||
		uint64(c.Chunking.TargetBytes) > math.MaxUint32 ||
		uint64(c.Chunking.MaxBytes) > math.MaxUint32 ||
		uint64(c.Chunking.OverlapBytes) > math.MaxUint32 || c.Chunking.Validate() != nil {
		return ServiceConfig{}, ErrInvalidConfig
	}
	indexConfig, err := c.Index.normalized()
	if err != nil {
		return ServiceConfig{}, err
	}
	c.Index = indexConfig
	encoding, err := c.Encoding.normalized(
		c.Index.Dimensions,
		max(c.Index.Limits.MaxChunksPerDocument, c.Index.Search.MaxQueryChunks),
	)
	if err != nil {
		return ServiceConfig{}, err
	}
	c.Encoding = encoding
	return c, nil
}

// IndexConfig configures vector indexing, static search ceilings, and HNSW.
type IndexConfig struct {
	// Dimensions is the length of every document and query vector.
	Dimensions int
	Metric     vector.Metric
	Limits     LimitsConfig
	Search     SearchConfig
	HNSW       hnsw.Config
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
	// MaxChunksPerDocument bounds one encoded document mutation.
	MaxChunksPerDocument int
}

// Validate checks lifecycle and ingestion limits. Zero stale-vector and
// segment limits select defaults during normalization.
func (c LimitsConfig) Validate() error {
	if c.MaxLiveVectors <= 0 || c.MaxStaleVectors < 0 || c.MaxSegments < 0 ||
		c.MaxChunksPerDocument <= 0 || c.MaxChunksPerDocument > c.MaxLiveVectors {
		return ErrInvalidConfig
	}
	return nil
}

// Validate checks vector indexing, search ceilings, and HNSW configuration.
func (c IndexConfig) Validate() error {
	_, err := c.normalized()
	return err
}

func (c IndexConfig) normalized() (IndexConfig, error) {
	if err := c.Limits.Validate(); err != nil {
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
	if err := c.Search.Validate(); err != nil {
		return IndexConfig{}, err
	}
	if c.Dimensions <= 0 || !c.Metric.Valid() ||
		limits.MaxLiveVectors <= 0 ||
		limits.MaxStaleVectors < 0 ||
		limits.MaxSegments <= 0 ||
		limits.MaxChunksPerDocument <= 0 ||
		limits.MaxChunksPerDocument > limits.MaxLiveVectors ||
		c.Search.MaxCandidateChunks > limits.MaxLiveVectors ||
		c.Search.MaxChunksPerResult > limits.MaxChunksPerDocument {
		return IndexConfig{}, ErrInvalidConfig
	}
	c.HNSW = normalizeHNSWConfig(c.HNSW, limits, c.Search)
	c.Search = normalizeSearchConfig(c.Search, limits, c.HNSW.Search)
	if c.Search.MaxQueryChunks <= 0 || c.Search.MaxEfSearch <= 0 || c.Search.MaxVisitLimit <= 0 ||
		c.HNSW.Search.EfSearch > c.Search.MaxEfSearch ||
		c.HNSW.Search.VisitLimit > c.Search.MaxVisitLimit {
		return IndexConfig{}, ErrInvalidConfig
	}
	for _, value := range []int{
		c.Dimensions,
		limits.MaxLiveVectors,
		limits.MaxStaleVectors,
		limits.MaxSegments,
		limits.MaxChunksPerDocument,
		c.Search.MaxResults,
		c.Search.MaxCandidateChunks,
		c.Search.MaxChunksPerResult,
		c.Search.MaxQueryChunks,
		c.Search.MaxEfSearch,
		c.Search.MaxVisitLimit,
		c.HNSW.Build.MaxNeighbors,
		c.HNSW.Build.EfConstruction,
		c.HNSW.Search.EfSearch,
		c.HNSW.Search.VisitLimit,
	} {
		if uint64(value) > math.MaxUint32 {
			return IndexConfig{}, ErrInvalidConfig
		}
	}

	if err := c.hnswConfig().Validate(); err != nil {
		return IndexConfig{}, ErrInvalidConfig
	}

	maxInt := int(^uint(0) >> 1)
	if limits.MaxLiveVectors > maxInt-limits.MaxStaleVectors ||
		!validVectorCapacity(c.Dimensions, limits.MaxLiveVectors+limits.MaxStaleVectors) {
		return IndexConfig{}, ErrInvalidConfig
	}
	if err := c.runtimeLimits().validate(); err != nil {
		return IndexConfig{}, err
	}

	return c, nil
}

func normalizeHNSWConfig(config hnsw.Config, limits LimitsConfig, search SearchConfig) hnsw.Config {
	if config.Build.MaxNeighbors == 0 {
		config.Build.MaxNeighbors = 16
	}
	if config.Build.EfConstruction == 0 {
		config.Build.EfConstruction = max(64, config.Build.MaxNeighbors)
	}
	if config.Search.EfSearch == 0 {
		config.Search.EfSearch = max(search.MaxResults, min(search.MaxCandidateChunks, 64))
		if search.MaxEfSearch > 0 {
			config.Search.EfSearch = min(config.Search.EfSearch, search.MaxEfSearch)
		}
	}
	if config.Search.VisitLimit == 0 {
		config.Search.VisitLimit = limits.MaxLiveVectors
		if search.MaxVisitLimit > 0 {
			config.Search.VisitLimit = min(config.Search.VisitLimit, search.MaxVisitLimit)
		}
	}
	return config
}

func normalizeSearchConfig(config SearchConfig, limits LimitsConfig, defaults hnsw.SearchConfig) SearchConfig {
	if config.MaxQueryChunks == 0 {
		config.MaxQueryChunks = limits.MaxChunksPerDocument
	}
	if config.MaxEfSearch == 0 {
		config.MaxEfSearch = max(config.MaxCandidateChunks, defaults.EfSearch)
	}
	if config.MaxVisitLimit == 0 {
		config.MaxVisitLimit = limits.MaxLiveVectors
	}
	return config
}

func (c IndexConfig) hnswConfig() hnsw.Config {
	return c.HNSW
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

	return n <= (math.MaxUint64-128)/(d*4)
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
			maxQueryChunks:     c.Search.MaxQueryChunks,
			maxEfSearch:        c.Search.MaxEfSearch,
			maxVisitLimit:      c.Search.MaxVisitLimit,
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
