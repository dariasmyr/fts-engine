package semantic

import (
	"fmt"

	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

const (
	defaultMaxFields               = 16
	defaultMaxSourceBytes          = uint64(1 << 20)
	defaultMaxChunksPerDocument    = 64
	defaultMaxLiveVectors          = 1_000_000
	defaultMaxSearchResults        = 20
	defaultMaxChunkCandidates      = 200
	defaultMaxChunksPerDocumentHit = 3
)

// Chunker splits document fields into byte ranges.
type Chunker interface {
	Split(text string) ([]chunk.Range, error)
}

// LongQueryStrategy controls text queries larger than QueryConfig.MaxBytes.
type LongQueryStrategy uint8

const (
	LongQueryStrategyUnknown LongQueryStrategy = iota
	LongQueryReject
	LongQueryTruncate
	LongQueryChunk
)

// QueryConfig controls only query input preparation. It never changes the
// persisted document schema.
type QueryConfig struct {
	MaxBytes  int
	LongQuery LongQueryStrategy
}

// SearchLimits defines service-wide ceilings for request-local search options
// and result shape. Every field is required.
type SearchLimits struct {
	MaxResults         int
	MaxCandidateChunks int
	MaxChunksPerResult int
}

// Option configures the high-level semantic service.
type Option func(*serviceOptions) error

type serviceOptions struct {
	metric               vector.Metric
	chunker              Chunker
	chunking             ChunkingSchema
	queryConfig          QueryConfig
	maxLiveVectors       int
	maxChunksPerDocument int
	searchLimits         SearchLimits
}

// WithMetric selects the vector distance metric. The default is cosine.
func WithMetric(metric vector.Metric) Option {
	return func(options *serviceOptions) error {
		if !metric.Valid() {
			return ErrInvalidConfig
		}
		options.metric = metric
		return nil
	}
}

// WithMaxIndexedVectors sets the maximum number of visible chunk embeddings.
func WithMaxIndexedVectors(maxVectors int) Option {
	return func(options *serviceOptions) error {
		if maxVectors <= 0 {
			return ErrInvalidConfig
		}
		options.maxLiveVectors = maxVectors
		return nil
	}
}

// WithMaxChunksPerDocument sets one authoritative ingestion and embedding
// memory bound for both the document pipeline and Index.
func WithMaxChunksPerDocument(maxChunks int) Option {
	return func(options *serviceOptions) error {
		if maxChunks <= 0 {
			return ErrInvalidConfig
		}
		options.maxChunksPerDocument = maxChunks
		return nil
	}
}

// WithSearchLimits sets service-wide request ceilings and result shape.
func WithSearchLimits(limits SearchLimits) Option {
	return func(options *serviceOptions) error {
		if limits.MaxResults <= 0 || limits.MaxCandidateChunks < limits.MaxResults ||
			limits.MaxChunksPerResult <= 0 {
			return ErrInvalidConfig
		}
		options.searchLimits = limits
		return nil
	}
}

// WithChunking configures the built-in deterministic document splitter.
func WithChunking(config chunk.Config) Option {
	return func(options *serviceOptions) error {
		chunker, chunking, err := newConfiguredChunker(config)
		if err != nil {
			return err
		}
		options.chunker = chunker
		options.chunking = chunking
		return nil
	}
}

// WithCustomChunker configures a custom document splitter and its explicit
// persisted compatibility identity.
func WithCustomChunker(chunker Chunker, chunking ChunkingSchema) Option {
	return func(options *serviceOptions) error {
		if chunker == nil || !chunking.IsValid() {
			return ErrInvalidConfig
		}
		options.chunker = chunker
		options.chunking = chunking
		return nil
	}
}

// WithQueryConfig selects reject, truncate, or independent query chunking for
// long text queries. Short queries are always embedded as one input.
func WithQueryConfig(config QueryConfig) Option {
	return func(options *serviceOptions) error {
		if config.MaxBytes <= 0 || config.LongQuery <= LongQueryStrategyUnknown || config.LongQuery > LongQueryChunk {
			return ErrInvalidConfig
		}
		options.queryConfig = config
		return nil
	}
}

func defaultServiceOptions(model embedding.Model) (serviceOptions, error) {
	if err := model.Validate(); err != nil {
		return serviceOptions{}, err
	}
	maxChunkBytes := min(model.ContextTokens, 4096)
	targetBytes := max(1, maxChunkBytes*3/4)
	overlapBytes := min(targetBytes-1, maxChunkBytes/8)
	splitter, chunking, err := newConfiguredChunker(chunk.Config{
		TargetBytes:  targetBytes,
		MaxBytes:     maxChunkBytes,
		OverlapBytes: overlapBytes,
	})
	if err != nil {
		return serviceOptions{}, fmt.Errorf("default chunking: %w", err)
	}
	return serviceOptions{
		metric:               vector.MetricCosine,
		chunker:              splitter,
		chunking:             chunking,
		maxLiveVectors:       defaultMaxLiveVectors,
		maxChunksPerDocument: defaultMaxChunksPerDocument,
		searchLimits: SearchLimits{
			MaxResults:         defaultMaxSearchResults,
			MaxCandidateChunks: defaultMaxChunkCandidates,
			MaxChunksPerResult: defaultMaxChunksPerDocumentHit,
		},
		queryConfig: QueryConfig{
			MaxBytes:  model.ContextTokens,
			LongQuery: LongQueryReject,
		},
	}, nil
}

func newConfiguredChunker(config chunk.Config) (Chunker, ChunkingSchema, error) {
	splitter, err := chunk.NewSplitter(config)
	if err != nil {
		return nil, ChunkingSchema{}, fmt.Errorf("%w: chunking config: %w", ErrInvalidConfig, err)
	}
	fingerprint, err := config.Fingerprint()
	if err != nil {
		return nil, ChunkingSchema{}, fmt.Errorf("%w: chunking config: %w", ErrInvalidConfig, err)
	}
	return splitter, ChunkingSchema{
		ID: chunk.SplitterID, Version: chunk.SplitterVersion, Fingerprint: fingerprint,
	}, nil
}

func defaultInputLimits(dimensions, maxChunks int) (modelInputLimits, error) {
	if dimensions <= 0 || uint64(dimensions) > ^uint64(0)/4 ||
		maxChunks <= 0 || uint64(maxChunks) > ^uint64(0)/(uint64(dimensions)*4) {
		return modelInputLimits{}, ErrInvalidConfig
	}
	return modelInputLimits{
		maxFields:         defaultMaxFields,
		maxSourceBytes:    defaultMaxSourceBytes,
		maxChunks:         maxChunks,
		maxEmbeddingBytes: uint64(maxChunks) * uint64(dimensions) * 4,
	}, nil
}

type modelServiceComponents struct {
	indexConfig IndexConfig
	documents   *modelEncoder
	queries     *modelEncoder
}

func prepareModelService(
	client embedding.Client,
	options []Option,
) (modelServiceComponents, error) {
	if client == nil {
		return modelServiceComponents{}, ErrInvalidConfig
	}
	settings, err := defaultServiceOptions(client.Model())
	if err != nil {
		return modelServiceComponents{}, err
	}
	for _, option := range options {
		if option == nil {
			return modelServiceComponents{}, ErrInvalidConfig
		}
		if err := option(&settings); err != nil {
			return modelServiceComponents{}, err
		}
	}
	if settings.queryConfig.MaxBytes > client.Model().ContextTokens {
		return modelServiceComponents{}, ErrInvalidConfig
	}
	if settings.maxChunksPerDocument > settings.maxLiveVectors ||
		settings.searchLimits.MaxResults > settings.maxLiveVectors ||
		settings.searchLimits.MaxCandidateChunks > settings.maxLiveVectors ||
		settings.searchLimits.MaxChunksPerResult > settings.maxChunksPerDocument {
		return modelServiceComponents{}, ErrInvalidConfig
	}
	schema, err := DeriveSchema(client.Model(), settings.chunking, settings.metric)
	if err != nil {
		return modelServiceComponents{}, err
	}
	inputLimits, err := defaultInputLimits(
		client.Model().Dimensions,
		settings.maxChunksPerDocument,
	)
	if err != nil {
		return modelServiceComponents{}, err
	}
	documents, err := newModelEncoder(
		client,
		modelInputDocument,
		settings.chunker,
		schema,
		inputLimits,
		settings.queryConfig,
	)
	if err != nil {
		return modelServiceComponents{}, err
	}
	queries, err := newModelEncoder(
		client,
		modelInputQuery,
		settings.chunker,
		schema,
		inputLimits,
		settings.queryConfig,
	)
	if err != nil {
		return modelServiceComponents{}, err
	}
	return modelServiceComponents{
		indexConfig: IndexConfig{Schema: schema, Limits: Limits{
			MaxLiveVectors:          settings.maxLiveVectors,
			MaxChunksPerDocument:    inputLimits.maxChunks,
			MaxDocumentsPerSearch:   settings.searchLimits.MaxResults,
			MaxChunkCandidates:      settings.searchLimits.MaxCandidateChunks,
			MaxChunksPerDocumentHit: settings.searchLimits.MaxChunksPerResult,
		}},
		documents: documents,
		queries:   queries,
	}, nil
}
