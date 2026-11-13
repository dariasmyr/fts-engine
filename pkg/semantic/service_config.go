package semantic

import (
	"math"

	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
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

// Config combines an Index contract with its model-backed pipeline contract.
type Config struct {
	Index    IndexConfig
	Pipeline PipelineConfig
}

// PipelineConfig is the frozen, reproducible contract of the standard encoder.
type PipelineConfig struct {
	Model                   embedding.Model
	Chunking                chunk.Config
	EncodingBehaviorVersion uint32
	Operational             PipelineOperationalLimits
}

// PipelineOperationalLimits groups frozen limits owned by the encoding pipeline.
type PipelineOperationalLimits struct{ Encoding EncodingLimits }

// EncodingLimits bounds source text and embedding material per encoding call.
type EncodingLimits struct {
	MaxFields         int
	MaxSourceBytes    uint64
	MaxEmbeddingBytes uint64
}

// DefaultConfig returns the canonical defaults for model.
func DefaultConfig(model embedding.Model) (Config, error) {
	if err := model.Validate(); err != nil {
		return Config{}, err
	}
	maxChunkBytes := min(model.ContextTokens, 4096)
	targetBytes := max(1, maxChunkBytes*3/4)
	c := Config{
		Index: IndexConfig{
			Dimensions: model.Dimensions,
			Metric:     vector.MetricCosine,
			Operational: OperationalLimits{
				Capacity:  CapacityLimits{MaxLiveVectors: defaultMaxLiveVectors},
				Ingestion: IngestionLimits{MaxChunksPerDocument: defaultMaxChunksPerDocument},
				Search: SearchLimits{
					MaxResults:         defaultMaxSearchResults,
					MaxCandidateChunks: defaultMaxChunkCandidates,
					MaxChunksPerResult: defaultMaxChunksPerDocumentHit,
				},
			},
		},
		Pipeline: PipelineConfig{
			Model: model,
			Chunking: chunk.Config{
				TargetBytes:  targetBytes,
				MaxBytes:     maxChunkBytes,
				OverlapBytes: min(targetBytes-1, maxChunkBytes/8),
			},
			EncodingBehaviorVersion: CurrentEncodingBehaviorVersion,
		},
	}
	return c.normalized()
}

// Validate checks whether c can be normalized into a valid new service config.
func (c Config) Validate() error { _, err := c.normalized(); return err }
func (c Config) normalized() (Config, error) {
	index, err := c.Index.normalized()
	if err != nil {
		return Config{}, err
	}
	c.Index = index
	pipeline, err := c.Pipeline.normalized(index)
	if err != nil {
		return Config{}, err
	}
	c.Pipeline = pipeline
	return c, nil
}
func (c Config) validateCanonical() error {
	n, err := c.normalized()
	if err != nil || n != c {
		return ErrInvalidConfig
	}
	return nil
}
func (p PipelineConfig) normalized(index IndexConfig) (PipelineConfig, error) {
	if p.EncodingBehaviorVersion == 0 {
		p.EncodingBehaviorVersion = CurrentEncodingBehaviorVersion
	}
	if p.EncodingBehaviorVersion != CurrentEncodingBehaviorVersion {
		return PipelineConfig{}, ErrUnsupportedEncodingBehavior
	}
	if p.Model.Validate() != nil || p.Model.Dimensions != index.Dimensions ||
		p.Chunking.TargetBytes < 0 || p.Chunking.MaxBytes < 0 || p.Chunking.OverlapBytes < 0 ||
		uint64(p.Chunking.TargetBytes) > math.MaxUint32 || uint64(p.Chunking.MaxBytes) > math.MaxUint32 ||
		uint64(p.Chunking.OverlapBytes) > math.MaxUint32 || p.Chunking.Validate() != nil {
		return PipelineConfig{}, ErrInvalidConfig
	}
	maxChunks := max(
		index.Operational.Ingestion.MaxChunksPerDocument,
		index.Operational.Search.MaxQueryChunks,
	)
	encoding, err := p.Operational.Encoding.normalized(index.Dimensions, maxChunks)
	if err != nil {
		return PipelineConfig{}, err
	}
	p.Operational.Encoding = encoding
	return p, nil
}
func (p PipelineConfig) validateCanonical(index IndexConfig) error {
	n, err := p.normalized(index)
	if err != nil || n != p {
		return ErrInvalidConfig
	}
	return nil
}

// ValidateCanonical checks a persisted pipeline without applying defaults.
func (p PipelineConfig) ValidateCanonical(index IndexConfig) error { return p.validateCanonical(index) }
func (c EncodingLimits) normalized(dimensions, maxChunks int) (EncodingLimits, error) {
	if c.MaxFields < 0 || dimensions <= 0 || maxChunks <= 0 || uint64(dimensions) > math.MaxUint64/4 {
		return EncodingLimits{}, ErrInvalidConfig
	}
	bpv := uint64(dimensions) * 4
	if uint64(maxChunks) > math.MaxUint64/bpv {
		return EncodingLimits{}, ErrInvalidConfig
	}
	if c.MaxFields == 0 {
		c.MaxFields = defaultMaxFields
	}
	if c.MaxSourceBytes == 0 {
		c.MaxSourceBytes = defaultMaxSourceBytes
	}
	if c.MaxEmbeddingBytes == 0 {
		c.MaxEmbeddingBytes = uint64(maxChunks) * bpv
	}
	if c.MaxEmbeddingBytes < bpv {
		return EncodingLimits{}, ErrInvalidConfig
	}
	return c, nil
}
