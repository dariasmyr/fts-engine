package semantic

import "github.com/dariasmyr/fts-engine/v2/pkg/vector"

// CompatibilityFormatVersion identifies the canonical semantic compatibility
// encoding used to derive pipeline fingerprints.
const CompatibilityFormatVersion = uint32(1)

// EmbeddingSchema identifies the model output and document transformation used
// by vectors stored in an index.
type EmbeddingSchema struct {
	CompatibilityVersion uint32
	ModelID              string
	ModelRevision        string
	PipelineFingerprint  string
	Dimensions           int
	Metric               vector.Metric
	VectorFormatVersion  uint32
}

// NewEmbeddingSchema validates and creates low-level embedding compatibility
// metadata. Normal callers should let New derive it from a client.
func NewEmbeddingSchema(
	modelID string,
	modelRevision string,
	pipelineFingerprint string,
	dimensions int,
	metric vector.Metric,
	vectorFormatVersion uint32,
) (EmbeddingSchema, error) {
	if modelID == "" || modelRevision == "" || pipelineFingerprint == "" {
		return EmbeddingSchema{}, ErrInvalidConfig
	}
	if _, err := vector.NewSpace(dimensions, metric); err != nil {
		return EmbeddingSchema{}, err
	}
	if vectorFormatVersion == 0 {
		return EmbeddingSchema{}, ErrInvalidConfig
	}
	return EmbeddingSchema{
		CompatibilityVersion: CompatibilityFormatVersion,
		ModelID:              modelID, ModelRevision: modelRevision,
		PipelineFingerprint: pipelineFingerprint, Dimensions: dimensions, Metric: metric,
		VectorFormatVersion: vectorFormatVersion,
	}, nil
}

// VectorSpace returns the vector operations required by this schema.
func (schema EmbeddingSchema) VectorSpace() (vector.Space, error) {
	return vector.NewSpace(schema.Dimensions, schema.Metric)
}

func (schema EmbeddingSchema) IsValid() bool {
	return schema.CompatibilityVersion == CompatibilityFormatVersion &&
		schema.ModelID != "" && schema.ModelRevision != "" &&
		schema.PipelineFingerprint != "" && schema.Dimensions > 0 && schema.VectorFormatVersion > 0 && schema.Metric.Valid()
}

// ChunkingSchema identifies the chunk boundaries used by stored vectors.
type ChunkingSchema struct {
	ID          string
	Version     uint32
	Fingerprint string
}

func (schema ChunkingSchema) IsValid() bool {
	return schema.ID != "" && schema.Version > 0 && schema.Fingerprint != ""
}

// Schema identifies the semantic representation stored in an Index. Segments,
// encoders and restored index states must use exactly the same schema.
type Schema struct {
	Embedding EmbeddingSchema
	Chunking  ChunkingSchema
}

func (schema Schema) IsValid() bool {
	return schema.Embedding.IsValid() && schema.Chunking.IsValid()
}

func validateSchemaCompatibility(left, right Schema) error {
	if left.Embedding != right.Embedding {
		return ErrEmbeddingMismatch
	}
	if left.Chunking != right.Chunking {
		return ErrChunkingMismatch
	}
	return nil
}
