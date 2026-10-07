package semantic

import "github.com/dariasmyr/fts-engine/pkg/vector"

type EmbeddingDescriptor struct {
	ProviderID          string
	ModelID             string
	ModelVersion        string
	PipelineFingerprint string
	Dimensions          int
	Metric              vector.Metric
	VectorFormatVersion uint32
}

func NewEmbeddingDescriptor(providerID, modelID, modelVersion, pipelineFingerprint string, dimensions int, metric vector.Metric, vectorFormatVersion uint32) (EmbeddingDescriptor, error) {
	if providerID == "" || modelID == "" || modelVersion == "" || pipelineFingerprint == "" {
		return EmbeddingDescriptor{}, ErrInvalidConfig
	}
	if _, err := vector.NewCalculator(dimensions, metric); err != nil {
		return EmbeddingDescriptor{}, err
	}
	if vectorFormatVersion == 0 {
		return EmbeddingDescriptor{}, ErrInvalidConfig
	}
	return EmbeddingDescriptor{
		ProviderID: providerID, ModelID: modelID, ModelVersion: modelVersion,
		PipelineFingerprint: pipelineFingerprint, Dimensions: dimensions, Metric: metric,
		VectorFormatVersion: vectorFormatVersion,
	}, nil
}

func (descriptor EmbeddingDescriptor) Calculator() (vector.Calculator, error) {
	return vector.NewCalculator(descriptor.Dimensions, descriptor.Metric)
}

func (descriptor EmbeddingDescriptor) IsValid() bool {
	return descriptor.ProviderID != "" && descriptor.ModelID != "" && descriptor.ModelVersion != "" &&
		descriptor.PipelineFingerprint != "" && descriptor.Dimensions > 0 && descriptor.VectorFormatVersion > 0 && descriptor.Metric.Valid()
}

type ChunkingDescriptor struct {
	ID          string
	Version     uint32
	Fingerprint string
}

func (descriptor ChunkingDescriptor) IsValid() bool {
	return descriptor.ID != "" && descriptor.Version > 0 && descriptor.Fingerprint != ""
}

// Schema identifies the semantic representation stored in an Index. Segments,
// encoders and persisted State must use exactly the same schema.
type Schema struct {
	Embedding EmbeddingDescriptor
	Chunking  ChunkingDescriptor
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
