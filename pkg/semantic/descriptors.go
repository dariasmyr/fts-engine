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

type ChunkingDescriptor struct {
	ID          string
	Version     uint32
	Fingerprint string
}

type PipelineDescriptor struct {
	Embedding EmbeddingDescriptor
	Chunking  ChunkingDescriptor
}

func (descriptor EmbeddingDescriptor) IsValid() bool {
	return descriptor.ProviderID != "" && descriptor.ModelID != "" && descriptor.ModelVersion != "" &&
		descriptor.PipelineFingerprint != "" && descriptor.Dimensions > 0 && descriptor.VectorFormatVersion > 0 && descriptor.Metric.Valid()
}

func (descriptor ChunkingDescriptor) IsValid() bool {
	return descriptor.ID != "" && descriptor.Version > 0 && descriptor.Fingerprint != ""
}

func descriptorsEqual(left, right PipelineDescriptor) (bool, error) {
	if left.Embedding != right.Embedding {
		return false, ErrEmbeddingMismatch
	}
	if left.Chunking != right.Chunking {
		return false, ErrChunkingMismatch
	}
	return true, nil
}
