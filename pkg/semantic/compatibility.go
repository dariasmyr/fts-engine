package semantic

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"

	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// DeriveSchema creates persisted document compatibility from model metadata,
// document preprocessing, chunking, and vector metric. Query-only config is
// intentionally excluded.
func DeriveSchema(
	model embedding.Model,
	chunking ChunkingSchema,
	metric vector.Metric,
) (Schema, error) {
	if err := model.Validate(); err != nil || !chunking.IsValid() || !metric.Valid() {
		return Schema{}, ErrInvalidConfig
	}
	pipeline := newCompatibilityHash("semantic/document")
	pipeline.writeString(model.ID)
	pipeline.writeString(model.Revision)
	pipeline.writeUint32(uint32(model.Dimensions))
	pipeline.writeUint32(uint32(metric))
	pipeline.writeString("prefix-before-embed")
	pipeline.writeString(model.DocumentPrefix)
	embeddingSchema, err := NewEmbeddingSchema(
		model.ID,
		model.Revision,
		pipeline.sum(),
		model.Dimensions,
		metric,
		1,
	)
	if err != nil {
		return Schema{}, err
	}
	return Schema{Embedding: embeddingSchema, Chunking: chunking}, nil
}

// DeriveRetrievalFingerprint identifies the model's document/query retrieval
// contract for evaluation. The high-level service extends this fingerprint
// with its query config. Unlike Schema, the result is not persisted as document
// compatibility.
func DeriveRetrievalFingerprint(schema Schema, model embedding.Model) (string, error) {
	return deriveRetrievalFingerprint(schema, model, nil)
}

func deriveRetrievalFingerprint(schema Schema, model embedding.Model, queryConfig *QueryConfig) (string, error) {
	if !schema.IsValid() || model.Validate() != nil ||
		schema.Embedding.ModelID != model.ID ||
		schema.Embedding.ModelRevision != model.Revision ||
		schema.Embedding.Dimensions != model.Dimensions {
		return "", ErrEmbeddingMismatch
	}
	fingerprint := newCompatibilityHash("semantic/retrieval")
	fingerprint.writeString(schema.Embedding.PipelineFingerprint)
	fingerprint.writeString(schema.Chunking.ID)
	fingerprint.writeUint32(schema.Chunking.Version)
	fingerprint.writeString(schema.Chunking.Fingerprint)
	fingerprint.writeUint32(uint32(model.ContextTokens))
	fingerprint.writeString("query-prefix-before-embed")
	fingerprint.writeString(model.QueryPrefix)
	if queryConfig != nil {
		// Keep the canonical label stable across public API naming changes.
		fingerprint.writeString("query-policy")
		fingerprint.writeUint64(uint64(queryConfig.MaxBytes))
		fingerprint.writeUint32(uint32(queryConfig.LongQuery))
	}
	return fingerprint.sum(), nil
}

type compatibilityHash struct {
	hash hash.Hash
}

func newCompatibilityHash(kind string) compatibilityHash {
	hash := sha256.New()
	result := compatibilityHash{hash: hash}
	result.writeString("fts-engine/semantic-compatibility")
	result.writeUint32(CompatibilityFormatVersion)
	result.writeString(kind)
	return result
}

func (h compatibilityHash) writeUint32(value uint32) {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	_, _ = h.hash.Write(encoded[:])
}

func (h compatibilityHash) writeUint64(value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = h.hash.Write(encoded[:])
}

func (h compatibilityHash) writeString(value string) {
	h.writeUint64(uint64(len(value)))
	_, _ = h.hash.Write([]byte(value))
}

func (h compatibilityHash) sum() string {
	return "v1:" + hex.EncodeToString(h.hash.Sum(nil))
}
