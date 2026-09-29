package format

import (
	"errors"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func TestStateRoundTripAcceptsMinimumSizedRow(t *testing.T) {
	value := State{
		Embedding: semantic.EmbeddingDescriptor{
			ProviderID: "p", ModelID: "m", ModelVersion: "v", PipelineFingerprint: "f",
			Dimensions: 1, Metric: vector.MetricL2Squared, VectorFormatVersion: 1,
		},
		Chunking:                semantic.ChunkingDescriptor{ID: "c", Version: 1, Fingerprint: "f"},
		MaxAllocatedVectorID:    1,
		ComponentID:             1,
		MaxK:                    1,
		MaxChunkCandidates:      1,
		MaxChunksPerDocumentHit: 1,
		Rows: []semantic.VectorRow{{
			VectorID: 1,
			Chunk:    chunk.Ref{ID: "i", DocID: fts.DocID("d"), Field: "f"},
		}},
	}
	limits := Limits{MaxFileBytes: 4096, MaxDimensions: 1, MaxVectors: 1, MaxDocuments: 1, MaxStringBytes: 8, MaxChunksPerDocument: 1, MaxK: 1}
	data, _, err := EncodeState(value, limits)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeState(data, limits); err != nil {
		t.Fatalf("DecodeState() = %v", err)
	}
}

func TestStateRejectsWatermarkBelowRows(t *testing.T) {
	value := State{
		Embedding: semantic.EmbeddingDescriptor{
			ProviderID: "p", ModelID: "m", ModelVersion: "v", PipelineFingerprint: "f",
			Dimensions: 1, Metric: vector.MetricL2Squared, VectorFormatVersion: 1,
		},
		Chunking:                semantic.ChunkingDescriptor{ID: "c", Version: 1, Fingerprint: "f"},
		MaxAllocatedVectorID:    0,
		ComponentID:             1,
		MaxK:                    1,
		MaxChunkCandidates:      1,
		MaxChunksPerDocumentHit: 1,
		Rows: []semantic.VectorRow{{
			VectorID: 1,
			Chunk:    chunk.Ref{ID: "i", DocID: fts.DocID("d"), Field: "f"},
		}},
	}
	limits := Limits{MaxFileBytes: 4096, MaxDimensions: 1, MaxVectors: 1, MaxDocuments: 1, MaxStringBytes: 8, MaxChunksPerDocument: 1, MaxK: 1}
	if _, _, err := EncodeState(value, limits); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("EncodeState() = %v, want %v", err, ErrCorrupt)
	}
}
