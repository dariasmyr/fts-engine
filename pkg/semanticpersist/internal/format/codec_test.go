package format

import (
	"errors"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func TestEncoderFinishIncludesChecksumInFileLimit(t *testing.T) {
	encoder := newEncoder("TEST", 1, 12, Limits{MaxFileBytes: 11})

	if _, _, err := encoder.finish(); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("finish error = %v, want %v", err, ErrLimitExceeded)
	}
}

func TestEncodeStateReportsInvalidVectorIDLocation(t *testing.T) {
	limits := testLimits()
	value := State{
		Config: semantic.Config{
			Embedding: semantic.EmbeddingDescriptor{
				ProviderID: "provider", ModelID: "model", ModelVersion: "v1", PipelineFingerprint: "pipeline",
				Dimensions: 2, Metric: vector.MetricL2Squared, VectorFormatVersion: 1,
			},
			Chunking: semantic.ChunkingDescriptor{ID: "chunking", Version: 1, Fingerprint: "chunking-v1"},
			Limits: semantic.Limits{
				MaxLiveVectors: 10, MaxChunksPerDocument: 10, MaxDocumentsPerSearch: 1,
				MaxChunkCandidates: 10, MaxChunksPerDocumentHit: 1,
			},
			HNSW: semantic.HNSWTuning{
				MaxNeighbors: 4, EfConstruction: 8, Seed: 1, DefaultEfSearch: 4,
				MaxEfSearch: 10, DefaultVisitLimit: 10, MaxVisitLimit: 10,
			},
		},
		MaxAllocatedVectorID: 1,
		NextComponentID:      2,
		Segments: []StateSegment{{
			ComponentID:   1,
			LivenessWords: []uint64{1},
			Rows: []semantic.VectorRow{{
				Chunk: chunk.Ref{ID: "chunk", DocID: fts.DocID("document"), Field: fts.DefaultField},
			}},
		}},
	}

	_, _, err := EncodeState(value, limits)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("EncodeState error = %v, want category %v", err, ErrCorrupt)
	}
	const want = "state segment 0 row 0 has zero vector ID"
	if err.Error() != want {
		t.Fatalf("EncodeState error = %q, want %q", err, want)
	}
}

func testLimits() Limits {
	return Limits{
		MaxFileBytes: 1 << 20, MaxDimensions: 1_024, MaxVectors: 100,
		MaxDocuments: 100, MaxStringBytes: 1_024, MaxChunksPerDocument: 100,
		MaxK: 100, MaxVectorBytes: 1 << 20, MaxEfSearch: 100, MaxVisitLimit: 100,
	}
}
