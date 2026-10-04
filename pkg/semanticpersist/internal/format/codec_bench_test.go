package format

import (
	"crypto/sha256"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

var (
	benchmarkEncodedData []byte
	benchmarkReference   FileReference
)

func BenchmarkEncodeCurrent(b *testing.B) {
	limits := benchmarkLimits()
	value := Current{GenerationID: 1, ManifestHash: sha256.Sum256([]byte("manifest"))}
	b.ReportAllocs()
	for b.Loop() {
		var err error
		benchmarkEncodedData, benchmarkReference, err = EncodeCurrent(value, limits)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeManifest(b *testing.B) {
	limits := benchmarkLimits()
	ref := FileReference{Size: 1, SHA256: sha256.Sum256([]byte("object"))}
	segment := SegmentObject{ObjectID: "seg-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", Vectors: ref, Graph: ref}
	value := Manifest{GenerationID: 1, Segments: make([]SegmentObject, 1_000), State: ref}
	for i := range value.Segments {
		value.Segments[i] = segment
	}
	b.ReportAllocs()
	for b.Loop() {
		var err error
		benchmarkEncodedData, benchmarkReference, err = EncodeManifest(value, limits)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeState(b *testing.B) {
	limits := benchmarkLimits()
	rows := make([]semantic.VectorRow, 10_000)
	for i := range rows {
		rows[i] = semantic.VectorRow{
			VectorID: uint64(i + 1),
			Chunk: chunk.Ref{
				ID: "chunk", DocID: fts.DocID("document"), Field: fts.DefaultField,
				Ordinal: uint32(i), StartByte: uint64(i), EndByte: uint64(i + 1),
			},
		}
	}
	value := State{
		Config: semantic.Config{
			Embedding: semantic.EmbeddingDescriptor{
				ProviderID: "provider", ModelID: "model", ModelVersion: "v1", PipelineFingerprint: "pipeline",
				Dimensions: 2, Metric: vector.MetricL2Squared, VectorFormatVersion: 1,
			},
			Chunking: semantic.ChunkingDescriptor{ID: "chunking", Version: 1, Fingerprint: "chunking-v1"},
			Limits: semantic.Limits{
				MaxLiveVectors: 10_000, MaxChunksPerDocument: 10_000, MaxDocumentsPerSearch: 10,
				MaxChunkCandidates: 100, MaxChunksPerDocumentHit: 3,
			},
			HNSW: semantic.HNSWTuning{
				MaxNeighbors: 16, EfConstruction: 64, Seed: 1, DefaultEfSearch: 64,
				MaxEfSearch: 100, DefaultVisitLimit: 10_000, MaxVisitLimit: 10_000,
			},
		},
		Revision: 1, MaxAllocatedVectorID: 10_000, NextComponentID: 2,
		Segments: []StateSegment{{ComponentID: 1, Rows: rows, LivenessWords: make([]uint64, (len(rows)+63)/64)}},
	}
	b.ReportAllocs()
	for b.Loop() {
		var err error
		benchmarkEncodedData, benchmarkReference, err = EncodeState(value, limits)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkLimits() Limits {
	return Limits{
		MaxFileBytes: 16 << 20, MaxDimensions: 1_024, MaxVectors: 20_000,
		MaxDocuments: 20_000, MaxStringBytes: 1_024, MaxChunksPerDocument: 20_000,
		MaxK: 20_000, MaxVectorBytes: 1 << 30, MaxEfSearch: 20_000, MaxVisitLimit: 20_000,
	}
}
