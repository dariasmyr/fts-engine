package hnswformat

import (
	"bytes"
	"context"
	"crypto/sha256"
	"reflect"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/internal/memorystore"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"
)

func TestGraphFileRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	space, err := vector.NewSpace(2, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}
	vectors, err := memorystore.New(space, [][]float32{{0, 0}, {1, 0}, {3, 0}, {10, 0}})
	if err != nil {
		t.Fatal(err)
	}
	config := hnsw.Config{
		Build:  hnsw.BuildConfig{MaxNeighbors: 2, EfConstruction: 4, Seed: 7},
		Search: hnsw.SearchConfig{EfSearch: 4, VisitLimit: 4},
	}
	index, err := hnsw.Build(ctx, vectors, config)
	if err != nil {
		t.Fatal(err)
	}
	want, err := index.Search(ctx, []float32{1.1, 0}, 4, hnsw.QueryConfig{})
	if err != nil {
		t.Fatal(err)
	}

	vectorRef := FileRef{Size: 64, SHA256: sha256.Sum256([]byte("vectors"))}
	var encoded bytes.Buffer
	metadata, err := WriteGraphFile(ctx, &encoded, index, vectorRef)
	if err != nil {
		t.Fatal(err)
	}
	encodedSize, err := GraphFileSize(index.TopologyStats())
	if err != nil {
		t.Fatal(err)
	}
	if encodedSize != metadata.Size {
		t.Fatalf("GraphFileSize() = %d, want %d", encodedSize, metadata.Size)
	}
	limits := Config{
		MaxFileBytes:   1 << 20,
		MaxGraphBytes:  1 << 20,
		MaxVectorBytes: 1 << 20,
		MaxVectors:     16,
		MaxGraphLinks:  1024,
		MaxDimensions:  16,
		MaxK:           16,
	}
	restored, decodedMetadata, err := OpenGraphFile(ctx, encoded.Bytes(), vectors, vectorRef, config, limits)
	if err != nil {
		t.Fatal(err)
	}
	if decodedMetadata != metadata {
		t.Fatalf("decoded metadata = %+v, want %+v", decodedMetadata, metadata)
	}
	got, err := restored.Search(ctx, []float32{1.1, 0}, 4, hnsw.QueryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("restored search = %+v, want %+v", got, want)
	}
}
