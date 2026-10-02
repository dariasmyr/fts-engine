package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
	"github.com/dariasmyr/fts-engine/pkg/vectorstore"
)

func main() {
	ctx := context.Background()
	values := [][]float32{
		{0, 0}, {1, 0}, {0, 1}, {1, 1},
		{5, 5}, {6, 5}, {5, 6}, {6, 6},
		{10, 0}, {11, 0}, {10, 1}, {11, 1},
	}

	calculator, err := vector.NewCalculator(2, vector.MetricL2Squared)
	must(err)
	source, err := vectorstore.NewMemoryVectorStore(calculator, values)
	must(err)

	index, err := hnsw.Build(ctx, source, hnsw.BuildOptions{
		Build: hnsw.BuildConfig{
			Dimensions: 2, Metric: vector.MetricL2Squared,
			MaxVectors: len(values), MaxVectorBytes: uint64(len(values) * 2 * 4),
			MaxNeighbors: 2, EfConstruction: 8, Seed: 42,
		},
		Search: hnsw.SearchConfig{
			DefaultEfSearch: 8, MaxEfSearch: 32,
			DefaultVisitLimit: len(values), MaxVisitLimit: len(values), MaxK: 10,
		},
		Progress: func(progress hnsw.BuildProgress) {
			fmt.Printf("build phase=%-9s vectors=%2d/%d\n", progress.Phase, progress.Completed, progress.Total)
		},
	})
	must(err)

	report := index.Report()
	stats := report.Graph
	storageStats := report.Storage
	fmt.Printf("\ngraph nodes=%d links=%d reachable=%d unreachable=%d bytes=%d\n",
		stats.NodeCount, storageStats.DirectedLinks, stats.ReachableNodes, stats.UnreachableNodes, storageStats.TotalBytes)

	query := []float32{5.2, 5.1}
	result, err := index.Search(ctx, query, 3, vector.SearchOptions{EfSearch: 8})
	must(err)
	fmt.Printf("ANN query=%v hits=%v visited=%d distances=%d\n",
		query, result.Hits, result.Stats.VisitedNodes, result.Stats.DistanceComputations)

	vectorReference, err := callerVectorReference(ctx, source)
	must(err)
	var graphData bytes.Buffer
	graphMetadata, err := hnsw.WriteGraph(ctx, &graphData, index, vectorReference)
	must(err)
	fmt.Printf("\ncaller vector reference=%d bytes graph.bin=%d bytes\n", vectorReference.Size, graphMetadata.Size)

	openedReference, err := callerVectorReference(ctx, source)
	must(err)
	openedIndex, _, err := hnsw.OpenGraph(ctx, bytes.NewReader(graphData.Bytes()), source, openedReference, hnsw.DefaultGraphLimits())
	must(err)

	reopened, err := openedIndex.Search(ctx, query, 3, vector.SearchOptions{EfSearch: 8})
	must(err)
	fmt.Printf("reopened hits=%v build=%+v\n", reopened.Hits, openedIndex.Report().Build)
}

func callerVectorReference(ctx context.Context, source vectorstore.PreparedVectorStore) (hnsw.VectorFileReference, error) {
	hash := sha256.New()
	values := make([]float32, source.Dimensions())
	var encoded [4]byte
	for ordinal := range source.Len() {
		if err := source.ReadVectorInto(ctx, vector.Ordinal(ordinal), values); err != nil {
			return hnsw.VectorFileReference{}, err
		}
		for _, value := range values {
			binary.LittleEndian.PutUint32(encoded[:], math.Float32bits(value))
			_, _ = hash.Write(encoded[:])
		}
	}
	result := hnsw.VectorFileReference{Size: uint64(source.Len()) * uint64(source.Dimensions()) * 4}
	copy(result.SHA256[:], hash.Sum(nil))
	return result, nil
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
