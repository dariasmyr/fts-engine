package main

import (
	"bytes"
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/semanticpersist"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
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
	source, err := vector.NewMemorySource(calculator, values)
	must(err)

	searcher, err := hnsw.BuildSearcher(ctx, source, hnsw.BuildOptions{
		BuildConfig: hnsw.BuildConfig{
			Dimensions: 2, Metric: vector.MetricL2Squared,
			MaxVectors: len(values), MaxVectorBytes: uint64(len(values) * 2 * 4),
			MaxNeighbors: 2, EfConstruction: 8, Seed: 42,
		},
		SearchConfig: hnsw.SearchConfig{
			DefaultEfSearch: 8, MaxEfSearch: 32,
			DefaultVisitLimit: len(values), MaxVisitLimit: len(values), MaxK: 10,
		},
		Progress: func(progress hnsw.BuildProgress) {
			fmt.Printf("build phase=%-9s vectors=%2d/%d\n", progress.Phase, progress.Completed, progress.Total)
		},
	})
	must(err)

	entry, maxLevel, _ := searcher.EntryPoint()
	fmt.Printf("\nentry=node-%d max-level=%d\n", entry, maxLevel)
	for node := 0; node < searcher.NodeCount(); node++ {
		level, _ := searcher.NodeLevel(hnsw.NodeOrdinal(node))
		fmt.Printf("node-%d vector-row=%d max-level=%d\n", node, node, level)
		for currentLevel := int(level); currentLevel >= 0; currentLevel-- {
			neighbors, _ := searcher.Neighbors(hnsw.NodeOrdinal(node), currentLevel)
			fmt.Printf("  level=%d neighbors=%v\n", currentLevel, neighbors)
		}
	}

	stats := searcher.GraphStats()
	storageStats := searcher.StorageStats()
	fmt.Printf("\ngraph nodes=%d links=%d reachable=%d unreachable=%d bytes=%d\n",
		stats.NodeCount, storageStats.DirectedLinks, stats.ReachableNodes, stats.UnreachableNodes, storageStats.TotalBytes)

	query := []float32{5.2, 5.1}
	result, err := searcher.Search(ctx, query, 3, vector.SearchOptions{EfSearch: 8})
	must(err)
	fmt.Printf("ANN query=%v hits=%v visited=%d distances=%d\n",
		query, result.Hits, result.Stats.VisitedNodes, result.Stats.DistanceComputations)

	vectorData, vectorMetadata, err := semanticpersist.MarshalSource(source, 10)
	must(err)
	vectorReference := hnsw.VectorFileReference{Size: vectorMetadata.Size, SHA256: vectorMetadata.SHA256}
	graphData, graphMetadata, err := hnsw.MarshalGraph(searcher, vectorReference)
	must(err)
	fmt.Printf("\npersist vectors.bin=%d bytes graph.bin=%d bytes\n", vectorMetadata.Size, graphMetadata.Size)

	openedVectors, openedVectorMetadata, err := semanticpersist.OpenVectorSource(bytes.NewReader(vectorData), semanticpersist.DefaultCodecLimits())
	must(err)
	openedReference := hnsw.VectorFileReference{Size: openedVectorMetadata.Size, SHA256: openedVectorMetadata.SHA256}
	openedSearcher, _, err := hnsw.OpenSearcherContext(ctx, bytes.NewReader(graphData), openedVectors, openedReference, hnsw.DefaultGraphLimits())
	must(err)

	reopened, err := openedSearcher.Search(ctx, query, 3, vector.SearchOptions{EfSearch: 8})
	must(err)
	fmt.Printf("reopened hits=%v build=%+v\n", reopened.Hits, openedSearcher.BuildInfo())
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
