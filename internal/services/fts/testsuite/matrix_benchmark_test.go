package testsuite

import (
	"context"
	"fmt"
	"fts-hw/internal/services/fts"
	"fts-hw/internal/services/fts/factory"
	"fts-hw/internal/services/fts/filters"
	"testing"
)

func BenchmarkIndexerFilterMatrixIndexing(b *testing.B) {
	indexers := []string{"radix", "slicedradix", "hamt", "hamtpointered", "trigram"}
	filterOpts := []factory.FilterOptions{
		{Type: "none"},
		{Type: "bloom", Bloom: filters.BloomConfig{Capacity: 50000, Hashes: 5}},
		{Type: "cuckoo", Cuckoo: filters.CuckooConfig{Capacity: 50000, BucketSz: 4, MaxKicks: 100}},
		{Type: "ribbon", Ribbon: filters.RibbonConfig{Bits: 1 << 20, Width: 8}},
	}
	docs := benchmarkDocs(400)

	for _, indexerType := range indexers {
		for _, opts := range filterOpts {
			name := fmt.Sprintf("indexer=%s/filter=%s", indexerType, opts.Type)
			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					svc := buildSearchServiceForBench(b, indexerType, opts)
					for _, doc := range docs {
						if err := svc.IndexDocument(context.Background(), doc.id, doc.content); err != nil {
							b.Fatalf("index failed: %v", err)
						}
					}
				}
			})
		}
	}
}

func BenchmarkIndexerFilterMatrixSearchModes(b *testing.B) {
	indexers := []string{"radix", "slicedradix", "hamt", "hamtpointered", "trigram"}
	filterOpts := []factory.FilterOptions{
		{Type: "none"},
		{Type: "bloom", Bloom: filters.BloomConfig{Capacity: 50000, Hashes: 5}},
		{Type: "cuckoo", Cuckoo: filters.CuckooConfig{Capacity: 50000, BucketSz: 4, MaxKicks: 100}},
		{Type: "ribbon", Ribbon: filters.RibbonConfig{Bits: 1 << 20, Width: 8}},
	}
	queries := []string{"hotel", "HOTEL!!!", "rosa", "booking suite", "nonexistenttoken"}
	docs := benchmarkDocs(700)

	for _, indexerType := range indexers {
		for _, opts := range filterOpts {
			for _, query := range queries {
				name := fmt.Sprintf("indexer=%s/filter=%s/query=%s", indexerType, opts.Type, query)
				b.Run(name, func(b *testing.B) {
					svc := buildSearchServiceForBench(b, indexerType, opts)
					for _, doc := range docs {
						if err := svc.IndexDocument(context.Background(), doc.id, doc.content); err != nil {
							b.Fatalf("index failed: %v", err)
						}
					}

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if _, err := svc.SearchDocuments(context.Background(), query, 10); err != nil {
							b.Fatalf("search failed: %v", err)
						}
					}
				})
			}
		}
	}
}

func buildSearchServiceForBench(
	b *testing.B,
	indexerType string,
	filterOpts factory.FilterOptions,
) *fts.SearchService {
	b.Helper()

	indexer, keyGen, err := factory.NewIndexer(indexerType)
	if err != nil {
		b.Fatalf("new indexer failed: %v", err)
	}

	filter, err := factory.NewFilter(filterOpts)
	if err != nil {
		b.Fatalf("new filter failed: %v", err)
	}

	return fts.NewSearchService(indexer, keyGen, filter)
}

func benchmarkDocs(n int) []docFixture {
	base := []string{
		"hotel booking suite travel comfort",
		"rosa cruise barge river france tourism",
		"wikipedia article abstract document text",
		"search engine index token stem filter",
		"radix hamt trigram indexing strategy",
	}

	docs := make([]docFixture, 0, n)
	for i := 0; i < n; i++ {
		pattern := base[i%len(base)] + " " + base[(i+1)%len(base)] + " " + base[(i+2)%len(base)]
		docs = append(docs, docFixture{
			id:      fmt.Sprintf("%d", i+1),
			content: pattern,
		})
	}
	return docs
}
