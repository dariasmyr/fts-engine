package testsuite

import (
	"context"
	"fts-hw/internal/services/fts"
	"fts-hw/internal/services/fts/factory"
	"fts-hw/internal/services/fts/filters"
	"testing"
)

type docFixture struct {
	id      string
	content string
}

var fixtureDocs = []docFixture{
	{id: "1", content: "hotel hotel plaza"},
	{id: "2", content: "hotel booking suite"},
	{id: "3", content: "rosa cruise barge"},
}

func TestIndexerFilterMatrixBySearchModes(t *testing.T) {
	indexers := []string{"radix", "slicedradix", "hamt", "hamtpointered", "trigram"}
	filterOpts := []factory.FilterOptions{
		{Type: "none"},
		{
			Type:  "bloom",
			Bloom: filters.BloomConfig{Capacity: 1024, Hashes: 5},
		},
		{
			Type:   "cuckoo",
			Cuckoo: filters.CuckooConfig{Capacity: 1024, BucketSz: 4, MaxKicks: 100},
		},
		{
			Type:   "ribbon",
			Ribbon: filters.RibbonConfig{Bits: 4096, Width: 8},
		},
	}

	for _, indexerType := range indexers {
		for _, opts := range filterOpts {
			name := "indexer=" + indexerType + "/filter=" + opts.Type
			t.Run(name, func(t *testing.T) {
				svc := buildSearchService(t, indexerType, opts)

				for _, doc := range fixtureDocs {
					if err := svc.IndexDocument(context.Background(), doc.id, doc.content); err != nil {
						t.Fatalf("index failed: %v", err)
					}
				}

				t.Run("mode=exact", func(t *testing.T) {
					res, err := svc.SearchDocuments(context.Background(), "hotel", 10)
					if err != nil {
						t.Fatalf("search failed: %v", err)
					}
					if res.TotalResultsCount != 2 {
						t.Fatalf("expected 2 results, got %d", res.TotalResultsCount)
					}
					if len(res.ResultData) == 0 || res.ResultData[0].ID != "1" {
						t.Fatalf("expected top result ID=1, got %+v", res.ResultData)
					}
				})

				t.Run("mode=case_punctuation", func(t *testing.T) {
					res, err := svc.SearchDocuments(context.Background(), "HOTEL!!!", 10)
					if err != nil {
						t.Fatalf("search failed: %v", err)
					}
					if res.TotalResultsCount != 2 {
						t.Fatalf("expected 2 results, got %d", res.TotalResultsCount)
					}
				})

				t.Run("mode=other_exact", func(t *testing.T) {
					res, err := svc.SearchDocuments(context.Background(), "rosa", 10)
					if err != nil {
						t.Fatalf("search failed: %v", err)
					}
					if res.TotalResultsCount != 1 {
						t.Fatalf("expected 1 result, got %d", res.TotalResultsCount)
					}
					if len(res.ResultData) == 0 || res.ResultData[0].ID != "3" {
						t.Fatalf("expected result ID=3, got %+v", res.ResultData)
					}
				})

				t.Run("mode=missing_term", func(t *testing.T) {
					res, err := svc.SearchDocuments(context.Background(), "zzz", 10)
					if err != nil {
						t.Fatalf("search failed: %v", err)
					}
					if res.TotalResultsCount != 0 {
						t.Fatalf("expected 0 results, got %d", res.TotalResultsCount)
					}
				})

				t.Run("mode=repeat_indexing", func(t *testing.T) {
					if err := svc.IndexDocument(context.Background(), "1", fixtureDocs[0].content); err != nil {
						t.Fatalf("index failed: %v", err)
					}

					res, err := svc.SearchDocuments(context.Background(), "hotel", 10)
					if err != nil {
						t.Fatalf("search failed: %v", err)
					}
					if len(res.ResultData) < 2 {
						t.Fatalf("expected at least 2 results, got %+v", res.ResultData)
					}
					if res.ResultData[0].ID != "1" {
						t.Fatalf("expected top result ID=1, got %+v", res.ResultData)
					}
					if res.ResultData[0].TotalMatches <= res.ResultData[1].TotalMatches {
						t.Fatalf("expected repeated indexing to raise top score, got %+v", res.ResultData)
					}
				})
			})
		}
	}
}

func buildSearchService(
	t *testing.T,
	indexerType string,
	filterOpts factory.FilterOptions,
) *fts.SearchService {
	t.Helper()

	indexer, keyGen, err := factory.NewIndexer(indexerType)
	if err != nil {
		t.Fatalf("new indexer failed: %v", err)
	}

	filter, err := factory.NewFilter(filterOpts)
	if err != nil {
		t.Fatalf("new filter failed: %v", err)
	}

	return fts.NewSearchService(indexer, keyGen, filter)
}
