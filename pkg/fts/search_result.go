package fts

import (
	"cmp"
	"slices"
)

func searchResultFromOrdHits(hits map[DocOrd]docAccum, maxResults int, useScore bool, registry *DocRegistry) *SearchResult {
	results, totalFound := resultsFromOrdHits(hits, useScore, registry)
	if maxResults <= 0 || maxResults > totalFound {
		maxResults = totalFound
	}
	return &SearchResult{
		Results:           results[:maxResults],
		TotalResultsCount: totalFound,
	}
}

func resultsFromOrdHits(hits map[DocOrd]docAccum, useScore bool, registry *DocRegistry) ([]Result, int) {
	results := make([]Result, 0, len(hits))
	for ord, hit := range hits {
		if registry == nil {
			continue
		}
		// Deleted ords should already be filtered earlier, but keep the final projection defensive.
		if lookup := registry.Lookup(ord); lookup == "" {
			continue
		}
		id := registry.Lookup(ord)
		results = append(results, Result{
			ID:            id,
			UniqueMatches: hit.UniqueMatches,
			TotalMatches:  hit.TotalMatches,
			Score:         hit.Score,
		})
	}

	slices.SortFunc(results, func(a, b Result) int {
		if useScore && a.Score != b.Score {
			return compareFloatDescending(a.Score, b.Score)
		}
		if a.UniqueMatches != b.UniqueMatches {
			return cmp.Compare(b.UniqueMatches, a.UniqueMatches)
		}
		if a.TotalMatches != b.TotalMatches {
			return cmp.Compare(b.TotalMatches, a.TotalMatches)
		}
		return cmp.Compare(a.ID, b.ID)
	})

	return results, len(results)
}

func compareFloatDescending(a, b float64) int {
	switch {
	case a > b:
		return -1
	case a < b:
		return 1
	default:
		return 0
	}
}
