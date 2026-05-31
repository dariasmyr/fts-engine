package fts

import "sort"

func searchResultFromHits(hits map[DocID]docAccum, maxResults int, useScore bool) *SearchResult {
	results, totalFound := resultsFromHits(hits, useScore)
	if maxResults <= 0 || maxResults > totalFound {
		maxResults = totalFound
	}
	return &SearchResult{
		Results:           results[:maxResults],
		TotalResultsCount: totalFound,
	}
}

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

	sort.Slice(results, func(i, j int) bool {
		if useScore && results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		if results[i].UniqueMatches != results[j].UniqueMatches {
			return results[i].UniqueMatches > results[j].UniqueMatches
		}
		if results[i].TotalMatches != results[j].TotalMatches {
			return results[i].TotalMatches > results[j].TotalMatches
		}
		return results[i].ID < results[j].ID
	})

	return results, len(results)
}

func searchResultFromHitsDeprecated(hits map[DocID]docAccum, maxResults int, useScore bool) *SearchResult {
	results, totalFound := resultsFromHits(hits, useScore)
	if maxResults <= 0 || maxResults > totalFound {
		maxResults = totalFound
	}
	return &SearchResult{
		Results:           results[:maxResults],
		TotalResultsCount: totalFound,
	}
}

func resultsFromHits(hits map[DocID]docAccum, useScore bool) ([]Result, int) {
	results := make([]Result, 0, len(hits))
	for id, hit := range hits {
		results = append(results, Result{
			ID:            id,
			UniqueMatches: hit.UniqueMatches,
			TotalMatches:  hit.TotalMatches,
			Score:         hit.Score,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		if useScore && results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		if results[i].UniqueMatches != results[j].UniqueMatches {
			return results[i].UniqueMatches > results[j].UniqueMatches
		}
		if results[i].TotalMatches != results[j].TotalMatches {
			return results[i].TotalMatches > results[j].TotalMatches
		}
		return results[i].ID < results[j].ID
	})

	return results, len(results)
}
