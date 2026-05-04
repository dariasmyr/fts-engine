package fts

import "sort"

func searchResultFromHits(hits map[DocID]docAccum, maxResults int, timings map[string]string, useScore bool) *SearchResult {
	results, totalFound := resultsFromHits(hits, useScore)
	if maxResults <= 0 || maxResults > totalFound {
		maxResults = totalFound
	}
	return &SearchResult{
		Results:           results[:maxResults],
		TotalResultsCount: totalFound,
		Timings:           timings,
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
