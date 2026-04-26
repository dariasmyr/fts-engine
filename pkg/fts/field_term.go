package fts

import (
	"context"
	"fmt"
	"sync"
)

type termFieldDocsResult struct {
	expansions [][]DocRef
	totalDocs  int
	err        error
}

func (s *Service) collectTermFieldExpansions(ctx context.Context, fields []string, keys []string) ([][]DocRef, int, error) {
	expansions := make([][]DocRef, 0, len(fields)*len(keys))
	totalDocs := 0
	if len(fields) <= 1 {
		for _, field := range fields {
			index, ok := s.lookupIndex(field)
			if !ok {
				continue
			}

			res, err := s.searchKeysInField(ctx, field, index, keys)
			if err != nil {
				return nil, 0, err
			}
			expansions = append(expansions, res.expansions...)
			totalDocs += res.totalDocs
		}
		return expansions, totalDocs, nil
	}

	results := make(chan termFieldDocsResult, len(fields))
	var wg sync.WaitGroup
	for _, field := range fields {
		index, ok := s.lookupIndex(field)
		if !ok {
			continue
		}

		wg.Go(func() {
			res, err := s.searchKeysInField(ctx, field, index, keys)
			if err != nil {
				results <- termFieldDocsResult{err: err}
				return
			}
			results <- res
		})
	}

	wg.Wait()
	close(results)
	for res := range results {
		if res.err != nil {
			return nil, 0, res.err
		}
		expansions = append(expansions, res.expansions...)
		totalDocs += res.totalDocs
	}

	return expansions, totalDocs, nil
}

func (s *Service) searchKeysInField(ctx context.Context, field string, index Index, keys []string) (termFieldDocsResult, error) {
	if err := ctx.Err(); err != nil {
		return termFieldDocsResult{}, err
	}

	res := termFieldDocsResult{expansions: make([][]DocRef, 0, len(keys))}
	for _, key := range keys {
		if s.filter != nil && !s.filter.Contains([]byte(key)) {
			continue
		}

		docs, err := index.Search(key)
		if err != nil {
			return termFieldDocsResult{}, fmt.Errorf("fts: term query field %q: index search: %w", field, err)
		}
		if len(docs) == 0 {
			continue
		}

		res.expansions = append(res.expansions, docs)
		res.totalDocs += len(docs)
	}
	return res, nil
}
