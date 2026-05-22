package fts

import (
	"context"
	"fmt"
	"sync"
)

type termFieldDocsResult struct {
	expansions []termExpansion
	totalDocs  int
	err        error
}

func (s *Service) collectTermFieldExpansions(ctx context.Context, fields []string, term string, keys []string) ([]termExpansion, int, error) {
	expansions := make([]termExpansion, 0, len(fields)*len(keys))
	totalDocs := 0
	if len(fields) <= 1 {
		for _, field := range fields {
			index, ok := s.lookupIndex(field)
			if !ok {
				continue
			}

			res, err := s.searchKeysInField(ctx, field, index, term, keys)
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
			res, err := s.searchKeysInField(ctx, field, index, term, keys)
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

func (s *Service) searchKeysInField(ctx context.Context, field string, index Index, term string, keys []string) (termFieldDocsResult, error) {
	if err := ctx.Err(); err != nil {
		return termFieldDocsResult{}, err
	}

	res := termFieldDocsResult{expansions: make([]termExpansion, 0, len(keys))}
	fieldStats := s.fieldStatsFor(field)
	exec := diagnosticsFromContext(ctx)
	var filterChecks, filterRejects, indexLookups, postingsRead int
	defer func() {
		if exec == nil {
			return
		}
		exec.addFilterChecks(filterChecks, filterRejects)
		exec.addIndexLookups(indexLookups)
		exec.addPostingsRead(postingsRead)
	}()
	for _, key := range keys {
		if s.filter != nil {
			miss := !s.filter.Contains([]byte(key))
			filterChecks++
			if miss {
				filterRejects++
			}
			if miss {
				continue
			}
		}

		indexLookups++
		docs, err := index.Search(key)
		if err != nil {
			return termFieldDocsResult{}, fmt.Errorf("fts: term query field %q: index search: %w", field, err)
		}
		docs = s.normalizePostings(docs)
		postingsRead += len(docs)
		if len(docs) == 0 {
			continue
		}

		res.expansions = append(res.expansions, termExpansion{
			field:      field,
			term:       term,
			df:         uint32(len(docs)),
			fieldStats: fieldStats,
			docs:       docs,
		})
		res.totalDocs += len(docs)
	}
	return res, nil
}
