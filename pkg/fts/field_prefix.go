package fts

import (
	"context"
	"fmt"
	"sync"
)

type prefixFieldDocsResult struct {
	expansion termExpansion
	err       error
}

func (s *Service) collectPrefixFieldDocs(ctx context.Context, fields []string, prefix string) ([]termExpansion, error) {
	expansions := make([]termExpansion, 0, len(fields))
	if len(fields) <= 1 {
		for _, field := range fields {
			index, ok := s.lookupIndex(field)
			if !ok {
				continue
			}
			prefixer, ok := index.(PrefixIndex)
			if !ok {
				continue
			}

			expansion, err := s.searchPrefixInField(ctx, field, prefixer, prefix)
			if err != nil {
				return nil, err
			}
			if len(expansion.docs) > 0 {
				expansions = append(expansions, expansion)
			}
		}
		return expansions, nil
	}

	results := make(chan prefixFieldDocsResult, len(fields))
	var wg sync.WaitGroup
	for _, field := range fields {
		index, ok := s.lookupIndex(field)
		if !ok {
			continue
		}
		prefixer, ok := index.(PrefixIndex)
		if !ok {
			continue
		}

		wg.Go(func() {
			expansion, err := s.searchPrefixInField(ctx, field, prefixer, prefix)
			if err != nil {
				results <- prefixFieldDocsResult{err: err}
				return
			}
			results <- prefixFieldDocsResult{expansion: expansion}
		})
	}

	wg.Wait()
	close(results)
	for res := range results {
		if res.err != nil {
			return nil, res.err
		}
		if len(res.expansion.docs) > 0 {
			expansions = append(expansions, res.expansion)
		}
	}

	return expansions, nil
}

func (s *Service) searchPrefixInField(ctx context.Context, field string, prefixer PrefixIndex, prefix string) (termExpansion, error) {
	if err := ctx.Err(); err != nil {
		return termExpansion{}, err
	}

	if exec := diagnosticsFromContext(ctx); exec != nil {
		exec.addIndexLookups(1)
	}
	docs, err := prefixer.SearchPrefix(prefix)
	if err != nil {
		return termExpansion{}, fmt.Errorf("fts: prefix query field %q: %w", field, err)
	}
	if exec := diagnosticsFromContext(ctx); exec != nil {
		exec.addPostingsRead(len(docs))
	}
	return termExpansion{
		field:      field,
		term:       prefix + "*",
		df:         uint32(len(docs)),
		fieldStats: s.fieldStatsFor(field),
		docs:       docs,
	}, nil
}
