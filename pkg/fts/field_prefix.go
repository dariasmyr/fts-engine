package fts

import (
	"context"
	"fmt"
	"sync"
)

type prefixFieldDocsResult struct {
	docs []DocRef
	err  error
}

func (s *Service) collectPrefixFieldDocs(ctx context.Context, fields []string, prefix string) ([]DocRef, error) {
	docs := make([]DocRef, 0)
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

			fieldDocs, err := s.searchPrefixInField(ctx, field, prefixer, prefix)
			if err != nil {
				return nil, err
			}
			docs = append(docs, fieldDocs...)
		}
		return docs, nil
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
			fieldDocs, err := s.searchPrefixInField(ctx, field, prefixer, prefix)
			if err != nil {
				results <- prefixFieldDocsResult{err: err}
				return
			}
			results <- prefixFieldDocsResult{docs: fieldDocs}
		})
	}

	wg.Wait()
	close(results)
	for res := range results {
		if res.err != nil {
			return nil, res.err
		}
		docs = append(docs, res.docs...)
	}

	return docs, nil
}

func (s *Service) searchPrefixInField(ctx context.Context, field string, prefixer PrefixIndex, prefix string) ([]DocRef, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	docs, err := prefixer.SearchPrefix(prefix)
	if err != nil {
		return nil, fmt.Errorf("fts: prefix query field %q: %w", field, err)
	}
	return docs, nil
}
