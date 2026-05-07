package fts

import (
	"context"
	"fmt"
)

func addAccum(a, b docAccum) docAccum {
	return docAccum{
		UniqueMatches: a.UniqueMatches + b.UniqueMatches,
		TotalMatches:  a.TotalMatches + b.TotalMatches,
		Score:         a.Score + b.Score,
	}
}

func (s *Service) Search(ctx context.Context, q Query, maxResults int) (*SearchResult, error) {
	ctx, _ = ensureDiagnosticsContext(ctx)
	res, err := s.searchResultForQuery(ctx, q, maxResults, queryFieldScope{})
	if err != nil {
		return nil, err
	}
	return attachDiagnostics(ctx, res), nil
}

func (s *Service) SearchQueryFields(ctx context.Context, fields []string, q Query, maxResults int) (*SearchResult, error) {
	ctx, _ = ensureDiagnosticsContext(ctx)
	res, err := s.searchResultForQuery(ctx, q, maxResults, newQueryFieldScope(fields))
	if err != nil {
		return nil, err
	}
	return attachDiagnostics(ctx, res), nil
}

func (s *Service) searchResultForQuery(ctx context.Context, q Query, maxResults int, scope queryFieldScope) (*SearchResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ctx, exec := ensureDiagnosticsContext(ctx)
	if q == nil {
		exec.setQueryTypeIfEmpty("empty")
		exec.setStrategy("empty")
		exec.setTotalTiming(0)
		return &SearchResult{Results: []Result{}}, nil
	}

	start := exec.startTimer()

	searchStart := exec.startTimer()
	hits, err := s.executeQuery(ctx, q, maxResults, scope)
	if err != nil {
		return nil, err
	}
	exec.observeSearchTokens(searchStart)
	exec.observeTotal(start)

	return searchResultFromHits(hits, maxResults, s.scorer != nil), nil
}

func (s *Service) executeQuery(ctx context.Context, q Query, candidateLimit int, scope queryFieldScope) (map[DocID]docAccum, error) {
	if exec := diagnosticsFromContext(ctx); exec != nil {
		exec.setQueryTypeIfEmpty(queryTypeOf(q))
	}
	switch t := q.(type) {
	case TermQuery:
		return s.execTerm(ctx, t, scope)
	case *TermQuery:
		if t == nil {
			return map[DocID]docAccum{}, nil
		}
		return s.execTerm(ctx, *t, scope)
	case PhraseQuery:
		return s.execPhrase(ctx, t, scope)
	case *PhraseQuery:
		if t == nil {
			return map[DocID]docAccum{}, nil
		}
		return s.execPhrase(ctx, *t, scope)
	case PrefixQuery:
		return s.execPrefix(ctx, t, scope)
	case *PrefixQuery:
		if t == nil {
			return map[DocID]docAccum{}, nil
		}
		return s.execPrefix(ctx, *t, scope)
	case *BooleanQuery:
		return s.execBoolean(ctx, t, candidateLimit, scope)
	default:
		return nil, fmt.Errorf("fts: unsupported query type %T", q)
	}
}
