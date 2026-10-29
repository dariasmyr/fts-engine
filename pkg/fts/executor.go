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

// SearchQuery executes a typed query with a unified set of search options.
func (s *Service) SearchQuery(ctx context.Context, q Query, options SearchOptions) (*SearchResult, error) {
	return s.searchQueryWithOptions(ctx, q, options)
}

func (s *Service) searchQueryWithOptions(ctx context.Context, q Query, options SearchOptions) (*SearchResult, error) {
	candidates, err := s.newCandidateSet(ctx, options.CandidateIDs)
	if err != nil {
		return nil, err
	}
	ctx, _ = ensureDiagnosticsContext(ctx)
	res, err := s.searchResultForQuery(
		ctx,
		candidates,
		q,
		options.MaxResults,
		optionalQueryFieldScope(options.Fields),
	)
	if err != nil {
		return nil, err
	}
	return attachDiagnostics(ctx, res), nil
}

func (s *Service) searchResultForQuery(ctx context.Context, candidates *candidateSet, q Query, maxResults int, scope queryFieldScope) (*SearchResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ctx, exec := ensureDiagnosticsContext(ctx)
	if q == nil {
		exec.setQueryTypeIfEmpty("empty")
		exec.setStrategy(strategyEmpty)
		exec.setTotalTiming(0)
		return &SearchResult{Results: []Result{}}, nil
	}

	start := exec.startTimer()

	searchStart := exec.startTimer()
	hits, err := s.executeQuery(ctx, candidates, q, maxResults, scope)
	if err != nil {
		return nil, err
	}
	filterCandidateMap(candidates, hits)
	exec.observeSearchTokens(searchStart)
	exec.observeTotal(start)

	return searchResultFromOrdHits(hits, maxResults, s.scorer != nil, s.registry), nil
}

func (s *Service) executeQuery(ctx context.Context, candidates *candidateSet, q Query, candidateLimit int, scope queryFieldScope) (map[DocOrd]docAccum, error) {
	if exec := diagnosticsFromContext(ctx); exec != nil {
		exec.setQueryTypeIfEmpty(queryTypeOf(q))
	}
	switch t := q.(type) {
	case TermQuery:
		return s.execTerm(ctx, candidates, t, scope)
	case *TermQuery:
		if t == nil {
			return map[DocOrd]docAccum{}, nil
		}
		return s.execTerm(ctx, candidates, *t, scope)
	case PhraseQuery:
		return s.execPhrase(ctx, candidates, t, scope)
	case *PhraseQuery:
		if t == nil {
			return map[DocOrd]docAccum{}, nil
		}
		return s.execPhrase(ctx, candidates, *t, scope)
	case NearPhraseQuery:
		return s.execNearPhrase(ctx, candidates, t, scope)
	case *NearPhraseQuery:
		if t == nil {
			return map[DocOrd]docAccum{}, nil
		}
		return s.execNearPhrase(ctx, candidates, *t, scope)
	case PrefixQuery:
		return s.execPrefix(ctx, candidates, t, scope)
	case *PrefixQuery:
		if t == nil {
			return map[DocOrd]docAccum{}, nil
		}
		return s.execPrefix(ctx, candidates, *t, scope)
	case *BooleanQuery:
		return s.execBoolean(ctx, candidates, t, candidateLimit, scope)
	default:
		return nil, fmt.Errorf("fts: unsupported query type %T", q)
	}
}
