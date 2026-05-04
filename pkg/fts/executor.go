package fts

import (
	"context"
	"fmt"
	"time"
)

func addAccum(a, b docAccum) docAccum {
	return docAccum{
		UniqueMatches: a.UniqueMatches + b.UniqueMatches,
		TotalMatches:  a.TotalMatches + b.TotalMatches,
		Score:         a.Score + b.Score,
	}
}

func (s *Service) Search(ctx context.Context, q Query, maxResults int) (*SearchResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if q == nil {
		return &SearchResult{Results: []Result{}, Timings: map[string]string{}}, nil
	}

	start := time.Now()
	timings := make(map[string]string, 2)

	searchStart := time.Now()
	hits, err := s.executeQuery(ctx, q, maxResults)
	if err != nil {
		return nil, err
	}
	timings["search_tokens"] = formatDuration(time.Since(searchStart))

	timings["total"] = formatDuration(time.Since(start))
	return searchResultFromHits(hits, maxResults, timings, s.scorer != nil), nil
}

func (s *Service) executeQuery(ctx context.Context, q Query, candidateLimit int) (map[DocID]docAccum, error) {
	switch t := q.(type) {
	case TermQuery:
		return s.execTerm(ctx, t)
	case *TermQuery:
		if t == nil {
			return map[DocID]docAccum{}, nil
		}
		return s.execTerm(ctx, *t)
	case PhraseQuery:
		return s.execPhrase(ctx, t)
	case *PhraseQuery:
		if t == nil {
			return map[DocID]docAccum{}, nil
		}
		return s.execPhrase(ctx, *t)
	case PrefixQuery:
		return s.execPrefix(ctx, t)
	case *PrefixQuery:
		if t == nil {
			return map[DocID]docAccum{}, nil
		}
		return s.execPrefix(ctx, *t)
	case *BooleanQuery:
		return s.execBoolean(ctx, t, candidateLimit)
	default:
		return nil, fmt.Errorf("fts: unsupported query type %T", q)
	}
}
