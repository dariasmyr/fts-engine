package fts

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

type ScoreExplanation struct {
	ID            DocID
	Matched       bool
	UniqueMatches int
	TotalMatches  int
	Score         float64
	Contributions []ScoreContribution
}

type ScoreContribution struct {
	Field          string
	Term           string
	MatchType      MatchType
	TF             uint32
	DF             uint32
	DocLength      uint32
	FieldDocs      int
	AvgFieldLength float64
	BaseScore      float64
	FieldWeight    float64
	MatchWeight    float64
	Score          float64
}

type explanationContextKey struct{}

type explanationCollector struct {
	mu            sync.Mutex
	target        DocOrd
	contributions []ScoreContribution
}

func (s *Service) Explain(ctx context.Context, query string, docID DocID) (*ScoreExplanation, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if docID == "" {
		return nil, fmt.Errorf("fts: explain: document id is empty")
	}
	ord, ok := s.registry.Has(docID)
	if !ok {
		return &ScoreExplanation{ID: docID}, nil
	}

	parsed, err := ParseQuery(query)
	if err != nil {
		return nil, err
	}
	collector := &explanationCollector{target: ord}
	ctx = context.WithValue(ctx, explanationContextKey{}, collector)

	hits, err := s.executeQuery(ctx, parsed, 0, queryFieldScope{})
	if err != nil {
		return nil, err
	}

	accum, matched := hits[ord]
	contributions := collector.snapshot()
	if !matched {
		contributions = nil
	}
	return &ScoreExplanation{
		ID:            docID,
		Matched:       matched,
		UniqueMatches: accum.UniqueMatches,
		TotalMatches:  accum.TotalMatches,
		Score:         accum.Score,
		Contributions: contributions,
	}, nil
}

func explanationFromContext(ctx context.Context) *explanationCollector {
	if ctx == nil {
		return nil
	}
	collector, _ := ctx.Value(explanationContextKey{}).(*explanationCollector)
	return collector
}

func (e *explanationCollector) add(ord DocOrd, c ScoreContribution) {
	if e == nil {
		return
	}
	if ord != e.target || c.Field == "" {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.contributions = append(e.contributions, c)
}

func (e *explanationCollector) snapshot() []ScoreContribution {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	out := append([]ScoreContribution(nil), e.contributions...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Field != out[j].Field {
			return out[i].Field < out[j].Field
		}
		if out[i].MatchType != out[j].MatchType {
			return out[i].MatchType < out[j].MatchType
		}
		if out[i].Term != out[j].Term {
			return out[i].Term < out[j].Term
		}
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].BaseScore > out[j].BaseScore
	})
	return out
}
