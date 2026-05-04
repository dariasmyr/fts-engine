package fts

import (
	"context"
	"strings"
)

type positionalPhraseSearch func(context.Context, PositionalIndex, []string) (map[DocID]uint32, error)

type phrasePlan struct {
	tokens   []string
	fallback *TermQuery
}

func (s *Service) execPhrase(ctx context.Context, q PhraseQuery, scope queryFieldScope) (map[DocID]docAccum, error) {
	return s.evalPhraseHits(ctx, s.resolveScopedFields(q.Field, scope), q.Phrase, scope)
}

func (s *Service) preparePhrase(fields []string, phrase string) phrasePlan {
	tokens := s.pipeline.Process(phrase)
	plan := phrasePlan{tokens: tokens}
	if len(tokens) != 1 {
		return plan
	}
	if len(fields) == 1 {
		plan.fallback = &TermQuery{Field: fields[0], Term: phrase}
		return plan
	}
	plan.fallback = &TermQuery{Term: phrase}
	return plan
}

func (s *Service) evalPhraseHits(ctx context.Context, fields []string, phrase string, scope queryFieldScope) (map[DocID]docAccum, error) {
	plan := s.preparePhrase(fields, phrase)
	if len(plan.tokens) == 0 {
		return map[DocID]docAccum{}, nil
	}
	if plan.fallback != nil {
		return s.executeQuery(ctx, *plan.fallback, 0, scope)
	}
	return s.evalExactPhraseTokenHits(ctx, fields, plan.tokens)
}

func (s *Service) evalExactPhraseTokenHits(ctx context.Context, fields []string, tokens []string) (map[DocID]docAccum, error) {
	return s.evalScoredPhraseTokenHits(ctx, fields, tokens, s.searchExactPhraseCountsInField)
}

func (s *Service) evalNearPhraseTokenHits(ctx context.Context, fields []string, tokens []string, maxGap uint32) (map[DocID]docAccum, error) {
	return s.evalScoredPhraseTokenHits(ctx, fields, tokens, func(ctx context.Context, positional PositionalIndex, tokens []string) (map[DocID]uint32, error) {
		return s.searchNearPhraseCountsInField(ctx, positional, tokens, maxGap)
	})
}

func (s *Service) evalScoredPhraseTokenHits(ctx context.Context, fields []string, tokens []string, search positionalPhraseSearch) (map[DocID]docAccum, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(tokens) == 0 {
		return map[DocID]docAccum{}, nil
	}

	phraseTerm := strings.Join(tokens, " ")
	hits := make(map[DocID]docAccum)
	fieldMatches, err := s.collectPositionalFieldMatches(ctx, fields, func(positional PositionalIndex) (map[DocID]uint32, error) {
		return search(ctx, positional, tokens)
	})
	if err != nil {
		return nil, err
	}
	for _, fieldMatch := range fieldMatches {
		fieldStats := s.fieldStatsFor(fieldMatch.field)
		// For phrase scoring, treat the whole phrase as one term: df is docs with the phrase, cnt is that phrase TF in one doc.
		df := uint32(len(fieldMatch.matchesByDoc))
		for docID, count := range fieldMatch.matchesByDoc {
			accum := hits[docID]
			accum.UniqueMatches = 1
			accum.TotalMatches += int(count)
			accum.Score += s.scoreTermHit(fieldMatch.field, phraseTerm, docID, count, df, fieldStats)
			hits[docID] = accum
		}
	}
	return hits, nil
}
