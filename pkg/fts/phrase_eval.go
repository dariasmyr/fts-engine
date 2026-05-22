package fts

import (
	"context"
	"strings"
)

type positionalPhraseSearch func(context.Context, PositionalIndex, []string) (map[DocOrd]uint32, error)

type phrasePlan struct {
	tokens   []string
	fallback *TermQuery
}

func (s *Service) execPhrase(ctx context.Context, q PhraseQuery, scope queryFieldScope) (map[DocOrd]docAccum, error) {
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

func (s *Service) evalPhraseHits(ctx context.Context, fields []string, phrase string, scope queryFieldScope) (map[DocOrd]docAccum, error) {
	plan := s.preparePhrase(fields, phrase)
	if exec := diagnosticsFromContext(ctx); exec != nil {
		exec.addFields(len(fields))
		exec.addTokens(len(plan.tokens))
	}
	if len(plan.tokens) == 0 {
		return map[DocOrd]docAccum{}, nil
	}
	if plan.fallback != nil {
		return s.executeQuery(ctx, *plan.fallback, 0, scope)
	}
	if exec := diagnosticsFromContext(ctx); exec != nil {
		exec.setStrategy(strategyPhraseExact)
	}
	return s.evalExactPhraseTokenHits(ctx, fields, plan.tokens)
}

func (s *Service) evalExactPhraseTokenHits(ctx context.Context, fields []string, tokens []string) (map[DocOrd]docAccum, error) {
	return s.evalScoredPhraseTokenHits(ctx, fields, tokens, s.searchExactPhraseCountsInField)
}

func (s *Service) evalNearPhraseTokenHits(ctx context.Context, fields []string, tokens []string, maxGap uint32) (map[DocOrd]docAccum, error) {
	return s.evalScoredPhraseTokenHits(ctx, fields, tokens, func(ctx context.Context, positional PositionalIndex, tokens []string) (map[DocOrd]uint32, error) {
		return s.searchNearPhraseCountsInField(ctx, positional, tokens, maxGap)
	})
}

func (s *Service) evalScoredPhraseTokenHits(ctx context.Context, fields []string, tokens []string, search positionalPhraseSearch) (map[DocOrd]docAccum, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(tokens) == 0 {
		return map[DocOrd]docAccum{}, nil
	}

	phraseTerm := strings.Join(tokens, " ")
	hits := make(map[DocOrd]docAccum)
	fieldMatches, err := s.collectPositionalFieldMatches(ctx, fields, func(positional PositionalIndex) (map[DocOrd]uint32, error) {
		return search(ctx, positional, tokens)
	})
	if err != nil {
		return nil, err
	}
	for _, fieldMatch := range fieldMatches {
		fieldStats := s.fieldStatsFor(fieldMatch.field)
		// For phrase scoring, treat the whole phrase as one term: df is docs with the phrase, cnt is that phrase TF in one doc.
		df := uint32(len(fieldMatch.matchesByDoc))
		for ord, count := range fieldMatch.matchesByDoc {
			accum := hits[ord]
			accum.UniqueMatches = 1
			accum.TotalMatches += int(count)
			accum.Score += s.scoreTermHit(fieldMatch.field, phraseTerm, ord, count, df, fieldStats)
			hits[ord] = accum
		}
	}
	return hits, nil
}
