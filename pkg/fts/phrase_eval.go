package fts

import (
	"context"
	"strings"
)

type positionalPhraseSearch func(context.Context, PositionalIndex, []string) (map[DocOrd]uint32, error)

func (s *Service) execPhrase(ctx context.Context, q PhraseQuery, scope queryFieldScope) (map[DocOrd]docAccum, error) {
	fields := s.resolveScopedFields(q.Field, scope)
	groups, err := s.processFields(fields, q.Phrase)
	if err != nil {
		return nil, err
	}
	if exec := diagnosticsFromContext(ctx); exec != nil {
		exec.addFields(len(fields))
		exec.addTokens(processedTokenCount(groups))
		exec.setStrategy(strategyPhraseExact)
	}
	return s.evalProcessedPhraseHits(ctx, groups, QueryTypePhrase, 0)
}

func (s *Service) evalProcessedPhraseHits(ctx context.Context, groups []processedFieldGroup, queryType QueryType, maxGap uint32) (map[DocOrd]docAccum, error) {
	hits := make(map[DocOrd]docAccum)
	for _, group := range groups {
		if len(group.tokens) == 0 {
			continue
		}

		var groupHits map[DocOrd]docAccum
		var err error
		if len(group.tokens) == 1 {
			groupHits, err = s.evalProcessedTermsType(ctx, []processedTermGroup{{fields: group.fields, term: group.tokens[0]}}, queryType)
		} else if queryType == QueryTypeNearPhrase {
			groupHits, err = s.evalNearPhraseTokenHits(ctx, group.fields, group.tokens, maxGap)
		} else {
			groupHits, err = s.evalExactPhraseTokenHits(ctx, group.fields, group.tokens)
		}
		if err != nil {
			return nil, err
		}
		mergeSingleClauseHits(hits, groupHits)
	}
	return hits, nil
}

func mergeSingleClauseHits(dst, src map[DocOrd]docAccum) {
	for ord, hit := range src {
		accum := dst[ord]
		if hit.UniqueMatches > 0 {
			accum.UniqueMatches = 1
		}
		accum.TotalMatches += hit.TotalMatches
		accum.Score += hit.Score
		dst[ord] = accum
	}
}

func (s *Service) evalExactPhraseTokenHits(ctx context.Context, fields []string, tokens []string) (map[DocOrd]docAccum, error) {
	return s.evalScoredPhraseTokenHits(ctx, fields, tokens, QueryTypePhrase, s.searchExactPhraseCountsInField)
}

func (s *Service) evalNearPhraseTokenHits(ctx context.Context, fields []string, tokens []string, maxGap uint32) (map[DocOrd]docAccum, error) {
	return s.evalScoredPhraseTokenHits(ctx, fields, tokens, QueryTypeNearPhrase, func(ctx context.Context, positional PositionalIndex, tokens []string) (map[DocOrd]uint32, error) {
		return s.searchNearPhraseCountsInField(ctx, positional, tokens, maxGap)
	})
}

func (s *Service) evalScoredPhraseTokenHits(ctx context.Context, fields []string, tokens []string, queryType QueryType, search positionalPhraseSearch) (map[DocOrd]docAccum, error) {
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
			accum.Score += s.scoreTermHit(ctx, fieldMatch.field, phraseTerm, queryType, ord, count, df, fieldStats)
			hits[ord] = accum
		}
	}
	return hits, nil
}
