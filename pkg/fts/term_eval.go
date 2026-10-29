package fts

import (
	"context"
	"fmt"
)

type processedTermGroup struct {
	fields []string
	term   string
}

type termOccurrence struct {
	term       string
	occurrence int
}

func (s *Service) execTerm(ctx context.Context, candidates *candidateSet, q TermQuery, scope queryFieldScope) (map[DocOrd]docAccum, error) {
	if exec := diagnosticsFromContext(ctx); exec != nil {
		exec.setStrategy(strategyTerm)
	}
	if q.Term == "" {
		return map[DocOrd]docAccum{}, nil
	}

	fields := s.resolveScopedFields(q.Field, scope)
	groups, err := s.processFields(fields, q.Term)
	if err != nil {
		return nil, err
	}
	if exec := diagnosticsFromContext(ctx); exec != nil {
		exec.addTokens(processedTokenCount(groups))
		exec.addFields(len(fields))
	}
	return s.evalProcessedTerms(ctx, candidates, mergeProcessedTerms(groups))
}

func mergeProcessedTerms(groups []processedFieldGroup) []processedTermGroup {
	merged := make([]processedTermGroup, 0, processedTokenCount(groups))
	byTermOccurrence := make(map[termOccurrence]int, len(merged))
	for _, group := range groups {
		occurrences := make(map[string]int, len(group.tokens))
		for _, token := range group.tokens {
			occurrence := occurrences[token]
			occurrences[token]++
			key := termOccurrence{term: token, occurrence: occurrence}
			if index, ok := byTermOccurrence[key]; ok {
				merged[index].fields = append(merged[index].fields, group.fields...)
				continue
			}
			byTermOccurrence[key] = len(merged)
			merged = append(merged, processedTermGroup{fields: group.fields, term: token})
		}
	}
	return merged
}

func (s *Service) evalProcessedTerms(ctx context.Context, candidates *candidateSet, terms []processedTermGroup) (map[DocOrd]docAccum, error) {
	return s.evalProcessedTermsType(ctx, candidates, terms, QueryTypeTerm)
}

func (s *Service) evalProcessedTermsType(ctx context.Context, candidates *candidateSet, terms []processedTermGroup, queryType QueryType) (map[DocOrd]docAccum, error) {
	plan := make([]tokenGroup, 0, len(terms))
	totalCap := 0
	for _, processed := range terms {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		keys, err := s.keyGen(processed.term)
		if err != nil {
			return nil, fmt.Errorf("fts: term query: keygen: %w", err)
		}
		if exec := diagnosticsFromContext(ctx); exec != nil {
			exec.addKeys(len(keys))
		}

		expansions, groupTotalDocs, err := s.collectTermFieldExpansions(ctx, candidates, processed.fields, processed.term, keys)
		if err != nil {
			return nil, err
		}
		group := tokenGroup{expansions: expansions, totalDocs: groupTotalDocs}
		totalCap += groupTotalDocs
		if len(group.expansions) == 0 {
			continue
		}
		group.single = len(group.expansions) == 1
		plan = append(plan, group)
	}

	hits := make(map[DocOrd]docAccum, totalCap)
	for _, group := range plan {
		var seenInGroup map[DocOrd]struct{}
		if !group.single {
			seenInGroup = make(map[DocOrd]struct{}, group.totalDocs)
		}

		for _, expansion := range group.expansions {
			for _, doc := range expansion.docs {
				ord := doc.Ord
				accum := hits[ord]
				if group.single {
					accum.UniqueMatches++
				} else if _, seen := seenInGroup[ord]; !seen {
					accum.UniqueMatches++
					seenInGroup[ord] = struct{}{}
				}
				accum.TotalMatches += int(doc.Count)
				accum.Score += s.scoreTermExpansionDocType(ctx, expansion, doc, queryType)
				hits[ord] = accum
			}
		}
	}

	return hits, nil
}
