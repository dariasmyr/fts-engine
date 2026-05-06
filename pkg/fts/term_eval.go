package fts

import (
	"context"
	"fmt"
)

func (s *Service) execTerm(ctx context.Context, q TermQuery, scope queryFieldScope) (map[DocID]docAccum, error) {
	if exec := diagnosticsFromContext(ctx); exec != nil {
		exec.setStrategy("term")
	}
	if q.Term == "" {
		return map[DocID]docAccum{}, nil
	}

	tokens := s.pipeline.Process(q.Term)
	if len(tokens) == 0 {
		return map[DocID]docAccum{}, nil
	}
	fields := s.resolveScopedFields(q.Field, scope)
	if exec := diagnosticsFromContext(ctx); exec != nil {
		exec.addTokens(len(tokens))
		exec.addFields(len(fields))
	}
	keyGroups := make([][]string, len(tokens))
	for i, token := range tokens {
		keys, err := s.keyGen(token)
		if err != nil {
			return nil, fmt.Errorf("fts: term query: keygen: %w", err)
		}
		keyGroups[i] = keys
		if exec := diagnosticsFromContext(ctx); exec != nil {
			exec.addKeys(len(keys))
		}
	}

	plan := make([]tokenGroup, 0, len(tokens))
	totalCap := 0
	for i := range tokens {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		expansions, groupTotalDocs, err := s.collectTermFieldExpansions(ctx, fields, tokens[i], keyGroups[i])
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

	hits := make(map[DocID]docAccum, totalCap)
	for _, group := range plan {
		var seenInGroup map[DocID]struct{}
		if !group.single {
			seenInGroup = make(map[DocID]struct{}, group.totalDocs)
		}

		for _, expansion := range group.expansions {
			for _, doc := range expansion.docs {
				accum := hits[doc.ID]
				if group.single {
					accum.UniqueMatches++
				} else if _, seen := seenInGroup[doc.ID]; !seen {
					accum.UniqueMatches++
					seenInGroup[doc.ID] = struct{}{}
				}
				accum.TotalMatches += int(doc.Count)
				accum.Score += s.scoreTermExpansionDoc(expansion, doc)
				hits[doc.ID] = accum
			}
		}
	}

	return hits, nil
}
