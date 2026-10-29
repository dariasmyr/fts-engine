package fts

import "context"

func (s *Service) execPrefix(ctx context.Context, candidates *candidateSet, q PrefixQuery, scope queryFieldScope) (map[DocOrd]docAccum, error) {
	if exec := diagnosticsFromContext(ctx); exec != nil {
		exec.setStrategy(strategyPrefix)
	}
	if q.Prefix == "" {
		return map[DocOrd]docAccum{}, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	fields := s.resolveScopedFields(q.Field, scope)
	groups, err := s.processFields(fields, q.Prefix)
	if err != nil {
		return nil, err
	}
	if exec := diagnosticsFromContext(ctx); exec != nil {
		exec.addFields(len(fields))
		exec.addTokens(processedTokenCount(groups))
	}
	expansions := make([]termExpansion, 0, len(fields))
	for _, group := range groups {
		seenPrefixes := make(map[string]struct{}, len(group.tokens))
		for _, prefix := range group.tokens {
			if prefix == "" {
				continue
			}
			if _, seen := seenPrefixes[prefix]; seen {
				continue
			}
			seenPrefixes[prefix] = struct{}{}
			groupExpansions, err := s.collectPrefixFieldDocs(ctx, candidates, group.fields, prefix)
			if err != nil {
				return nil, err
			}
			expansions = append(expansions, groupExpansions...)
		}
	}

	hits := make(map[DocOrd]docAccum)
	seen := make(map[DocOrd]struct{})
	for _, expansion := range expansions {
		for _, doc := range expansion.docs {
			ord := doc.Ord
			accum := hits[ord]
			if _, ok := seen[ord]; !ok {
				accum.UniqueMatches++
				seen[ord] = struct{}{}
			}
			accum.TotalMatches += int(doc.Count)
			accum.Score += s.scoreTermExpansionDocType(ctx, expansion, doc, QueryTypePrefix)
			hits[ord] = accum
		}
	}
	return hits, nil
}
