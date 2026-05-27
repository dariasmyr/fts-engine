package fts

import "context"

func (s *Service) execPrefix(ctx context.Context, q PrefixQuery, scope queryFieldScope) (map[DocOrd]docAccum, error) {
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
	if exec := diagnosticsFromContext(ctx); exec != nil {
		exec.addFields(len(fields))
	}
	expansions, err := s.collectPrefixFieldDocs(ctx, fields, q.Prefix)
	if err != nil {
		return nil, err
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
			accum.Score += s.scoreTermExpansionDoc(expansion, doc)
			hits[ord] = accum
		}
	}
	return hits, nil
}
