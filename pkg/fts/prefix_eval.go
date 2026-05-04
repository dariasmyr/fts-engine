package fts

import "context"

func (s *Service) execPrefix(ctx context.Context, q PrefixQuery, scope queryFieldScope) (map[DocID]docAccum, error) {
	if q.Prefix == "" {
		return map[DocID]docAccum{}, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	fields := s.resolveScopedFields(q.Field, scope)
	expansions, err := s.collectPrefixFieldDocs(ctx, fields, q.Prefix)
	if err != nil {
		return nil, err
	}

	hits := make(map[DocID]docAccum)
	seen := make(map[DocID]struct{})
	for _, expansion := range expansions {
		for _, doc := range expansion.docs {
			accum := hits[doc.ID]
			if _, ok := seen[doc.ID]; !ok {
				accum.UniqueMatches++
				seen[doc.ID] = struct{}{}
			}
			accum.TotalMatches += int(doc.Count)
			accum.Score += s.scoreTermExpansionDoc(expansion, doc)
			hits[doc.ID] = accum
		}
	}
	return hits, nil
}
