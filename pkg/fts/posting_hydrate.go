package fts

func (s *Service) ordForPosting(posting Posting) (DocOrd, bool) {
	if posting.ID != "" {
		ord, ok := s.registry.Has(posting.ID)
		if !ok && (s.tombstones == nil || !s.tombstones.Any()) {
			return s.registry.GetOrAssign(posting.ID), true
		}
		return ord, ok
	}
	return posting.Ord, true
}

func (s *Service) normalizePostings(postings []Posting) []Posting {
	if len(postings) == 0 {
		return postings
	}
	out := make([]Posting, 0, len(postings))
	for _, posting := range postings {
		ord, ok := s.ordForPosting(posting)
		if !ok {
			continue
		}
		posting.Ord = ord
		if s.tombstones != nil && s.tombstones.IsSet(posting.Ord) {
			continue
		}
		out = append(out, posting)
	}
	return out
}

func (s *Service) ordForPositionalPosting(posting PositionalPosting) (DocOrd, bool) {
	if posting.ID != "" {
		ord, ok := s.registry.Has(posting.ID)
		if !ok && (s.tombstones == nil || !s.tombstones.Any()) {
			return s.registry.GetOrAssign(posting.ID), true
		}
		return ord, ok
	}
	return posting.Ord, true
}

func (s *Service) normalizePositionalPostings(postings []PositionalPosting) []PositionalPosting {
	if len(postings) == 0 {
		return postings
	}
	out := make([]PositionalPosting, 0, len(postings))
	for _, posting := range postings {
		ord, ok := s.ordForPositionalPosting(posting)
		if !ok {
			continue
		}
		posting.Ord = ord
		if s.tombstones != nil && s.tombstones.IsSet(posting.Ord) {
			continue
		}
		out = append(out, posting)
	}
	return out
}

func (s *Service) lookupDocID(ord DocOrd) (DocID, bool) {
	id := s.registry.Lookup(ord)
	if id == "" {
		return "", false
	}
	return id, true
}
