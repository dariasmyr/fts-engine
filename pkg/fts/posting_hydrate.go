package fts

func (s *Service) ordForPosting(posting Posting) DocOrd {
	if posting.ID != "" {
		return s.registry.GetOrAssign(posting.ID)
	}
	return posting.Ord
}

func (s *Service) normalizePostings(postings []Posting) []Posting {
	if len(postings) == 0 {
		return postings
	}
	out := make([]Posting, 0, len(postings))
	for _, posting := range postings {
		posting.Ord = s.ordForPosting(posting)
		out = append(out, posting)
	}
	return out
}

func (s *Service) ordForPositionalPosting(posting PositionalPosting) DocOrd {
	if posting.ID != "" {
		return s.registry.GetOrAssign(posting.ID)
	}
	return posting.Ord
}

func (s *Service) lookupDocID(ord DocOrd) (DocID, bool) {
	id := s.registry.Lookup(ord)
	if id == "" {
		return "", false
	}
	return id, true
}
