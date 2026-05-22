package fts

func (s *Service) hydratePostings(postings []Posting) []Posting {
	if len(postings) == 0 {
		return postings
	}
	out := make([]Posting, 0, len(postings))
	for _, posting := range postings {
		if posting.ID == "" {
			posting.ID = s.registry.Lookup(posting.Ord)
			if posting.ID == "" {
				continue
			}
		}
		out = append(out, posting)
	}
	return out
}

func (s *Service) hydratePositionalPostings(postings []PositionalPosting) []PositionalPosting {
	if len(postings) == 0 {
		return postings
	}
	out := make([]PositionalPosting, 0, len(postings))
	for _, posting := range postings {
		if posting.ID == "" {
			posting.ID = s.registry.Lookup(posting.Ord)
			if posting.ID == "" {
				continue
			}
		}
		out = append(out, posting)
	}
	return out
}
