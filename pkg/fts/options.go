package fts

type Option func(*Service)

func WithPipeline(p Pipeline) Option {
	return func(s *Service) {
		if p != nil {
			s.pipeline = p
		}
	}
}

func WithFilter(f Filter) Option {
	return func(s *Service) {
		s.filter = f
	}
}
