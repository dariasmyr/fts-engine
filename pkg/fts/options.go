package fts

import "time"

type Option func(*Service)

func WithPipeline(p Pipeline) Option {
	return func(s *Service) {
		if p != nil {
			s.pipeline = p
		}
	}
}

func WithDurationFormatter(formatter func(time.Duration) string) Option {
	return func(s *Service) {
		if formatter != nil {
			s.durationFormatter = formatter
		}
	}
}
