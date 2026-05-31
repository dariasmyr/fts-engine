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

func WithScorer(scorer Scorer) Option {
	return func(s *Service) {
		s.scorer = scorer
	}
}

func WithCollectionStatsSnapshot(snapshot *CollectionStatsSnapshot) Option {
	return func(s *Service) {
		s.pendingCollectionStatsSnapshot = snapshot
	}
}

func WithDocRegistrySnapshot(ids []DocID) Option {
	return func(s *Service) {
		s.pendingRegistrySnapshot = append([]DocID(nil), ids...)
	}
}

func WithTombstonesSnapshot(words []uint64) Option {
	return func(s *Service) {
		s.pendingTombstonesSnapshot = append([]uint64(nil), words...)
	}
}

func WithCompactionLoadFactor(limit float64) Option {
	return func(s *Service) {
		if limit <= 0 || limit > 1 {
			s.compactionLoadFactor = 0
			return
		}
		s.compactionLoadFactor = limit
	}
}

func WithAutoCompactionCheck(enabled bool) Option {
	return func(s *Service) {
		s.autoCompactionCheck = enabled
	}
}

func WithCompactionCallback(fn func(CompactionStats)) Option {
	return func(s *Service) {
		s.compactionCallback = fn
	}
}
