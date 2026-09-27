package fts

type Option func(*Service)

func WithPipeline(p Pipeline) Option {
	return func(s *Service) {
		if p != nil {
			s.pipeline = p
		}
	}
}

// WithFieldPipelines sets pipelines used when a field does not provide an
// explicit pipeline in its document value.
func WithFieldPipelines(pipelines map[string]Pipeline) Option {
	return func(s *Service) {
		for name, pipeline := range pipelines {
			if name != "" && pipeline != nil {
				s.fieldPipelines[name] = pipeline
			}
		}
	}
}

// WithKeyGeneratorDescriptor sets the persisted identity of the key generator.
func WithKeyGeneratorDescriptor(descriptor KeyGeneratorDescriptor) Option {
	return func(s *Service) {
		if descriptor.Name != "" && descriptor.Fingerprint != "" {
			s.keyGenerator = &descriptor
		}
	}
}

// WithAnalyzerDescriptors restores persisted analyzer identities for fields.
// It is used by persistence adapters to keep subsequent indexing compatible.
func WithAnalyzerDescriptors(descriptors map[string]AnalyzerDescriptor) Option {
	return func(s *Service) {
		for name, descriptor := range descriptors {
			if name != "" && descriptor.Fingerprint != "" {
				s.fieldAnalyzers[name] = descriptor
			}
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

func WithRankProfile(profile RankProfile) Option {
	return func(s *Service) {
		s.scorer = WeightedScorer{
			Base:             profile.Base,
			FieldWeights:     profile.FieldWeights,
			QueryTypeWeights: profile.QueryTypeWeights,
		}
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
