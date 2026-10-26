package fts

// Option configures a Service during construction. Its implementation is
// intentionally opaque so callers cannot apply configuration after construction.
type Option struct {
	apply func(*Service)
}

func newOption(apply func(*Service)) *Option {
	return &Option{apply: apply}
}

func WithPipeline(p Pipeline) *Option {
	return newOption(func(s *Service) {
		if p != nil {
			s.pipeline = p
		}
	})
}

// WithFieldPipelines sets pipelines for named fields. Fields not present in the
// map use the service-level pipeline.
func WithFieldPipelines(pipelines map[string]Pipeline) *Option {
	return newOption(func(s *Service) {
		for name, pipeline := range pipelines {
			if name != "" && pipeline != nil {
				s.fieldPipelines[name] = pipeline
			}
		}
	})
}

// WithKeyGeneratorDescriptor sets the persisted identity of the key generator.
func WithKeyGeneratorDescriptor(descriptor KeyGeneratorDescriptor) *Option {
	return newOption(func(s *Service) {
		if descriptor.Name != "" && descriptor.Fingerprint != "" {
			s.keyGenerator = &descriptor
		}
	})
}

// WithPipelineDescriptors restores persisted pipeline identities for fields.
// It is used by persistence adapters to keep subsequent indexing compatible.
func WithPipelineDescriptors(descriptors map[string]PipelineDescriptor) *Option {
	return newOption(func(s *Service) {
		for name, descriptor := range descriptors {
			if name != "" && descriptor.Fingerprint != "" {
				s.fieldPipelineDescriptors[name] = descriptor
			}
		}
	})
}

func WithFilter(f Filter) *Option {
	return newOption(func(s *Service) {
		s.filter = f
	})
}

func WithScorer(scorer Scorer) *Option {
	return newOption(func(s *Service) {
		s.scorer = scorer
	})
}

func WithRankProfile(profile RankProfile) *Option {
	return newOption(func(s *Service) {
		s.scorer = WeightedScorer{
			Base:             profile.Base,
			FieldWeights:     profile.FieldWeights,
			QueryTypeWeights: profile.QueryTypeWeights,
		}
	})
}

func WithCollectionStatsSnapshot(snapshot *CollectionStatsSnapshot) *Option {
	return newOption(func(s *Service) {
		s.pendingCollectionStatsSnapshot = snapshot
	})
}

func WithDocRegistrySnapshot(ids []DocID) *Option {
	return newOption(func(s *Service) {
		s.pendingRegistrySnapshot = append([]DocID(nil), ids...)
	})
}

func WithTombstonesSnapshot(words []uint64) *Option {
	return newOption(func(s *Service) {
		s.pendingTombstonesSnapshot = append([]uint64(nil), words...)
	})
}

func WithCompactionLoadFactor(limit float64) *Option {
	return newOption(func(s *Service) {
		if limit <= 0 || limit > 1 {
			s.compactionLoadFactor = 0
			return
		}
		s.compactionLoadFactor = limit
	})
}

func WithAutoCompactionCheck(enabled bool) *Option {
	return newOption(func(s *Service) {
		s.autoCompactionCheck = enabled
	})
}

func WithCompactionCallback(fn func(CompactionStats)) *Option {
	return newOption(func(s *Service) {
		s.compactionCallback = fn
	})
}
