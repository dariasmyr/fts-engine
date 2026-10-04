package semantic

import (
	"context"
	"math"

	"github.com/dariasmyr/fts-engine/internal/vector/contextcheck"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vectorstore"
)

// Flush builds one HNSW segment outside the state lock and atomically publishes
// it. A concurrent mutation rejects the stale build with ErrPublicationConflict;
// the caller may retry without losing pending state.
func (s *Service) Flush(ctx context.Context) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := s.lockFlush(ctx); err != nil {
		return err
	}
	defer s.unlockFlush()
	return s.flushPending(ctx)
}

// flushPending publishes the current pending state. The caller must hold
// flushGate so flush and compact cannot build competing publications.
func (s *Service) flushPending(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	state, err := s.captureFlushState(ctx)
	if err != nil {
		return err
	}
	if state.version == state.base.generation {
		return nil
	}
	if len(state.pendingVectors) > 0 && state.componentID == uint64(math.MaxUint64) {
		return ErrComponentIDExhausted
	}
	segment, err := buildPendingSegment(ctx, state.componentID, state.pendingVectors, state.config)
	if err != nil {
		return err
	}
	published, locations, err := publishIndex(ctx, state.base, state.locations, state.disabledIDs, segment, state.componentID, state.version)
	if err != nil {
		return err
	}
	return s.commitFlush(ctx, state, published, locations, segment != nil)
}

func (s *Service) commitFlush(ctx context.Context, state flushState, published *ReadView, locations map[uint64]vectorLocation, allocatedComponent bool) error {
	if err := s.lockState(ctx); err != nil {
		return err
	}
	defer s.unlockState()
	if err := ctx.Err(); err != nil {
		return err
	}
	if state.version != s.mutationVersion {
		return ErrPublicationConflict
	}
	s.published = published
	s.locations = locations
	s.pendingVectors = nil
	s.pendingDisabledIDs = nil
	if allocatedComponent {
		s.nextComponentID++
	}
	return nil
}

type flushState struct {
	version        uint64
	componentID    uint64
	pendingVectors []pendingVector
	disabledIDs    []uint64
	locations      map[uint64]vectorLocation
	base           *ReadView
	config         Config
}

func (s *Service) captureFlushState(ctx context.Context) (flushState, error) {
	if err := s.lockState(ctx); err != nil {
		return flushState{}, err
	}
	defer s.unlockState()
	pendingVectors := make([]pendingVector, len(s.pendingVectors))
	for i, item := range s.pendingVectors {
		if err := contextcheck.PeriodicError(ctx, i); err != nil {
			return flushState{}, err
		}

		pendingVectors[i] = pendingVector{row: item.row, vector: append([]float32(nil), item.vector...)}
	}
	disabledIDs := append([]uint64(nil), s.pendingDisabledIDs...)
	locations, err := cloneLocations(ctx, s.locations)
	if err != nil {
		return flushState{}, err
	}
	return flushState{
		version: s.mutationVersion, componentID: s.nextComponentID,
		pendingVectors: pendingVectors, disabledIDs: disabledIDs,
		locations: locations, base: s.published, config: s.config,
	}, nil
}

func buildPendingSegment(ctx context.Context, componentID uint64, pending []pendingVector, config Config) (*segment, error) {
	if len(pending) == 0 {
		return nil, nil
	}
	values := make([][]float32, len(pending))
	rows := make([]VectorRow, len(pending))
	for i, item := range pending {
		if err := contextcheck.PeriodicError(ctx, i); err != nil {
			return nil, err
		}

		values[i] = item.vector
		rows[i] = item.row
	}
	source, err := newInMemoryVectorStoreFromConfig(config, values)
	if err != nil {
		return nil, err
	}
	buildOptions, ok := config.hnswOptions()
	if !ok {
		return nil, ErrInvalidConfig
	}
	return buildSegment(ctx, componentID, config.pipelineDescriptor(), source, rows, buildOptions)
}

func newInMemoryVectorStoreFromConfig(config Config, values [][]float32) (*vectorstore.MemoryVectorStore, error) {
	calculator, err := config.Embedding.Calculator()
	if err != nil {
		return nil, err
	}
	return vectorstore.NewMemoryVectorStore(calculator, values)
}
