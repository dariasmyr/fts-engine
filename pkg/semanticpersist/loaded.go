package semanticpersist

import (
	"context"
	"sync"

	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

type Generation struct {
	ID uint64
}

// Store owns one writable semantic service and the exclusive store lock.
type Store struct {
	service     *semantic.Service
	paths       storePaths
	limits      Limits
	generation  Generation
	storeLock   *storeLock
	publishGate chan struct{}
	closeOnce   sync.Once
	closeErr    error
}

func (s *Store) Service() *semantic.Service {
	if s == nil || s.publishGate == nil {
		return nil
	}
	return s.service
}

// Publish writes the service's next committed generation through the lifetime
// writer lock held by the store.
func (s *Store) Publish(ctx context.Context, options Options) (Generation, error) {
	if ctx == nil {
		return Generation{}, vector.ErrNilContext
	}
	if s == nil || s.service == nil || s.publishGate == nil {
		return Generation{}, ErrStoreClosed
	}
	if err := s.lock(ctx); err != nil {
		return Generation{}, err
	}
	defer s.unlock()
	if s.storeLock == nil {
		return Generation{}, ErrStoreClosed
	}
	options.ExpectedGeneration = s.generation.ID
	options.Limits = mergeLimits(options.Limits, s.limits)
	if err := normalizeOptions(&options); err != nil {
		return Generation{}, err
	}
	generation, err := publishLocked(ctx, s.paths, s.service, options)
	if err == nil {
		s.generation = generation
	}
	return generation, err
}

// Close releases the lifetime writer lock. The detached semantic service must
// not be used to publish through this handle afterward.
func (s *Store) Close() error {
	if s == nil || s.publishGate == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.lockUninterruptible()
		defer s.unlock()
		if s.storeLock != nil {
			s.closeErr = s.storeLock.Close()
			s.storeLock = nil
		}
	})
	return s.closeErr
}

func (s *Store) lock(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case s.publishGate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			s.unlock()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Store) lockUninterruptible() { s.publishGate <- struct{}{} }
func (s *Store) unlock()              { <-s.publishGate }

func mergeLimits(requested, opened Limits) Limits {
	if requested == (Limits{}) {
		return opened
	}
	return requested
}
