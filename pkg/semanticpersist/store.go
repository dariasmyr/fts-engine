package semanticpersist

import (
	"context"
	"path/filepath"
	"sync"

	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// Store owns one writable semantic service and the exclusive filesystem lock.
type Store struct {
	service   *semantic.Service
	publisher *publisher
	storeLock *storeLock
	publishMu publishLock
	closeOnce sync.Once
	closeErr  error
}

func Publish(ctx context.Context, root string, service *semantic.Service, options Options) (Generation, error) {
	if ctx == nil {
		return Generation{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return Generation{}, err
	}
	if service == nil || root == "" {
		return Generation{}, ErrCorrupt
	}
	if err := normalizeOptions(&options); err != nil {
		return Generation{}, err
	}
	l := newLayout(root)
	rootCreated, err := prepareLayout(l)
	if err != nil {
		return Generation{}, err
	}
	lock, err := acquireStoreLock(l.lock)
	if err != nil {
		return Generation{}, err
	}
	defer lock.Close()
	durability := durabilityPolicy{mode: options.Durability}
	if durability.synchronous() {
		for _, path := range []string{l.segments, l.objects, l.generations, l.root} {
			if err := durability.syncDirectory(path); err != nil {
				return Generation{}, err
			}
		}
		if rootCreated {
			if err := durability.syncDirectory(filepath.Dir(l.root)); err != nil {
				return Generation{}, err
			}
		}
	}
	p := &publisher{layout: l}
	return p.publish(ctx, service, options, true)
}

func Open(ctx context.Context, root string, options OpenOptions) (*Store, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if root == "" {
		return nil, ErrCorrupt
	}
	limits := normalizeLimits(options.Limits)
	if err := validateLimits(limits); err != nil {
		return nil, err
	}
	l := newLayout(root)
	if err := validateLayout(l); err != nil {
		return nil, err
	}
	lock, err := acquireStoreLock(l.lock)
	if err != nil {
		return nil, err
	}
	restored, err := newRestorer(l, limits).openCurrent(ctx, options.ExpectedDescriptors)
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	return &Store{
		service:   restored.service,
		publisher: &publisher{layout: l, openedLimits: limits, generation: restored.generation, reusable: restored.reusable},
		storeLock: lock, publishMu: newPublishLock(),
	}, nil
}

func (s *Store) Service() *semantic.Service {
	if s == nil || s.publisher == nil {
		return nil
	}
	return s.service
}

func (s *Store) Publish(ctx context.Context, options Options) (Generation, error) {
	if ctx == nil {
		return Generation{}, vector.ErrNilContext
	}
	if s == nil || s.service == nil || s.publisher == nil {
		return Generation{}, ErrStoreClosed
	}
	if err := s.publishMu.Lock(ctx); err != nil {
		return Generation{}, err
	}
	defer s.publishMu.Unlock()
	if s.storeLock == nil {
		return Generation{}, ErrStoreClosed
	}
	options.ExpectedGeneration = s.publisher.generation.ID
	options.Limits = mergeLimits(options.Limits, s.publisher.openedLimits)
	if err := normalizeOptions(&options); err != nil {
		return Generation{}, err
	}
	return s.publisher.publish(ctx, s.service, options, false)
}

func (s *Store) Close() error {
	if s == nil || s.publisher == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.publishMu.LockUninterruptible()
		defer s.publishMu.Unlock()
		if s.storeLock != nil {
			s.closeErr = s.storeLock.Close()
			s.storeLock = nil
		}
	})
	return s.closeErr
}

func mergeLimits(requested, opened Limits) Limits {
	if requested == (Limits{}) {
		return opened
	}
	return requested
}
