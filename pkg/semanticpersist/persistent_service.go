package semanticpersist

import (
	"context"
	"path/filepath"
	"sync"

	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// PersistentService owns one writable semantic service and the exclusive filesystem lock.
type PersistentService struct {
	index       *semantic.Index
	publisher   *publisher
	serviceLock *serviceLock
	publishMu   publishLock
	closeOnce   sync.Once
	closeErr    error
}

func Publish(ctx context.Context, root string, index *semantic.Index, options PublishOptions) (Generation, error) {
	if ctx == nil {
		return Generation{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return Generation{}, err
	}
	if index == nil || root == "" {
		return Generation{}, ErrCorrupt
	}
	if options.ExpectedGeneration == nil {
		return Generation{}, ErrExpectedGenerationRequired
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
	return p.publish(ctx, index, options, true)
}

func Open(ctx context.Context, root string, options OpenOptions) (*PersistentService, error) {
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
	headStore := newHeadStore(l, PublishOptions{
		Durability: DurabilityAsynchronous,
		Limits:     limits,
	})

	head, err := headStore.read(ctx)
	if err != nil {
		_ = lock.Close()
		return nil, err
	}

	generations := newGenerationStore(l, PublishOptions{
		Durability: DurabilityAsynchronous,
		Limits:     limits,
	})

	persisted, err := generations.open(
		ctx,
		head.GenerationID,
		&head.ManifestHash,
	)
	if err != nil {
		_ = lock.Close()
		return nil, corruptMissingReference(err)
	}

	objects := newObjectStore(l, PublishOptions{
		Durability: DurabilityAsynchronous,
		Limits:     limits,
	})

	restored, err := restoreIndex(
		ctx,
		Generation{ID: head.GenerationID},
		persisted,
		objects,
		options.ExpectedSchema,
	)
	if err != nil {
		_ = lock.Close()
		return nil, corruptMissingReference(err)
	}

	return &PersistentService{
		index: restored.index,
		publisher: &publisher{
			layout:       l,
			openedLimits: limits,
			generation:   restored.generation,
			reusable:     restored.reusable,
		},
		serviceLock: lock, publishMu: newPublishLock(),
	}, nil
}

func (s *PersistentService) Index() *semantic.Index {
	if s == nil || s.publisher == nil {
		return nil
	}

	return s.index
}

func (s *PersistentService) Publish(ctx context.Context, options PublishOptions) (Generation, error) {
	if ctx == nil {
		return Generation{}, vector.ErrNilContext
	}
	if s == nil || s.index == nil || s.publisher == nil {
		return Generation{}, ErrStoreClosed
	}
	if err := s.publishMu.Lock(ctx); err != nil {
		return Generation{}, err
	}
	defer s.publishMu.Unlock()
	if s.serviceLock == nil {
		return Generation{}, ErrStoreClosed
	}
	options.ExpectedGeneration = &Generation{
		ID: s.publisher.generation.ID,
	}
	options.Limits = mergeLimits(options.Limits, s.publisher.openedLimits)
	if err := normalizeOptions(&options); err != nil {
		return Generation{}, err
	}
	return s.publisher.publish(ctx, s.index, options, false)
}

func (s *PersistentService) Close() error {
	if s == nil || s.publisher == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.publishMu.LockUninterruptible()
		defer s.publishMu.Unlock()
		if s.serviceLock != nil {
			s.closeErr = s.serviceLock.Close()
			s.serviceLock = nil
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
