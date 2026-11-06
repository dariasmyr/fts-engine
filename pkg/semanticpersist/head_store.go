package semanticpersist

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/generationformat"
)

type headStore struct {
	layout     layout
	limits     Limits
	durability durabilityPolicy
	hooks      PublishOptions
}

func newHeadStore(l layout, options PublishOptions) headStore {
	return headStore{layout: l, limits: options.Limits, durability: durabilityPolicy{mode: options.Durability}, hooks: options}
}

func (s headStore) read(ctx context.Context) (generationformat.Head, error) {
	if err := ctx.Err(); err != nil {
		return generationformat.Head{}, err
	}
	data, err := readRegularFile(s.layout.current, s.limits.MaxFileBytes)
	if errors.Is(err, os.ErrNotExist) {
		return generationformat.Head{}, ErrCurrentMissing
	}
	if err != nil {
		return generationformat.Head{}, err
	}
	head, err := generationformat.DecodeHead(data, fileFormatLimits(s.limits))
	if err != nil {
		return generationformat.Head{}, mapFormatError(err)
	}
	return head, nil
}

func (s headStore) commit(ctx context.Context, head generationformat.Head) error {
	if err := beforeStep(ctx, s.hooks, stepWriteCurrent); err != nil {
		return err
	}
	data, _, err := generationformat.EncodeHead(head, fileFormatLimits(s.limits))
	if err != nil {
		return mapFormatError(err)
	}
	name, err := writeTemporaryData(s.layout.root, ".tmp-current-", data, s.durability)
	if err != nil {
		return err
	}
	defer os.Remove(name)
	if err := afterStep(s.hooks, stepWriteCurrent, false); err != nil {
		return err
	}
	if err := beforeStep(ctx, s.hooks, stepReplaceCurrent); err != nil {
		return err
	}
	if err := atomicReplace(name, s.layout.current); err != nil {
		return fmt.Errorf("semanticpersist: replace CURRENT: %w", err)
	}
	if err := afterStep(s.hooks, stepReplaceCurrent, true); err != nil {
		return err
	}
	if s.durability.synchronous() {
		if err := beforeStep(context.Background(), s.hooks, stepSyncStore); err != nil {
			return fmt.Errorf("%w: %v", ErrIndeterminate, err)
		}
		if err := s.durability.syncDirectory(s.layout.root); err != nil {
			return fmt.Errorf("%w: %v", ErrIndeterminate, err)
		}
		if err := afterStep(s.hooks, stepSyncStore, true); err != nil {
			return err
		}
	}
	return nil
}

func (s headStore) replace(head generationformat.Head) error {
	data, _, err := generationformat.EncodeHead(head, fileFormatLimits(s.limits))
	if err != nil {
		return mapFormatError(err)
	}
	name, err := writeTemporaryData(s.layout.root, ".tmp-current-replace-", data, s.durability)
	if err != nil {
		return err
	}
	defer os.Remove(name)
	if err := atomicReplace(name, s.layout.current); err != nil {
		return err
	}
	if err := s.durability.syncDirectory(s.layout.root); err != nil {
		return fmt.Errorf("%w: %v", ErrIndeterminate, err)
	}
	return nil
}
