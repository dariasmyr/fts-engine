package semanticpersist

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/dariasmyr/fts-engine/pkg/semanticpersist/internal/semanticformat"
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

func (s headStore) read(ctx context.Context) (semanticformat.Head, error) {
	if err := ctx.Err(); err != nil {
		return semanticformat.Head{}, err
	}
	data, err := readRegularFile(s.layout.current, s.limits.MaxFileBytes)
	if errors.Is(err, os.ErrNotExist) {
		return semanticformat.Head{}, ErrCurrentMissing
	}
	if err != nil {
		return semanticformat.Head{}, err
	}
	head, err := semanticformat.DecodeHead(data, fileFormatLimits(s.limits))
	if err != nil {
		return semanticformat.Head{}, mapFormatError(err)
	}
	return head, nil
}

func (s headStore) commit(ctx context.Context, head semanticformat.Head) error {
	if err := beforeStep(ctx, s.hooks, stepWriteCurrent); err != nil {
		return err
	}
	data, _, err := semanticformat.EncodeHead(head, fileFormatLimits(s.limits))
	if err != nil {
		return mapFormatError(err)
	}
	temp, err := os.CreateTemp(s.layout.root, ".tmp-current-")
	if err != nil {
		return err
	}
	name := temp.Name()
	defer os.Remove(name)
	if err := writeAllFile(temp, data); err != nil {
		_ = temp.Close()
		return err
	}
	if err := s.durability.syncFile(temp); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
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

func (s headStore) replace(head semanticformat.Head) error {
	data, _, err := semanticformat.EncodeHead(head, fileFormatLimits(s.limits))
	if err != nil {
		return mapFormatError(err)
	}
	temp, err := os.CreateTemp(s.layout.root, ".tmp-current-replace-")
	if err != nil {
		return err
	}
	name := temp.Name()
	defer os.Remove(name)
	if err := writeAllFile(temp, data); err != nil {
		_ = temp.Close()
		return err
	}
	if err := s.durability.syncFile(temp); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := atomicReplace(name, s.layout.current); err != nil {
		return err
	}
	if err := s.durability.syncDirectory(s.layout.root); err != nil {
		return fmt.Errorf("%w: %v", ErrIndeterminate, err)
	}
	return nil
}
