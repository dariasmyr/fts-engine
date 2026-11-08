package semanticpersist

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/dariasmyr/fts-engine/v2/internal/persistfs"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/generationformat"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/storage"
)

type currentFile struct {
	layout layout
	limits Limits
	hooks  saveHooks
}

func newCurrentFile(l layout, cfg settings) currentFile {
	return currentFile{layout: l, limits: cfg.limits, hooks: cfg.hooks}
}

func (s currentFile) read(ctx context.Context) (generationformat.Head, error) {
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

func (s currentFile) commit(ctx context.Context, head generationformat.Head) error {
	if err := beforeStep(ctx, s.hooks, stepWriteCurrent); err != nil {
		return err
	}
	data, err := generationformat.EncodeHead(head, fileFormatLimits(s.limits))
	if err != nil {
		return mapFormatError(err)
	}
	name, err := writeTemporaryData(s.layout.root, ".tmp-current-", data)
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
	if err := storage.AtomicReplace(name, s.layout.current); err != nil {
		return fmt.Errorf("semanticpersist: replace CURRENT: %w", err)
	}
	if err := afterStep(s.hooks, stepReplaceCurrent, true); err != nil {
		return err
	}
	if err := beforeStep(context.Background(), s.hooks, stepSyncRoot); err != nil {
		return fmt.Errorf("%w: %v", ErrIndeterminate, err)
	}
	if err := persistfs.SyncDirectory(s.layout.root); err != nil {
		return fmt.Errorf("%w: %v", ErrIndeterminate, err)
	}
	if err := afterStep(s.hooks, stepSyncRoot, true); err != nil {
		return err
	}
	return nil
}

func (s currentFile) replace(head generationformat.Head) error {
	data, err := generationformat.EncodeHead(head, fileFormatLimits(s.limits))
	if err != nil {
		return mapFormatError(err)
	}
	name, err := writeTemporaryData(s.layout.root, ".tmp-current-replace-", data)
	if err != nil {
		return err
	}
	defer os.Remove(name)
	if err := storage.AtomicReplace(name, s.layout.current); err != nil {
		return err
	}
	if err := persistfs.SyncDirectory(s.layout.root); err != nil {
		return fmt.Errorf("%w: %v", ErrIndeterminate, err)
	}
	return nil
}
