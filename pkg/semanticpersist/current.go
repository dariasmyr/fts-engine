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
	layout      layout
	limits      Config
	checkpoints saveCheckpoints
}

func newCurrentFile(l layout, cfg settings) currentFile {
	return currentFile{layout: l, limits: cfg.limits, checkpoints: cfg.checkpoints}
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
	head, err := generationformat.DecodeHead(data, generationFormatConfig(s.limits))
	if err != nil {
		return generationformat.Head{}, mapFormatError(err)
	}
	return head, nil
}

func (s currentFile) commit(ctx context.Context, head generationformat.Head) error {
	if err := s.checkpoints.before(ctx, checkpointWriteCurrent); err != nil {
		return err
	}
	data, err := generationformat.EncodeHead(head, generationFormatConfig(s.limits))
	if err != nil {
		return mapFormatError(err)
	}
	name, err := writeTemporaryData(s.layout.root, ".tmp-current-", data)
	if err != nil {
		return err
	}
	defer os.Remove(name)
	if err := s.checkpoints.after(checkpointWriteCurrent); err != nil {
		return err
	}
	if err := s.checkpoints.before(ctx, checkpointReplaceCurrent); err != nil {
		return err
	}
	if err := storage.AtomicReplace(name, s.layout.current); err != nil {
		return fmt.Errorf("semanticpersist: replace CURRENT: %w", err)
	}
	if err := s.checkpoints.after(checkpointReplaceCurrent); err != nil {
		return indeterminateError(err)
	}
	if err := s.checkpoints.before(context.Background(), checkpointSyncRoot); err != nil {
		return indeterminateError(err)
	}
	if err := persistfs.SyncDirectory(s.layout.root); err != nil {
		return indeterminateError(err)
	}
	if err := s.checkpoints.after(checkpointSyncRoot); err != nil {
		return indeterminateError(err)
	}
	return nil
}

func (s currentFile) replace(head generationformat.Head) error {
	data, err := generationformat.EncodeHead(head, generationFormatConfig(s.limits))
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
		return indeterminateError(err)
	}
	return nil
}
