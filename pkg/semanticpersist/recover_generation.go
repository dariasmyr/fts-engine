package semanticpersist

import (
	"context"
	"path/filepath"

	"github.com/dariasmyr/fts-engine/v2/internal/persistfs"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/generationformat"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/storage"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// RecoverGeneration explicitly validates generation and makes it current.
// Normal Load never scans for or promotes orphan generations.
func RecoverGeneration(
	ctx context.Context,
	root string,
	generation Generation,
	limits Limits,
) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if root == "" || generation.ID == 0 {
		return ErrCorrupt
	}
	cfg, err := newSettings(limits)
	if err != nil {
		return err
	}
	l := newLayout(root)
	if err := l.validate(); err != nil {
		return err
	}
	lock, err := storage.AcquireOperationLock(l.lock)
	if err != nil {
		return err
	}
	defer lock.Close()

	generations := newGenerationFiles(l, cfg)

	persisted, err := generations.open(
		ctx,
		generation.ID,
		nil,
	)
	if err != nil {
		return err
	}

	segments := newSegmentStore(l, cfg)

	_, err = restoreIndex(
		ctx,
		persisted,
		segments,
	)
	if err != nil {
		return err
	}

	if err := syncSavedGeneration(l, persisted.manifest); err != nil {
		return err
	}

	return newCurrentFile(l, cfg).replace(
		generationformat.Head{
			GenerationID: generation.ID,
			ManifestHash: persisted.manifestRef.SHA256,
		},
	)
}

func syncSavedGeneration(l layout, manifest generationformat.GenerationManifest) error {
	files := []string{l.state(manifest.GenerationID), l.manifest(manifest.GenerationID)}
	directories := []string{l.generation(manifest.GenerationID)}
	for _, segment := range manifest.Segments {
		path := l.segment(segment.ObjectID)
		files = append(files, filepath.Join(path, vectorsFileName), filepath.Join(path, graphFileName))
		directories = append(directories, path)
	}
	for _, file := range files {
		if err := mapPersistFSError(persistfs.SyncRegularFile(file), ErrCorrupt); err != nil {
			return err
		}
	}
	for _, dir := range append(directories, l.segments, l.objects, l.generations, l.root) {
		if err := persistfs.SyncDirectory(dir); err != nil {
			return err
		}
	}
	return nil
}
