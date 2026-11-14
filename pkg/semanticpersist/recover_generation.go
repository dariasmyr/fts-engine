package semanticpersist

import (
	"context"
	"path/filepath"

	"github.com/dariasmyr/fts-engine/v2/internal/persistfs"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/generationformat"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// RecoverGeneration explicitly validates generation and makes it current.
// Normal Load never scans for or promotes orphan generations. Recovery is
// strict: an existing CURRENT must be readable and compatible with the selected
// generation. A corrupt CURRENT returns ErrCorrupt and is left unchanged.
func RecoverGeneration(
	ctx context.Context,
	root string,
	options RecoverOptions,
) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if root == "" || options.Generation.ID == 0 {
		return ErrCorrupt
	}
	cfg, err := newSettings(options.Limits)
	if err != nil {
		return err
	}
	return withStorageOperation(root, operationLayoutExisting, func(operation *storageOperation) error {
		l := operation.layout
		generations := newGenerationFiles(l, cfg)
		candidate, err := generations.open(
			ctx,
			options.Generation.ID,
			nil,
		)
		if err != nil {
			return err
		}

		_, current, hasCurrent, err := operation.openCurrentGeneration(ctx, cfg)
		if err != nil {
			return err
		}
		if hasCurrent {
			if err := validateImmutableLineage(current.state, candidate.state); err != nil {
				return err
			}
		}

		segments := newSegmentStore(l, cfg)
		if _, err := restoreIndex(ctx, candidate, segments); err != nil {
			return err
		}
		if err := syncSavedGeneration(l, candidate.manifest); err != nil {
			return err
		}
		if err := newCurrentFile(l, cfg).replace(
			generationformat.Head{
				GenerationID: options.Generation.ID,
				ManifestHash: candidate.manifestRef.SHA256,
			},
		); err != nil {
			return err
		}
		operation.markPublishedToStorage()
		return nil
	})
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
