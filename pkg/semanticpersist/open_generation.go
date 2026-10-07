package semanticpersist

import (
	"context"
	"path/filepath"

	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/semanticpersist/internal/semanticformat"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// OpenByGeneration explicitly validates and selects generationID. Normal Open
// never scans for or promotes orphan generations.
func OpenByGeneration(ctx context.Context, root string, generationID uint64, options PublishOptions) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if root == "" || generationID == 0 {
		return ErrCorrupt
	}
	if err := normalizeOptions(&options); err != nil {
		return err
	}
	l := newLayout(root)
	if err := validateLayout(l); err != nil {
		return err
	}
	lock, err := acquireStoreLock(l.lock)
	if err != nil {
		return err
	}
	defer lock.Close()

	generations := newGenerationStore(l, options)

	persisted, err := generations.open(
		ctx,
		generationID,
		nil,
	)
	if err != nil {
		return err
	}

	objects := newObjectStore(l, options)

	_, err = restoreIndex(
		ctx,
		Generation{ID: generationID},
		persisted,
		objects,
		semantic.Schema{},
	)
	if err != nil {
		return err
	}

	if err := syncPublishedGeneration(
		l,
		persisted.manifest,
		options.Durability,
	); err != nil {
		return err
	}

	return newHeadStore(l, options).replace(
		semanticformat.Head{
			GenerationID: generationID,
			ManifestHash: persisted.manifestRef.SHA256,
		},
	)
}

func syncPublishedGeneration(l layout, manifest semanticformat.GenerationManifest, mode DurabilityMode) error {
	d := durabilityPolicy{mode: mode}
	if !d.synchronous() {
		return nil
	}
	files := []string{l.state(manifest.GenerationID), l.manifest(manifest.GenerationID)}
	directories := []string{l.generation(manifest.GenerationID)}
	for _, segment := range manifest.Segments {
		path := l.segment(segment.ObjectID)
		files = append(files, filepath.Join(path, vectorsFileName), filepath.Join(path, graphFileName))
		directories = append(directories, path)
	}
	for _, file := range files {
		if err := d.syncRegularFile(file); err != nil {
			return err
		}
	}
	for _, dir := range append(directories, l.segments, l.objects, l.generations, l.root) {
		if err := d.syncDirectory(dir); err != nil {
			return err
		}
	}
	return nil
}
