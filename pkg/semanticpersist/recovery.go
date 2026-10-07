package semanticpersist

import (
	"context"
	"crypto/sha256"
	"path/filepath"

	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/semanticpersist/internal/semanticformat"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// RepairCurrent explicitly validates and selects generationID. Normal Open
// never scans for or promotes orphan generations.
func RepairCurrent(ctx context.Context, root string, generationID uint64, options Options) error {
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

	// A full restore validates state, every referenced object and semantic.Restore.
	result, err := newRestorer(l, options.Limits).openGeneration(ctx, generationID, [sha256.Size]byte{}, false, semantic.PipelineDescriptor{})
	if err != nil {
		return err
	}
	if err := syncPublishedGeneration(l, result.manifest, options.Durability); err != nil {
		return err
	}
	return newHeadStore(l, options).repair(semanticformat.Head{GenerationID: generationID, ManifestHash: result.manifestRef.SHA256})
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
