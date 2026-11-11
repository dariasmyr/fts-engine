package semanticpersist

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/dariasmyr/fts-engine/v2/internal/persistfs"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/generationformat"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// PruneResult reports entries completely removed by Prune.
type PruneResult struct {
	RemovedGenerations      int
	RemovedSegmentObjects   int
	RemovedTemporaryEntries int
}

// Prune removes generations outside the retention set, segment objects not
// reachable from any retained manifest, and recognized save temporary entries.
// It holds the operation lock for the complete operation.
func Prune(ctx context.Context, root string, config PruneConfig) (PruneResult, error) {
	cfg, err := newSettings(config.Config)
	if err != nil {
		return PruneResult{}, err
	}
	return prune(ctx, root, config.RetainGenerations, cfg)
}

// Unknown entries and symlinks are never removed.
func prune(
	ctx context.Context,
	root string,
	retainGenerations int,
	cfg settings,
) (PruneResult, error) {
	if ctx == nil {
		return PruneResult{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return PruneResult{}, err
	}
	if root == "" || retainGenerations <= 0 {
		return PruneResult{}, ErrCorrupt
	}
	return withStorageOperationResult(root, operationLayoutExisting, func(operation *storageOperation) (PruneResult, error) {
		return pruneLocked(ctx, operation.layout, retainGenerations, cfg)
	})
}

func pruneLocked(ctx context.Context, l layout, retainGenerations int, cfg settings) (result PruneResult, err error) {
	dirtyDirectories := make(map[string]struct{})
	defer func() {
		paths := make([]string, 0, len(dirtyDirectories))
		for path := range dirtyDirectories {
			paths = append(paths, path)
		}
		sort.Strings(paths)
		for _, path := range paths {
			err = errors.Join(err, persistfs.SyncDirectory(path))
		}
	}()

	head, err := newCurrentFile(l, cfg).read(ctx)
	if err != nil {
		return result, err
	}
	generations := newGenerationFiles(l, cfg)
	segments := newSegmentStore(l, cfg)
	current, err := generations.open(ctx, head.GenerationID, &head.ManifestHash)
	if err != nil {
		return result, corruptMissingReference(err)
	}
	if err := validatePruneGeneration(ctx, current, segments); err != nil {
		return result, corruptMissingReference(err)
	}

	entries, err := os.ReadDir(l.generations)
	if err != nil {
		return result, err
	}
	ids := make([]uint64, 0, len(entries))
	for _, entry := range entries {
		if id, ok := parseGenerationName(entry.Name()); ok {
			ids = append(ids, id)
		}
	}
	slices.SortFunc(ids, func(a, b uint64) int {
		switch {
		case a < b:
			return -1
		case a > b:
			return 1
		default:
			return 0
		}
	})

	retained := map[uint64]generationformat.GenerationManifest{head.GenerationID: current.manifest}
	for _, id := range ids {
		if len(retained) >= retainGenerations || id >= head.GenerationID {
			continue
		}
		persisted, openErr := generations.open(ctx, id, nil)
		if openErr == nil {
			openErr = validatePruneGeneration(ctx, persisted, segments)
		}
		if openErr == nil {
			retained[id] = persisted.manifest
			continue
		}
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		if !errors.Is(openErr, ErrCorrupt) {
			return result, openErr
		}
	}

	reachable := make(map[string]struct{})
	for _, manifest := range retained {
		for _, segment := range manifest.Segments {
			reachable[segment.ObjectID] = struct{}{}
		}
	}

	generationTargets := make([]string, 0)
	for _, entry := range entries {
		name := entry.Name()
		if id, ok := parseGenerationName(name); ok {
			if _, keep := retained[id]; !keep {
				generationTargets = append(generationTargets, filepath.Join(l.generations, name))
			}
			continue
		}
		if recognizedTempName(name, ".tmp-gen-") {
			generationTargets = append(generationTargets, filepath.Join(l.generations, name))
		}
	}

	segmentEntries, err := os.ReadDir(l.segments)
	if err != nil {
		return result, err
	}
	segmentTargets := make([]string, 0)
	for _, entry := range segmentEntries {
		name := entry.Name()
		if validObjectID(name) {
			if _, keep := reachable[name]; !keep {
				segmentTargets = append(segmentTargets, filepath.Join(l.segments, name))
			}
			continue
		}
		if recognizedTempName(name, ".tmp-seg-") {
			segmentTargets = append(segmentTargets, filepath.Join(l.segments, name))
		}
	}

	rootEntries, err := os.ReadDir(l.root)
	if err != nil {
		return result, err
	}
	rootTemps := make([]string, 0)
	for _, entry := range rootEntries {
		if recognizedTempName(entry.Name(), ".tmp-current-") || recognizedTempName(entry.Name(), ".tmp-current-replace-") {
			rootTemps = append(rootTemps, filepath.Join(l.root, entry.Name()))
		}
	}

	for _, path := range generationTargets {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		removed, err := removeKnownContainer(path, map[string]struct{}{manifestFileName: {}, stateFileName: {}})
		if err != nil {
			return result, err
		}
		if removed {
			dirtyDirectories[l.generations] = struct{}{}
			if strings.HasPrefix(filepath.Base(path), ".tmp-gen-") {
				result.RemovedTemporaryEntries++
			} else {
				result.RemovedGenerations++
			}
		}
	}
	for _, path := range segmentTargets {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		removed, err := removeKnownContainer(path, map[string]struct{}{vectorsFileName: {}, graphFileName: {}})
		if err != nil {
			return result, err
		}
		if removed {
			dirtyDirectories[l.segments] = struct{}{}
			if strings.HasPrefix(filepath.Base(path), ".tmp-seg-") {
				result.RemovedTemporaryEntries++
			} else {
				result.RemovedSegmentObjects++
			}
		}
	}
	for _, path := range rootTemps {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		removed, err := removeKnownRegularFile(path)
		if err != nil {
			return result, err
		}
		if removed {
			dirtyDirectories[l.root] = struct{}{}
			result.RemovedTemporaryEntries++
		}
	}

	return result, nil
}

func validatePruneGeneration(ctx context.Context, generation persistedGeneration, store segmentStore) error {
	for i, ref := range generation.manifest.Segments {
		if i%16 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if err := store.verify(ref); err != nil {
			return err
		}
	}
	return nil
}

func parseGenerationName(name string) (uint64, bool) {
	if len(name) != len(generationName(1)) {
		return 0, false
	}
	id, err := strconv.ParseUint(name, 10, 64)
	return id, err == nil && id != 0 && name == generationName(id)
}

func recognizedTempName(name, prefix string) bool {
	return strings.HasPrefix(name, prefix) && len(name) > len(prefix)
}

func removeKnownContainer(path string, allowed map[string]struct{}) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return false, nil
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if _, ok := allowed[entry.Name()]; !ok {
			return false, nil
		}
		childInfo, err := os.Lstat(filepath.Join(path, entry.Name()))
		if err != nil {
			return false, err
		}
		if childInfo.Mode()&os.ModeSymlink != 0 || !childInfo.Mode().IsRegular() {
			return false, nil
		}
	}
	for _, entry := range entries {
		if err := os.Remove(filepath.Join(path, entry.Name())); err != nil {
			return false, errors.Join(err, persistfs.SyncDirectory(path))
		}
	}
	if err := persistfs.SyncDirectory(path); err != nil {
		return false, err
	}
	if err := os.Remove(path); err != nil {
		return false, err
	}
	return true, nil
}

func removeKnownRegularFile(path string) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return false, nil
	}
	if err := os.Remove(path); err != nil {
		return false, err
	}
	return true, nil
}
