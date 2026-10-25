package semanticpersist

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/semanticpersist/internal/semanticformat"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// PruneOptions controls retention and durability during Prune.
type PruneOptions struct {
	// RetainGenerations is the total number of valid committed generations to
	// retain, including the generation named by CURRENT. It must be positive.
	RetainGenerations int
	Durability        DurabilityMode
	Limits            Limits
}

// PruneResult reports entries completely removed by Prune.
type PruneResult struct {
	RemovedGenerations      int
	RemovedSegmentObjects   int
	RemovedTemporaryEntries int
}

// Prune removes generations outside the retention set, segment objects not
// reachable from any retained manifest, and recognized publication temporary
// entries. It holds the store's exclusive lock for the complete operation.
// Unknown entries and symlinks are never removed.
func Prune(ctx context.Context, root string, options PruneOptions) (result PruneResult, err error) {
	if ctx == nil {
		return result, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if root == "" || options.RetainGenerations <= 0 {
		return result, ErrCorrupt
	}
	if options.Durability == 0 {
		options.Durability = DurabilitySynchronous
	}
	if options.Durability != DurabilitySynchronous && options.Durability != DurabilityAsynchronous {
		return result, ErrCorrupt
	}
	options.Limits = normalizeLimits(options.Limits)
	if err := validateLimits(options.Limits); err != nil {
		return result, err
	}

	l := newLayout(root)
	if err := validateLayout(l); err != nil {
		return result, err
	}
	lock, err := acquireStoreLock(l.lock)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	if err := validateLayout(l); err != nil {
		return result, err
	}

	durability := durabilityPolicy{mode: options.Durability}
	dirtyDirectories := make(map[string]struct{})
	defer func() {
		if !durability.synchronous() {
			return
		}
		paths := make([]string, 0, len(dirtyDirectories))
		for path := range dirtyDirectories {
			paths = append(paths, path)
		}
		sort.Strings(paths)
		for _, path := range paths {
			err = errors.Join(err, durability.syncDirectory(path))
		}
	}()

	publishOptions := PublishOptions{Durability: DurabilityAsynchronous, Limits: options.Limits}
	head, err := newHeadStore(l, publishOptions).read(ctx)
	if err != nil {
		return result, err
	}
	generations := newGenerationStore(l, publishOptions)
	objects := newObjectStore(l, publishOptions)
	current, err := generations.open(ctx, head.GenerationID, &head.ManifestHash)
	if err != nil {
		return result, corruptMissingReference(err)
	}
	if err := validatePruneGeneration(ctx, current, objects); err != nil {
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
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })

	retained := map[uint64]semanticformat.GenerationManifest{head.GenerationID: current.manifest}
	for _, id := range ids {
		if len(retained) >= options.RetainGenerations || id >= head.GenerationID {
			continue
		}
		persisted, openErr := generations.open(ctx, id, nil)
		if openErr == nil {
			openErr = validatePruneGeneration(ctx, persisted, objects)
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
		removed, err := removeKnownContainer(path, map[string]struct{}{manifestFileName: {}, stateFileName: {}}, durability)
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
		removed, err := removeKnownContainer(path, map[string]struct{}{vectorsFileName: {}, graphFileName: {}}, durability)
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

func validatePruneGeneration(ctx context.Context, generation persistedGeneration, objects objectStore) error {
	_, err := restoreIndex(ctx, Generation{ID: generation.id}, generation, objects, semantic.Schema{})
	return err
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

func removeKnownContainer(path string, allowed map[string]struct{}, durability durabilityPolicy) (bool, error) {
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
			return false, errors.Join(err, durability.syncDirectory(path))
		}
	}
	if err := durability.syncDirectory(path); err != nil {
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
