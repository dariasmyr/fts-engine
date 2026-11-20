package semanticpersist

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dariasmyr/fts-engine/v2/internal/persistfs"
)

const (
	currentFileName      = "CURRENT"
	lockFileName         = "LOCK"
	objectsDirectory     = "objects"
	segmentsDirectory    = "segments"
	generationsDirectory = "generations"
	vectorsFileName      = "vectors.bin"
	graphFileName        = "graph.bin"
	manifestFileName     = "manifest.bin"
	stateFileName        = "semantic-state.bin"
)

type layout struct {
	root, objects, segments, generations, current, lock string
}

func newLayout(root string) layout {
	objects := filepath.Join(root, objectsDirectory)
	return layout{
		root: root, objects: objects,
		segments:    filepath.Join(objects, segmentsDirectory),
		generations: filepath.Join(root, generationsDirectory),
		current:     filepath.Join(root, currentFileName), lock: filepath.Join(root, lockFileName),
	}
}

func (l layout) segment(objectID string) string { return filepath.Join(l.segments, objectID) }
func (l layout) generation(id uint64) string    { return filepath.Join(l.generations, generationName(id)) }
func (l layout) manifest(id uint64) string      { return filepath.Join(l.generation(id), manifestFileName) }
func (l layout) state(id uint64) string         { return filepath.Join(l.generation(id), stateFileName) }

func (l layout) prepare() (bool, error) {
	rootCreated := false
	if info, err := os.Lstat(l.root); errors.Is(err, os.ErrNotExist) {
		if err := validateDirectory(filepath.Dir(l.root)); err != nil {
			return false, err
		}
		if err := os.Mkdir(l.root, 0o700); err != nil {
			return false, err
		}
		rootCreated = true
	} else if err != nil {
		return false, err
	} else if info.Mode()&os.ModeSymlink != 0 {
		return false, ErrSymlink
	} else if !info.IsDir() {
		return false, ErrCorrupt
	}
	for _, path := range []string{l.root, l.objects, l.segments, l.generations} {
		if err := ensureDirectory(path); err != nil {
			return false, err
		}
	}
	return rootCreated, nil
}

func (l layout) validate() error {
	for _, path := range []string{l.root, l.objects, l.segments, l.generations} {
		if err := validateDirectory(path); err != nil {
			return err
		}
	}
	return nil
}

func (l layout) ensureContained(path string) error {
	relative, err := filepath.Rel(l.root, path)
	if err != nil || relative == ".." || filepath.IsAbs(relative) ||
		(len(relative) >= 3 && relative[:3] == ".."+string(filepath.Separator)) {
		return ErrPathEscape
	}
	return nil
}

func ensureDirectory(path string) error {
	if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	return validateDirectory(path)
}

func validateDirectory(path string) error {
	return mapPersistFSError(persistfs.ValidateDirectory(path), ErrCorrupt)
}

func generationName(id uint64) string { return fmt.Sprintf("%020d", id) }
