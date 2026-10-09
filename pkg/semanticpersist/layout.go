package semanticpersist

import (
	"fmt"
	"path/filepath"
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
func generationName(id uint64) string           { return fmt.Sprintf("%020d", id) }
