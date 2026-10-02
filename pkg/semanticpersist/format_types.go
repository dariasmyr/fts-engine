package semanticpersist

import (
	"crypto/sha256"

	"github.com/dariasmyr/fts-engine/internal/persist"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
)

type fileReference = persist.Reference

type manifest struct {
	Version      uint16
	GenerationID uint64
	Segments     []manifestSegment
	State        fileReference
}

type manifestSegment struct {
	ObjectID    string
	SegmentKind semantic.SegmentKind
	Vectors     fileReference
	Graph       fileReference
}

type currentRecord struct {
	GenerationID uint64
	ManifestHash [sha256.Size]byte
}

type decodedState struct {
	Config               semantic.Config
	Revision             uint64
	MaxAllocatedVectorID semantic.VectorID
	NextComponentID      semantic.ComponentID
	Segments             []decodedStateSegment
}

type decodedStateSegment struct {
	ComponentID   semantic.ComponentID
	Rows          []semantic.VectorRow
	LivenessWords []uint64
}
