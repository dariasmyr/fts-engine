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
	ObjectID     string
	SegmentKind  semantic.SegmentKind
	Vectors      fileReference
	Graph        fileReference
	State        fileReference
}

type currentRecord struct {
	GenerationID uint64
	ManifestHash [sha256.Size]byte
}

type decodedState struct {
	Embedding               semantic.EmbeddingDescriptor
	Chunking                semantic.ChunkingDescriptor
	MaxAllocatedVectorID    semantic.VectorID
	ComponentID             semantic.ComponentID
	Rows                    []semantic.VectorRow
	MaxK                    int
	MaxChunkCandidates      int
	MaxChunksPerDocumentHit int
}
