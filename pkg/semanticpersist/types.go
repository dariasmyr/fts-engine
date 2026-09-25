// Package semanticpersist stores coherent immutable semantic generations.
package semanticpersist

import (
	"crypto/sha256"
	"errors"
	"sync"

	"github.com/dariasmyr/fts-engine/pkg/persist"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
)

var (
	ErrCorrupt            = errors.New("semanticpersist: corrupt data")
	ErrUnsupportedVersion = errors.New("semanticpersist: unsupported version")
	ErrLimitExceeded      = errors.New("semanticpersist: configured limit exceeded")
	ErrCurrentMissing     = errors.New("semanticpersist: CURRENT is missing")
	ErrInvalidObjectID    = errors.New("semanticpersist: invalid object ID")
	ErrPathEscape         = errors.New("semanticpersist: path escapes store root")
	ErrSymlink            = errors.New("semanticpersist: symlink is not allowed")
	ErrGenerationExists   = errors.New("semanticpersist: generation already exists")
	ErrIndeterminate      = errors.New("semanticpersist: publication outcome is indeterminate")
	ErrStoreLocked        = errors.New("semanticpersist: store is locked by another writer")
	ErrStaleGeneration    = errors.New("semanticpersist: expected generation does not match CURRENT")
	ErrEmbeddingMismatch  = errors.New("semanticpersist: embedding descriptor mismatch")
	ErrChunkingMismatch   = errors.New("semanticpersist: chunking descriptor mismatch")
)

type DurabilityMode uint8

const (
	// DurabilitySynchronous fsyncs published files and affected directories.
	DurabilitySynchronous DurabilityMode = iota + 1
	// DurabilityAsynchronous preserves atomic visibility but does not promise
	// that the latest generation survives sudden power loss.
	DurabilityAsynchronous
)

type Limits struct {
	MaxFileBytes         uint64
	MaxVectorBytes       uint64
	MaxGraphBytes        uint64
	MaxGraphLinks        uint64
	MaxOpenBytes         uint64
	MaxEfSearch          int
	MaxVisitLimit        int
	MaxDimensions        int
	MaxVectors           int
	MaxDocuments         int
	MaxStringBytes       int
	MaxChunksPerDocument int
	MaxK                 int
}

func DefaultLimits() Limits {
	return Limits{
		MaxFileBytes: 512 << 20, MaxVectorBytes: 512 << 20, MaxGraphBytes: 512 << 20, MaxGraphLinks: 100_000_000, MaxOpenBytes: 1 << 30,
		MaxEfSearch: 1_000_000, MaxVisitLimit: 10_000_000, MaxDimensions: 65_536,
		MaxVectors: 10_000_000, MaxDocuments: 10_000_000, MaxStringBytes: 1 << 20,
		MaxChunksPerDocument: 1_000_000, MaxK: 1_000_000,
	}
}

type PublicationStep string

const (
	StepWriteVectors     PublicationStep = "write_vectors"
	StepWriteGraph       PublicationStep = "write_graph"
	StepSyncSegment      PublicationStep = "sync_segment"
	StepRenameSegment    PublicationStep = "rename_segment"
	StepWriteState       PublicationStep = "write_state"
	StepWriteManifest    PublicationStep = "write_manifest"
	StepSyncGeneration   PublicationStep = "sync_generation"
	StepRenameGeneration PublicationStep = "rename_generation"
	StepWriteCurrent     PublicationStep = "write_current"
	StepReplaceCurrent   PublicationStep = "replace_current"
	StepSyncStore        PublicationStep = "sync_store"
)

type Options struct {
	Durability DurabilityMode
	Limits     Limits
	BeforeStep func(PublicationStep) error
	AfterStep  func(PublicationStep) error
	// ExpectedGeneration is the CURRENT generation on which this publication is
	// based. A mismatch rejects a stale writer before any generation is committed.
	ExpectedGeneration uint64
}

type OpenOptions struct {
	Limits              Limits
	ExpectedDescriptors semantic.PipelineDescriptor
}

type Generation struct {
	ID       uint64
	ObjectID string
}

// SealedSegment is the persistence payload for one immutable semantic ANN
// component. Segment.Metadata is the single source of truth for embedding and
// chunking compatibility. It deliberately contains no mutable service state or
// generation publication metadata.
type SealedSegment struct {
	Segment                 *semantic.Segment
	MaxAllocatedVectorID    semantic.VectorID
	MaxK                    int
	MaxChunkCandidates      int
	MaxChunksPerDocumentHit int
}

// LoadedSealedSegment is a standalone sealed segment opened without CURRENT.
type LoadedSealedSegment struct {
	Sealed SealedSegment
}

type Loaded struct {
	Generation Generation
	Sealed     SealedSegment
	storeLock  *storeLock
	closeOnce  sync.Once
	closeErr   error
}

// Close closes the immutable segment and releases the shared store lock. It is
// safe to call more than once.
func (l *Loaded) Close() error {
	if l == nil {
		return nil
	}
	l.closeOnce.Do(func() {
		if l.Sealed.Segment != nil {
			l.closeErr = l.Sealed.Segment.Close()
		}
		if l.storeLock != nil {
			if err := l.storeLock.Close(); l.closeErr == nil {
				l.closeErr = err
			}
		}
	})
	return l.closeErr
}

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
