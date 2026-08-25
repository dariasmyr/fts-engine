// Package semanticpersist stores coherent immutable semantic generations.
package semanticpersist

import (
	"crypto/sha256"
	"errors"
	"sync"

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
	MaxOpenBytes         uint64
	MaxDimensions        int
	MaxVectors           int
	MaxDocuments         int
	MaxStringBytes       int
	MaxChunksPerDocument int
	MaxK                 int
}

func DefaultLimits() Limits {
	return Limits{
		MaxFileBytes: 512 << 20, MaxVectorBytes: 512 << 20, MaxOpenBytes: 1 << 30, MaxDimensions: 65_536,
		MaxVectors: 10_000_000, MaxDocuments: 10_000_000, MaxStringBytes: 1 << 20,
		MaxChunksPerDocument: 1_000_000, MaxK: 1_000_000,
	}
}

type PublicationStep string

const (
	StepWriteVectors     PublicationStep = "write_vectors"
	StepWriteSegmentMeta PublicationStep = "write_segment_meta"
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

type Generation struct {
	ID       uint64
	ObjectID string
}

type Loaded struct {
	Generation Generation
	Reader     *semantic.Reader
	Checkpoint semantic.Checkpoint
	storeLock  *storeLock
	closeOnce  sync.Once
	closeErr   error
}

// Close closes the immutable reader and releases the shared store lock. It is
// safe to call more than once.
func (l *Loaded) Close() error {
	if l == nil {
		return nil
	}
	l.closeOnce.Do(func() {
		if l.Reader != nil {
			l.closeErr = l.Reader.Close()
		}
		if l.storeLock != nil {
			if err := l.storeLock.Close(); l.closeErr == nil {
				l.closeErr = err
			}
		}
	})
	return l.closeErr
}

type fileReference struct {
	Size   uint64
	SHA256 [sha256.Size]byte
}

type manifest struct {
	GenerationID uint64
	ObjectID     string
	Vectors      fileReference
	SegmentMeta  fileReference
	State        fileReference
}

type currentRecord struct {
	GenerationID uint64
	ManifestHash [sha256.Size]byte
}

type decodedState struct {
	Space                   semantic.SpaceDescriptor
	Chunking                semantic.ChunkingDescriptor
	MaxAllocatedVectorID    semantic.VectorID
	Documents               []semantic.DocumentRecord
	Refs                    []semantic.RefRecord
	MaxK                    int
	MaxChunkCandidates      int
	MaxChunksPerDocumentHit int
}

type decodedSegmentMeta struct {
	VectorIDs           []semantic.VectorID
	Live                []uint64
	LiveSize            uint32
	Vectors             fileReference
	DuplicateStatistics *semantic.DuplicateStatistics
}
