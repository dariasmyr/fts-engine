package semanticpersist

import (
	"sync"

	"github.com/dariasmyr/fts-engine/pkg/semantic"
)

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
