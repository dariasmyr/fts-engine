package semantic

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/internal/contextcheck"
	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// Index owns mutable semantic-index state and one immutable committed snapshot.
// Mutations are staged in workingState and become searchable only after Flush.
type Index struct {
	stateGate   chan struct{}
	publishGate chan struct{}

	config     Config
	calculator vector.Calculator
	snapshot   *Snapshot
	state      workingState
}

func NewIndex(config Config) (*Index, error) {
	config, err := config.normalized()
	if err != nil {
		return nil, err
	}
	calculator, err := config.Schema.Embedding.Calculator()
	if err != nil {
		return nil, ErrInvalidConfig
	}
	buildOptions, ok := config.hnswOptions()
	if !ok {
		return nil, ErrInvalidConfig
	}

	snapshot, err := newSnapshot(context.Background(), 0, nil, config.Schema, config.searchPolicy(), buildOptions.Search)
	if err != nil {
		return nil, err
	}
	return &Index{
		stateGate:   make(chan struct{}, 1),
		publishGate: make(chan struct{}, 1),
		config:      config,
		calculator:  calculator,
		snapshot:    snapshot,
		state: workingState{
			nextComponentID: SegmentID(1),
			documents:       make(map[fts.DocID]documentVectors),
			locations:       make(map[VectorID]vectorLocation),
		},
	}, nil
}

func (i *Index) Schema() Schema                 { return i.config.Schema }
func (i *Index) Embedding() EmbeddingDescriptor { return i.config.Schema.Embedding }
func (i *Index) Chunking() ChunkingDescriptor   { return i.config.Schema.Chunking }

// Add stages one encoded document. It is invisible to search until Flush.
func (i *Index) Add(ctx context.Context, docID fts.DocID, batch []EncodedChunk) error {
	prepared, err := i.prepareBatch(ctx, docID, batch)
	if err != nil {
		return err
	}
	if err := i.lockState(ctx); err != nil {
		return err
	}
	defer i.unlockState()
	if err := ctx.Err(); err != nil {
		return err
	}
	err = i.state.addDocument(docID, prepared, i.config.Limits)
	if err == ErrDocumentExists {
		return fmt.Errorf("%w: %s", err, docID)
	}
	return err
}

// Replace stages a replacement for the current document version.
func (i *Index) Replace(ctx context.Context, docID fts.DocID, batch []EncodedChunk) error {
	prepared, err := i.prepareBatch(ctx, docID, batch)
	if err != nil {
		return err
	}
	if err := i.lockState(ctx); err != nil {
		return err
	}
	defer i.unlockState()
	if err := ctx.Err(); err != nil {
		return err
	}
	err = i.state.replaceDocument(docID, prepared, i.config.Limits)
	if err == ErrDocumentNotFound {
		return fmt.Errorf("%w: %s", err, docID)
	}
	return err
}

// Delete stages a deletion. The committed snapshot is unchanged until Flush.
func (i *Index) Delete(ctx context.Context, docID fts.DocID) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if docID == "" {
		return chunk.ErrInvalidDocID
	}
	if err := i.lockState(ctx); err != nil {
		return err
	}
	defer i.unlockState()

	err := i.state.deleteDocument(docID)
	if err == ErrDocumentNotFound {
		return fmt.Errorf("%w: %s", err, docID)
	}
	return err
}

// Snapshot returns the current committed immutable snapshot. Pending mutations
// are intentionally not included.
func (i *Index) Snapshot() *Snapshot {
	i.lockStateUninterruptible()
	snapshot := i.snapshot
	i.unlockState()
	return snapshot
}

func (i *Index) snapshotForRead(ctx context.Context) (*Snapshot, error) {
	if err := i.lockState(ctx); err != nil {
		return nil, err
	}
	snapshot := i.snapshot
	i.unlockState()
	return snapshot, nil
}

func (i *Index) prepareBatch(ctx context.Context, docID fts.DocID, batch []EncodedChunk) ([]EncodedChunk, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(batch) == 0 || len(batch) > i.config.Limits.MaxChunksPerDocument {
		return nil, ErrInvalidBatch
	}
	if docID == "" {
		return nil, chunk.ErrInvalidDocID
	}
	seenChunks := make(map[chunk.ID]struct{}, len(batch))
	prepared := make([]EncodedChunk, len(batch))
	for n, item := range batch {
		if err := contextcheck.PeriodicError(ctx, n); err != nil {
			return nil, err
		}
		if item.Ref.DocID != docID || item.Ref.ID == "" || item.Ref.Field == "" || item.Ref.StartByte > item.Ref.EndByte {
			return nil, ErrInvalidBatch
		}
		if _, exists := seenChunks[item.Ref.ID]; exists {
			return nil, ErrInvalidBatch
		}
		seenChunks[item.Ref.ID] = struct{}{}
		preparedVector, err := i.calculator.Prepare(item.Vector)
		if err != nil {
			return nil, err
		}
		prepared[n] = EncodedChunk{Ref: item.Ref, Vector: preparedVector}
	}
	return prepared, nil
}

func (i *Index) physicalVectorCountLocked() int {
	count := len(i.state.pending.additions)
	if i.snapshot != nil {
		for _, view := range i.snapshot.segments {
			count += view.segment.len()
		}
	}
	return count
}

func (i *Index) Statistics() Statistics {
	i.lockStateUninterruptible()
	defer i.unlockState()
	physical := i.physicalVectorCountLocked()
	return Statistics{
		Documents:            len(i.state.documents),
		PhysicalVectors:      physical,
		LiveVectors:          i.state.liveVectorCount,
		StaleVectors:         physical - i.state.liveVectorCount,
		MaxAllocatedVectorID: i.state.maxAllocatedVectorID,
	}
}

func (i *Index) lockState(ctx context.Context) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case i.stateGate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			i.unlockState()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (i *Index) lockStateUninterruptible() { i.stateGate <- struct{}{} }
func (i *Index) unlockState()              { <-i.stateGate }

func (i *Index) lockPublication(ctx context.Context) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case i.publishGate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			i.unlockPublication()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (i *Index) unlockPublication() { <-i.publishGate }
