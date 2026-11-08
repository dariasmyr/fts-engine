package semantic

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"

	"github.com/dariasmyr/fts-engine/v2/internal/contextcheck"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// Index owns staged document mutations and one immutable committed view.
// AddDocument, ReplaceDocument, and DeleteDocument only stage changes. Commit
// builds an HNSW segment and makes all staged changes searchable atomically.
type Index struct {
	// mutationGate serializes staging, commit, and compaction. It is a channel
	// rather than sync.Mutex so callers can stop waiting through context.
	mutationGate chan struct{}
	stateMu      sync.RWMutex

	id     IndexID
	config IndexConfig
	limits indexLimits
	space  vector.Space
	state  indexState
}

func NewIndex(config IndexConfig) (*Index, error) {
	config, err := config.normalized()
	if err != nil {
		return nil, err
	}
	space, err := config.Schema.Embedding.VectorSpace()
	if err != nil {
		return nil, ErrInvalidConfig
	}
	searchConfig, ok := config.hnswSearchConfig()
	if !ok {
		return nil, ErrInvalidConfig
	}

	limits := config.runtimeLimits()
	view, err := newIndexView(
		context.Background(),
		0,
		nil,
		config.Schema,
		limits.search,
		limits.results,
		searchConfig,
	)
	if err != nil {
		return nil, err
	}
	var id IndexID
	if _, err := rand.Read(id[:]); err != nil {
		return nil, fmt.Errorf("semantic: generate index ID: %w", err)
	}
	return &Index{
		mutationGate: make(chan struct{}, 1),
		id:           id,
		config:       config,
		limits:       limits,
		space:        space,
		state: indexState{
			view:          view,
			nextSegmentID: SegmentID(1),
			documents:     make(map[fts.DocID]documentVectors),
			locations:     make(map[VectorID]vectorLocation),
		},
	}, nil
}

func (i *Index) Schema() Schema { return i.config.Schema }

// AddDocument validates, copies, prepares, and stages one document's raw chunk
// embeddings. It never retains caller-owned embedding buffers and is invisible
// to search until Commit.
func (i *Index) AddDocument(ctx context.Context, docID fts.DocID, batch []ChunkEmbedding) error {
	prepared, err := i.prepareBatch(ctx, docID, batch)
	if err != nil {
		return err
	}
	err = i.applyMutation(ctx, func(state *indexState) error {
		return state.addDocument(docID, prepared, i.limits.capacity)
	})
	if err == ErrDocumentExists {
		return fmt.Errorf("%w: %s", err, docID)
	}
	return err
}

// ReplaceDocument stages a replacement for the current document version.
func (i *Index) ReplaceDocument(ctx context.Context, docID fts.DocID, batch []ChunkEmbedding) error {
	prepared, err := i.prepareBatch(ctx, docID, batch)
	if err != nil {
		return err
	}
	err = i.applyMutation(ctx, func(state *indexState) error {
		return state.replaceDocument(docID, prepared, i.limits.capacity)
	})
	if err == ErrDocumentNotFound {
		return fmt.Errorf("%w: %s", err, docID)
	}
	return err
}

// DeleteDocument stages a deletion. The committed view is unchanged until
// Commit.
func (i *Index) DeleteDocument(ctx context.Context, docID fts.DocID) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if docID == "" {
		return ErrInvalidDocID
	}
	err := i.applyMutation(ctx, func(state *indexState) error {
		return state.deleteDocument(docID)
	})
	if err == ErrDocumentNotFound {
		return fmt.Errorf("%w: %s", err, docID)
	}
	return err
}

// View returns the current committed immutable view. Pending mutations
// are intentionally not included.
func (i *Index) View() *IndexView {
	i.stateMu.RLock()
	view := i.state.view
	i.stateMu.RUnlock()
	return view
}

func (i *Index) viewForRead(ctx context.Context) (*IndexView, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	i.stateMu.RLock()
	view := i.state.view
	i.stateMu.RUnlock()
	return view, nil
}

type preparedChunk struct {
	ref    Ref
	vector vector.PreparedVector
}

func (i *Index) prepareBatch(ctx context.Context, docID fts.DocID, batch []ChunkEmbedding) ([]preparedChunk, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(batch) == 0 || len(batch) > i.limits.ingestion.maxChunksPerDocument {
		return nil, ErrInvalidBatch
	}
	if docID == "" {
		return nil, ErrInvalidDocID
	}
	seenChunks := make(map[ChunkID]struct{}, len(batch))
	prepared := make([]preparedChunk, len(batch))
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
		preparedVector, err := i.space.Prepare(item.Embedding)
		if err != nil {
			return nil, err
		}
		prepared[n] = preparedChunk{ref: item.Ref, vector: preparedVector}
	}
	return prepared, nil
}

func (i *Index) physicalVectorCountLocked() int {
	count := len(i.state.pending.additions)
	if i.state.view != nil {
		for _, view := range i.state.view.segments {
			count += view.segment.len()
		}
	}
	return count
}

func (i *Index) Statistics() Statistics {
	i.stateMu.RLock()
	defer i.stateMu.RUnlock()
	physical := i.physicalVectorCountLocked()
	return Statistics{
		Documents:            len(i.state.documents),
		PhysicalVectors:      physical,
		LiveVectors:          i.state.liveVectorCount,
		StaleVectors:         physical - i.state.liveVectorCount,
		MaxAllocatedVectorID: i.state.maxAllocatedVectorID,
	}
}

func (i *Index) applyMutation(ctx context.Context, apply func(*indexState) error) error {
	if err := i.lockMutations(ctx); err != nil {
		return err
	}
	defer i.unlockMutations()
	if err := ctx.Err(); err != nil {
		return err
	}
	i.stateMu.Lock()
	defer i.stateMu.Unlock()
	return apply(&i.state)
}

func (i *Index) lockMutations(ctx context.Context) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case i.mutationGate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			i.unlockMutations()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (i *Index) unlockMutations() { <-i.mutationGate }

func (i *Index) Search(
	ctx context.Context,
	queries []ChunkEmbedding,
	k int,
	options SearchOptions,
) (DocumentSearchResult, error) {
	if ctx == nil {
		return DocumentSearchResult{}, vector.ErrNilContext
	}

	view, err := i.viewForRead(ctx)
	if err != nil {
		return DocumentSearchResult{}, err
	}

	return view.Search(ctx, queries, k, options)
}
