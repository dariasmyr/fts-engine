package semanticpersist

import (
	"context"
	"crypto/sha256"

	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/semanticpersist/internal/semanticformat"
)

type restoreResult struct {
	service     *semantic.Service
	generation  Generation
	reusable    map[uint64]semanticformat.SegmentRef
	manifest    semanticformat.GenerationManifest
	manifestRef semanticformat.FileRef
}

type restorer struct {
	layout      layout
	limits      Limits
	objects     objectStore
	generations generationStore
	head        headStore
}

func newRestorer(l layout, limits Limits) restorer {
	options := Options{Durability: DurabilityAsynchronous, Limits: limits}
	return restorer{layout: l, limits: limits, objects: newObjectStore(l, options), generations: newGenerationStore(l, options), head: newHeadStore(l, options)}
}

func (r restorer) openCurrent(ctx context.Context, expected semantic.PipelineDescriptor) (restoreResult, error) {
	head, err := r.head.read(ctx)
	if err != nil {
		return restoreResult{}, err
	}
	return r.openGeneration(ctx, head.GenerationID, head.ManifestHash, true, expected)
}

func (r restorer) openGeneration(ctx context.Context, generationID uint64, expectedHash [sha256.Size]byte, checkHash bool, expected semantic.PipelineDescriptor) (restoreResult, error) {
	generation, err := r.generations.open(ctx, generationID, expectedHash, checkHash)
	if err != nil {
		return restoreResult{}, err
	}
	if err := validateExpectedDescriptors(generation.state.Config, expected); err != nil {
		return restoreResult{}, err
	}

	stored := make([]semantic.StoredSegment, len(generation.state.Segments))
	reusable := make(map[uint64]semanticformat.SegmentRef, len(generation.state.Segments))
	for i, stateSegment := range generation.state.Segments {
		ref := generation.manifest.Segments[i]
		segment, err := r.objects.open(ctx, ref, generation.state.Config, stateSegment)
		if err != nil {
			return restoreResult{}, err
		}
		stored[i] = semantic.StoredSegment{Data: segment, LivenessWords: stateSegment.LivenessWords}
		reusable[stateSegment.ComponentID] = ref
	}
	service, err := semantic.Restore(ctx, semantic.RestoreState{
		Config: generation.state.Config, Revision: generation.state.Revision,
		MaxAllocatedVectorID: generation.state.MaxAllocatedVectorID,
		NextComponentID:      generation.state.NextComponentID, Segments: stored,
	})
	if err != nil {
		if ctx.Err() != nil {
			return restoreResult{}, ctx.Err()
		}
		return restoreResult{}, ErrCorrupt
	}
	return restoreResult{
		service: service, generation: Generation{ID: generationID}, reusable: reusable,
		manifest: generation.manifest, manifestRef: generation.manifestRef,
	}, nil
}

func validateExpectedDescriptors(config semantic.Config, expected semantic.PipelineDescriptor) error {
	if expected == (semantic.PipelineDescriptor{}) {
		return nil
	}
	if config.Embedding != expected.Embedding {
		return ErrEmbeddingMismatch
	}
	if config.Chunking != expected.Chunking {
		return ErrChunkingMismatch
	}
	return nil
}
