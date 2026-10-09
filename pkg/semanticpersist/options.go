package semanticpersist

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/semantic"
)

type publicationStep string

const (
	stepWriteVectors       publicationStep = "write_vectors"
	stepWriteGraph         publicationStep = "write_graph"
	stepSyncSegment        publicationStep = "sync_segment"
	stepRenameSegment      publicationStep = "rename_segment"
	stepSyncReusedSegments publicationStep = "sync_reused_segments"
	stepWriteState         publicationStep = "write_state"
	stepWriteManifest      publicationStep = "write_manifest"
	stepSyncGeneration     publicationStep = "sync_generation"
	stepRenameGeneration   publicationStep = "rename_generation"
	stepWriteCurrent       publicationStep = "write_current"
	stepReplaceCurrent     publicationStep = "replace_current"
	stepSyncStore          publicationStep = "sync_store"
)

type PublishOptions struct {
	Durability         DurabilityMode
	Limits             Limits
	beforeStep         func(publicationStep) error
	afterStep          func(publicationStep) error
	ExpectedGeneration *Generation
}

type OpenOptions struct {
	Limits         Limits
	ExpectedSchema semantic.Schema
}

func normalizeOptions(options *PublishOptions) error {
	if options.Durability == 0 {
		options.Durability = DurabilitySynchronous
	}
	if options.Durability != DurabilitySynchronous && options.Durability != DurabilityAsynchronous {
		return ErrCorrupt
	}
	options.Limits = normalizeLimits(options.Limits)
	return validateLimits(options.Limits)
}

func beforeStep(ctx context.Context, options PublishOptions, step publicationStep) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if options.beforeStep != nil {
		return options.beforeStep(step)
	}
	return nil
}

func afterStep(options PublishOptions, step publicationStep, committed bool) error {
	var err error
	if options.afterStep != nil {
		err = options.afterStep(step)
	}
	if err != nil && committed {
		return fmt.Errorf("%w: %v", ErrIndeterminate, err)
	}
	return err
}
