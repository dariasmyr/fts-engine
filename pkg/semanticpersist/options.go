package semanticpersist

import "github.com/dariasmyr/fts-engine/pkg/semantic"

type publicationStep string

const (
	stepWriteVectors     publicationStep = "write_vectors"
	stepWriteGraph       publicationStep = "write_graph"
	stepSyncSegment      publicationStep = "sync_segment"
	stepRenameSegment    publicationStep = "rename_segment"
	stepWriteState       publicationStep = "write_state"
	stepWriteManifest    publicationStep = "write_manifest"
	stepSyncGeneration   publicationStep = "sync_generation"
	stepRenameGeneration publicationStep = "rename_generation"
	stepWriteCurrent     publicationStep = "write_current"
	stepReplaceCurrent   publicationStep = "replace_current"
	stepSyncStore        publicationStep = "sync_store"
)

type Options struct {
	Durability DurabilityMode
	Limits     Limits
	beforeStep func(publicationStep) error
	afterStep  func(publicationStep) error
	// ExpectedGeneration is the CURRENT generation on which this publication is
	// based. A mismatch rejects a stale writer before any generation is committed.
	ExpectedGeneration uint64
}

type OpenOptions struct {
	Limits              Limits
	ExpectedDescriptors semantic.PipelineDescriptor
}
