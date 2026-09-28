package semanticpersist

import "github.com/dariasmyr/fts-engine/pkg/semantic"

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
