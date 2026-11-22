package semanticpersist

import "context"

type saveCheckpoint string

const (
	checkpointWriteVectors       saveCheckpoint = "write_vectors"
	checkpointWriteGraph         saveCheckpoint = "write_graph"
	checkpointSyncSegment        saveCheckpoint = "sync_segment"
	checkpointRenameSegment      saveCheckpoint = "rename_segment"
	checkpointSyncReusedSegments saveCheckpoint = "sync_reused_segments"
	checkpointWriteState         saveCheckpoint = "write_state"
	checkpointWriteManifest      saveCheckpoint = "write_manifest"
	checkpointSyncGeneration     saveCheckpoint = "sync_generation"
	checkpointRenameGeneration   saveCheckpoint = "rename_generation"
	checkpointWriteCurrent       saveCheckpoint = "write_current"
	checkpointReplaceCurrent     saveCheckpoint = "replace_current"
	checkpointSyncRoot           saveCheckpoint = "sync_root"
)

type saveCheckpointProbe interface {
	before(saveCheckpoint) error
	after(saveCheckpoint) error
}

type saveCheckpoints struct {
	probe saveCheckpointProbe
}

func (c saveCheckpoints) before(ctx context.Context, checkpoint saveCheckpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.probe == nil {
		return nil
	}
	return c.probe.before(checkpoint)
}

func (c saveCheckpoints) after(checkpoint saveCheckpoint) error {
	if c.probe == nil {
		return nil
	}
	return c.probe.after(checkpoint)
}
