package semanticpersist

import (
	"context"
	"fmt"
)

type saveStep string

const (
	stepWriteVectors       saveStep = "write_vectors"
	stepWriteGraph         saveStep = "write_graph"
	stepSyncSegment        saveStep = "sync_segment"
	stepRenameSegment      saveStep = "rename_segment"
	stepSyncReusedSegments saveStep = "sync_reused_segments"
	stepWriteState         saveStep = "write_state"
	stepWriteManifest      saveStep = "write_manifest"
	stepSyncGeneration     saveStep = "sync_generation"
	stepRenameGeneration   saveStep = "rename_generation"
	stepWriteCurrent       saveStep = "write_current"
	stepReplaceCurrent     saveStep = "replace_current"
	stepSyncRoot           saveStep = "sync_root"
)

type saveHooks struct {
	before func(saveStep) error
	after  func(saveStep) error
}

type settings struct {
	limits Config
	hooks  saveHooks
}

func newSettings(config Config) (settings, error) {
	config, err := config.normalized()
	if err != nil {
		return settings{}, err
	}
	return settings{limits: config}, nil
}

func beforeStep(ctx context.Context, hooks saveHooks, step saveStep) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if hooks.before != nil {
		return hooks.before(step)
	}
	return nil
}

func afterStep(hooks saveHooks, step saveStep, committed bool) error {
	var err error
	if hooks.after != nil {
		err = hooks.after(step)
	}
	if err != nil && committed {
		return fmt.Errorf("%w: %v", ErrIndeterminate, err)
	}
	return err
}
