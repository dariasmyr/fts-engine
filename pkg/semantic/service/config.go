package service

import (
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
)

// CurrentEncodingBehaviorVersion identifies the standard encoder semantics.
const CurrentEncodingBehaviorVersion = semantic.CurrentEncodingBehaviorVersion

// Config combines an Index contract with its model-backed pipeline contract.
type Config = semantic.Config

// PipelineConfig is the frozen, reproducible contract of the standard encoder.
type PipelineConfig = semantic.PipelineConfig

// PipelineOperationalLimits groups frozen limits owned by the encoding pipeline.
type PipelineOperationalLimits = semantic.PipelineOperationalLimits

// EncodingLimits bounds source text and embedding material per encoding call.
type EncodingLimits = semantic.EncodingLimits

// DefaultConfig returns the canonical defaults for model.
func DefaultConfig(model embedding.Model) (Config, error) { return semantic.DefaultConfig(model) }
