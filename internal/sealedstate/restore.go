// Package sealedstate restores immutable index readers and their shared service
// metadata. Persistence adapters remain responsible for decoding containers and
// owning reader resources.
package sealedstate

import (
	"fmt"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
)

// State contains storage-independent data required to restore a sealed service.
type State struct {
	Fields          map[string]fts.Index
	MultiField      bool
	Filter          fts.Filter
	CollectionStats *fts.CollectionStatsSnapshot
	Registry        []fts.DocID
	Tombstones      []uint64
	Pipelines       map[string]fts.PipelineDescriptor
	KeyGenerator    *fts.KeyGeneratorDescriptor
}

// Restore creates a search-only service from immutable field readers.
func Restore(state State, keyGen fts.KeyGenerator, opts ...*fts.Option) (*fts.Service, error) {
	if len(state.Fields) == 0 && !state.MultiField {
		return nil, fmt.Errorf("sealed state has no field readers")
	}

	fieldNames := make([]string, 0, len(state.Fields))
	for fieldName, index := range state.Fields {
		if index == nil {
			return nil, fmt.Errorf("nil reader for field %q", fieldName)
		}
		fieldNames = append(fieldNames, fieldName)
	}
	if err := fts.ValidatePipelineFields(fieldNames, state.Pipelines); err != nil {
		return nil, err
	}

	builtOpts := append([]*fts.Option(nil), opts...)
	builtOpts = append(builtOpts, fts.WithPipelineDescriptors(state.Pipelines))
	if state.Filter != nil {
		builtOpts = append(builtOpts, fts.WithFilter(state.Filter))
	}
	if len(state.Registry) > 0 {
		builtOpts = append(builtOpts, fts.WithDocRegistrySnapshot(state.Registry))
	}
	if len(state.Tombstones) > 0 {
		builtOpts = append(builtOpts, fts.WithTombstonesSnapshot(state.Tombstones))
	}
	if state.CollectionStats != nil {
		builtOpts = append(builtOpts, fts.WithCollectionStatsSnapshot(state.CollectionStats))
	}

	var service *fts.Service
	if state.MultiField && len(state.Fields) == 0 {
		service = fts.NewMultiFieldFromIndexes(map[string]fts.Index{}, keyGen, builtOpts...)
	} else if !state.MultiField && len(state.Fields) == 1 {
		if index := state.Fields[fts.DefaultField]; index != nil {
			service = fts.New(index, keyGen, builtOpts...)
		}
	}
	if service == nil {
		service = fts.NewMultiFieldFromIndexes(state.Fields, keyGen, builtOpts...)
	}

	if state.KeyGenerator == nil {
		return nil, fmt.Errorf("key generator descriptor is unavailable")
	}
	restoredKeyGenerator, ok := service.KeyGeneratorDescriptor()
	if !ok || restoredKeyGenerator.Fingerprint != state.KeyGenerator.Fingerprint {
		return nil, fmt.Errorf("key generator fingerprint mismatch")
	}
	if err := service.ValidatePipelineDescriptors(state.Pipelines); err != nil {
		return nil, err
	}
	return service, nil
}
