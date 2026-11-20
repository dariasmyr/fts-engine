package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/internal/contextcheck"
)

// locationTable maps a stable VectorID to its current physical location. A
// published table is immutable; withChanges returns a modified copy.
type locationTable map[VectorID]vectorLocation

func newLocationTable() locationTable {
	return make(locationTable)
}

func (t locationTable) get(id VectorID) (vectorLocation, bool) {
	if id == 0 {
		return vectorLocation{}, false
	}
	location, ok := t[id]
	return location, ok && location.segment != 0
}

func (t locationTable) withChanges(
	ctx context.Context,
	removed []VectorID,
	added map[VectorID]vectorLocation,
) (locationTable, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(removed) == 0 && len(added) == 0 {
		return t, nil
	}

	result := make(locationTable, len(t)+len(added))
	n := 0
	for id, location := range t {
		if err := contextcheck.PeriodicError(ctx, n); err != nil {
			return nil, err
		}
		result[id] = location
		n++
	}
	for n, id := range removed {
		if err := contextcheck.PeriodicError(ctx, n); err != nil {
			return nil, err
		}
		if _, ok := result[id]; !ok {
			return nil, ErrInternalState
		}
		delete(result, id)
	}
	n = 0
	for id, location := range added {
		if err := contextcheck.PeriodicError(ctx, n); err != nil {
			return nil, err
		}
		if id == 0 || location.segment == 0 {
			return nil, ErrInternalState
		}
		if _, exists := t[id]; exists {
			return nil, ErrInternalState
		}
		result[id] = location
		n++
	}
	return result, nil
}

// set is only for constructing a fresh, unpublished table during restore or
// compaction.
func (t locationTable) set(id VectorID, location vectorLocation) error {
	if t == nil || id == 0 || location.segment == 0 {
		return ErrInternalState
	}
	if _, exists := t[id]; exists {
		return ErrInternalState
	}
	t[id] = location
	return nil
}
