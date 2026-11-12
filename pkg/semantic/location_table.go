package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/internal/contextcheck"
)

const (
	locationPageBits = 8
	locationPageSize = 1 << locationPageBits
	locationPageMask = locationPageSize - 1
)

type locationPage [locationPageSize]vectorLocation

// locationTable is immutable after publication. Candidate construction clones
// only the page directory and pages touched by the pending delta.
type locationTable struct {
	pages map[uint64]*locationPage
	count int
}

func newLocationTable() *locationTable {
	return &locationTable{pages: make(map[uint64]*locationPage)}
}

func (t *locationTable) get(id VectorID) (vectorLocation, bool) {
	if t == nil || id == 0 {
		return vectorLocation{}, false
	}
	page := t.pages[locationPageID(id)]
	if page == nil {
		return vectorLocation{}, false
	}
	location := page[locationPageOffset(id)]
	return location, location.segment != 0
}

func (t *locationTable) withChanges(
	ctx context.Context,
	removed []VectorID,
	added map[VectorID]vectorLocation,
) (*locationTable, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(removed) == 0 && len(added) == 0 {
		return t, nil
	}
	for n, id := range removed {
		if err := contextcheck.PeriodicError(ctx, n); err != nil {
			return nil, err
		}
		if _, ok := t.get(id); !ok {
			return nil, ErrInternalState
		}
	}
	n := 0
	for id, location := range added {
		if err := contextcheck.PeriodicError(ctx, n); err != nil {
			return nil, err
		}
		if location.segment == 0 {
			return nil, ErrInternalState
		}
		if _, ok := t.get(id); ok {
			return nil, ErrInternalState
		}
		n++
	}

	pages := make(map[uint64]*locationPage, len(t.pages)+len(added)/locationPageSize+1)
	n = 0
	for id, page := range t.pages {
		if err := contextcheck.PeriodicError(ctx, n); err != nil {
			return nil, err
		}
		pages[id] = page
		n++
	}
	result := &locationTable{pages: pages, count: t.count}
	mutable := make(map[uint64]*locationPage, len(removed)+len(added))
	mutablePage := func(id VectorID) *locationPage {
		pageID := locationPageID(id)
		if page := mutable[pageID]; page != nil {
			return page
		}
		page := new(locationPage)
		if current := result.pages[pageID]; current != nil {
			*page = *current
		}
		result.pages[pageID] = page
		mutable[pageID] = page
		return page
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for n, id := range removed {
		if err := contextcheck.PeriodicError(ctx, n); err != nil {
			return nil, err
		}
		page := mutablePage(id)
		offset := locationPageOffset(id)
		if page[offset].segment == 0 {
			return nil, ErrInternalState
		}
		page[offset] = vectorLocation{}
		result.count--
	}
	n = 0
	for id, location := range added {
		if err := contextcheck.PeriodicError(ctx, n); err != nil {
			return nil, err
		}
		page := mutablePage(id)
		page[locationPageOffset(id)] = location
		result.count++
		n++
	}
	return result, nil
}

// set is only for constructing a fresh, unpublished table during restore or
// compaction.
func (t *locationTable) set(id VectorID, location vectorLocation) error {
	if t == nil || id == 0 {
		return ErrInternalState
	}
	pageID := locationPageID(id)
	page := t.pages[pageID]
	if page == nil {
		page = new(locationPage)
		t.pages[pageID] = page
	}
	offset := locationPageOffset(id)
	if page[offset].segment != 0 || location.segment == 0 {
		return ErrInternalState
	}
	page[offset] = location
	t.count++
	return nil
}

func locationPageID(id VectorID) uint64 {
	return uint64(id) >> locationPageBits
}

func locationPageOffset(id VectorID) uint8 {
	return uint8(uint64(id) & locationPageMask)
}
