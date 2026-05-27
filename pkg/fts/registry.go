package fts

import "sync"

// DocRegistry stores stable internal ordinals for external document IDs.
// It is safe for concurrent use.
type DocRegistry struct {
	mu      sync.RWMutex
	idToOrd map[DocID]DocOrd
	ordToID []DocID
}

func NewDocRegistry() *DocRegistry {
	return &DocRegistry{idToOrd: make(map[DocID]DocOrd)}
}

func (r *DocRegistry) GetOrAssign(id DocID) DocOrd {
	if r == nil {
		return 0
	}

	r.mu.RLock()
	if ord, ok := r.idToOrd[id]; ok {
		r.mu.RUnlock()
		return ord
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureInitLocked()
	if ord, ok := r.idToOrd[id]; ok {
		return ord
	}
	ord := DocOrd(len(r.ordToID))
	r.idToOrd[id] = ord
	r.ordToID = append(r.ordToID, id)
	return ord
}

func (r *DocRegistry) Lookup(ord DocOrd) DocID {
	if r == nil {
		return ""
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	if int(ord) >= len(r.ordToID) {
		return ""
	}
	return r.ordToID[ord]
}

func (r *DocRegistry) Has(id DocID) (DocOrd, bool) {
	if r == nil {
		return 0, false
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	ord, ok := r.idToOrd[id]
	return ord, ok
}

func (r *DocRegistry) Forget(id DocID) {
	if r == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.idToOrd, id)
}

func (r *DocRegistry) Len() int {
	if r == nil {
		return 0
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.ordToID)
}

func (r *DocRegistry) TotalAssigned() int {
	if r == nil {
		return 0
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.ordToID)
}

func (r *DocRegistry) ActiveLen() int {
	if r == nil {
		return 0
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.idToOrd)
}

func (r *DocRegistry) Snapshot() []DocID {
	if r == nil {
		return nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]DocID, len(r.ordToID))
	copy(out, r.ordToID)
	return out
}

func RestoreDocRegistry(ids []DocID) *DocRegistry {
	r := &DocRegistry{
		idToOrd: make(map[DocID]DocOrd, len(ids)),
		ordToID: make([]DocID, len(ids)),
	}
	copy(r.ordToID, ids)
	for i, id := range ids {
		r.idToOrd[id] = DocOrd(i)
	}
	return r
}

func RestoreDocRegistryActive(ids []DocID, tombstones *Tombstones) *DocRegistry {
	r := &DocRegistry{
		idToOrd: make(map[DocID]DocOrd, len(ids)),
		ordToID: make([]DocID, len(ids)),
	}
	copy(r.ordToID, ids)
	for i, id := range ids {
		ord := DocOrd(i)
		if tombstones != nil && tombstones.IsSet(ord) {
			continue
		}
		r.idToOrd[id] = ord
	}
	return r
}

func (r *DocRegistry) ensureInitLocked() {
	if r.idToOrd == nil {
		r.idToOrd = make(map[DocID]DocOrd)
	}
}
