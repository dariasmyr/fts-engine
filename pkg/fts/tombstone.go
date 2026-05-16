package fts

import (
	"math/bits"
	"sync"
)

// Tombstones tracks deleted document ords. Search paths can use it to
// filter logically deleted documents without rewriting posting lists.
// It is safe for concurrent use.
type Tombstones struct {
	mu   sync.RWMutex
	bits []uint64
	any  bool
}

func NewTombstones() *Tombstones {
	return &Tombstones{}
}

func (t *Tombstones) Set(ord DocOrd) {
	if t == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	word := uint32(ord) / 64
	for int(word) >= len(t.bits) {
		t.bits = append(t.bits, 0)
	}
	t.bits[word] |= 1 << (uint32(ord) % 64)
	t.any = true
}

func (t *Tombstones) IsSet(ord DocOrd) bool {
	if t == nil {
		return false
	}

	t.mu.RLock()
	defer t.mu.RUnlock()
	if !t.any {
		return false
	}
	word := uint32(ord) / 64
	if int(word) >= len(t.bits) {
		return false
	}
	return t.bits[word]&(1<<(uint32(ord)%64)) != 0
}

func (t *Tombstones) Any() bool {
	if t == nil {
		return false
	}

	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.any
}

func (t *Tombstones) Count() int {
	if t == nil {
		return 0
	}

	t.mu.RLock()
	defer t.mu.RUnlock()
	if !t.any {
		return 0
	}
	count := 0
	for _, word := range t.bits {
		count += bits.OnesCount64(word)
	}
	return count
}

func (t *Tombstones) Snapshot() []uint64 {
	if t == nil {
		return nil
	}

	t.mu.RLock()
	defer t.mu.RUnlock()
	if len(t.bits) == 0 {
		return nil
	}
	out := make([]uint64, len(t.bits))
	copy(out, t.bits)
	return out
}

func RestoreTombstones(words []uint64) *Tombstones {
	t := &Tombstones{bits: make([]uint64, len(words))}
	copy(t.bits, words)
	for _, word := range t.bits {
		if word != 0 {
			t.any = true
			break
		}
	}
	return t
}
