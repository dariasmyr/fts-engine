package fts

import (
	"fmt"
	"sync"
)

type IndexFactory func() (Index, error)

var (
	registryMu sync.RWMutex
	registry   = make(map[string]IndexFactory)
)

func RegisterIndex(name string, factory IndexFactory) error {
	if name == "" {
		return fmt.Errorf("fts: register index: empty name")
	}
	if factory == nil {
		return fmt.Errorf("fts: register index: nil factory")
	}

	registryMu.Lock()
	defer registryMu.Unlock()

	if _, exists := registry[name]; exists {
		return fmt.Errorf("fts: register index: duplicate name %q", name)
	}

	registry[name] = factory
	return nil
}

func NewIndex(name string) (Index, error) {
	registryMu.RLock()
	factory, ok := registry[name]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("fts: unknown index %q", name)
	}

	return factory()
}
