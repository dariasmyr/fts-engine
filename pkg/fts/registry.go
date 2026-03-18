package fts

import (
	"fmt"
	"sort"
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

func RegisteredIndexes() []string {
	registryMu.RLock()
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	registryMu.RUnlock()

	sort.Strings(names)
	return names
}

func IsIndexRegistered(name string) bool {
	registryMu.RLock()
	_, ok := registry[name]
	registryMu.RUnlock()
	return ok
}
