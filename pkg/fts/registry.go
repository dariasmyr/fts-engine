package fts

import (
	"fmt"
	"sync"
)

type IndexFactory func() (Index, error)
type FilterFactory func() (Filter, error)

var (
	indexRegistryMu  sync.RWMutex
	indexRegistry    = make(map[string]IndexFactory)
	filterRegistryMu sync.RWMutex
	filterRegistry   = make(map[string]FilterFactory)
)

func RegisterIndex(name string, factory IndexFactory) error {
	if name == "" {
		return fmt.Errorf("fts: register index: empty name")
	}
	if factory == nil {
		return fmt.Errorf("fts: register index: nil factory")
	}

	indexRegistryMu.Lock()
	defer indexRegistryMu.Unlock()

	if _, exists := indexRegistry[name]; exists {
		return fmt.Errorf("fts: register index: duplicate name %q", name)
	}

	indexRegistry[name] = factory
	return nil
}

func NewIndex(name string) (Index, error) {
	indexRegistryMu.RLock()
	factory, ok := indexRegistry[name]
	indexRegistryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("fts: unknown index %q", name)
	}

	return factory()
}

func RegisterFilter(name string, factory FilterFactory) error {
	if name == "" {
		return fmt.Errorf("filter: register: empty name")
	}
	if factory == nil {
		return fmt.Errorf("filter: register: nil factory")
	}

	filterRegistryMu.Lock()
	defer filterRegistryMu.Unlock()

	if _, exists := filterRegistry[name]; exists {
		return fmt.Errorf("filter: register: duplicate name %q", name)
	}

	filterRegistry[name] = factory
	return nil
}

func NewFilter(name string) (Filter, error) {
	filterRegistryMu.RLock()
	factory, ok := filterRegistry[name]
	filterRegistryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("filter: unknown %q", name)
	}

	return factory()
}
