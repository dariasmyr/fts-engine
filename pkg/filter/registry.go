package filter

import (
	"fmt"
	"sort"
	"sync"
)

type Filter interface {
	Add(item []byte) bool
	Contains(item []byte) bool
}

type Factory func() (Filter, error)

var (
	registryMu sync.RWMutex
	registry   = make(map[string]Factory)
)

func Register(name string, factory Factory) error {
	if name == "" {
		return fmt.Errorf("filter: register: empty name")
	}
	if factory == nil {
		return fmt.Errorf("filter: register: nil factory")
	}

	registryMu.Lock()
	defer registryMu.Unlock()

	if _, exists := registry[name]; exists {
		return fmt.Errorf("filter: register: duplicate name %q", name)
	}

	registry[name] = factory
	return nil
}

func New(name string) (Filter, error) {
	registryMu.RLock()
	factory, ok := registry[name]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("filter: unknown %q", name)
	}

	return factory()
}

func Registered() []string {
	registryMu.RLock()
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	registryMu.RUnlock()

	sort.Strings(names)
	return names
}

func IsRegistered(name string) bool {
	registryMu.RLock()
	_, ok := registry[name]
	registryMu.RUnlock()
	return ok
}
