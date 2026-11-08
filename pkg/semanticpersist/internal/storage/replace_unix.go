//go:build !windows

package storage

import "github.com/dariasmyr/fts-engine/v2/internal/persistfs"

func AtomicReplace(source, destination string) error {
	return persistfs.Replace(source, destination)
}
