//go:build !windows

package semanticpersist

import "github.com/dariasmyr/fts-engine/v2/internal/persistfs"

func atomicReplace(source, destination string) error {
	return persistfs.Replace(source, destination)
}
