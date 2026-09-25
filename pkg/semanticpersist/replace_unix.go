//go:build !windows

package semanticpersist

import "os"

func atomicReplace(source, destination string) error {
	return os.Rename(source, destination)
}
