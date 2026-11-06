package semanticpersist

import (
	"os"

	"github.com/dariasmyr/fts-engine/v2/internal/persistfs"
)

type durabilityPolicy struct{ mode DurabilityMode }

func (d durabilityPolicy) synchronous() bool { return d.mode == DurabilitySynchronous }

func (d durabilityPolicy) syncFile(file *os.File) error {
	if !d.synchronous() {
		return nil
	}
	return persistfs.SyncFile(file)
}

func (d durabilityPolicy) syncRegularFile(path string) error {
	if !d.synchronous() {
		return nil
	}
	return mapPersistFSError(persistfs.SyncRegularFile(path), ErrCorrupt)
}

func (d durabilityPolicy) syncDirectory(path string) error {
	if !d.synchronous() {
		return nil
	}
	return persistfs.SyncDirectory(path)
}
