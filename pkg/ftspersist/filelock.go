package ftspersist

import "github.com/dariasmyr/fts-engine/v2/internal/persistfs"

type fileLock = persistfs.FileLock

func acquireFileLock(path string, shared, nonblocking bool) (*fileLock, bool, error) {
	return persistfs.AcquireFileLock(path, persistfs.LockOptions{
		Shared: shared, NonBlocking: nonblocking, Create: true,
	})
}
