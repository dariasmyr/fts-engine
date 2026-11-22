//go:build !windows && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd

package persistfs

import "os"

type FileLock struct{ file *os.File }

// AcquireFileLock preserves the historical best-effort behavior on platforms
// without an advisory-lock implementation: the file is opened but not locked.
func AcquireFileLock(path string, options LockOptions) (*FileLock, bool, error) {
	file, err := openPortableLockFile(path, options)
	if err != nil {
		return nil, false, err
	}
	return &FileLock{file: file}, true, nil
}

func (l *FileLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	file := l.file
	l.file = nil
	return file.Close()
}
