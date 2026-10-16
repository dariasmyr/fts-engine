//go:build !windows && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd

package ftspersist

import (
	"errors"
	"os"
)

type fileLock struct {
	file *os.File
}

func acquireFileLock(path string, shared, _ bool) (*fileLock, bool, error) {
	var file *os.File
	var err error
	if shared {
		file, err = os.Open(path)
		if errors.Is(err, os.ErrNotExist) {
			file, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
		}
	} else {
		file, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	}
	if err != nil {
		return nil, false, err
	}
	return &fileLock{file: file}, true, nil
}

func (l *fileLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}
