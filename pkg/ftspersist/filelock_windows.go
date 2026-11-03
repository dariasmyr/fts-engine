//go:build windows

package ftspersist

import (
	"errors"
	"os"
	"syscall"
	"unsafe"
)

const (
	lockfileFailImmediately = 0x00000001
	lockfileExclusiveLock   = 0x00000002
	errorLockViolation      = syscall.Errno(33)
)

var (
	kernel32ProcLockFileEx   = syscall.NewLazyDLL("kernel32.dll").NewProc("LockFileEx")
	kernel32ProcUnlockFileEx = syscall.NewLazyDLL("kernel32.dll").NewProc("UnlockFileEx")
)

type fileLock struct {
	file       *os.File
	overlapped syscall.Overlapped
}

func acquireFileLock(path string, shared, nonblocking bool) (*fileLock, bool, error) {
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

	lock := &fileLock{file: file}
	var flags uintptr
	if !shared {
		flags |= lockfileExclusiveLock
	}
	if nonblocking {
		flags |= lockfileFailImmediately
	}
	result, _, callErr := kernel32ProcLockFileEx.Call(
		file.Fd(),
		flags,
		0,
		1,
		0,
		uintptr(unsafe.Pointer(&lock.overlapped)),
	)
	if result == 0 {
		_ = file.Close()
		if nonblocking && errors.Is(callErr, errorLockViolation) {
			return nil, false, nil
		}
		return nil, false, callErr
	}
	return lock, true, nil
}

func (l *fileLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	result, _, callErr := kernel32ProcUnlockFileEx.Call(
		l.file.Fd(),
		0,
		1,
		0,
		uintptr(unsafe.Pointer(&l.overlapped)),
	)
	var unlockErr error
	if result == 0 {
		unlockErr = callErr
	}
	closeErr := l.file.Close()
	l.file = nil
	return errors.Join(unlockErr, closeErr)
}
