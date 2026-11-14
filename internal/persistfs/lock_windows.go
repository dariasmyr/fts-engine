//go:build windows

package persistfs

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

type FileLock struct {
	file       *os.File
	overlapped syscall.Overlapped
}

func AcquireFileLock(path string, options LockOptions) (*FileLock, bool, error) {
	file, err := openPortableLockFile(path, options)
	if err != nil {
		return nil, false, err
	}
	lock := &FileLock{file: file}
	var flags uintptr
	if !options.Shared {
		flags |= lockfileExclusiveLock
	}
	if options.NonBlocking {
		flags |= lockfileFailImmediately
	}
	result, _, callErr := kernel32ProcLockFileEx.Call(
		file.Fd(), flags, 0, 1, 0, uintptr(unsafe.Pointer(&lock.overlapped)),
	)
	if result == 0 {
		_ = file.Close()
		if options.NonBlocking && errors.Is(callErr, errorLockViolation) {
			return nil, false, nil
		}
		return nil, false, callErr
	}
	return lock, true, nil
}

func (l *FileLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	result, _, callErr := kernel32ProcUnlockFileEx.Call(
		l.file.Fd(), 0, 1, 0, uintptr(unsafe.Pointer(&l.overlapped)),
	)
	var unlockErr error
	if result == 0 {
		unlockErr = callErr
	}
	closeErr := l.file.Close()
	l.file = nil
	return errors.Join(unlockErr, closeErr)
}
