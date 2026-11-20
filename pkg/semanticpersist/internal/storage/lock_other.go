//go:build !linux && !darwin && !freebsd

package storage

type OperationLock struct{}

func AcquireOperationLock(string) (*OperationLock, error) { return nil, ErrLockUnsupported }

func (*OperationLock) Close() error { return nil }
