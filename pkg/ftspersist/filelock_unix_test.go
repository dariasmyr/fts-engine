//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package ftspersist

import (
	"path/filepath"
	"testing"
)

func TestFileLockPreventsExclusiveAccessWhileShared(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lease")
	shared, acquired, err := acquireFileLock(path, true, false)
	if err != nil || !acquired {
		t.Fatalf("acquire shared lock = (%t, %v), want (true, nil)", acquired, err)
	}
	exclusive, acquired, err := acquireFileLock(path, false, true)
	if err != nil {
		t.Fatalf("try exclusive lock error = %v", err)
	}
	if acquired {
		_ = exclusive.Close()
		t.Fatal("exclusive lock acquired while shared lock is held")
	}
	if err := shared.Close(); err != nil {
		t.Fatal(err)
	}
	exclusive, acquired, err = acquireFileLock(path, false, true)
	if err != nil || !acquired {
		t.Fatalf("acquire exclusive lock = (%t, %v), want (true, nil)", acquired, err)
	}
	if err := exclusive.Close(); err != nil {
		t.Fatal(err)
	}
}
