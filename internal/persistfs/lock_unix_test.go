//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package persistfs

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFileLockSharedAndExclusive(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "lease")
	shared, acquired, err := AcquireFileLock(path, LockOptions{Shared: true, Create: true})
	if err != nil || !acquired {
		t.Fatalf("shared lock = (%t, %v), want (true, nil)", acquired, err)
	}
	defer shared.Close()

	exclusive, acquired, err := AcquireFileLock(path, LockOptions{NonBlocking: true, Create: true})
	if err != nil {
		t.Fatalf("exclusive lock error = %v", err)
	}
	if acquired {
		_ = exclusive.Close()
		t.Fatal("exclusive lock acquired while shared lock is held")
	}
}

func TestFileLockRejectsSymlink(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	link := filepath.Join(dir, "lease")
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("Symlink() error = %v", err)
	}
	if _, _, err := AcquireFileLock(link, LockOptions{
		NonBlocking: true, Create: true, RejectSymlink: true,
	}); !errors.Is(err, ErrSymlink) {
		t.Fatalf("AcquireFileLock() error = %v, want %v", err, ErrSymlink)
	}
}
