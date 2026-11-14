package persistfs

import (
	"errors"
	"os"
)

type LockOptions struct {
	Shared        bool
	NonBlocking   bool
	Create        bool
	RejectSymlink bool
}

func openPortableLockFile(path string, options LockOptions) (*os.File, error) {
	if options.Shared {
		file, err := os.Open(path)
		if !errors.Is(err, os.ErrNotExist) || !options.Create {
			return file, err
		}
	}
	flags := os.O_RDWR
	if options.Create {
		flags |= os.O_CREATE
	}
	return os.OpenFile(path, flags, 0o600)
}
