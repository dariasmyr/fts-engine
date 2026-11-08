package hnswformat

import (
	"errors"
	"fmt"
)

var (
	ErrCorrupt            = errors.New("hnswformat: corrupt data")
	ErrUnsupportedVersion = errors.New("hnswformat: unsupported version")
	ErrLimitExceeded      = errors.New("hnswformat: configured limit exceeded")
)

func codecErrorf(category error, format string, args ...any) error {
	return fmt.Errorf("%w: %s", category, fmt.Sprintf(format, args...))
}
