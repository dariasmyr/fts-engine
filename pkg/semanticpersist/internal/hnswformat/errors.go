package hnswformat

import (
	"errors"
	"fmt"
)

var (
	ErrCorrupt            = errors.New("semanticformat: corrupt data")
	ErrUnsupportedVersion = errors.New("semanticformat: unsupported version")
	ErrLimitExceeded      = errors.New("semanticformat: configured limit exceeded")
)

type categorizedError struct {
	category error
	detail   string
}

func (e categorizedError) Error() string        { return e.detail }
func (e categorizedError) Is(target error) bool { return target == e.category }

func codecErrorf(category error, format string, args ...any) error {
	return categorizedError{category: category, detail: fmt.Sprintf(format, args...)}
}
