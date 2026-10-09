package semanticformat

import (
	"errors"
)

var (
	ErrCorrupt = errors.New(
		"semanticformat: corrupt data",
	)

	ErrUnsupportedVersion = errors.New(
		"semanticformat: unsupported version",
	)

	ErrLimitExceeded = errors.New(
		"semanticformat: configured limit exceeded",
	)
)
