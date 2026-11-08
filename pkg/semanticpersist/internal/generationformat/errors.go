package generationformat

import "errors"

var (
	ErrCorrupt            = errors.New("generationformat: corrupt data")
	ErrUnsupportedVersion = errors.New("generationformat: unsupported version")
	ErrLimitExceeded      = errors.New("generationformat: configured limit exceeded")
)
