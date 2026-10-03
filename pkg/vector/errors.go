package vector

import "errors"

var (
	ErrNilContext               = errors.New("vector: nil context")
	ErrInvalidK                 = errors.New("vector: k must be positive")
	ErrInvalidSearchOptions     = errors.New("vector: search options must not be negative")
	ErrResultFilterSizeMismatch = errors.New("vector: result filter size does not match vector count")
)
