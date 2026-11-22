package encoding

import "errors"

var (
	ErrInvalidConfig          = errors.New("semantic encoding: invalid configuration")
	ErrInvalidQuery           = errors.New("semantic encoding: invalid query")
	ErrInvalidDocument        = errors.New("semantic encoding: invalid document")
	ErrInputLimitExceeded     = errors.New("semantic encoding: input limit exceeded")
	ErrEmbeddingCountMismatch = errors.New("semantic encoding: embedding count mismatch")
)
