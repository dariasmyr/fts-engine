package chunk

import "errors"

const WholeID ID = "_whole"

var (
	ErrInvalidDocID   = errors.New("chunk: document ID must not be empty")
	ErrInvalidField   = errors.New("chunk: field must not be empty")
	ErrInvalidRange   = errors.New("chunk: invalid byte range")
	ErrInvalidChunkID = errors.New("chunk: chunk ID must not be empty")
	ErrInvalidUTF8    = errors.New("chunk: field value must be valid UTF-8")
)
