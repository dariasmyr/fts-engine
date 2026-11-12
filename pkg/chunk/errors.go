package chunk

import "errors"

var (
	ErrInvalidRange        = errors.New("chunk: invalid byte range")
	ErrInvalidUTF8         = errors.New("chunk: source must be valid UTF-8")
	ErrInvalidSplitConfig  = errors.New("chunk: invalid split configuration")
	ErrRuneExceedsMaxBytes = errors.New("chunk: UTF-8 rune exceeds maximum chunk bytes")
)
