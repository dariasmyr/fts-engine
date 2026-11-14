package embedding

import "errors"

var (
	ErrNilContext    = errors.New("embedding: nil context")
	ErrInvalidModel  = errors.New("embedding: invalid model")
	ErrNilClient     = errors.New("embedding: nil client")
	ErrInvalidResult = errors.New("embedding: invalid result")
)
