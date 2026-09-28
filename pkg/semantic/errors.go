package semantic

import "errors"

var (
	ErrInvalidConfig        = errors.New("semantic: invalid configuration")
	ErrInvalidBatch         = errors.New("semantic: invalid document batch")
	ErrDocumentExists       = errors.New("semantic: document already exists")
	ErrDocumentNotFound     = errors.New("semantic: document not found")
	ErrVectorIDExhausted    = errors.New("semantic: vector ID exhausted")
	ErrCapacityExceeded     = errors.New("semantic: vector capacity exceeded")
	ErrInternalState        = errors.New("semantic: inconsistent internal state")
	ErrInvalidSegment       = errors.New("semantic: invalid segment")
	ErrEmbeddingMismatch    = errors.New("semantic: embedding descriptor mismatch")
	ErrChunkingMismatch     = errors.New("semantic: chunking descriptor mismatch")
	ErrInvalidSearchOptions = errors.New("semantic: invalid search options")
)
