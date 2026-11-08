package semantic

import "errors"

var (
	ErrInvalidDocID           = errors.New("semantic: document ID must not be empty")
	ErrInvalidChunkID         = errors.New("semantic: chunk ID must not be empty")
	ErrInvalidField           = errors.New("semantic: field must not be empty")
	ErrInvalidRange           = errors.New("semantic: invalid byte range")
	ErrInvalidUTF8            = errors.New("semantic: invalid UTF-8")
	ErrInvalidConfig          = errors.New("semantic: invalid configuration")
	ErrInvalidBatch           = errors.New("semantic: invalid document batch")
	ErrDocumentExists         = errors.New("semantic: document already exists")
	ErrDocumentNotFound       = errors.New("semantic: document not found")
	ErrVectorIDExhausted      = errors.New("semantic: vector ID exhausted")
	ErrSegmentIDExhausted     = errors.New("semantic: segment id exhausted")
	ErrRevisionExhausted      = errors.New("semantic: mutation revision exhausted")
	ErrPendingMutations       = errors.New("semantic: pending mutations must be committed")
	ErrCapacityExceeded       = errors.New("semantic: vector capacity exceeded")
	ErrInternalState          = errors.New("semantic: inconsistent internal state")
	ErrInvalidSegment         = errors.New("semantic: invalid segment")
	ErrEmbeddingMismatch      = errors.New("semantic: embedding schema mismatch")
	ErrChunkingMismatch       = errors.New("semantic: chunking schema mismatch")
	ErrInvalidSearchOptions   = errors.New("semantic: invalid search options")
	ErrInvalidQuery           = errors.New("semantic: invalid query")
	ErrInvalidDocument        = errors.New("semantic: invalid document")
	ErrInputLimitExceeded     = errors.New("semantic: input limit exceeded")
	ErrEmbeddingCountMismatch = errors.New("semantic: embedding count mismatch")
)
