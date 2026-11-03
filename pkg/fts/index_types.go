package fts

// Index is the minimal contract for a searchable index.
type Index interface {
	Search(key string) ([]Posting, error)
}

// MutableIndex extends a searchable index with document insertion.
type MutableIndex interface {
	Index
	Insert(key string, ord DocOrd) error
}

// PrefixIndex extends a searchable index with prefix lookup.
type PrefixIndex interface {
	Index
	SearchPrefix(prefix string) ([]Posting, error)
}

// PositionalIndex extends a searchable index with positional lookup.
type PositionalIndex interface {
	Index
	SearchPositional(key string) ([]PositionalPosting, error)
}

// MutablePositionalIndex extends a positional index with positioned insertion.
type MutablePositionalIndex interface {
	PositionalIndex
	MutableIndex
	InsertAt(key string, position uint32, ord DocOrd) error
}

// IndexFactory creates mutable indexes for fields added to a service.
type IndexFactory func(fieldName string) (MutableIndex, error)

type KeyGenerator func(token string) ([]string, error)
