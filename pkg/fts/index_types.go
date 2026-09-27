package fts

type Index interface {
	Insert(key string, ord DocOrd) error
	Search(key string) ([]Posting, error)
}

type PrefixIndex interface {
	Index
	SearchPrefix(prefix string) ([]Posting, error)
}

type PositionalIndex interface {
	Index
	InsertAt(key string, position uint32, ord DocOrd) error
	SearchPositional(key string) ([]PositionalPosting, error)
}

type IndexFactory func(fieldName string) (Index, error)

type KeyGenerator func(token string) ([]string, error)
