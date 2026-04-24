package fts

type Query interface{ isQuery() }

type TermQuery struct {
	Field string
	Term  string
}

func (TermQuery) isQuery() {}

type PhraseQuery struct {
	Field  string
	Phrase string
}

func (PhraseQuery) isQuery() {}

type PrefixQuery struct {
	Field  string
	Prefix string
}

func (PrefixQuery) isQuery() {}

type Occur int

const (
	Should Occur = iota
	Must
	MustNot
)

type BoolClause struct {
	Occur Occur
	Query Query
}

type BooleanQuery struct {
	Clauses []BoolClause
}

func (*BooleanQuery) isQuery() {}

func MustClause(q Query) BoolClause    { return BoolClause{Occur: Must, Query: q} }
func ShouldClause(q Query) BoolClause  { return BoolClause{Occur: Should, Query: q} }
func MustNotClause(q Query) BoolClause { return BoolClause{Occur: MustNot, Query: q} }
