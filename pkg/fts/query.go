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

func bindDefaultField(q Query, field string) Query {
	if q == nil || field == "" {
		return q
	}

	switch query := q.(type) {
	case TermQuery:
		if query.Field == "" {
			query.Field = field
		}
		return query
	case *TermQuery:
		if query == nil {
			return nil
		}
		bound := *query
		if bound.Field == "" {
			bound.Field = field
		}
		return &bound
	case PhraseQuery:
		if query.Field == "" {
			query.Field = field
		}
		return query
	case *PhraseQuery:
		if query == nil {
			return nil
		}
		bound := *query
		if bound.Field == "" {
			bound.Field = field
		}
		return &bound
	case PrefixQuery:
		if query.Field == "" {
			query.Field = field
		}
		return query
	case *PrefixQuery:
		if query == nil {
			return nil
		}
		bound := *query
		if bound.Field == "" {
			bound.Field = field
		}
		return &bound
	case *BooleanQuery:
		if query == nil {
			return nil
		}
		bound := &BooleanQuery{Clauses: make([]BoolClause, 0, len(query.Clauses))}
		for _, clause := range query.Clauses {
			bound.Clauses = append(bound.Clauses, BoolClause{
				Occur: clause.Occur,
				Query: bindDefaultField(clause.Query, field),
			})
		}
		return bound
	default:
		return q
	}
}
