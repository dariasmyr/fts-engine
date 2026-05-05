package fts

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// FieldQueryClause binds a string subquery to a specific field and top-level boolean occur.
type FieldQueryClause struct {
	Field string
	Query string
	Occur Occur
}

func MustFieldQuery(field, query string) FieldQueryClause {
	return FieldQueryClause{Field: field, Query: query, Occur: Must}
}

func ShouldFieldQuery(field, query string) FieldQueryClause {
	return FieldQueryClause{Field: field, Query: query, Occur: Should}
}

func MustNotFieldQuery(field, query string) FieldQueryClause {
	return FieldQueryClause{Field: field, Query: query, Occur: MustNot}
}

// SearchFieldClauses executes different string subqueries against different fields.
//
// Each clause is parsed independently, bound to its field as the default field,
// then combined into a top-level BooleanQuery using the clause's Occur.
func (s *Service) SearchFieldClauses(ctx context.Context, clauses []FieldQueryClause, maxResults int) (*SearchResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(clauses) == 0 {
		return &SearchResult{Results: []Result{}, Timings: map[string]string{}}, nil
	}

	start := time.Now()
	timings := make(map[string]string, 3)

	preStart := time.Now()

	boolClauses := make([]BoolClause, 0, len(clauses))
	for i, clause := range clauses {
		if strings.TrimSpace(clause.Query) == "" {
			return nil, fmt.Errorf("fts: field query clause %d: empty query", i)
		}

		parsed, err := ParseQuery(clause.Query)
		if err != nil {
			return nil, fmt.Errorf("fts: field query clause %d: parse query: %w", i, err)
		}

		boolClauses = append(boolClauses, BoolClause{
			Occur: clause.Occur,
			Query: bindDefaultField(parsed, clause.Field),
		})
	}
	timings["preprocess"] = formatDuration(time.Since(preStart))

	res, err := s.searchResultForQuery(ctx, clausesToQuery(boolClauses), maxResults, queryFieldScope{})
	if err != nil {
		return nil, err
	}
	timings["search_tokens"] = res.Timings["search_tokens"]
	timings["total"] = formatDuration(time.Since(start))
	res.Timings = timings
	return res, nil
}
