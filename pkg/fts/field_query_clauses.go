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
	ctx, exec := ensureDiagnosticsContext(ctx)
	if len(clauses) == 0 {
		exec.setQueryTypeIfEmpty("empty")
		exec.setStrategy("empty")
		return attachDiagnostics(ctx, &SearchResult{Results: []Result{}}), nil
	}

	start := time.Now()

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
	preprocess := time.Since(preStart)
	exec.setPreprocessTiming(preprocess)

	res, err := s.searchResultForQuery(ctx, clausesToQuery(boolClauses), maxResults, queryFieldScope{})
	if err != nil {
		return nil, err
	}
	total := time.Since(start)
	exec.setTotalTiming(total)
	return attachDiagnostics(ctx, res), nil
}
