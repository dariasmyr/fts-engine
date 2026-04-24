package fts

import "testing"

func TestParseQueryTwoTermsAreShouldClauses(t *testing.T) {
	got, err := ParseQuery("hello world")
	if err != nil {
		t.Fatalf("ParseQuery() error = %v", err)
	}
	b, ok := got.(*BooleanQuery)
	if !ok {
		t.Fatalf("want BooleanQuery, got %T", got)
	}
	if len(b.Clauses) != 2 {
		t.Fatalf("len(Clauses) = %d, want 2", len(b.Clauses))
	}
	if b.Clauses[0].Occur != Should || b.Clauses[1].Occur != Should {
		t.Fatalf("unexpected occurs: %+v", b.Clauses)
	}
}

func TestParseQueryMustAndMustNot(t *testing.T) {
	got, err := ParseQuery(`+apple -banana "fruit salad"`)
	if err != nil {
		t.Fatalf("ParseQuery() error = %v", err)
	}
	b, ok := got.(*BooleanQuery)
	if !ok {
		t.Fatalf("want BooleanQuery, got %T", got)
	}
	if len(b.Clauses) != 3 {
		t.Fatalf("len(Clauses) = %d, want 3", len(b.Clauses))
	}
	if b.Clauses[0].Occur != Must {
		t.Fatalf("first occur = %v, want Must", b.Clauses[0].Occur)
	}
	if b.Clauses[1].Occur != MustNot {
		t.Fatalf("second occur = %v, want MustNot", b.Clauses[1].Occur)
	}
	if _, ok := b.Clauses[2].Query.(PhraseQuery); !ok {
		t.Fatalf("third query type = %T, want PhraseQuery", b.Clauses[2].Query)
	}
}

func TestParseQueryUnterminatedQuoteFails(t *testing.T) {
	if _, err := ParseQuery(`"oops`); err == nil {
		t.Fatal("expected parse error for unterminated quote")
	}
}
