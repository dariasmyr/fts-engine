package fts

import (
	"errors"
	"testing"
)

type testPipeline struct {
	tokens []string
}

func (p testPipeline) Process(_ string) []string {
	return p.tokens
}

type testFilter struct {
	allowed map[string]bool
}

func (f testFilter) Add(_ []byte) bool {
	return true
}

func (f testFilter) Contains(item []byte) bool {
	return f.allowed[string(item)]
}

func TestNormalizeToKeysUsesDefaults(t *testing.T) {
	keys, err := NormalizeToKeys("Hello, World!", nil, nil)
	if err != nil {
		t.Fatalf("NormalizeToKeys() error = %v", err)
	}

	if len(keys) != 2 || keys[0] != "hello" || keys[1] != "world" {
		t.Fatalf("NormalizeToKeys() = %v, want [hello world]", keys)
	}
}

func TestContainsNormalizedMatchesAllKeys(t *testing.T) {
	filter := testFilter{allowed: map[string]bool{"alpha": true, "beta": true}}

	ok, err := ContainsNormalized(filter, "ignored", testPipeline{tokens: []string{"alpha", "beta"}}, WordKeys)
	if err != nil {
		t.Fatalf("ContainsNormalized() error = %v", err)
	}
	if !ok {
		t.Fatal("ContainsNormalized() = false, want true")
	}

	filter.allowed["beta"] = false
	ok, err = ContainsNormalized(filter, "ignored", testPipeline{tokens: []string{"alpha", "beta"}}, WordKeys)
	if err != nil {
		t.Fatalf("ContainsNormalized() error = %v", err)
	}
	if ok {
		t.Fatal("ContainsNormalized() = true, want false")
	}
}

func TestContainsNormalizedReturnsFalseForEmptyKeys(t *testing.T) {
	filter := testFilter{allowed: map[string]bool{}}

	ok, err := ContainsNormalized(filter, "ignored", testPipeline{tokens: nil}, WordKeys)
	if err != nil {
		t.Fatalf("ContainsNormalized() error = %v", err)
	}
	if ok {
		t.Fatal("ContainsNormalized() = true, want false")
	}
}

func TestContainsNormalizedKeygenError(t *testing.T) {
	filter := testFilter{allowed: map[string]bool{}}
	boom := errors.New("boom")

	_, err := ContainsNormalized(filter, "ignored", testPipeline{tokens: []string{"alpha"}}, func(string) ([]string, error) {
		return nil, boom
	})
	if err == nil {
		t.Fatal("ContainsNormalized() error = nil, want non-nil")
	}
}
