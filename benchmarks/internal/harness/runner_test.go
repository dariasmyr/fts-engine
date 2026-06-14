package harness

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestPercentiles(t *testing.T) {
	lats := []time.Duration{4 * time.Millisecond, 1 * time.Millisecond, 3 * time.Millisecond, 2 * time.Millisecond}
	got := Percentiles(lats, 0, 0.50, 0.95, 1)
	want := []time.Duration{1 * time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond, 4 * time.Millisecond}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Percentiles()[%d] = %s, want %s", i, got[i], want[i])
		}
	}
}

func TestPercentilesEmpty(t *testing.T) {
	got := Percentiles(nil, 0.5, 0.95)
	if len(got) != 2 {
		t.Fatalf("len(Percentiles(nil)) = %d, want 2", len(got))
	}
	for i, v := range got {
		if v != 0 {
			t.Fatalf("Percentiles(nil)[%d] = %s, want 0", i, v)
		}
	}
}

func TestPrepareAndRunQueriesReuseSingleBuiltIndex(t *testing.T) {
	eng := &countingEngine{}
	docs := []Document{{ID: "doc-1", Body: "alpha"}, {ID: "doc-2", Body: "beta"}}
	ctx := context.Background()

	prep, err := Prepare(ctx, eng, docs, RunConfig{Dir: t.TempDir(), BatchSize: 1})
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	defer eng.Close()

	if eng.openCalls != 1 || eng.commitCalls != 1 {
		t.Fatalf("unexpected prepare lifecycle: open=%d commit=%d", eng.openCalls, eng.commitCalls)
	}
	if eng.indexCalls != 2 {
		t.Fatalf("indexCalls = %d, want 2", eng.indexCalls)
	}

	queriesA := []Query{{ID: "q1", Text: "alpha", K: 10}}
	queriesB := []Query{{ID: "q2", Text: "beta", K: 10}}
	if _, err := RunQueries(ctx, eng, queriesA, RunConfig{}, prep); err != nil {
		t.Fatalf("RunQueries(A) error = %v", err)
	}
	if _, err := RunQueries(ctx, eng, queriesB, RunConfig{}, prep); err != nil {
		t.Fatalf("RunQueries(B) error = %v", err)
	}

	if eng.openCalls != 1 {
		t.Fatalf("openCalls = %d, want 1", eng.openCalls)
	}
	if eng.commitCalls != 1 {
		t.Fatalf("commitCalls = %d, want 1", eng.commitCalls)
	}
	if eng.searchCalls != 2 {
		t.Fatalf("searchCalls = %d, want 2", eng.searchCalls)
	}
}

type countingEngine struct {
	openCalls   int
	indexCalls  int
	commitCalls int
	searchCalls int
	closed      bool
}

func (e *countingEngine) Name() string { return "counting" }

func (e *countingEngine) Open(context.Context, string) error {
	e.openCalls++
	e.closed = false
	return nil
}

func (e *countingEngine) Index(_ context.Context, docs []Document) error {
	e.indexCalls++
	if len(docs) == 0 {
		return fmt.Errorf("empty batch")
	}
	return nil
}

func (e *countingEngine) Commit(context.Context) error {
	e.commitCalls++
	return nil
}

func (e *countingEngine) Search(_ context.Context, q Query) ([]SearchHit, error) {
	e.searchCalls++
	return []SearchHit{{DocID: q.ID, Score: 1}}, nil
}

func (e *countingEngine) IndexSizeBytes() (int64, error) { return 42, nil }

func (e *countingEngine) Close() error {
	e.closed = true
	return nil
}
