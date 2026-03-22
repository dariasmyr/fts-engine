package fts

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

type testStaticFilter struct {
	set        map[string]struct{}
	builds     int
	buildErr   error
	serialized bool
}

func (f *testStaticFilter) Build(items [][]byte) error {
	f.builds++
	if f.buildErr != nil {
		return f.buildErr
	}

	f.set = make(map[string]struct{}, len(items))
	for _, item := range items {
		f.set[string(item)] = struct{}{}
	}

	return nil
}

func (f *testStaticFilter) Contains(item []byte) bool {
	_, ok := f.set[string(item)]
	return ok
}

func (f *testStaticFilter) Serialize(w io.Writer) error {
	f.serialized = true
	_, err := w.Write([]byte("ok"))
	return err
}

func TestBufferedStaticFilterBuildsOnExplicitBuild(t *testing.T) {
	static := &testStaticFilter{}
	filter := NewBufferedStaticFilter(static)

	filter.Add([]byte("alpha"))
	filter.Add([]byte("alpha"))
	filter.Add([]byte("beta"))

	if !filter.Contains([]byte("alpha")) {
		t.Fatal("Contains(alpha) before Build = false, want true (pass-through)")
	}

	if static.builds != 0 {
		t.Fatalf("builds before Build() = %d, want 0", static.builds)
	}

	if err := filter.Build(); err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	if static.builds != 1 {
		t.Fatalf("builds after Build() = %d, want 1", static.builds)
	}

	if !filter.Contains([]byte("alpha")) {
		t.Fatal("Contains(alpha) after Build = false, want true")
	}

	if !filter.Contains([]byte("beta")) {
		t.Fatal("Contains(beta) after Build = false, want true")
	}

	filter.Add([]byte("gamma"))

	if !filter.Contains([]byte("gamma")) {
		t.Fatal("Contains(gamma) before second Build = false, want true (pass-through)")
	}

	if err := filter.Build(); err != nil {
		t.Fatalf("Build() after Add(gamma) error = %v", err)
	}

	if static.builds != 2 {
		t.Fatalf("builds after second Build() = %d, want 2", static.builds)
	}

	if !filter.Contains([]byte("gamma")) {
		t.Fatal("Contains(gamma) after second Build = false, want true")
	}
}

func TestBufferedStaticFilterBuildFailureReturnsErrorAndContainsPassThrough(t *testing.T) {
	static := &testStaticFilter{buildErr: errors.New("boom")}
	filter := NewBufferedStaticFilter(static)

	filter.Add([]byte("alpha"))
	if err := filter.Build(); err == nil {
		t.Fatal("Build() error = nil, want non-nil")
	}

	if !filter.Contains([]byte("unknown")) {
		t.Fatal("Contains(unknown) = false, want true after failed build")
	}

	if static.builds != 1 {
		t.Fatalf("builds = %d, want 1", static.builds)
	}
}

func TestBufferedStaticFilterSerializeDelegatesToStatic(t *testing.T) {
	static := &testStaticFilter{}
	filter := NewBufferedStaticFilter(static)
	filter.Add([]byte("alpha"))

	var out bytes.Buffer
	if err := filter.Serialize(&out); err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}

	if static.builds != 1 {
		t.Fatalf("builds during Serialize = %d, want 1", static.builds)
	}

	if !static.serialized {
		t.Fatal("Serialize() did not call static serializer")
	}
}
