package main

import (
	"bytes"
	"testing"
)

func TestCandidateIDs(t *testing.T) {
	var out bytes.Buffer
	if err := run(&out); err != nil {
		t.Fatal(err)
	}
	if got, want := out.String(), "event-3\n"; got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}
