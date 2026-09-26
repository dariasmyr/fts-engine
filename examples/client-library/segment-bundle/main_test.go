package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunRoundTrip(t *testing.T) {
	var out bytes.Buffer
	if err := run(&out); err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if !strings.Contains(out.String(), "results=1") {
		t.Fatalf("run() output = %q, want results=1", out.String())
	}
}
