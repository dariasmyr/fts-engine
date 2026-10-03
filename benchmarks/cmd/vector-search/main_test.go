package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"strings"
	"testing"
)

func TestHelpReturnsFlagErrHelp(t *testing.T) {
	var stderr bytes.Buffer
	_, _, _, err := parseFlags([]string{"-h"}, &stderr)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("help error = %v, want flag.ErrHelp", err)
	}
	if !strings.Contains(stderr.String(), "Synthetic Phase 6 ANN baseline") {
		t.Fatalf("help omitted baseline scope:\n%s", stderr.String())
	}
}

func TestPositionalArgumentsRejected(t *testing.T) {
	var stderr bytes.Buffer
	_, _, _, err := parseFlags([]string{"unexpected"}, &stderr)
	if err == nil || !strings.Contains(err.Error(), "unexpected positional arguments") {
		t.Fatalf("positional argument error = %v", err)
	}
}

func TestRunHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	args := []string{
		"-datasets=uniform", "-metrics=l2_squared", "-dimensions=2", "-vectors=8", "-queries=1", "-k=1",
		"-max-neighbors=2", "-ef-construction=4", "-ef-search=1", "-build-seeds=1", "-build-orders=ascending",
		"-filter-selectivities=1", "-format=table",
	}
	var stdout, stderr bytes.Buffer
	if err := run(ctx, args, &stdout, &stderr); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled run error = %v, want context.Canceled", err)
	}
}
