package main

import (
	"bytes"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/posting"
)

func TestNumericIDsThroughFile(t *testing.T) {
	var out bytes.Buffer
	if err := run(&out); err != nil {
		t.Fatal(err)
	}
	if out.String() != "100000001\n100000042\n" {
		t.Fatal(out.String())
	}
}

func TestCandidateBitmapUsesGenerationOrdinals(t *testing.T) {
	words, count := candidateBitmap(
		[]uint64{100, 200, 300},
		[]uint64{300, 100, 300, 999},
	)
	if count != 2 {
		t.Fatalf("candidate count = %d, want 2", count)
	}
	iterator := posting.NewBitmapIterator(words)
	defer iterator.Close()
	var got []posting.Ord
	for iterator.Next() {
		got = append(got, iterator.Ord())
	}
	if err := iterator.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != 0 || got[1] != 2 {
		t.Fatalf("candidate ordinals = %v, want [0 2]", got)
	}
}
