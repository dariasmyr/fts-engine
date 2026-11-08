package semanticpersist

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestOperationsRejectCorruptSegmentObject(t *testing.T) {
	tests := []struct {
		name string
		run  func(context.Context, string, uint64) error
	}{
		{
			name: "load",
			run: func(ctx context.Context, root string, _ uint64) error {
				_, _, err := Load(ctx, root, LoadOptions{})
				return err
			},
		},
		{
			name: "prune",
			run: func(ctx context.Context, root string, _ uint64) error {
				_, err := Prune(ctx, root, PruneOptions{RetainGenerations: 1})
				return err
			},
		},
		{
			name: "recover",
			run: func(ctx context.Context, root string, generationID uint64) error {
				return RecoverGeneration(ctx, root, Generation{ID: generationID}, Limits{})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			root := t.TempDir()
			index := newTestIndex(t)
			addTestSegment(t, ctx, index)
			generation := saveTestGenerations(t, ctx, root, index, 1)[0]
			corruptFirstSegmentVectors(t, root)

			if err := tt.run(ctx, root, generation.ID); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("operation error = %v, want ErrCorrupt", err)
			}
		})
	}
}

func corruptFirstSegmentVectors(t *testing.T, root string) {
	t.Helper()

	l := newLayout(root)
	entries, err := os.ReadDir(l.segments)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("segment objects = %d, want 1", len(entries))
	}
	path := filepath.Join(l.segments, entries[0].Name(), vectorsFileName)
	if err := os.WriteFile(path, []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
}
