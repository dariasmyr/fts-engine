package semanticpersist

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	semanticformat "github.com/dariasmyr/fts-engine/pkg/semanticpersist/internal/format"
)

func TestMapCodecErrorPreservesContext(t *testing.T) {
	internalErr := fmt.Errorf("state segment 0 row 0 has zero vector ID: %w", semanticformat.ErrCorrupt)

	err := mapCodecError(internalErr)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("mapped error = %v, want category %v", err, ErrCorrupt)
	}
	if !strings.Contains(err.Error(), "state segment 0 row 0 has zero vector ID") {
		t.Fatalf("mapped error lost context: %v", err)
	}
}
