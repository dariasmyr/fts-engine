package format

import (
	"math"
	"testing"
)

func TestCheckedUint64Arithmetic(t *testing.T) {
	t.Parallel()

	if got, ok := CheckedMultiplyUint64(6, 7); !ok || got != 42 {
		t.Fatalf("CheckedMultiplyUint64(6, 7) = %d, %t; want 42, true", got, ok)
	}
	if _, ok := CheckedMultiplyUint64(math.MaxUint64, 2); ok {
		t.Fatal("CheckedMultiplyUint64 overflow reported success")
	}
	if got, ok := CheckedAddUint64(40, 2); !ok || got != 42 {
		t.Fatalf("CheckedAddUint64(40, 2) = %d, %t; want 42, true", got, ok)
	}
	if _, ok := CheckedAddUint64(math.MaxUint64, 1); ok {
		t.Fatal("CheckedAddUint64 overflow reported success")
	}
}
