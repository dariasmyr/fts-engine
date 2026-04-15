package keygen

import (
	"reflect"
	"testing"
)

func TestWord(t *testing.T) {
	got, err := Word("hotel")
	if err != nil {
		t.Fatalf("Word() error = %v", err)
	}

	want := []string{"hotel"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Word() = %v, want %v", got, want)
	}
}
