package keygen

import (
	"errors"
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

func TestTrigram(t *testing.T) {
	got, err := Trigram("hotel")
	if err != nil {
		t.Fatalf("Trigram() error = %v", err)
	}

	want := []string{"hot", "ote", "tel"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Trigram() = %v, want %v", got, want)
	}
}

func TestTrigramShortToken(t *testing.T) {
	_, err := Trigram("hi")
	if !errors.Is(err, ErrInvalidTrigramSize) {
		t.Fatalf("Trigram() error = %v, want %v", err, ErrInvalidTrigramSize)
	}
}
