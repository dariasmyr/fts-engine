package textproc

import (
	"reflect"
	"testing"
)

func TestAlnumTokenizer(t *testing.T) {
	tok := AlnumTokenizer{}

	got := tok.Tokenize("Wikipedia: The Sans Souci Hotel was built in 1803.")
	want := []string{"Wikipedia", "The", "Sans", "Souci", "Hotel", "was", "built", "in", "1803"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Tokenize() = %v, want %v", got, want)
	}
}

func TestDefaultEnglishPipeline_Golden(t *testing.T) {
	p := DefaultEnglishPipeline()

	got := p.Process("The Rosa hotel was in 1990")
	want := []string{"rosa", "hotel", "1990"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Process() = %v, want %v", got, want)
	}
}

func TestMinLengthOrNumericFilter(t *testing.T) {
	f := MinLengthOrNumericFilter{MinLength: 3}

	got := f.Apply([]string{"go", "api", "404", "x"})
	want := []string{"api", "404"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Apply() = %v, want %v", got, want)
	}
}
