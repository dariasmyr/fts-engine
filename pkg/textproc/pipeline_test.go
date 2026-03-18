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

func TestDefaultRussianPipeline_Golden(t *testing.T) {
	p := DefaultRussianPipeline()

	got := p.Process("И машины были в 2024 году")
	want := []string{"машин", "2024", "год"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Process() = %v, want %v", got, want)
	}
}

func TestDefaultMultilingualPipeline_Golden(t *testing.T) {
	p := DefaultMultilingualPipeline()

	got := p.Process("The cars и машины were in 2024")
	want := []string{"car", "машин", "2024"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Process() = %v, want %v", got, want)
	}
}

func TestMultilingualStemFilter_ByScript(t *testing.T) {
	f := MultilingualStemFilter{}

	got := f.Apply([]string{"cars", "машины", "abcдеф", "2024"})
	want := []string{"car", "машин", "abcдеф", "2024"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Apply() = %v, want %v", got, want)
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

func TestRussianStopwordFilter(t *testing.T) {
	f := RussianStopwordFilter{}

	got := f.Apply([]string{"и", "машины", "в", "2024"})
	want := []string{"машины", "2024"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Apply() = %v, want %v", got, want)
	}
}

func TestRussianStemFilter(t *testing.T) {
	f := RussianStemFilter{}

	got := f.Apply([]string{"машины", "поездов", "2024"})
	want := []string{"машин", "поезд", "2024"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Apply() = %v, want %v", got, want)
	}
}
