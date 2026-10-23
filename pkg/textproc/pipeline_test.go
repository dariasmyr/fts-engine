package textproc

import (
	"reflect"
	"runtime/debug"
	"strings"
	"testing"
	"unicode"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

type testTokenizer struct {
	descriptor ComponentDescriptor
	token      string
}

func (tokenizer *testTokenizer) Tokenize(string) []string {
	return []string{tokenizer.token}
}

func (tokenizer *testTokenizer) Descriptor() ComponentDescriptor {
	return tokenizer.descriptor
}

type testFilter struct {
	descriptor ComponentDescriptor
	suffix     string
}

func (filter *testFilter) Apply(tokens []string) []string {
	result := append([]string(nil), tokens...)
	for i := range result {
		result[i] += filter.suffix
	}
	return result
}

func (filter *testFilter) Descriptor() ComponentDescriptor {
	return filter.descriptor
}

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

func TestMinLengthOrNumericFilterCountsRunes(t *testing.T) {
	f := MinLengthOrNumericFilter{MinLength: 3}
	got := f.Apply([]string{"go", "юю", "api", "ошибка"})
	want := []string{"api", "ошибка"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Apply() = %v, want %v", got, want)
	}
}

func TestObservabilityPipelineGolden(t *testing.T) {
	p := ObservabilityPipeline()
	got := p.Process("request not found 10.0.0.1 io.EOF checkout-api/v2")
	want := []string{
		"request", "not", "found",
		"10.0.0.1", "10", "0", "1",
		"io.eof", "io", "eof",
		"checkout-api/v2", "checkout", "api", "v2",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Process() = %v, want %v", got, want)
	}
	if got := p.Descriptor(); got.Name != "observability" || got.FingerprintScheme != fts.AnalyzerFingerprintSchemeV1 || len(got.Fingerprint) != 64 {
		t.Fatalf("Descriptor() = %+v", got)
	}
}

func TestPipelineDescriptorStable(t *testing.T) {
	a := DefaultMultilingualPipeline().Descriptor()
	b := DefaultMultilingualPipeline().Descriptor()
	if a != b || a.Fingerprint == "" {
		t.Fatalf("descriptors differ: %+v != %+v", a, b)
	}
	if a == ObservabilityPipeline().Descriptor() {
		t.Fatal("different pipelines have the same descriptor")
	}
}

func TestPipelineFingerprintGolden(t *testing.T) {
	tokenizer := &testTokenizer{descriptor: ComponentDescriptor{
		Name: "custom_tokenizer", Version: 2,
		Params: map[string]string{"zeta": "last", "alpha": "first"},
	}}
	filter := &testFilter{descriptor: ComponentDescriptor{
		Name: "custom_filter", Version: 3,
		Params: map[string]string{"mode": "strict"},
	}}
	pipeline, err := NewNamedPipeline("golden", tokenizer, filter)
	if err != nil {
		t.Fatalf("NewNamedPipeline() error = %v", err)
	}
	const want = "29e2fccffcec25794be78ec4af25cf9acc1ef0400fff0f18b13b99cf46a2c508"
	if got := pipeline.Descriptor().Fingerprint; got != want {
		t.Fatalf("Fingerprint = %q, want %q", got, want)
	}
}

func TestPipelineFingerprintDeterministicParameterOrder(t *testing.T) {
	first := &testTokenizer{descriptor: ComponentDescriptor{Name: "custom", Version: 1, Params: map[string]string{"a": "1", "b": "2"}}}
	second := &testTokenizer{descriptor: ComponentDescriptor{Name: "custom", Version: 1, Params: map[string]string{"b": "2", "a": "1"}}}
	a, err := NewNamedPipeline("deterministic", first)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewNamedPipeline("deterministic", second)
	if err != nil {
		t.Fatal(err)
	}
	if a.Descriptor() != b.Descriptor() {
		t.Fatalf("descriptors differ: %+v != %+v", a.Descriptor(), b.Descriptor())
	}
}

func TestPipelineFingerprintTracksConfiguration(t *testing.T) {
	baseTokenizer := &testTokenizer{descriptor: ComponentDescriptor{Name: "tokenizer", Version: 1}}
	otherTokenizer := &testTokenizer{descriptor: ComponentDescriptor{Name: "other_tokenizer", Version: 1}}
	firstFilter := &testFilter{descriptor: ComponentDescriptor{Name: "first", Version: 1}}
	secondFilter := &testFilter{descriptor: ComponentDescriptor{Name: "second", Version: 1}}
	parameterFilter := &testFilter{descriptor: ComponentDescriptor{Name: "parameter", Version: 1, Params: map[string]string{"value": "1"}}}
	versionFilter := &testFilter{descriptor: ComponentDescriptor{Name: "first", Version: 2}}

	base := mustTestPipeline(t, "pipeline", baseTokenizer, firstFilter, secondFilter)
	tests := []struct {
		name     string
		pipeline Pipeline
	}{
		{name: "tokenizer", pipeline: mustTestPipeline(t, "pipeline", otherTokenizer, firstFilter, secondFilter)},
		{name: "filter added", pipeline: mustTestPipeline(t, "pipeline", baseTokenizer, firstFilter, secondFilter, parameterFilter)},
		{name: "filter removed", pipeline: mustTestPipeline(t, "pipeline", baseTokenizer, firstFilter)},
		{name: "filter order", pipeline: mustTestPipeline(t, "pipeline", baseTokenizer, secondFilter, firstFilter)},
		{name: "filter type", pipeline: mustTestPipeline(t, "pipeline", baseTokenizer, firstFilter, parameterFilter)},
		{name: "component version", pipeline: mustTestPipeline(t, "pipeline", baseTokenizer, versionFilter, secondFilter)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.pipeline.Descriptor().Fingerprint == base.Descriptor().Fingerprint {
				t.Fatalf("Fingerprint = %q, want different from base", test.pipeline.Descriptor().Fingerprint)
			}
		})
	}
	parameterChanged := &testFilter{descriptor: ComponentDescriptor{Name: "parameter", Version: 1, Params: map[string]string{"value": "2"}}}
	withParameter := mustTestPipeline(t, "parameterized", baseTokenizer, parameterFilter)
	withChangedParameter := mustTestPipeline(t, "parameterized", baseTokenizer, parameterChanged)
	if withParameter.Descriptor().Fingerprint == withChangedParameter.Descriptor().Fingerprint {
		t.Fatal("fingerprint did not change with filter parameter")
	}
}

func TestPipelineNameDoesNotAffectCompatibility(t *testing.T) {
	first := mustTestPipeline(t, "first name", AlnumTokenizer{}, LowercaseFilter{})
	second := mustTestPipeline(t, "second name", AlnumTokenizer{}, LowercaseFilter{})
	if first.Descriptor().Fingerprint != second.Descriptor().Fingerprint {
		t.Fatalf("fingerprints differ: %q != %q", first.Descriptor().Fingerprint, second.Descriptor().Fingerprint)
	}
	if first.Descriptor().FingerprintScheme != second.Descriptor().FingerprintScheme {
		t.Fatalf("schemes differ: %d != %d", first.Descriptor().FingerprintScheme, second.Descriptor().FingerprintScheme)
	}
}

func TestPipelineFingerprintTracksMutableCustomDescriptor(t *testing.T) {
	tokenizer := &testTokenizer{descriptor: ComponentDescriptor{Name: "custom", Version: 1, Params: map[string]string{"mode": "first"}}, token: "first"}
	pipeline := mustTestPipeline(t, "mutable", tokenizer)
	first := pipeline.Descriptor()
	tokenizer.descriptor.Params["mode"] = "second"
	tokenizer.token = "second"
	second := pipeline.Descriptor()
	if first.Fingerprint == second.Fingerprint {
		t.Fatal("fingerprint did not change with custom component descriptor")
	}
	if got := pipeline.Process(""); !reflect.DeepEqual(got, []string{"second"}) {
		t.Fatalf("Process() = %v", got)
	}
}

func TestNewNamedPipelineCopiesFilterSlice(t *testing.T) {
	tokenizer := &testTokenizer{descriptor: ComponentDescriptor{Name: "custom", Version: 1}, token: "token"}
	first := &testFilter{descriptor: ComponentDescriptor{Name: "first", Version: 1}, suffix: "-first"}
	second := &testFilter{descriptor: ComponentDescriptor{Name: "second", Version: 1}, suffix: "-second"}
	filters := []Filter{first}
	pipeline := mustTestPipeline(t, "copy", tokenizer, filters...)
	descriptor := pipeline.Descriptor()
	filters[0] = second
	if pipeline.Descriptor() != descriptor {
		t.Fatal("descriptor changed after caller mutated filter slice")
	}
	if got := pipeline.Process(""); !reflect.DeepEqual(got, []string{"token-first"}) {
		t.Fatalf("Process() = %v", got)
	}
}

func TestBuiltInComponentDescriptorsValid(t *testing.T) {
	tests := []struct {
		name string
		got  ComponentDescriptor
		want ComponentDescriptor
	}{
		{name: "alnum", got: AlnumTokenizer{}.Descriptor(), want: component("alnum", unicodeParams())},
		{name: "observability", got: ObservabilityTokenizer{}.Descriptor(), want: component("observability", unicodeParams())},
		{name: "lowercase", got: LowercaseFilter{}.Descriptor(), want: component("lowercase", unicodeParams())},
		{name: "minimum length", got: MinLengthOrNumericFilter{MinLength: 3}.Descriptor(), want: component("min_length_or_numeric", map[string]string{"min_length": "3"})},
		{name: "unique", got: UniqueFilter{}.Descriptor(), want: component("unique", nil)},
		{name: "english stopwords", got: EnglishStopwordFilter{}.Descriptor(), want: component("english_stopwords", map[string]string{"dictionary": snowballVersion + "/english-stopwords"})},
		{name: "english stem", got: EnglishStemFilter{}.Descriptor(), want: component("english_stem", map[string]string{"implementation": snowballVersion + "/english", "stem_stop_words": "false"})},
		{name: "russian stopwords", got: RussianStopwordFilter{}.Descriptor(), want: component("russian_stopwords", map[string]string{"dictionary": snowballVersion + "/russian-stopwords"})},
		{name: "russian stem", got: RussianStemFilter{}.Descriptor(), want: component("russian_stem", map[string]string{"implementation": snowballVersion + "/russian", "stem_stop_words": "false"})},
		{name: "multilingual stopwords", got: MultilingualStopwordFilter{}.Descriptor(), want: component("multilingual_stopwords", map[string]string{"english_dictionary": snowballVersion + "/english-stopwords", "russian_dictionary": snowballVersion + "/russian-stopwords", "unicode_version": unicode.Version})},
		{name: "multilingual stem", got: MultilingualStemFilter{}.Descriptor(), want: component("multilingual_stem", map[string]string{"english_implementation": snowballVersion + "/english", "russian_implementation": snowballVersion + "/russian", "stem_stop_words": "false", "unicode_version": unicode.Version})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.got.Validate(); err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
			if !reflect.DeepEqual(test.got, test.want) {
				t.Fatalf("Descriptor() = %+v, want %+v", test.got, test.want)
			}
		})
	}
}

func TestComponentDescriptorRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name       string
		descriptor ComponentDescriptor
	}{
		{name: "empty name", descriptor: ComponentDescriptor{Version: 1}},
		{name: "invalid name", descriptor: ComponentDescriptor{Name: "bad name", Version: 1}},
		{name: "zero version", descriptor: ComponentDescriptor{Name: "valid"}},
		{name: "invalid parameter name", descriptor: ComponentDescriptor{Name: "valid", Version: 1, Params: map[string]string{"bad name": "value"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.descriptor.Validate(); err == nil {
				t.Fatal("Validate() error = nil")
			}
		})
	}
}

func TestComponentDescriptorAllowsEmptyParameterValue(t *testing.T) {
	descriptor := ComponentDescriptor{Name: "valid", Version: 1, Params: map[string]string{"separator": ""}}
	if err := descriptor.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestSnowballDescriptorVersionMatchesDependency(t *testing.T) {
	buildInfo, ok := debug.ReadBuildInfo()
	if !ok {
		t.Fatal("ReadBuildInfo() = false")
	}
	for _, dependency := range buildInfo.Deps {
		if dependency.Path != "github.com/kljensen/snowball" {
			continue
		}
		if got := dependency.Path + "@" + dependency.Version; got != snowballVersion {
			t.Fatalf("snowball descriptor version = %q, linked dependency = %q", snowballVersion, got)
		}
		return
	}
	t.Fatal("snowball dependency is unavailable in build info")
}

func TestMinLengthDescriptorUsesEffectiveValue(t *testing.T) {
	zero := MinLengthOrNumericFilter{MinLength: 0}.Descriptor()
	negative := MinLengthOrNumericFilter{MinLength: -10}.Descriptor()
	one := MinLengthOrNumericFilter{MinLength: 1}.Descriptor()
	if !reflect.DeepEqual(zero, one) || !reflect.DeepEqual(negative, one) {
		t.Fatalf("effective descriptors differ: zero=%+v negative=%+v one=%+v", zero, negative, one)
	}
}

func TestNewNamedPipelineRejectsInvalidDescriptor(t *testing.T) {
	if _, err := NewNamedPipeline("", AlnumTokenizer{}); err == nil {
		t.Fatal("NewNamedPipeline(empty name) error = nil")
	}
	invalidTokenizer := &testTokenizer{descriptor: ComponentDescriptor{Name: "", Version: 1}}
	if _, err := NewNamedPipeline("custom", invalidTokenizer); err == nil || !strings.Contains(err.Error(), "tokenizer") {
		t.Fatalf("NewNamedPipeline(invalid tokenizer) error = %v", err)
	}
	invalidFilter := &testFilter{descriptor: ComponentDescriptor{Name: "bad name", Version: 1}}
	if _, err := NewNamedPipeline("custom", AlnumTokenizer{}, invalidFilter); err == nil || !strings.Contains(err.Error(), "filter 0") {
		t.Fatalf("NewNamedPipeline(invalid filter) error = %v", err)
	}
	if _, err := NewNamedPipeline("custom", AlnumTokenizer{}, nil); err == nil {
		t.Fatal("NewNamedPipeline(nil filter) error = nil")
	}
}

func mustTestPipeline(t *testing.T, name string, tokenizer Tokenizer, filters ...Filter) Pipeline {
	t.Helper()
	pipeline, err := NewNamedPipeline(name, tokenizer, filters...)
	if err != nil {
		t.Fatalf("NewNamedPipeline() error = %v", err)
	}
	return pipeline
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
