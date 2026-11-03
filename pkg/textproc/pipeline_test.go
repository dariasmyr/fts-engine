package textproc

import (
	"reflect"
	"runtime/debug"
	"strings"
	"testing"
)

type testTokenizer struct {
	fingerprint string
	token       string
}

func (tokenizer *testTokenizer) Tokenize(string) []string {
	return []string{tokenizer.token}
}

func (tokenizer *testTokenizer) Fingerprint() string {
	return tokenizer.fingerprint
}

type testFilter struct {
	fingerprint string
	suffix      string
}

func (filter *testFilter) Apply(tokens []string) []string {
	result := append([]string(nil), tokens...)
	for i := range result {
		result[i] += filter.suffix
	}
	return result
}

func (filter *testFilter) Fingerprint() string {
	return filter.fingerprint
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
	if got := p.Descriptor(); got.Name != "observability" || !strings.HasPrefix(got.Fingerprint, "sha256:") || len(got.Fingerprint) != len("sha256:")+64 {
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
	tokenizer := &testTokenizer{fingerprint: componentFingerprint("custom-tokenizer/v2", "alpha=first", "zeta=last")}
	filter := &testFilter{fingerprint: componentFingerprint("custom-filter/v3", "mode=strict")}
	pipeline, err := NewNamedPipeline("golden", tokenizer, filter)
	if err != nil {
		t.Fatalf("NewNamedPipeline() error = %v", err)
	}
	const want = "sha256:20a85ff3b786ba0ac03a31a62c3e497860eb01c4399e58ef884de9ddb3122665"
	if got := pipeline.Descriptor().Fingerprint; got != want {
		t.Fatalf("Fingerprint = %q, want %q", got, want)
	}
}

func TestPipelineFingerprintDeterministic(t *testing.T) {
	fingerprint := componentFingerprint("custom/v1", "a=1", "b=2")
	first := &testTokenizer{fingerprint: fingerprint}
	second := &testTokenizer{fingerprint: fingerprint}
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
	baseTokenizer := &testTokenizer{fingerprint: componentFingerprint("tokenizer/v1")}
	otherTokenizer := &testTokenizer{fingerprint: componentFingerprint("other-tokenizer/v1")}
	firstFilter := &testFilter{fingerprint: componentFingerprint("first/v1")}
	secondFilter := &testFilter{fingerprint: componentFingerprint("second/v1")}
	parameterFilter := &testFilter{fingerprint: componentFingerprint("parameter/v1", "value=1")}
	versionFilter := &testFilter{fingerprint: componentFingerprint("first/v2")}

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
	parameterChanged := &testFilter{fingerprint: componentFingerprint("parameter/v1", "value=2")}
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
}

func TestPipelineFingerprintTracksMutableCustomComponent(t *testing.T) {
	tokenizer := &testTokenizer{fingerprint: componentFingerprint("custom/v1", "mode=first"), token: "first"}
	pipeline := mustTestPipeline(t, "mutable", tokenizer)
	first := pipeline.Descriptor()
	tokenizer.fingerprint = componentFingerprint("custom/v1", "mode=second")
	tokenizer.token = "second"
	second := pipeline.Descriptor()
	if first.Fingerprint == second.Fingerprint {
		t.Fatal("fingerprint did not change with custom component")
	}
	if got := pipeline.Process(""); !reflect.DeepEqual(got, []string{"second"}) {
		t.Fatalf("Process() = %v", got)
	}
}

func TestNewNamedPipelineCopiesFilterSlice(t *testing.T) {
	tokenizer := &testTokenizer{fingerprint: componentFingerprint("custom/v1"), token: "token"}
	first := &testFilter{fingerprint: componentFingerprint("first/v1"), suffix: "-first"}
	second := &testFilter{fingerprint: componentFingerprint("second/v1"), suffix: "-second"}
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

func TestBuiltInComponentFingerprintsValid(t *testing.T) {
	tests := []struct {
		name        string
		fingerprint string
	}{
		{name: "alnum", fingerprint: AlnumTokenizer{}.Fingerprint()},
		{name: "observability", fingerprint: ObservabilityTokenizer{}.Fingerprint()},
		{name: "lowercase", fingerprint: LowercaseFilter{}.Fingerprint()},
		{name: "minimum length", fingerprint: MinLengthOrNumericFilter{MinLength: 3}.Fingerprint()},
		{name: "unique", fingerprint: UniqueFilter{}.Fingerprint()},
		{name: "english stopwords", fingerprint: EnglishStopwordFilter{}.Fingerprint()},
		{name: "english stem", fingerprint: EnglishStemFilter{}.Fingerprint()},
		{name: "russian stopwords", fingerprint: RussianStopwordFilter{}.Fingerprint()},
		{name: "russian stem", fingerprint: RussianStemFilter{}.Fingerprint()},
		{name: "multilingual stopwords", fingerprint: MultilingualStopwordFilter{}.Fingerprint()},
		{name: "multilingual stem", fingerprint: MultilingualStemFilter{}.Fingerprint()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateComponentFingerprint(test.fingerprint); err != nil {
				t.Fatalf("invalid fingerprint %q: %v", test.fingerprint, err)
			}
		})
	}
}

func TestSnowballSourceMatchesDependency(t *testing.T) {
	buildInfo, ok := debug.ReadBuildInfo()
	if !ok {
		t.Fatal("ReadBuildInfo() = false")
	}
	for _, dependency := range buildInfo.Deps {
		if dependency.Path != "github.com/kljensen/snowball" {
			continue
		}
		if got := dependency.Path + "@" + dependency.Version; got != snowballSource {
			t.Fatalf("snowball source = %q, linked dependency = %q", snowballSource, got)
		}
		return
	}
	t.Fatal("snowball dependency is unavailable in build info")
}

func TestMinLengthFingerprintUsesEffectiveValue(t *testing.T) {
	zero := MinLengthOrNumericFilter{MinLength: 0}.Fingerprint()
	negative := MinLengthOrNumericFilter{MinLength: -10}.Fingerprint()
	one := MinLengthOrNumericFilter{MinLength: 1}.Fingerprint()
	if zero != one || negative != one {
		t.Fatalf("effective fingerprints differ: zero=%q negative=%q one=%q", zero, negative, one)
	}
}

func TestNewNamedPipelineRejectsInvalidFingerprint(t *testing.T) {
	if _, err := NewNamedPipeline("", AlnumTokenizer{}); err == nil {
		t.Fatal("NewNamedPipeline(empty name) error = nil")
	}
	invalidTokenizer := &testTokenizer{fingerprint: "invalid"}
	if _, err := NewNamedPipeline("custom", invalidTokenizer); err == nil || !strings.Contains(err.Error(), "tokenizer") {
		t.Fatalf("NewNamedPipeline(invalid tokenizer) error = %v", err)
	}
	invalidFilter := &testFilter{fingerprint: "invalid"}
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
