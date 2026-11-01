package textproc

import "testing"

var pipelineBenchmarkSink []string

func BenchmarkPipelineProcess(b *testing.B) {
	benchmarks := []struct {
		name     string
		pipeline Pipeline
		input    string
	}{
		{
			name:     "english",
			pipeline: DefaultEnglishPipeline(),
			input:    "The runners were quickly searching indexed documents with repeated words and identifiers 12345",
		},
		{
			name:     "observability",
			pipeline: ObservabilityPipeline(),
			input:    "checkout-api/v2 returned io.EOF from 10.0.0.1 trace_id=01JABCDEF123 status=503",
		},
		{
			name:     "multilingual",
			pipeline: DefaultMultilingualPipeline(),
			input:    "Searching documents и поиск документов для checkout-api/v2 with mixed Unicode identifiers",
		},
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(benchmark.input)))
			for b.Loop() {
				pipelineBenchmarkSink = benchmark.pipeline.Process(benchmark.input)
			}
		})
	}
}
