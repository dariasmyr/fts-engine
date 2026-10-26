package fts

// Pipeline implementations must return a stable descriptor whose fingerprint
// covers every declared input to token generation. The engine validates the
// descriptor's structure but cannot verify an external implementation's claim.
type Pipeline interface {
	Process(text string) []string
	Descriptor() PipelineDescriptor
}
