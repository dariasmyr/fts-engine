package semanticpersist

// Generation identifies one persistence generation.
type Generation struct{ ID uint64 }

// SaveOptions configures generation validation and persistence limits.
// ExpectedGeneration is required for optimistic concurrency control.
type SaveOptions struct {
	// Limits bounds files and decoded data. Zero fields use DefaultLimits.
	Limits Limits
	// ExpectedGeneration must match CURRENT. Use a zero Generation for the first
	// save and the value returned by Load or the previous save thereafter.
	ExpectedGeneration *Generation
}

// LoadOptions configures validation limits for a persisted generation.
type LoadOptions struct {
	// Limits bounds files and decoded data. Zero fields use DefaultLimits.
	Limits Limits
}
