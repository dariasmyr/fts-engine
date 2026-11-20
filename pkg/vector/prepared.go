package vector

// Prepared is an immutable vector owned by a compatible Spec. Its components
// are validated, canonicalized, and normalized. The zero value is invalid.
type Prepared struct {
	value         []float32
	metric        Metric
	normalization Normalization
}
