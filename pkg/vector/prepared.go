package vector

// Prepared is a vector produced by a compatible Spec. Its components are
// validated, canonicalized, and normalized. A value returned by PrepareInto
// remains valid while its destination buffer is unchanged. The zero value is
// invalid.
type Prepared struct {
	value         []float32
	metric        Metric
	normalization Normalization
}
