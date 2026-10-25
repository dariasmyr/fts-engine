package vector

// Metric defines how vectors are ordered. Every supported metric returns a
// distance where a smaller value is better.
type Metric uint8

const (
	MetricCosine Metric = iota + 1
	MetricL2Squared
)

func (m Metric) String() string {
	switch m {
	case MetricCosine:
		return "cosine"
	case MetricL2Squared:
		return "l2_squared"
	default:
		return "unknown"
	}
}

// Valid reports whether the metric is supported.
func (m Metric) Valid() bool {
	return m == MetricCosine || m == MetricL2Squared
}

// Normalization describes how vectors are stored before distance evaluation.
type Normalization uint8

const (
	NormalizationNone Normalization = iota
	NormalizationUnitLength
)

func (n Normalization) String() string {
	switch n {
	case NormalizationNone:
		return "none"
	case NormalizationUnitLength:
		return "unit_length"
	default:
		return "unknown"
	}
}

// Ordinal is a dense index-local vector row number. It is not a durable
// document, chunk, or vector identity.
type Ordinal uint32
