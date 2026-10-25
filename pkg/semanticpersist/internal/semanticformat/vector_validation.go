package semanticformat

import (
	"errors"
	"math"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

const (
	// tolerance for unit normalization check. This is a loose tolerance to account for floating point rounding errors.
	// 0.0001
	unitNormTolerance = 1e-4
	// negative zero is not allowed in vectors, as it can cause issues with certain calculations and comparisons.
	// -0.0 is mathematically equivalent to 0.0, but it has a different bit representation, which can lead to unexpected behavior in some cases.
	// The bit representation of -0.0 in IEEE 754 floating point format is 0x80000000 for float32 and 0x8000000000000000 for float64.
	negativeZeroBits = uint32(1) << 31
)

func validatePreparedRow(calculator vector.Calculator, value []float32) error {
	if err := calculator.Validate(value); err != nil {
		return err
	}
	if calculator.Normalization() == vector.NormalizationUnitLength {
		var normSquared float64
		for _, component := range value {
			normSquared += float64(component) * float64(component)
		}
		if math.Abs(normSquared-1) > unitNormTolerance {
			return errors.New("vector is not unit normalized")
		}
	}
	for _, component := range value {
		if math.Float32bits(component) == negativeZeroBits {
			return errors.New("vector contains negative zero")
		}
	}
	return nil
}
