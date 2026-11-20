//go:build !arm64

package vector

func dotFloat64(a, b []float32) float64 {
	// Callers validate dimensions before reaching this hot path.
	var sum0, sum1, sum2, sum3 float64
	i := 0
	for ; i+3 < len(a); i += 4 {
		sum0 += float64(a[i]) * float64(b[i])
		sum1 += float64(a[i+1]) * float64(b[i+1])
		sum2 += float64(a[i+2]) * float64(b[i+2])
		sum3 += float64(a[i+3]) * float64(b[i+3])
	}
	sum := (sum0 + sum1) + (sum2 + sum3)
	for ; i < len(a); i++ {
		sum += float64(a[i]) * float64(b[i])
	}
	return sum
}
