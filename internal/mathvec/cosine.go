package mathvec

// Cosine similarity for non-zero vectors (L2-normalized is recommended).
func Cosine(a, b []float64) float64 {
	if len(a) == 0 || len(a) != len(b) {
		return 0
	}
	var d float64
	for i := range a {
		d += a[i] * b[i]
	}
	return d
}
