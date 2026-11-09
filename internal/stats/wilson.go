// Package stats has the statistics behind pass rates. Agents are random, so one
// rerun proves nothing: every comparison is N runs and an interval.
package stats

import "math"

// Z95 is the normal quantile for a two-sided 95% interval.
const Z95 = 1.959963984540054

// Wilson returns the Wilson score interval for k passes out of n. Unlike the
// normal approximation it stays inside [0, 1] and behaves at 0/n and n/n, which
// is where small agent samples usually land.
func Wilson(k, n int, z float64) (lo, hi float64) {
	if n <= 0 {
		return 0, 1
	}
	p := float64(k) / float64(n)
	nf := float64(n)
	z2 := z * z
	denom := 1 + z2/nf
	center := (p + z2/(2*nf)) / denom
	half := z * math.Sqrt(p*(1-p)/nf+z2/(4*nf*nf)) / denom
	return max(0, center-half), min(1, center+half)
}

// Overlap reports whether two intervals overlap; bisect needs them apart.
func Overlap(lo1, hi1, lo2, hi2 float64) bool { return lo1 <= hi2 && lo2 <= hi1 }
