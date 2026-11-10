package stats

import (
	"math"
	"testing"
)

// Newcombe (1998), "Interval estimation for the difference between independent
// proportions", example (a): 56/70 against 48/80, method 10.
func TestNewcombeMatchesThePaper(t *testing.T) {
	d, lo, hi := Newcombe(56, 70, 48, 80, Z95)
	if math.Abs(d-0.2) > 1e-9 || math.Abs(lo-0.0524) > 5e-4 || math.Abs(hi-0.3339) > 5e-4 {
		t.Fatalf("got %.4f (%.4f, %.4f), want 0.2000 (0.0524, 0.3339)", d, lo, hi)
	}
}

// The case that made bisect give up on a real failure: the drop is real, but the
// two Wilson intervals touch.
func TestNewcombeSeparatesWhereWilsonIntervalsTouch(t *testing.T) {
	l1, u1 := Wilson(6, 20, Z95)
	l2, u2 := Wilson(0, 20, Z95)
	if !Overlap(l1, u1, l2, u2) {
		t.Fatal("the Wilson intervals of 6/20 and 0/20 should touch")
	}
	if _, lo, _ := Newcombe(6, 20, 0, 20, Z95); lo <= 0 {
		t.Fatalf("6/20 against 0/20: difference interval starts at %.3f, want above 0", lo)
	}
	if _, lo, hi := Newcombe(3, 10, 3, 10, Z95); lo >= 0 || hi <= 0 {
		t.Fatalf("equal rates: (%.3f, %.3f) should contain 0", lo, hi)
	}
}
