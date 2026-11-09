package controller

import "context"

// Slots admits sandboxes under a per-process limit and queues the rest. A bisect
// or an eval suite can ask for hundreds of forks at once; the host has a VM and
// memory budget (tine once let 551 VMs past a 512 limit). A nil *Slots admits
// everything.
type Slots struct{ sem chan struct{} }

// NewSlots allows at most max sandboxes at once; max <= 0 means no limit.
func NewSlots(max int) *Slots {
	if max <= 0 {
		return nil
	}
	return &Slots{sem: make(chan struct{}, max)}
}

// Cap is the limit, or 0 for none.
func (s *Slots) Cap() int {
	if s == nil {
		return 0
	}
	return cap(s.sem)
}

// Acquire waits for exactly n slots. release(k) gives back k of them.
func (s *Slots) Acquire(ctx context.Context, n int) (release func(int), err error) {
	if s == nil {
		return func(int) {}, nil
	}
	if n > cap(s.sem) {
		n = cap(s.sem) // never wait for more than can ever be free
	}
	got := 0
	for got < n {
		select {
		case s.sem <- struct{}{}:
			got++
		case <-ctx.Done():
			s.release(got)
			return nil, ctx.Err()
		}
	}
	return s.release, nil
}

// AcquireUpTo waits until at least one slot is free, then takes as many as are
// free, up to n. It returns how many it took.
func (s *Slots) AcquireUpTo(ctx context.Context, n int) (int, func(int), error) {
	if s == nil {
		return n, func(int) {}, nil
	}
	select {
	case s.sem <- struct{}{}:
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
	got := 1
	for got < n {
		select {
		case s.sem <- struct{}{}:
			got++
			continue
		default:
		}
		break
	}
	return got, s.release, nil
}

func (s *Slots) release(n int) {
	for range n {
		<-s.sem
	}
}
