// Package blob handles payload identity: hashing, and remembering which payloads
// are already stored so a system prompt repeated across a million spans is
// written once.
//
// The bytes live in the ClickHouse payloads table (see internal/store). The design
// doc left the backend open between that and S3-compatible storage. A first
// version wrote one file per payload, and profiling showed 87% of collector CPU in
// file-system syscalls, so the table won. S3 can still slot in at the writer.
package blob

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"sync/atomic"
)

// Hash returns the hex SHA-256 of data.
func Hash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// Seen remembers hashes confirmed stored, in two generations: when the current
// one fills, it becomes the old one and the oldest is dropped. That bounds memory
// at about 2×Capacity entries while keeping hot hashes (a shared system prompt)
// resident. A miss only costs a redundant insert, which the table dedupes.
type Seen struct {
	capacity int

	mu       sync.Mutex
	cur, old map[string]struct{}

	lookups, hits atomic.Uint64
}

// NewSeen returns a cache that holds at least capacity hashes.
func NewSeen(capacity int) *Seen {
	if capacity <= 0 {
		capacity = 200_000
	}
	return &Seen{capacity: capacity, cur: map[string]struct{}{}, old: map[string]struct{}{}}
}

// Has reports whether hash is known to be stored.
func (s *Seen) Has(hash string) bool {
	s.lookups.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.cur[hash]; ok {
		s.hits.Add(1)
		return true
	}
	if _, ok := s.old[hash]; ok {
		s.cur[hash] = struct{}{} // promote
		s.hits.Add(1)
		return true
	}
	return false
}

// Add records hashes as stored. Call it only after the insert succeeded.
func (s *Seen) Add(hashes ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, h := range hashes {
		s.cur[h] = struct{}{}
	}
	if len(s.cur) >= s.capacity {
		s.old, s.cur = s.cur, map[string]struct{}{}
	}
}

// Stats is lookups and hits since start.
func (s *Seen) Stats() (lookups, hits uint64) { return s.lookups.Load(), s.hits.Load() }
