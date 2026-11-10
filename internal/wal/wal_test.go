package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func open(t *testing.T, dir string, seg int64) *WAL {
	t.Helper()
	w, err := Open(dir, seg)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func replayAll(t *testing.T, w *WAL) map[uint64]string {
	t.Helper()
	got := map[uint64]string{}
	if err := w.Replay(func(seq uint64, p []byte) error { got[seq] = string(p); return nil }); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestAppendReplay(t *testing.T) {
	dir := t.TempDir()
	w := open(t, dir, 1<<20)
	replayAll(t, w)
	for i := 1; i <= 100; i++ {
		seq, err := w.Append([]byte(fmt.Sprintf("rec-%d", i)), nil)
		if err != nil || seq != uint64(i) {
			t.Fatalf("append %d: seq %d err %v", i, seq, err)
		}
	}
	w.Close()

	w2 := open(t, dir, 1<<20)
	got := replayAll(t, w2)
	if len(got) != 100 || got[42] != "rec-42" {
		t.Fatalf("replayed %d records, rec 42 = %q", len(got), got[42])
	}
	// Seqs continue where they left off.
	if seq, _ := w2.Append([]byte("next"), nil); seq != 101 {
		t.Fatalf("next seq = %d, want 101", seq)
	}
}

func TestTornTailIsCut(t *testing.T) {
	dir := t.TempDir()
	w := open(t, dir, 1<<20)
	replayAll(t, w)
	w.Append([]byte("good-1"), nil)
	w.Append([]byte("good-2"), nil)
	w.Close()

	// Simulate a crash mid-write: half a record at the end of the last segment.
	segs, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	last := segs[len(segs)-1]
	f, _ := os.OpenFile(last, os.O_APPEND|os.O_WRONLY, 0)
	f.Write([]byte{0x10, 0, 0, 0, 1, 2, 3})
	f.Close()

	w2 := open(t, dir, 1<<20)
	got := replayAll(t, w2)
	if len(got) != 2 {
		t.Fatalf("got %d records, want 2", len(got))
	}
}

func TestCorruptMiddleSegmentFails(t *testing.T) {
	dir := t.TempDir()
	w := open(t, dir, 64) // tiny segments: one record each
	replayAll(t, w)
	for i := 0; i < 3; i++ {
		w.Append([]byte(fmt.Sprintf("record-%d-padding-padding", i)), nil)
	}
	w.Close()
	segs, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	if len(segs) < 3 {
		t.Fatalf("want >= 3 segments, got %d", len(segs))
	}
	data, _ := os.ReadFile(segs[0])
	data[len(data)-1] ^= 0xff // flip a payload byte: crc mismatch
	os.WriteFile(segs[0], data, 0o644)

	w2 := open(t, dir, 64)
	if err := w2.Replay(func(uint64, []byte) error { return nil }); err == nil {
		t.Fatal("want an error for a corrupt record outside the tail")
	}
}

func TestReleaseDropsOnlyFinishedSegments(t *testing.T) {
	dir := t.TempDir()
	w := open(t, dir, 64)
	replayAll(t, w)
	for i := 0; i < 5; i++ {
		w.Append([]byte(fmt.Sprintf("record-%d-padding-padding", i)), nil)
	}
	before := w.Segments()
	// Records 1 and 2 are done, 3 is still pending.
	n, err := w.Release(3)
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 || w.Segments() != before-n {
		t.Fatalf("released %d of %d segments", n, before)
	}
	w.Close()
	got := replayAll(t, open(t, dir, 64))
	if _, ok := got[2]; ok {
		t.Fatal("record 2 should have been released")
	}
	for seq := uint64(3); seq <= 5; seq++ {
		if _, ok := got[seq]; !ok {
			t.Fatalf("record %d was lost", seq)
		}
	}
}

func TestConcurrentAppendsGroupCommit(t *testing.T) {
	dir := t.TempDir()
	w := open(t, dir, 4096)
	replayAll(t, w)
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := map[uint64]bool{}
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				seq, err := w.Append([]byte(fmt.Sprintf("g%d-%d", g, i)), func(uint64) {})
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				seen[seq] = true
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()
	w.Close()
	if len(seen) != 800 {
		t.Fatalf("got %d distinct seqs, want 800", len(seen))
	}
	if got := replayAll(t, open(t, dir, 4096)); len(got) != 800 {
		t.Fatalf("replayed %d, want 800", len(got))
	}
}

func TestReleaseEmptiesLogWhenAllFlushed(t *testing.T) {
	dir := t.TempDir()
	w := open(t, dir, 1<<20)
	replayAll(t, w)
	for i := 0; i < 3; i++ {
		w.Append([]byte("rec"), nil)
	}
	// Everything is flushed: even the open segment goes.
	if _, err := w.Release(4); err != nil {
		t.Fatal(err)
	}
	if w.Segments() != 1 {
		t.Fatalf("want only a fresh open segment, have %d", w.Segments())
	}
	w.Close()

	w2 := open(t, dir, 1<<20)
	if got := replayAll(t, w2); len(got) != 0 {
		t.Fatalf("replayed %d records after a full release", len(got))
	}
	// Seqs keep counting up, so the watermark can never skip a new record.
	if seq, _ := w2.Append([]byte("new"), nil); seq != 4 {
		t.Fatalf("next seq = %d, want 4", seq)
	}
}

func TestReplaySkipsFlushedRecords(t *testing.T) {
	dir := t.TempDir()
	w := open(t, dir, 1<<20) // one segment holds everything
	replayAll(t, w)
	for i := 1; i <= 5; i++ {
		w.Append([]byte(fmt.Sprintf("rec-%d", i)), nil)
	}
	w.Release(3) // 1 and 2 reached the store; the segment can't go yet
	w.Close()

	got := replayAll(t, open(t, dir, 1<<20))
	if len(got) != 3 || got[3] != "rec-3" || got[5] != "rec-5" {
		t.Fatalf("replayed %v, want records 3..5 only", got)
	}
}

func TestReleaseBeforeReplay(t *testing.T) {
	w := open(t, t.TempDir(), 1<<20)
	if n, err := w.Release(10); n != 0 || err != nil {
		t.Fatalf("release on a log never opened: %d %v", n, err)
	}
}
