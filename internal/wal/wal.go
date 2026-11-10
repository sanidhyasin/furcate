// Package wal is the collector's write-ahead log. An OTLP request is appended and
// fsynced before the collector acknowledges it, so a crash loses nothing that was
// acknowledged. Records are replayed on startup, and whole segments are deleted
// once every span they produced has reached ClickHouse.
//
// Record layout: [u32 length][u32 crc32c of payload][u64 seq][payload].
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const headerSize = 16

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// MaxRecord bounds one record, so a corrupt length can't make replay allocate GBs.
const MaxRecord = 64 << 20

type segment struct {
	id      uint64
	lastSeq uint64 // 0 while empty
}

// WAL is safe for concurrent use.
type WAL struct {
	dir     string
	segSize int64

	mu      sync.Mutex
	f       *os.File
	size    int64
	segs    []segment // oldest first; the last one is open for writing
	nextSeq uint64
	written uint64 // highest seq written to f
	rotated uint64 // highest seq fsynced by a rotation
	flushed uint64 // every seq below this has reached the store (persisted in flushedFile)

	// Lock order: syncMu before mu, never the reverse.
	syncMu sync.Mutex
	synced uint64 // highest seq known to be on disk
}

// Open opens the log in dir, creating it if needed. Call Replay before Append.
func Open(dir string, segSize int64) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if segSize <= 0 {
		segSize = 64 << 20
	}
	return &WAL{dir: dir, segSize: segSize, nextSeq: 1}, nil
}

func (w *WAL) segPath(id uint64) string {
	return filepath.Join(w.dir, fmt.Sprintf("%016d.wal", id))
}

func (w *WAL) listSegments() ([]uint64, error) {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, err
	}
	var ids []uint64
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".wal") {
			continue
		}
		var id uint64
		if _, err := fmt.Sscanf(strings.TrimSuffix(name, ".wal"), "%d", &id); err == nil {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

// flushedFile holds the low-water mark from the last Release: records below it
// already reached the store, so Replay skips them instead of inserting them twice.
const flushedFile = "flushed"

// Replay calls fn for every intact record not yet flushed, oldest first, then
// opens a fresh segment for writing. A torn record at the tail of the last segment
// (a crash mid-write) is cut off; damage anywhere else is an error.
func (w *WAL) Replay(fn func(seq uint64, payload []byte) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f != nil {
		return errors.New("wal: Replay after Append")
	}
	flushed, err := w.readFlushed()
	if err != nil {
		return err
	}
	w.flushed = flushed
	unflushed := func(seq uint64, payload []byte) error {
		if seq < flushed {
			return nil
		}
		return fn(seq, payload)
	}
	ids, err := w.listSegments()
	if err != nil {
		return err
	}
	for i, id := range ids {
		last := i == len(ids)-1
		lastSeq, goodSize, err := w.replaySegment(id, unflushed)
		if err != nil {
			if !last || !errors.Is(err, errTorn) {
				return fmt.Errorf("wal: segment %d: %w", id, err)
			}
			if err := os.Truncate(w.segPath(id), goodSize); err != nil {
				return err
			}
		}
		w.segs = append(w.segs, segment{id: id, lastSeq: lastSeq})
		if lastSeq >= w.nextSeq {
			w.nextSeq = lastSeq + 1
		}
	}
	// Seqs never go backwards, even when every segment was released.
	w.nextSeq = max(w.nextSeq, flushed)
	w.written = w.nextSeq - 1
	w.synced = w.written
	next := uint64(1)
	if len(ids) > 0 {
		next = ids[len(ids)-1] + 1
	}
	return w.openSegment(next)
}

var errTorn = errors.New("torn record")

func (w *WAL) replaySegment(id uint64, fn func(uint64, []byte) error) (lastSeq uint64, goodSize int64, err error) {
	f, err := os.Open(w.segPath(id))
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	var hdr [headerSize]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			if err == io.EOF {
				return lastSeq, goodSize, nil
			}
			return lastSeq, goodSize, errTorn
		}
		n := binary.LittleEndian.Uint32(hdr[0:4])
		sum := binary.LittleEndian.Uint32(hdr[4:8])
		seq := binary.LittleEndian.Uint64(hdr[8:16])
		if n > MaxRecord {
			return lastSeq, goodSize, errTorn
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			return lastSeq, goodSize, errTorn
		}
		if crc32.Checksum(payload, crcTable) != sum {
			return lastSeq, goodSize, errTorn
		}
		if err := fn(seq, payload); err != nil {
			return lastSeq, goodSize, err
		}
		lastSeq = seq
		goodSize += headerSize + int64(n)
	}
}

// openSegment must be called with w.mu held.
func (w *WAL) openSegment(id uint64) error {
	f, err := os.OpenFile(w.segPath(id), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	w.f = f
	w.size = st.Size()
	if n := len(w.segs); n == 0 || w.segs[n-1].id != id {
		w.segs = append(w.segs, segment{id: id})
	}
	return nil
}

// Append writes payload as one record and returns once it is on disk.
// Concurrent appends share fsyncs (group commit). If onSeq is not nil it is called
// with the record's seq while the log is still locked, so a caller can register
// the seq before any Release can run past it. It must not call back into the WAL.
func (w *WAL) Append(payload []byte, onSeq func(seq uint64)) (uint64, error) {
	if len(payload) > MaxRecord {
		return 0, fmt.Errorf("wal: record of %d bytes exceeds %d", len(payload), MaxRecord)
	}
	buf := make([]byte, headerSize+len(payload))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(buf[4:8], crc32.Checksum(payload, crcTable))
	copy(buf[headerSize:], payload)

	w.mu.Lock()
	if w.f == nil {
		w.mu.Unlock()
		return 0, errors.New("wal: not open (call Replay first)")
	}
	if w.size > 0 && w.size+int64(len(buf)) > w.segSize {
		if err := w.rotate(); err != nil {
			w.mu.Unlock()
			return 0, err
		}
	}
	seq := w.nextSeq
	binary.LittleEndian.PutUint64(buf[8:16], seq)
	if _, err := w.f.Write(buf); err != nil {
		w.mu.Unlock()
		return 0, err
	}
	w.nextSeq++
	w.size += int64(len(buf))
	w.written = seq
	w.segs[len(w.segs)-1].lastSeq = seq
	if onSeq != nil {
		onSeq(seq)
	}
	w.mu.Unlock()

	return seq, w.syncThrough(seq)
}

// rotate syncs and closes the current segment and opens the next. w.mu held.
func (w *WAL) rotate() error {
	if err := w.f.Sync(); err != nil {
		return err
	}
	w.rotated = w.written
	if err := w.f.Close(); err != nil {
		return err
	}
	return w.openSegment(w.segs[len(w.segs)-1].id + 1)
}

// syncThrough returns once every record up to seq is on disk. The first waiter
// fsyncs everything written so far; the ones queued behind it find their seq
// already covered and return without a syscall.
func (w *WAL) syncThrough(seq uint64) error {
	w.syncMu.Lock()
	defer w.syncMu.Unlock()
	if w.synced >= seq {
		return nil
	}
	w.mu.Lock()
	f, upTo, rotated := w.f, w.written, w.rotated
	w.mu.Unlock()
	if rotated >= seq {
		w.synced = max(w.synced, rotated)
		return nil
	}
	if err := f.Sync(); err != nil {
		// A rotation closed f after syncing it, so everything in it is durable.
		if !errors.Is(err, os.ErrClosed) {
			return err
		}
	}
	w.synced = max(w.synced, upTo)
	return nil
}

// Release deletes every segment whose records all have seq < lowWater, meaning
// nothing in it is still waiting to reach ClickHouse. A fully flushed open segment
// is rotated out first, so a quiet collector ends up with an empty log. It then
// records lowWater, so a restart skips records that were already flushed.
func (w *WAL) Release(lowWater uint64) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.segs) == 0 { // never replayed, so never opened: nothing to release
		return 0, nil
	}
	if cur := w.segs[len(w.segs)-1]; w.f != nil && cur.lastSeq != 0 && cur.lastSeq < lowWater {
		if err := w.rotate(); err != nil {
			return 0, err
		}
	}
	removed := 0
	for len(w.segs) > 1 { // never the open segment
		s := w.segs[0]
		if s.lastSeq != 0 && s.lastSeq >= lowWater {
			break
		}
		if err := os.Remove(w.segPath(s.id)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return removed, err
		}
		w.segs = w.segs[1:]
		removed++
	}
	if lowWater > w.flushed {
		if err := w.writeFlushed(lowWater); err != nil {
			return removed, err
		}
		w.flushed = lowWater
	}
	return removed, nil
}

func (w *WAL) readFlushed() (uint64, error) {
	b, err := os.ReadFile(filepath.Join(w.dir, flushedFile))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("wal: bad %s file: %w", flushedFile, err)
	}
	return v, nil
}

// writeFlushed replaces the watermark atomically: write, fsync, rename.
func (w *WAL) writeFlushed(v uint64) error {
	tmp := filepath.Join(w.dir, flushedFile+".tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(strconv.FormatUint(v, 10) + "\n"); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(w.dir, flushedFile))
}

// NextSeq is the seq the next Append will get.
func (w *WAL) NextSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.nextSeq
}

// Segments reports how many segment files exist, the open one included.
func (w *WAL) Segments() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.segs)
}

// Close syncs and closes the open segment.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Sync()
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	w.f = nil
	return err
}
