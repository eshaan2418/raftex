// Package wal persists Raft state: hard state (term, vote, commit), log entries and truncations,
// as CRC-framed records appended to one file. Recovery replays the records to rebuild the log and
// hard state, and stops cleanly at a torn or corrupt tail (a crash mid-write).
//
// Record: u32 length | u32 CRC32-C(body) | body, where body = u8 kind | payload.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"

	"github.com/eshaan2418/raftex/internal/raft"
)

const (
	kindHard     = 1
	kindEntry    = 2
	kindTruncate = 3
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// WAL is an append-only Raft log file.
type WAL struct {
	f        *os.File
	w        *bufio.Writer
	Syncs    uint64
	NoSync   bool // benchmarks/tests only: skip fsync
	buf      []byte
	lastHard raft.HardState
}

// Open opens (creating if needed) the WAL at path for appending.
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &WAL{f: f, w: bufio.NewWriterSize(f, 1<<20)}, nil
}

func (l *WAL) record(kind byte, payload []byte) error {
	body := append(l.buf[:0], kind)
	body = append(body, payload...)
	l.buf = body
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:], uint32(len(body)))
	binary.LittleEndian.PutUint32(hdr[4:], crc32.Checksum(body, castagnoli))
	if _, err := l.w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := l.w.Write(body)
	return err
}

// Save appends one Ready's worth of state and makes it durable (one fsync per call: group
// commit of everything the Ready contains). A change of only the commit index is written but not
// fsynced: Raft does not require the commit index to be durable (it is re-learned from the
// leader), so it rides along with the next fsync.
func (l *WAL) Save(rd raft.Ready) error {
	needSync := len(rd.Entries) > 0 || rd.TruncateFrom > 0 ||
		(rd.HardChanged && (rd.HardState.Term != l.lastHard.Term || rd.HardState.Vote != l.lastHard.Vote))
	if rd.TruncateFrom > 0 {
		var p [8]byte
		binary.LittleEndian.PutUint64(p[:], rd.TruncateFrom)
		if err := l.record(kindTruncate, p[:]); err != nil {
			return err
		}
	}
	for _, e := range rd.Entries {
		p := make([]byte, 17+len(e.Data))
		binary.LittleEndian.PutUint64(p[0:], e.Term)
		binary.LittleEndian.PutUint64(p[8:], e.Index)
		p[16] = byte(e.Type)
		copy(p[17:], e.Data)
		if err := l.record(kindEntry, p); err != nil {
			return err
		}
	}
	if rd.HardChanged || len(rd.Entries) > 0 || rd.TruncateFrom > 0 {
		var p [24]byte
		binary.LittleEndian.PutUint64(p[0:], rd.HardState.Term)
		binary.LittleEndian.PutUint64(p[8:], rd.HardState.Vote)
		binary.LittleEndian.PutUint64(p[16:], rd.HardState.Commit)
		if err := l.record(kindHard, p[:]); err != nil {
			return err
		}
	} else {
		return nil
	}
	l.lastHard = rd.HardState
	if !needSync {
		return nil
	}
	if err := l.w.Flush(); err != nil {
		return err
	}
	if l.NoSync {
		return nil
	}
	l.Syncs++
	return l.f.Sync()
}

// Close flushes and closes the file.
func (l *WAL) Close() error {
	err := l.w.Flush()
	return errors.Join(err, l.f.Close())
}

// Recovered is the state rebuilt from a WAL.
type Recovered struct {
	HardState raft.HardState
	Log       []raft.Entry
	Records   int
	TornTail  bool
}

// Recover reads a WAL file (missing file = empty state).
func Recover(path string) (Recovered, error) {
	var r Recovered
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return r, nil
	}
	if err != nil {
		return r, err
	}
	defer f.Close()
	br := bufio.NewReaderSize(f, 1<<20)
	for {
		var hdr [8]byte
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			if !errors.Is(err, io.EOF) {
				r.TornTail = true
			}
			return r, nil
		}
		n := binary.LittleEndian.Uint32(hdr[0:])
		if n == 0 || n > 64<<20 {
			r.TornTail = true
			return r, nil
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(br, body); err != nil {
			r.TornTail = true
			return r, nil
		}
		if crc32.Checksum(body, castagnoli) != binary.LittleEndian.Uint32(hdr[4:]) {
			r.TornTail = true
			return r, nil
		}
		r.Records++
		p := body[1:]
		switch body[0] {
		case kindHard:
			if len(p) != 24 {
				r.TornTail = true
				return r, nil
			}
			r.HardState = raft.HardState{Term: binary.LittleEndian.Uint64(p[0:]),
				Vote: binary.LittleEndian.Uint64(p[8:]), Commit: binary.LittleEndian.Uint64(p[16:])}
		case kindEntry:
			if len(p) < 17 {
				r.TornTail = true
				return r, nil
			}
			e := raft.Entry{Term: binary.LittleEndian.Uint64(p[0:]), Index: binary.LittleEndian.Uint64(p[8:]),
				Type: raft.EntryType(p[16]), Data: append([]byte(nil), p[17:]...)}
			if e.Index <= uint64(len(r.Log)) {
				r.Log = r.Log[:e.Index-1]
			}
			if e.Index != uint64(len(r.Log))+1 {
				return r, errors.New("wal: non-contiguous entry index")
			}
			r.Log = append(r.Log, e)
		case kindTruncate:
			idx := binary.LittleEndian.Uint64(p)
			if idx >= 1 && idx <= uint64(len(r.Log)) {
				r.Log = r.Log[:idx-1]
			}
		}
	}
}
