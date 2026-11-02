package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/eshaan2418/raftex/internal/raft"
)

func TestSaveRecoverWithTruncation(t *testing.T) {
	p := filepath.Join(t.TempDir(), "n.wal")
	w, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	e := func(term, idx uint64, d string) raft.Entry {
		return raft.Entry{Term: term, Index: idx, Data: []byte(d)}
	}
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(w.Save(raft.Ready{HardState: raft.HardState{Term: 1, Vote: 2}, HardChanged: true, Entries: []raft.Entry{e(1, 1, "a"), e(1, 2, "b"), e(1, 3, "c")}}))
	// Conflict: truncate at 2 and append term-2 entries.
	must(w.Save(raft.Ready{HardState: raft.HardState{Term: 2, Vote: 3, Commit: 1}, HardChanged: true, TruncateFrom: 2, Entries: []raft.Entry{e(2, 2, "B"), e(2, 3, "C"), e(2, 4, "D")}}))
	must(w.Close())
	r, err := Recover(p)
	if err != nil {
		t.Fatal(err)
	}
	if r.HardState != (raft.HardState{Term: 2, Vote: 3, Commit: 1}) || len(r.Log) != 4 || string(r.Log[1].Data) != "B" || r.Log[3].Term != 2 || r.TornTail {
		t.Fatalf("%+v %+v", r.HardState, r.Log)
	}
}

func TestTornTailIsIgnored(t *testing.T) {
	p := filepath.Join(t.TempDir(), "n.wal")
	w, _ := Open(p)
	for i := uint64(1); i <= 10; i++ {
		_ = w.Save(raft.Ready{HardState: raft.HardState{Term: 1}, HardChanged: true, Entries: []raft.Entry{{Term: 1, Index: i, Data: []byte("payload")}}})
	}
	_ = w.Close()
	st, _ := os.Stat(p)
	_ = os.Truncate(p, st.Size()-5)
	r, err := Recover(p)
	if err != nil || !r.TornTail || len(r.Log) != 10 {
		// The torn record is the final hard-state record; all 10 entries survive.
		t.Fatalf("err=%v torn=%v len=%d", err, r.TornTail, len(r.Log))
	}
	// Corrupt a byte in the middle: recovery stops at that record.
	b, _ := os.ReadFile(p)
	b[len(b)/2] ^= 0xFF
	_ = os.WriteFile(p, b, 0o644)
	r, _ = Recover(p)
	if !r.TornTail || len(r.Log) >= 10 {
		t.Fatalf("corruption not detected: len=%d", len(r.Log))
	}
}

// Recovery must never panic or return an inconsistent log on arbitrary file contents.
func FuzzWALRecover(f *testing.F) {
	p0 := filepath.Join(f.TempDir(), "seed.wal")
	w, _ := Open(p0)
	_ = w.Save(raft.Ready{HardState: raft.HardState{Term: 1}, HardChanged: true, Entries: []raft.Entry{{Term: 1, Index: 1, Data: []byte("x")}}})
	_ = w.Close()
	seed, _ := os.ReadFile(p0)
	f.Add(seed)
	f.Fuzz(func(t *testing.T, b []byte) {
		p := filepath.Join(t.TempDir(), "f.wal")
		_ = os.WriteFile(p, b, 0o644)
		r, err := Recover(p)
		if err != nil {
			return
		}
		for i, e := range r.Log {
			if e.Index != uint64(i+1) {
				t.Fatalf("non-contiguous recovered log at %d", i)
			}
		}
	})
}
