package raft

import (
	"math/rand"
	"testing"
)

func elect(t *testing.T, n *Node, voters ...uint64) {
	t.Helper()
	for n.State() != Candidate {
		n.Tick()
	}
	for _, v := range voters {
		n.Step(Message{Type: MsgVoteResp, From: v, To: n.ID(), Term: n.Term()})
	}
	if n.State() != Leader {
		t.Fatalf("not leader: %v", n.State())
	}
	n.Advance(n.Ready())
}

// Figure 8 of the Raft paper: a leader must not commit an entry from a previous term just because
// it is stored on a majority; only entries from its own term are committed by counting replicas.
func TestFigure8NoCommitOfOldTermByCounting(t *testing.T) {
	n := New(Config{ID: 1, Peers: []uint64{1, 2, 3, 4, 5}, ElectionTicks: 10, Rand: rand.New(rand.NewSource(1)),
		HardState: HardState{Term: 3}, Log: []Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}})
	elect(t, n, 2, 3) // term 4; appends a no-op at index 3
	if n.Term() != 4 || n.LastIndex() != 3 {
		t.Fatalf("term %d last %d", n.Term(), n.LastIndex())
	}
	// S2 and S3 acknowledge index 2 (the term-2 entry): a majority (with S1) holds it.
	n.Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: 4, Index: 2})
	n.Step(Message{Type: MsgAppResp, From: 3, To: 1, Term: 4, Index: 2})
	if n.Commit() != 0 {
		t.Fatalf("committed old-term entry by counting replicas (commit=%d)", n.Commit())
	}
	// Once the current-term no-op is on a majority, everything up to it commits.
	n.Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: 4, Index: 3})
	n.Step(Message{Type: MsgAppResp, From: 3, To: 1, Term: 4, Index: 3})
	if n.Commit() != 3 {
		t.Fatalf("commit=%d want 3", n.Commit())
	}
}

func TestVoteRequiresUpToDateLog(t *testing.T) {
	n := New(Config{ID: 1, Peers: []uint64{1, 2, 3}, Rand: rand.New(rand.NewSource(1)),
		HardState: HardState{Term: 5}, Log: []Entry{{Term: 5, Index: 1}, {Term: 5, Index: 2}}})
	// Candidate with a shorter log in the same last term: rejected.
	n.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 6, LogTerm: 5, Index: 1})
	rd := n.Ready()
	if len(rd.Messages) != 1 || !rd.Messages[0].Reject {
		t.Fatalf("%+v", rd.Messages)
	}
	n.Advance(rd)
	// Candidate with a higher last term: granted, and the vote is recorded (persisted via Ready).
	n.Step(Message{Type: MsgVote, From: 3, To: 1, Term: 7, LogTerm: 6, Index: 1})
	rd = n.Ready()
	if rd.Messages[0].Reject || !rd.HardChanged || rd.HardState.Vote != 3 || rd.HardState.Term != 7 {
		t.Fatalf("%+v %+v", rd.Messages, rd.HardState)
	}
	n.Advance(rd)
	// One vote per term.
	n.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 7, LogTerm: 9, Index: 9})
	rd = n.Ready()
	if !rd.Messages[0].Reject {
		t.Fatal("second vote in the same term granted")
	}
}

func TestFollowerTruncatesConflictingTail(t *testing.T) {
	n := New(Config{ID: 2, Peers: []uint64{1, 2, 3}, Rand: rand.New(rand.NewSource(1)),
		HardState: HardState{Term: 2}, Log: []Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 2, Index: 3}}})
	n.Step(Message{Type: MsgApp, From: 1, To: 2, Term: 3, Index: 1, LogTerm: 1,
		Entries: []Entry{{Term: 3, Index: 2, Data: []byte("x")}}, Commit: 2})
	rd := n.Ready()
	if rd.TruncateFrom != 2 || len(rd.Entries) != 1 || n.LastIndex() != 2 || n.Commit() != 2 {
		t.Fatalf("trunc %d entries %d last %d commit %d", rd.TruncateFrom, len(rd.Entries), n.LastIndex(), n.Commit())
	}
	if len(rd.Committed) != 2 || string(rd.Committed[1].Data) != "x" {
		t.Fatalf("%+v", rd.Committed)
	}
}

func TestRejectHintSkipsWholeTerm(t *testing.T) {
	log := []Entry{{Term: 1, Index: 1}}
	for i := uint64(2); i <= 10; i++ {
		log = append(log, Entry{Term: 2, Index: i})
	}
	n := New(Config{ID: 2, Peers: []uint64{1, 2, 3}, Rand: rand.New(rand.NewSource(1)), HardState: HardState{Term: 2}, Log: log})
	n.Step(Message{Type: MsgApp, From: 1, To: 2, Term: 3, Index: 10, LogTerm: 3})
	rd := n.Ready()
	if !rd.Messages[0].Reject || rd.Messages[0].RejectHint != 2 {
		t.Fatalf("%+v", rd.Messages[0])
	}
}

// The leader sends appends before persisting its own entries; it must not count itself toward a
// majority until the caller confirms durability (Advance). Otherwise leader + one follower could
// "commit" an entry that only one disk holds, and a leader crash could lose it.
func TestLeaderCountsItselfOnlyAfterPersist(t *testing.T) {
	n := New(Config{ID: 1, Peers: []uint64{1, 2, 3}, ElectionTicks: 10, Rand: rand.New(rand.NewSource(1))})
	elect(t, n, 2) // no-op at index 1 persisted by elect's Advance
	n.Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: n.Term(), Index: 1})
	n.Advance(n.Ready())
	if n.Commit() != 1 {
		t.Fatalf("commit=%d", n.Commit())
	}
	idx, _, _ := n.Propose([]byte("E"))
	rd := n.Ready() // contains entry E (not yet persisted) and the MsgApp carrying it
	// Follower 2 persists and acknowledges E before the leader's own fsync finishes.
	n.Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: n.Term(), Index: idx})
	if n.Commit() >= idx {
		t.Fatalf("committed index %d before the leader persisted it", idx)
	}
	n.Advance(rd) // leader's fsync done
	if n.Commit() != idx {
		t.Fatalf("commit=%d want %d after persist", n.Commit(), idx)
	}
}
