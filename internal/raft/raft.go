// Package raft is a from-scratch implementation of the Raft consensus algorithm (Ongaro &
// Ousterhout, 2014) written as a deterministic state machine: it never touches the network, the
// disk or a clock. The caller feeds it messages (Step), time (Tick) and proposals (Propose), and
// drains a Ready containing the state to persist, messages to send and entries to apply. This makes
// the whole cluster runnable inside a simulated network for randomised safety testing
// (sim_test.go), and keeps I/O policy (fsync, batching) in the caller.
//
// Implemented: leader election with randomized timeouts and Pre-Vote (§9.6 of the thesis), log
// replication with fast conflict backtracking, commitment only of current-term entries (§5.4.2)
// via a no-op appended on election, and check-quorum (a leader that cannot reach a majority
// steps down). Not implemented: membership changes, snapshots/log compaction, leader leases for
// local reads.
package raft

import (
	"math/rand"
	"sort"
)

// State of a node.
type State uint8

// Node states.
const (
	Follower State = iota
	PreCandidate
	Candidate
	Leader
)

func (s State) String() string {
	return [...]string{"follower", "pre-candidate", "candidate", "leader"}[s]
}

// EntryType distinguishes client data from internal no-ops.
type EntryType uint8

// Entry types.
const (
	EntryNormal EntryType = iota
	EntryNoop
)

// Entry is one log entry.
type Entry struct {
	Term  uint64
	Index uint64
	Type  EntryType
	Data  []byte
}

// HardState must be persisted before any message that depends on it is sent.
type HardState struct {
	Term   uint64
	Vote   uint64
	Commit uint64
}

// MsgType enumerates protocol messages.
type MsgType uint8

// Message types.
const (
	MsgPreVote MsgType = iota + 1
	MsgPreVoteResp
	MsgVote
	MsgVoteResp
	MsgApp
	MsgAppResp
	MsgHeartbeat
	MsgHeartbeatResp
)

// Message is a Raft RPC (requests and responses are both messages).
type Message struct {
	Type    MsgType
	From    uint64
	To      uint64
	Term    uint64
	LogTerm uint64 // vote: candidate's last log term; app: prev log term
	Index   uint64 // vote: candidate's last index; app: prev index; app resp: match or hint
	Entries []Entry
	Commit  uint64
	Reject  bool
	// RejectHint: on a rejected append, the follower's suggested next index (first index of the
	// conflicting term, or its last index + 1) so the leader skips a whole term per round trip.
	RejectHint uint64
}

// Ready is the batch of work produced by the node since the last Advance.
type Ready struct {
	HardState    HardState
	HardChanged  bool
	Entries      []Entry   // new/overwritten entries to persist (before Messages are sent)
	TruncateFrom uint64    // if > 0: persisted log must be truncated at this index first
	Messages     []Message // to send after persisting
	Committed    []Entry   // newly committed entries to apply, in order
}

// Config configures a node.
type Config struct {
	ID             uint64
	Peers          []uint64 // all voters including ID
	ElectionTicks  int      // base election timeout; actual is randomized in [ET, 2*ET)
	HeartbeatTicks int
	PreVote        bool
	CheckQuorum    bool
	Rand           *rand.Rand // deterministic randomness for simulations
	// Restored state (after a crash).
	HardState HardState
	Log       []Entry // entries 1..n
	Applied   uint64  // last index applied by the state machine before the crash
}

// Node is one Raft participant.
type Node struct {
	cfg   Config
	id    uint64
	peers []uint64
	state State
	term  uint64
	vote  uint64
	lead  uint64

	log       []Entry // log[i] has Index i+1
	commit    uint64
	applied   uint64 // highest index handed out in Ready.Committed
	persisted uint64 // highest index the caller has made durable (Ready.Entries + Advance)

	electionElapsed   int
	heartbeatElapsed  int
	randomizedTimeout int
	votes             map[uint64]bool
	// leader state
	next   map[uint64]uint64
	match  map[uint64]uint64
	active map[uint64]bool // check-quorum: peers heard from this election period

	// pending Ready
	msgs      []Message
	unstable  []Entry
	truncFrom uint64
	prevHard  HardState
	rnd       *rand.Rand
}

// New creates a node from config (and restored state).
func New(c Config) *Node {
	if c.ElectionTicks == 0 {
		c.ElectionTicks = 10
	}
	if c.HeartbeatTicks == 0 {
		c.HeartbeatTicks = 1
	}
	r := c.Rand
	if r == nil {
		r = rand.New(rand.NewSource(int64(c.ID)))
	}
	n := &Node{cfg: c, id: c.ID, peers: append([]uint64(nil), c.Peers...), rnd: r,
		term: c.HardState.Term, vote: c.HardState.Vote, commit: c.HardState.Commit,
		log: append([]Entry(nil), c.Log...), applied: c.Applied}
	sort.Slice(n.peers, func(i, j int) bool { return n.peers[i] < n.peers[j] })
	if n.commit > n.lastIndex() {
		n.commit = n.lastIndex()
	}
	if n.applied > n.commit {
		n.applied = n.commit
	}
	n.prevHard = n.hardState()
	n.persisted = n.lastIndex() // restored entries are durable
	n.becomeFollower(n.term, 0)
	return n
}

// ---- accessors

// ID returns the node id.
func (n *Node) ID() uint64 { return n.id }

// State returns the current role.
func (n *Node) State() State { return n.state }

// Term returns the current term.
func (n *Node) Term() uint64 { return n.term }

// Leader returns the known leader (0 if unknown).
func (n *Node) Leader() uint64 { return n.lead }

// Commit returns the commit index.
func (n *Node) Commit() uint64 { return n.commit }

// LastIndex returns the index of the last log entry.
func (n *Node) LastIndex() uint64 { return n.lastIndex() }

// EntryAt returns the entry at index i (1-based), ok=false if absent.
func (n *Node) EntryAt(i uint64) (Entry, bool) {
	if i == 0 || i > n.lastIndex() {
		return Entry{}, false
	}
	return n.log[i-1], true
}

func (n *Node) lastIndex() uint64 { return uint64(len(n.log)) }
func (n *Node) lastTerm() uint64 {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Term
}
func (n *Node) termAt(i uint64) uint64 {
	if i == 0 || i > n.lastIndex() {
		return 0
	}
	return n.log[i-1].Term
}
func (n *Node) quorum() int          { return len(n.peers)/2 + 1 }
func (n *Node) hardState() HardState { return HardState{Term: n.term, Vote: n.vote, Commit: n.commit} }

// ---- role transitions

func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0
	n.randomizedTimeout = n.cfg.ElectionTicks + n.rnd.Intn(n.cfg.ElectionTicks)
}

func (n *Node) becomeFollower(term, lead uint64) {
	if term > n.term {
		n.term = term
		n.vote = 0
	}
	n.state = Follower
	n.lead = lead
	n.resetElectionTimer()
}

func (n *Node) becomePreCandidate() {
	n.state = PreCandidate
	n.lead = 0
	n.votes = map[uint64]bool{n.id: true}
	n.resetElectionTimer()
	if len(n.peers) == 1 {
		n.becomeCandidate()
		return
	}
	for _, p := range n.peers {
		if p != n.id {
			// Pre-vote asks for the *next* term without incrementing ours.
			n.send(Message{Type: MsgPreVote, To: p, Term: n.term + 1, LogTerm: n.lastTerm(), Index: n.lastIndex()})
		}
	}
}

func (n *Node) becomeCandidate() {
	n.state = Candidate
	n.term++
	n.vote = n.id
	n.lead = 0
	n.votes = map[uint64]bool{n.id: true}
	n.resetElectionTimer()
	if len(n.peers) == 1 {
		n.becomeLeader()
		return
	}
	for _, p := range n.peers {
		if p != n.id {
			n.send(Message{Type: MsgVote, To: p, Term: n.term, LogTerm: n.lastTerm(), Index: n.lastIndex()})
		}
	}
}

func (n *Node) becomeLeader() {
	n.state = Leader
	n.lead = n.id
	n.heartbeatElapsed = 0
	n.electionElapsed = 0
	n.next = map[uint64]uint64{}
	n.match = map[uint64]uint64{}
	n.active = map[uint64]bool{n.id: true}
	for _, p := range n.peers {
		n.next[p] = n.lastIndex() + 1
		n.match[p] = 0
	}
	// A no-op in the new term lets the leader commit entries from earlier terms (§5.4.2).
	n.appendEntries(Entry{Type: EntryNoop})
	n.broadcastAppend()
}

// ---- input

// Tick advances logical time by one tick.
func (n *Node) Tick() {
	switch n.state {
	case Leader:
		n.heartbeatElapsed++
		n.electionElapsed++
		if n.heartbeatElapsed >= n.cfg.HeartbeatTicks {
			n.heartbeatElapsed = 0
			n.broadcastHeartbeat()
		}
		if n.cfg.CheckQuorum && n.electionElapsed >= n.cfg.ElectionTicks {
			n.electionElapsed = 0
			if len(n.active) < n.quorum() {
				// Cannot reach a majority: step down so clients find the real leader.
				n.becomeFollower(n.term, 0)
				return
			}
			n.active = map[uint64]bool{n.id: true}
		}
	default:
		n.electionElapsed++
		if n.electionElapsed >= n.randomizedTimeout {
			if n.cfg.PreVote {
				n.becomePreCandidate()
			} else {
				n.becomeCandidate()
			}
		}
	}
}

// Propose appends data to the log if this node is leader. Returns the entry's index and term,
// or ok=false if not leader.
func (n *Node) Propose(data []byte) (index, term uint64, ok bool) {
	if n.state != Leader {
		return 0, 0, false
	}
	n.appendEntries(Entry{Type: EntryNormal, Data: data})
	n.broadcastAppend()
	return n.lastIndex(), n.term, true
}

// Step processes one message.
func (n *Node) Step(m Message) {
	// Term handling.
	switch {
	case m.Type == MsgPreVote:
		// Pre-votes do not change our term.
	case m.Type == MsgPreVoteResp && !m.Reject:
		// A granted pre-vote carries the future term; ignore for term purposes.
	case m.Term > n.term:
		if m.Type == MsgVote && n.cfg.CheckQuorum && n.lead != 0 && n.electionElapsed < n.cfg.ElectionTicks {
			// Leader stickiness: we heard from a live leader recently; ignore the disruption.
			return
		}
		lead := uint64(0)
		if m.Type == MsgApp || m.Type == MsgHeartbeat {
			lead = m.From
		}
		n.becomeFollower(m.Term, lead)
	case m.Term < n.term:
		// Stale message. Answer so a stale leader/candidate learns the new term.
		if m.Type == MsgApp || m.Type == MsgHeartbeat {
			n.send(Message{Type: MsgAppResp, To: m.From, Term: n.term, Reject: true})
		} else if m.Type == MsgPreVote || m.Type == MsgVote {
			t := MsgVoteResp
			if m.Type == MsgPreVote {
				t = MsgPreVoteResp
			}
			n.send(Message{Type: t, To: m.From, Term: n.term, Reject: true})
		}
		return
	}

	switch m.Type {
	case MsgPreVote, MsgVote:
		n.handleVote(m)
	case MsgPreVoteResp:
		if n.state != PreCandidate {
			return
		}
		n.tally(m, func() { n.becomeCandidate() })
	case MsgVoteResp:
		if n.state != Candidate {
			return
		}
		n.tally(m, func() { n.becomeLeader() })
	case MsgApp:
		n.handleAppend(m)
	case MsgHeartbeat:
		n.handleHeartbeat(m)
	case MsgAppResp:
		n.handleAppendResp(m)
	case MsgHeartbeatResp:
		if n.state == Leader {
			n.active[m.From] = true
			if n.match[m.From] < n.lastIndex() {
				// The follower is behind (e.g. appends were lost). Its last index bounds where to
				// resume; never go below match+1.
				if m.Index+1 < n.next[m.From] {
					n.next[m.From] = m.Index + 1
				}
				if n.next[m.From] <= n.match[m.From] {
					n.next[m.From] = n.match[m.From] + 1
				}
				n.sendAppend(m.From)
			}
		}
	}
}

func (n *Node) logUpToDate(lastTerm, lastIndex uint64) bool {
	return lastTerm > n.lastTerm() || (lastTerm == n.lastTerm() && lastIndex >= n.lastIndex())
}

func (n *Node) handleVote(m Message) {
	respType := MsgVoteResp
	if m.Type == MsgPreVote {
		respType = MsgPreVoteResp
	}
	var grant bool
	if m.Type == MsgPreVote {
		// Grant a pre-vote if the candidate's log is up to date and we have not heard from a
		// leader within the election timeout (otherwise a partitioned node could disrupt us).
		grant = m.Term > n.term && n.logUpToDate(m.LogTerm, m.Index) &&
			(n.lead == 0 || n.electionElapsed >= n.cfg.ElectionTicks)
	} else {
		canVote := n.vote == 0 || n.vote == m.From
		grant = canVote && n.logUpToDate(m.LogTerm, m.Index)
		if grant {
			n.vote = m.From
			n.resetElectionTimer()
		}
	}
	term := n.term
	if m.Type == MsgPreVote && grant {
		term = m.Term
	}
	n.send(Message{Type: respType, To: m.From, Term: term, Reject: !grant})
}

func (n *Node) tally(m Message, win func()) {
	n.votes[m.From] = !m.Reject
	granted, rejected := 0, 0
	for _, v := range n.votes {
		if v {
			granted++
		} else {
			rejected++
		}
	}
	if granted >= n.quorum() {
		win()
	} else if rejected >= n.quorum() {
		n.becomeFollower(n.term, 0)
	}
}

func (n *Node) handleHeartbeat(m Message) {
	n.lead = m.From
	n.state = Follower
	n.electionElapsed = 0
	// Only advance commit to what we know matches the leader's log.
	if m.Commit > n.commit && m.Commit <= n.lastIndex() && n.termAt(m.Commit) == m.LogTerm && m.LogTerm != 0 {
		n.commit = m.Commit
	}
	n.send(Message{Type: MsgHeartbeatResp, To: m.From, Term: n.term, Index: n.lastIndex()})
}

func (n *Node) handleAppend(m Message) {
	n.lead = m.From
	n.state = Follower
	n.electionElapsed = 0
	if m.Index > n.lastIndex() || n.termAt(m.Index) != m.LogTerm {
		// Log doesn't contain prev entry: reject with a hint.
		hint := n.lastIndex() + 1
		if m.Index <= n.lastIndex() {
			ct := n.termAt(m.Index)
			hint = m.Index
			for hint > 1 && n.termAt(hint-1) == ct {
				hint--
			}
		}
		if hint > m.Index {
			hint = m.Index
		}
		if hint == 0 {
			hint = 1
		}
		n.send(Message{Type: MsgAppResp, To: m.From, Term: n.term, Reject: true, RejectHint: hint, Index: m.Index})
		return
	}
	// Append, truncating at the first conflict.
	for i, e := range m.Entries {
		idx := m.Index + 1 + uint64(i)
		if idx <= n.lastIndex() {
			if n.termAt(idx) == e.Term {
				continue
			}
			if idx <= n.commit {
				panic("raft: attempt to truncate committed entry") // safety violation
			}
			n.log = n.log[:idx-1]
			if n.persisted >= idx {
				n.persisted = idx - 1
			}
			if n.truncFrom == 0 || idx < n.truncFrom {
				n.truncFrom = idx
			}
			n.dropUnstableFrom(idx)
		}
		n.log = append(n.log, e)
		n.unstable = append(n.unstable, e)
	}
	last := m.Index + uint64(len(m.Entries))
	if m.Commit > n.commit {
		c := m.Commit
		if c > last {
			c = last
		}
		if c > n.commit {
			n.commit = c
		}
	}
	n.send(Message{Type: MsgAppResp, To: m.From, Term: n.term, Index: last})
}

func (n *Node) dropUnstableFrom(idx uint64) {
	k := 0
	for _, e := range n.unstable {
		if e.Index < idx {
			n.unstable[k] = e
			k++
		}
	}
	n.unstable = n.unstable[:k]
}

func (n *Node) handleAppendResp(m Message) {
	if n.state != Leader {
		return
	}
	n.active[m.From] = true
	if m.Reject {
		// Back off to the follower's hint (start of its conflicting term, or its log end), but
		// never below what it is known to match.
		nx := m.RejectHint
		if nx == 0 {
			nx = m.Index
		}
		if nx <= n.match[m.From] {
			nx = n.match[m.From] + 1
		}
		n.next[m.From] = nx
		n.sendAppend(m.From)
		return
	}
	if m.Index > n.match[m.From] {
		n.match[m.From] = m.Index
	}
	if m.Index+1 > n.next[m.From] {
		n.next[m.From] = m.Index + 1
	}
	n.maybeCommit()
	if n.next[m.From] <= n.lastIndex() {
		n.sendAppend(m.From)
	}
}

func (n *Node) maybeCommit() {
	ms := make([]uint64, 0, len(n.peers))
	for _, p := range n.peers {
		if p == n.id {
			// The leader counts toward a majority only for entries it has persisted (set in
			// Advance), which lets it replicate in parallel with its own fsync safely.
			ms = append(ms, n.persisted)
		} else {
			ms = append(ms, n.match[p])
		}
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i] > ms[j] })
	c := ms[n.quorum()-1]
	// Only entries from the current term are committed by counting replicas (§5.4.2).
	if c > n.commit && n.termAt(c) == n.term {
		n.commit = c
		n.broadcastAppendCommit()
	}
}

// ---- output helpers

func (n *Node) send(m Message) {
	m.From = n.id
	if m.Term == 0 {
		m.Term = n.term
	}
	n.msgs = append(n.msgs, m)
}

func (n *Node) appendEntries(es ...Entry) {
	for _, e := range es {
		e.Term = n.term
		e.Index = n.lastIndex() + 1
		n.log = append(n.log, e)
		n.unstable = append(n.unstable, e)
	}
}

const maxBatch = 256

func (n *Node) sendAppend(to uint64) {
	next := n.next[to]
	if next == 0 {
		next = 1
	}
	prev := next - 1
	var ents []Entry
	if next <= n.lastIndex() {
		end := n.lastIndex()
		if end-next+1 > maxBatch {
			end = next + maxBatch - 1
		}
		ents = append(ents, n.log[next-1:end]...)
	}
	n.send(Message{Type: MsgApp, To: to, Index: prev, LogTerm: n.termAt(prev), Entries: ents, Commit: n.commit})
	if len(ents) > 0 {
		// Pipelining: optimistically assume delivery; a rejection or a heartbeat response from a
		// lagging follower moves next back.
		n.next[to] = ents[len(ents)-1].Index + 1
	}
}

func (n *Node) broadcastAppend() {
	for _, p := range n.peers {
		if p != n.id {
			n.sendAppend(p)
		}
	}
}

// broadcastAppendCommit tells followers about a new commit index (piggybacks pending entries).
func (n *Node) broadcastAppendCommit() {
	for _, p := range n.peers {
		if p != n.id {
			n.sendAppend(p)
		}
	}
}

func (n *Node) broadcastHeartbeat() {
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		// Heartbeats carry the commit index only up to what the follower is known to match, so a
		// follower never commits an entry it might not have (or has from another term).
		c := n.commit
		if n.match[p] < c {
			c = n.match[p]
		}
		n.send(Message{Type: MsgHeartbeat, To: p, Commit: c, LogTerm: n.termAt(c)})
	}
}

// ---- Ready / Advance

// HasReady reports whether there is work for the caller.
func (n *Node) HasReady() bool {
	return len(n.msgs) > 0 || len(n.unstable) > 0 || n.truncFrom > 0 || n.hardState() != n.prevHard || n.commit > n.applied
}

// Ready returns pending work. The caller must persist HardState and Entries (after truncating
// at TruncateFrom), then send Messages, then apply Committed, then call Advance.
func (n *Node) Ready() Ready {
	rd := Ready{Messages: n.msgs, Entries: n.unstable, TruncateFrom: n.truncFrom}
	hs := n.hardState()
	if hs != n.prevHard {
		rd.HardState, rd.HardChanged = hs, true
	} else {
		rd.HardState = hs
	}
	if n.commit > n.applied {
		rd.Committed = append([]Entry(nil), n.log[n.applied:n.commit]...)
	}
	return rd
}

// Advance acknowledges that the last Ready has been fully handled.
func (n *Node) Advance(rd Ready) {
	n.msgs = nil
	n.unstable = nil
	n.truncFrom = 0
	n.prevHard = rd.HardState
	if len(rd.Committed) > 0 {
		n.applied = rd.Committed[len(rd.Committed)-1].Index
	}
	if len(rd.Entries) > 0 {
		last := rd.Entries[len(rd.Entries)-1].Index
		if last <= n.lastIndex() && n.termAt(last) == rd.Entries[len(rd.Entries)-1].Term {
			n.persisted = last
		}
	} else if n.persisted > n.lastIndex() {
		n.persisted = n.lastIndex()
	}
	if n.persisted > n.lastIndex() {
		n.persisted = n.lastIndex()
	}
	if n.state == Leader {
		n.maybeCommit() // our own durable entries may complete a majority (e.g. 1-node cluster)
	}
}
