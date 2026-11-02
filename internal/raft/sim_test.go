package raft

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// Deterministic cluster simulation: N nodes, a network that delays, drops, duplicates and reorders
// messages, partitions, and crash/restart from persisted state. Every step checks Raft's safety
// properties:
//   - Election Safety: at most one leader per term.
//   - Log Matching: if two logs contain an entry with the same index and term, the logs are
//     identical up to that index.
//   - Leader Completeness: a leader's log contains every entry committed in earlier terms.
//   - State Machine Safety: all nodes apply the same entry at each index (applied sequences are
//     prefixes of one history).
// After faults heal, the cluster must make progress (liveness under a stable network).

type simMsg struct {
	m       Message
	deliver int // tick at which it is delivered
}

type persisted struct {
	hs  HardState
	log []Entry
}

type simNode struct {
	n          *Node
	up         bool
	disk       persisted
	applied    [][]byte // applied normal entries (state machine history)
	appliedIdx []uint64
}

type cluster struct {
	t                  *testing.T
	rnd                *rand.Rand
	ids                []uint64
	nodes              map[uint64]*simNode
	net                []simMsg
	now                int
	part               map[uint64]int    // partition group per node (messages only flow within a group)
	leaders            map[uint64]uint64 // term -> leader id
	history            map[uint64][]byte // index -> committed data (global truth once applied anywhere)
	histTerm           map[uint64]uint64
	dropP, dupP        float64
	maxDelay           int
	proposed           int
	preVote            bool
	crashBeforePersist float64
}

func newCluster(t *testing.T, seed int64, size int) *cluster {
	return newClusterOpts(t, seed, size, true)
}

func newClusterOpts(t *testing.T, seed int64, size int, preVote bool) *cluster {
	c := &cluster{t: t, rnd: rand.New(rand.NewSource(seed)), nodes: map[uint64]*simNode{},
		part: map[uint64]int{}, leaders: map[uint64]uint64{}, history: map[uint64][]byte{},
		histTerm: map[uint64]uint64{}, maxDelay: 3, preVote: preVote}
	for i := 1; i <= size; i++ {
		c.ids = append(c.ids, uint64(i))
	}
	for _, id := range c.ids {
		c.nodes[id] = &simNode{up: true}
		c.start(id)
	}
	return c
}

func (c *cluster) start(id uint64) {
	sn := c.nodes[id]
	sn.n = New(Config{ID: id, Peers: c.ids, ElectionTicks: 10, HeartbeatTicks: 2, PreVote: c.preVote, CheckQuorum: c.preVote,
		Rand: rand.New(rand.NewSource(c.rnd.Int63())), HardState: sn.disk.hs, Log: sn.disk.log})
	sn.applied, sn.appliedIdx = nil, nil // state machine is rebuilt from the log on restart
	sn.up = true
}

func (c *cluster) crash(id uint64) { c.nodes[id].up = false }

// process drains Ready for a node: persist, send, apply, advance; checks invariants on apply.
func (c *cluster) process(id uint64) {
	sn := c.nodes[id]
	for sn.n.HasReady() {
		rd := sn.n.Ready()
		// Like the real node: a leader sends appends/heartbeats before persisting its entries, and
		// may crash in between (the entries are then lost from its disk but may exist on peers).
		leader := sn.n.State() == Leader
		if leader {
			for _, m := range rd.Messages {
				if m.Type == MsgApp || m.Type == MsgHeartbeat {
					c.enqueue(m)
				}
			}
			if len(rd.Entries) > 0 && c.rnd.Float64() < c.crashBeforePersist {
				sn.up = false
				return
			}
		}
		if rd.TruncateFrom > 0 && uint64(len(sn.disk.log)) >= rd.TruncateFrom {
			sn.disk.log = sn.disk.log[:rd.TruncateFrom-1]
		}
		for _, e := range rd.Entries {
			if e.Index <= uint64(len(sn.disk.log)) {
				sn.disk.log = sn.disk.log[:e.Index-1]
			}
			sn.disk.log = append(sn.disk.log, e)
		}
		sn.disk.hs = rd.HardState
		for _, m := range rd.Messages {
			if !leader || (m.Type != MsgApp && m.Type != MsgHeartbeat) {
				c.enqueue(m)
			}
		}
		for _, e := range rd.Committed {
			// State Machine Safety: the same index always carries the same entry.
			if prev, ok := c.history[e.Index]; ok {
				if c.histTerm[e.Index] != e.Term || !bytes.Equal(prev, e.Data) {
					c.t.Fatalf("state machine safety violated at index %d: node %d applies term %d, history term %d", e.Index, id, e.Term, c.histTerm[e.Index])
				}
			} else {
				c.history[e.Index] = e.Data
				c.histTerm[e.Index] = e.Term
			}
			if e.Type == EntryNormal {
				sn.applied = append(sn.applied, e.Data)
				sn.appliedIdx = append(sn.appliedIdx, e.Index)
			}
		}
		sn.n.Advance(rd)
		if sn.n.State() == Leader {
			if l, ok := c.leaders[sn.n.Term()]; ok && l != id {
				c.t.Fatalf("election safety violated: term %d has leaders %d and %d", sn.n.Term(), l, id)
			}
			if _, ok := c.leaders[sn.n.Term()]; !ok {
				c.leaders[sn.n.Term()] = id
				// Leader Completeness: every globally committed index is in the new leader's log
				// with the same term.
				for idx, term := range c.histTerm {
					e, ok := sn.n.EntryAt(idx)
					if !ok || e.Term != term {
						c.t.Fatalf("leader completeness violated: leader %d (term %d) lacks committed index %d", id, sn.n.Term(), idx)
					}
				}
			}
		}
	}
}

func (c *cluster) enqueue(m Message) {
	if c.rnd.Float64() < c.dropP {
		return
	}
	d := c.now + 1 + c.rnd.Intn(c.maxDelay)
	c.net = append(c.net, simMsg{m: m, deliver: d})
	if c.rnd.Float64() < c.dupP {
		c.net = append(c.net, simMsg{m: m, deliver: d + c.rnd.Intn(5)})
	}
}

func (c *cluster) step() {
	c.now++
	// Deliver due messages in random order (reordering).
	var due, rest []simMsg
	for _, sm := range c.net {
		if sm.deliver <= c.now {
			due = append(due, sm)
		} else {
			rest = append(rest, sm)
		}
	}
	c.net = rest
	c.rnd.Shuffle(len(due), func(i, j int) { due[i], due[j] = due[j], due[i] })
	for _, sm := range due {
		to := c.nodes[sm.m.To]
		if !to.up || !c.nodes[sm.m.From].up || c.part[sm.m.From] != c.part[sm.m.To] {
			continue
		}
		// Copy entries: the receiver must not alias the sender's slice.
		m := sm.m
		m.Entries = append([]Entry(nil), m.Entries...)
		to.n.Step(m)
		c.process(sm.m.To)
	}
	for _, id := range c.ids {
		if c.nodes[id].up {
			c.nodes[id].n.Tick()
			c.process(id)
		}
	}
	c.checkLogMatching()
}

func (c *cluster) checkLogMatching() {
	for i, a := range c.ids {
		for _, b := range c.ids[i+1:] {
			na, nb := c.nodes[a].n, c.nodes[b].n
			last := na.LastIndex()
			if nb.LastIndex() < last {
				last = nb.LastIndex()
			}
			// Find the highest index where terms agree; everything below must agree too.
			for idx := last; idx >= 1; idx-- {
				ea, _ := na.EntryAt(idx)
				eb, _ := nb.EntryAt(idx)
				if ea.Term == eb.Term {
					for k := idx; k >= 1; k-- {
						xa, _ := na.EntryAt(k)
						xb, _ := nb.EntryAt(k)
						if xa.Term != xb.Term || !bytes.Equal(xa.Data, xb.Data) {
							c.t.Fatalf("log matching violated between %d and %d at %d (agree at %d)", a, b, k, idx)
						}
					}
					break
				}
			}
		}
	}
}

func (c *cluster) leader() (uint64, bool) {
	var best uint64
	var bestTerm uint64
	for _, id := range c.ids {
		sn := c.nodes[id]
		if sn.up && sn.n.State() == Leader && sn.n.Term() >= bestTerm {
			best, bestTerm = id, sn.n.Term()
		}
	}
	return best, best != 0
}

func (c *cluster) propose() {
	if l, ok := c.leader(); ok {
		c.proposed++
		data := []byte(fmt.Sprintf("v%d", c.proposed))
		c.nodes[l].n.Propose(data)
		c.process(l)
	}
}

func (c *cluster) heal() {
	c.part = map[uint64]int{}
	c.dropP, c.dupP, c.crashBeforePersist = 0, 0, 0
	for _, id := range c.ids {
		if !c.nodes[id].up {
			c.start(id)
		}
	}
}

func (c *cluster) maxCommitted() int { return len(c.history) }

func TestSimSafetyUnderFaults(t *testing.T) {
	seeds := 200
	if testing.Short() {
		seeds = 30
	}
	for seed := 1; seed <= seeds; seed++ {
		size := 3
		if seed%3 == 0 {
			size = 5
		}
		// Half the seeds run plain Raft (no Pre-Vote / check-quorum) so the core voting and
		// commitment rules are exercised without the extra protection masking bugs.
		c := newClusterOpts(t, int64(seed), size, seed%2 == 0)
		c.dropP, c.dupP = 0.10, 0.05
		c.crashBeforePersist = 0.01
		for i := 0; i < 3000; i++ {
			switch r := c.rnd.Intn(100); {
			case r < 30:
				c.propose()
			case r < 32: // crash the leader (the interesting case: uncommitted tail entries)
				if l, ok := c.leader(); ok {
					c.crash(l)
				}
			case r < 33: // crash a random node
				id := c.ids[c.rnd.Intn(len(c.ids))]
				c.crash(id)
			case r < 36: // restart crashed nodes
				for _, id := range c.ids {
					if !c.nodes[id].up {
						c.start(id)
					}
				}
			case r < 37: // random partition into two groups
				for _, id := range c.ids {
					c.part[id] = c.rnd.Intn(2)
				}
			case r < 39: // heal partition
				c.part = map[uint64]int{}
			}
			c.step()
		}
		// Liveness: heal everything; the cluster must elect a leader and commit new entries.
		c.heal()
		before := c.maxCommitted()
		for i := 0; i < 600; i++ {
			if i%10 == 0 {
				c.propose()
			}
			c.step()
		}
		if c.maxCommitted() <= before {
			t.Fatalf("seed %d: no progress after healing (committed %d)", seed, c.maxCommitted())
		}
		// All up nodes converge to the same applied history.
		for _, id := range c.ids {
			sn := c.nodes[id]
			for k, idx := range sn.appliedIdx {
				if !bytes.Equal(c.history[idx], sn.applied[k]) {
					t.Fatalf("seed %d node %d applied divergent data at %d", seed, id, idx)
				}
			}
		}
	}
}

// Leader election under a clean network: a single leader emerges quickly and stays.
func TestSimStableLeader(t *testing.T) {
	c := newCluster(t, 99, 3)
	for i := 0; i < 300; i++ {
		c.step()
	}
	l, ok := c.leader()
	if !ok {
		t.Fatal("no leader")
	}
	term := c.nodes[l].n.Term()
	for i := 0; i < 1000; i++ {
		c.step()
	}
	if l2, _ := c.leader(); l2 != l || c.nodes[l].n.Term() != term {
		t.Fatalf("leader changed without faults: %d/%d -> %d/%d", l, term, l2, c.nodes[l2].n.Term())
	}
}

// Pre-Vote: a node partitioned away and rejoining must not disrupt the stable leader's term.
func TestSimPreVotePreventsDisruption(t *testing.T) {
	c := newCluster(t, 7, 3)
	for i := 0; i < 300; i++ {
		c.step()
	}
	l, _ := c.leader()
	term := c.nodes[l].n.Term()
	var iso uint64
	for _, id := range c.ids {
		if id != l {
			iso = id
			break
		}
	}
	c.part[iso] = 1
	for i := 0; i < 500; i++ { // isolated node keeps timing out
		c.step()
	}
	if c.nodes[iso].n.Term() != term {
		t.Fatalf("isolated node inflated its term: %d -> %d", term, c.nodes[iso].n.Term())
	}
	c.part = map[uint64]int{}
	for i := 0; i < 200; i++ {
		c.step()
	}
	if l2, _ := c.leader(); l2 != l || c.nodes[l].n.Term() != term {
		t.Fatal("rejoining node disrupted the leader")
	}
}

// Check-quorum: a leader cut off from the majority steps down; the majority elects a new leader.
func TestSimCheckQuorumStepDown(t *testing.T) {
	c := newCluster(t, 11, 5)
	for i := 0; i < 300; i++ {
		c.step()
	}
	l, _ := c.leader()
	c.part[l] = 1
	for i := 0; i < 300; i++ {
		c.step()
	}
	if c.nodes[l].n.State() == Leader {
		t.Fatal("isolated leader did not step down")
	}
	var others []uint64
	for _, id := range c.ids {
		if id != l && c.nodes[id].n.State() == Leader {
			others = append(others, id)
		}
	}
	sort.Slice(others, func(i, j int) bool { return others[i] < others[j] })
	if len(others) != 1 {
		t.Fatalf("majority side should have exactly one leader, got %v", others)
	}
}
