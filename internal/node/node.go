// Package node runs one raftex replica: Raft core + WAL + transport + exchange state machine +
// client API. All Raft and state-machine work happens on a single goroutine (the run loop), so
// the core needs no locks; client handlers talk to it through channels.
package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	"github.com/eshaan2418/raftex/internal/exchange"
	"github.com/eshaan2418/raftex/internal/raft"
	"github.com/eshaan2418/raftex/internal/transport"
	"github.com/eshaan2418/raftex/internal/wal"
)

// Config for a node.
type Config struct {
	ID            uint64
	RaftAddrs     map[uint64]string // where *this* node reaches each peer (may be proxies)
	RaftListen    string
	ClientAddr    string            // this node's HTTP API
	ClientAddrs   map[uint64]string // every node's HTTP API (for leader hints)
	DataDir       string
	Tick          time.Duration
	ElectionTicks int
	NoSync        bool
	Log           *slog.Logger
}

type proposal struct {
	data  []byte
	reply chan reply
}

type reply struct {
	res    exchange.Result
	err    error
	leader uint64
}

type waiter struct {
	term  uint64
	data  []byte
	reply chan reply
}

// Status is the node's externally visible state.
type Status struct {
	ID        uint64 `json:"id"`
	State     string `json:"state"`
	Term      uint64 `json:"term"`
	Leader    uint64 `json:"leader"`
	Commit    uint64 `json:"commit"`
	Applied   uint64 `json:"applied"`
	LastIndex uint64 `json:"last_index"`
	StateHash string `json:"state_hash"`
	Applies   uint64 `json:"applies"`
	Dups      uint64 `json:"dups"`
	WALSyncs  uint64 `json:"wal_syncs"`
	Sent      uint64 `json:"msgs_sent"`
	Dropped   uint64 `json:"msgs_dropped"`
	Recovered int    `json:"wal_records_recovered"`
}

// Node is a running replica.
type Node struct {
	cfg       Config
	rn        *raft.Node
	wal       *wal.WAL
	tr        *transport.Transport
	sm        *exchange.Machine
	props     chan proposal
	queries   chan chan any
	waiters   map[uint64]waiter
	status    atomic.Pointer[Status]
	recovered int
	// Applied log, kept for the chaos checker: index -> (command, result).
	appliedLog []AppliedEntry
}

// AppliedEntry is one applied normal entry.
type AppliedEntry struct {
	Index  uint64          `json:"index"`
	Term   uint64          `json:"term"`
	Cmd    json.RawMessage `json:"cmd"`
	Result exchange.Result `json:"result"`
}

// ErrNotLeader is returned to clients when this node cannot accept writes.
var ErrNotLeader = errors.New("not leader")

// Start recovers state from the WAL and starts the node.
func Start(ctx context.Context, cfg Config) (*Node, error) {
	if cfg.Tick == 0 {
		cfg.Tick = 10 * time.Millisecond
	}
	if cfg.ElectionTicks == 0 {
		cfg.ElectionTicks = 15
	}
	path := filepath.Join(cfg.DataDir, fmt.Sprintf("node%d.wal", cfg.ID))
	rec, err := wal.Recover(path)
	if err != nil {
		return nil, err
	}
	w, err := wal.Open(path)
	if err != nil {
		return nil, err
	}
	w.NoSync = cfg.NoSync
	peers := make([]uint64, 0, len(cfg.RaftAddrs)+1)
	peers = append(peers, cfg.ID)
	for id := range cfg.RaftAddrs {
		if id != cfg.ID {
			peers = append(peers, id)
		}
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i] < peers[j] })
	rn := raft.New(raft.Config{ID: cfg.ID, Peers: peers, ElectionTicks: cfg.ElectionTicks, HeartbeatTicks: 2,
		PreVote: true, CheckQuorum: true, Rand: rand.New(rand.NewSource(time.Now().UnixNano() + int64(cfg.ID))),
		HardState: rec.HardState, Log: rec.Log})
	tr, err := transport.New(cfg.ID, cfg.RaftListen, cfg.RaftAddrs, cfg.Log)
	if err != nil {
		return nil, err
	}
	n := &Node{cfg: cfg, rn: rn, wal: w, tr: tr, sm: exchange.New(), props: make(chan proposal, 4096),
		queries: make(chan chan any), waiters: map[uint64]waiter{}, recovered: rec.Records}
	tr.Start(ctx)
	go n.run(ctx)
	return n, nil
}

func (n *Node) run(ctx context.Context) {
	ticker := time.NewTicker(n.cfg.Tick)
	defer ticker.Stop()
	defer func() { _ = n.wal.Close() }()
	n.publish()
	for {
		select {
		case <-ctx.Done():
			n.failWaiters(ErrNotLeader)
			return
		case <-ticker.C:
			n.rn.Tick()
		case m := <-n.tr.Inbound():
			n.rn.Step(m)
			// Batch: drain whatever else arrived.
			for k := 0; k < 256; k++ {
				select {
				case m2 := <-n.tr.Inbound():
					n.rn.Step(m2)
					continue
				default:
				}
				break
			}
		case p := <-n.props:
			n.propose(p)
			for k := 0; k < 1024; k++ { // group proposals into one Ready / one fsync
				select {
				case p2 := <-n.props:
					n.propose(p2)
					continue
				default:
				}
				break
			}
		case q := <-n.queries:
			q <- n.snapshotForQuery()
		}
		if err := n.drain(); err != nil {
			n.cfg.Log.Error("persist failed; stopping", "err", err)
			return
		}
	}
}

func (n *Node) propose(p proposal) {
	idx, term, ok := n.rn.Propose(p.data)
	if !ok {
		p.reply <- reply{err: ErrNotLeader, leader: n.rn.Leader()}
		return
	}
	n.waiters[idx] = waiter{term: term, data: p.data, reply: p.reply}
}

// drain handles Ready: persist (fsync) -> send -> apply -> answer waiters.
func (n *Node) drain() error {
	for n.rn.HasReady() {
		rd := n.rn.Ready()
		// Raft thesis §10.2.1: the leader may send appends before persisting its own entries (it
		// only counts itself toward commitment once Advance confirms durability). Votes and
		// responses must wait for the fsync.
		leader := n.rn.State() == raft.Leader
		if leader {
			for _, m := range rd.Messages {
				if m.Type == raft.MsgApp || m.Type == raft.MsgHeartbeat {
					n.tr.Send(m)
				}
			}
		}
		if err := n.wal.Save(rd); err != nil {
			return err
		}
		for _, m := range rd.Messages {
			if !leader || (m.Type != raft.MsgApp && m.Type != raft.MsgHeartbeat) {
				n.tr.Send(m)
			}
		}
		for _, e := range rd.Committed {
			if e.Type != raft.EntryNormal {
				continue
			}
			res := n.sm.Apply(e.Index, e.Data)
			n.appliedLog = append(n.appliedLog, AppliedEntry{Index: e.Index, Term: e.Term, Cmd: e.Data, Result: res})
			if w, ok := n.waiters[e.Index]; ok {
				delete(n.waiters, e.Index)
				// The entry at this index might not be the one we proposed (we lost leadership
				// and another leader's entry took the slot). Only answer success if it is ours.
				if e.Term == w.term && string(e.Data) == string(w.data) {
					w.reply <- reply{res: res}
				} else {
					w.reply <- reply{err: ErrNotLeader, leader: n.rn.Leader()}
				}
			}
		}
		n.rn.Advance(rd)
	}
	// Proposals at indexes that can no longer commit as ours (leadership lost) are failed fast so
	// clients retry at the new leader; dedup makes the retry safe even if the entry survives.
	if n.rn.State() != raft.Leader && len(n.waiters) > 0 {
		n.failWaiters(ErrNotLeader)
	}
	n.publish()
	return nil
}

func (n *Node) failWaiters(err error) {
	for idx, w := range n.waiters {
		w.reply <- reply{err: err, leader: n.rn.Leader()}
		delete(n.waiters, idx)
	}
}

func (n *Node) publish() {
	st := Status{ID: n.cfg.ID, State: n.rn.State().String(), Term: n.rn.Term(), Leader: n.rn.Leader(),
		Commit: n.rn.Commit(), Applied: n.sm.Applied(), LastIndex: n.rn.LastIndex(),
		StateHash: fmt.Sprintf("%016x", n.sm.Hash()), Applies: n.sm.Applies, Dups: n.sm.Dups,
		WALSyncs: n.wal.Syncs, Sent: n.tr.Sent.Load(), Dropped: n.tr.Dropped.Load(), Recovered: n.recovered}
	n.status.Store(&st)
}

type queryResult struct {
	bids, asks []exchange.Level
	log        []AppliedEntry
}

func (n *Node) snapshotForQuery() any {
	b, a := n.sm.Book(10)
	return queryResult{bids: b, asks: a, log: append([]AppliedEntry(nil), n.appliedLog...)}
}

// Submit proposes a command and waits for its committed result.
func (n *Node) Submit(ctx context.Context, cmd []byte) (exchange.Result, uint64, error) {
	ch := make(chan reply, 1)
	select {
	case n.props <- proposal{data: cmd, reply: ch}:
	case <-ctx.Done():
		return exchange.Result{}, 0, ctx.Err()
	}
	select {
	case r := <-ch:
		return r.res, r.leader, r.err
	case <-ctx.Done():
		return exchange.Result{}, 0, ctx.Err()
	}
}

// Status returns the latest status snapshot.
func (n *Node) Status() Status { return *n.status.Load() }

// Serve runs the client HTTP API until ctx is done.
func (n *Node) Serve(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/command", func(w http.ResponseWriter, r *http.Request) {
		var cmd exchange.Command
		if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		data, _ := json.Marshal(cmd)
		cctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		res, leader, err := n.Submit(cctx, data)
		switch {
		case errors.Is(err, ErrNotLeader):
			w.Header().Set("X-Raftex-Leader", n.cfg.ClientAddrs[leader])
			http.Error(w, "not leader", http.StatusMisdirectedRequest)
		case err != nil:
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
		default:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(res)
		}
	})
	mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(n.Status())
	})
	mux.HandleFunc("GET /v1/book", func(w http.ResponseWriter, r *http.Request) {
		q := make(chan any, 1)
		select {
		case n.queries <- q:
		case <-r.Context().Done():
			return
		}
		res := (<-q).(queryResult)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"bids": res.bids, "asks": res.asks})
	})
	mux.HandleFunc("GET /v1/log", func(w http.ResponseWriter, r *http.Request) {
		q := make(chan any, 1)
		select {
		case n.queries <- q:
		case <-r.Context().Done():
			return
		}
		res := (<-q).(queryResult)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(res.log)
	})
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	lis, err := net.Listen("tcp", n.cfg.ClientAddr)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	if err := srv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
