// Package transport carries Raft messages between nodes over TCP. Each peer gets one outbound
// goroutine with a bounded queue and a persistent gob stream; messages to an unreachable peer are
// dropped (Raft tolerates loss and retransmits). Inbound connections are accepted on the node's
// raft address and decoded into a channel.
package transport

import (
	"context"
	"encoding/gob"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eshaan2418/raftex/internal/raft"
)

// Transport sends and receives Raft messages.
type Transport struct {
	self                    uint64
	peers                   map[uint64]string // id -> address (possibly a fault-injection proxy)
	in                      chan raft.Message
	queues                  map[uint64]chan raft.Message
	log                     *slog.Logger
	lis                     net.Listener
	wg                      sync.WaitGroup
	Sent, Dropped, Received atomic.Uint64
}

// New creates a transport; call Start.
func New(self uint64, listen string, peers map[uint64]string, log *slog.Logger) (*Transport, error) {
	lis, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}
	t := &Transport{self: self, peers: peers, in: make(chan raft.Message, 4096),
		queues: map[uint64]chan raft.Message{}, log: log, lis: lis}
	for id := range peers {
		if id != self {
			t.queues[id] = make(chan raft.Message, 4096)
		}
	}
	return t, nil
}

// Inbound returns the channel of received messages.
func (t *Transport) Inbound() <-chan raft.Message { return t.in }

// Send enqueues m for its destination (non-blocking; drops when the queue is full).
func (t *Transport) Send(m raft.Message) {
	q, ok := t.queues[m.To]
	if !ok {
		return
	}
	select {
	case q <- m:
	default:
		t.Dropped.Add(1)
	}
}

// Start runs the accept loop and one sender per peer until ctx is done.
func (t *Transport) Start(ctx context.Context) {
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		<-ctx.Done()
		_ = t.lis.Close()
	}()
	t.wg.Add(1)
	go t.acceptLoop(ctx)
	for id, q := range t.queues {
		t.wg.Add(1)
		go t.sender(ctx, t.peers[id], q)
	}
}

// Wait blocks until all goroutines exit.
func (t *Transport) Wait() { t.wg.Wait() }

func (t *Transport) acceptLoop(ctx context.Context) {
	defer t.wg.Done()
	for {
		c, err := t.lis.Accept()
		if err != nil {
			return
		}
		go func() {
			defer func() { _ = c.Close() }()
			go func() {
				<-ctx.Done()
				_ = c.Close()
			}()
			dec := gob.NewDecoder(c)
			for {
				var m raft.Message
				if err := dec.Decode(&m); err != nil {
					return
				}
				t.Received.Add(1)
				select {
				case t.in <- m:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
}

func (t *Transport) sender(ctx context.Context, addr string, q chan raft.Message) {
	defer t.wg.Done()
	var conn net.Conn
	var enc *gob.Encoder
	backoff := 20 * time.Millisecond
	for {
		var m raft.Message
		select {
		case m = <-q:
		case <-ctx.Done():
			if conn != nil {
				_ = conn.Close()
			}
			return
		}
		if conn == nil {
			d := net.Dialer{Timeout: 200 * time.Millisecond}
			c, err := d.DialContext(ctx, "tcp", addr)
			if err != nil {
				t.Dropped.Add(1)
				// Drain briefly so a dead peer doesn't build an ever-growing backlog of stale msgs.
				select {
				case <-time.After(backoff):
				case <-ctx.Done():
					return
				}
				if backoff < 500*time.Millisecond {
					backoff *= 2
				}
				continue
			}
			if tc, ok := c.(*net.TCPConn); ok {
				_ = tc.SetNoDelay(true)
			}
			conn, enc, backoff = c, gob.NewEncoder(c), 20*time.Millisecond
		}
		_ = conn.SetWriteDeadline(time.Now().Add(500 * time.Millisecond))
		if err := enc.Encode(&m); err != nil {
			t.Dropped.Add(1)
			_ = conn.Close()
			conn, enc = nil, nil
			continue
		}
		t.Sent.Add(1)
	}
}
