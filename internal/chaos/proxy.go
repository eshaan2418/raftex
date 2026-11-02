// Package chaos runs a real multi-process raftex cluster behind fault-injecting TCP proxies,
// drives it with concurrent clients, injects crashes/partitions/delays, and checks the outcome.
package chaos

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Proxy forwards one directed link (node i -> node j's raft port). It can be blocked (partition:
// existing connections are cut and new ones refused) or delayed (added per-write latency).
type Proxy struct {
	lis     net.Listener
	target  string
	blocked atomic.Bool
	delay   atomic.Int64 // ns
	mu      sync.Mutex
	conns   map[net.Conn]struct{}
}

// NewProxy listens on listen and forwards to target.
func NewProxy(listen, target string) (*Proxy, error) {
	lis, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}
	p := &Proxy{lis: lis, target: target, conns: map[net.Conn]struct{}{}}
	go p.accept()
	return p, nil
}

// Addr is the proxy's listen address.
func (p *Proxy) Addr() string { return p.lis.Addr().String() }

// Block cuts the link (true) or restores it (false).
func (p *Proxy) Block(b bool) {
	p.blocked.Store(b)
	if b {
		p.mu.Lock()
		for c := range p.conns {
			_ = c.Close()
		}
		p.mu.Unlock()
	}
}

// SetDelay adds latency to every forwarded write.
func (p *Proxy) SetDelay(d time.Duration) { p.delay.Store(int64(d)) }

// Close stops the proxy.
func (p *Proxy) Close() { _ = p.lis.Close(); p.Block(true) }

func (p *Proxy) track(c net.Conn, add bool) {
	p.mu.Lock()
	if add {
		p.conns[c] = struct{}{}
	} else {
		delete(p.conns, c)
	}
	p.mu.Unlock()
}

func (p *Proxy) accept() {
	for {
		in, err := p.lis.Accept()
		if err != nil {
			return
		}
		if p.blocked.Load() {
			_ = in.Close()
			continue
		}
		out, err := net.DialTimeout("tcp", p.target, 200*time.Millisecond)
		if err != nil {
			_ = in.Close()
			continue
		}
		p.track(in, true)
		p.track(out, true)
		go p.pipe(in, out)
		go p.pipe(out, in)
	}
}

func (p *Proxy) pipe(src, dst net.Conn) {
	defer func() {
		_ = src.Close()
		_ = dst.Close()
		p.track(src, false)
		p.track(dst, false)
	}()
	buf := make([]byte, 64<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if p.blocked.Load() {
				return
			}
			if d := p.delay.Load(); d > 0 {
				time.Sleep(time.Duration(d))
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			if err != io.EOF {
				return
			}
			return
		}
	}
}
