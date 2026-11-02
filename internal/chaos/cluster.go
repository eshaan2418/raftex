package chaos

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/eshaan2418/raftex/internal/node"
)

// Cluster is N raftexd processes wired through a proxy per directed link.
type Cluster struct {
	Bin      string
	Dir      string
	N        int
	Base     int // port base
	NoSync   bool
	procs    map[uint64]*exec.Cmd
	proxies  map[[2]uint64]*Proxy // (from, to)
	http     *http.Client
	LogFiles map[uint64]*os.File
}

func (c *Cluster) raftAddr(id uint64) string   { return fmt.Sprintf("127.0.0.1:%d", c.Base+10+int(id)) }
func (c *Cluster) clientAddr(id uint64) string { return fmt.Sprintf("127.0.0.1:%d", c.Base+20+int(id)) }
func (c *Cluster) proxyAddr(from, to uint64) string {
	return fmt.Sprintf("127.0.0.1:%d", c.Base+100+10*int(from)+int(to))
}

// IDs returns node ids.
func (c *Cluster) IDs() []uint64 {
	ids := make([]uint64, c.N)
	for i := range ids {
		ids[i] = uint64(i + 1)
	}
	return ids
}

// ClientAddrs returns all client API addresses.
func (c *Cluster) ClientAddrs() []string {
	ids := c.IDs()
	a := make([]string, 0, len(ids))
	for _, id := range ids {
		a = append(a, c.clientAddr(id))
	}
	return a
}

// Start creates proxies and launches every node. It refuses to start if any node port is already
// taken (e.g. by an orphaned node from an aborted run), which would silently mix clusters.
func (c *Cluster) Start() error {
	for _, id := range c.IDs() {
		for _, a := range []string{c.raftAddr(id), c.clientAddr(id)} {
			l, err := net.Listen("tcp", a)
			if err != nil {
				return fmt.Errorf("port %s already in use (orphaned node?): %w", a, err)
			}
			_ = l.Close()
		}
	}
	c.procs = map[uint64]*exec.Cmd{}
	c.proxies = map[[2]uint64]*Proxy{}
	c.LogFiles = map[uint64]*os.File{}
	c.http = &http.Client{Timeout: 500 * time.Millisecond}
	for _, f := range c.IDs() {
		for _, t := range c.IDs() {
			if f == t {
				continue
			}
			p, err := NewProxy(c.proxyAddr(f, t), c.raftAddr(t))
			if err != nil {
				return err
			}
			c.proxies[[2]uint64{f, t}] = p
		}
	}
	for _, id := range c.IDs() {
		if err := c.StartNode(id); err != nil {
			return err
		}
	}
	return nil
}

// StartNode (re)starts one node process; it recovers from its WAL.
func (c *Cluster) StartNode(id uint64) error {
	ids := c.IDs()
	peers := make([]string, 0, len(ids))
	clients := make([]string, 0, len(ids))
	for _, p := range ids {
		addr := c.raftAddr(p)
		if p != id {
			addr = c.proxyAddr(id, p)
		}
		peers = append(peers, fmt.Sprintf("%d=%s", p, addr))
		clients = append(clients, fmt.Sprintf("%d=%s", p, c.clientAddr(p)))
	}
	args := []string{"-id", fmt.Sprint(id), "-raft", c.raftAddr(id), "-client", c.clientAddr(id),
		"-peers", strings.Join(peers, ","), "-clients", strings.Join(clients, ","), "-data", c.Dir}
	if c.NoSync {
		args = append(args, "-nosync")
	}
	cmd := exec.Command(c.Bin, args...)
	lf, ok := c.LogFiles[id]
	if !ok {
		var err error
		lf, err = os.OpenFile(filepath.Join(c.Dir, fmt.Sprintf("node%d.log", id)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		c.LogFiles[id] = lf
	}
	cmd.Stdout, cmd.Stderr = lf, lf
	if err := cmd.Start(); err != nil {
		return err
	}
	c.procs[id] = cmd
	return nil
}

// Kill sends SIGKILL (no graceful shutdown: tests crash recovery from the WAL).
func (c *Cluster) Kill(id uint64) {
	if p, ok := c.procs[id]; ok && p.Process != nil {
		_ = p.Process.Signal(syscall.SIGKILL)
		_, _ = p.Process.Wait()
		delete(c.procs, id)
	}
}

// Running reports whether a node process is up.
func (c *Cluster) Running(id uint64) bool { _, ok := c.procs[id]; return ok }

// Isolate blocks every link to and from id.
func (c *Cluster) Isolate(id uint64, on bool) {
	for k, p := range c.proxies {
		if k[0] == id || k[1] == id {
			p.Block(on)
		}
	}
}

// Delay sets latency on both directions between a and b.
func (c *Cluster) Delay(a, b uint64, d time.Duration) {
	c.proxies[[2]uint64{a, b}].SetDelay(d)
	c.proxies[[2]uint64{b, a}].SetDelay(d)
}

// Heal removes all partitions and delays.
func (c *Cluster) Heal() {
	for _, p := range c.proxies {
		p.Block(false)
		p.SetDelay(0)
	}
}

// Status fetches a node's status.
func (c *Cluster) Status(id uint64) (node.Status, error) {
	var st node.Status
	resp, err := c.http.Get("http://" + c.clientAddr(id) + "/v1/status")
	if err != nil {
		return st, err
	}
	defer func() { _ = resp.Body.Close() }()
	return st, json.NewDecoder(resp.Body).Decode(&st)
}

// Leader returns the id of a node that reports itself leader (highest term), or 0.
func (c *Cluster) Leader() (uint64, uint64) {
	var best, term uint64
	for _, id := range c.IDs() {
		if !c.Running(id) {
			continue
		}
		st, err := c.Status(id)
		if err == nil && st.State == "leader" && st.Term >= term {
			best, term = id, st.Term
		}
	}
	return best, term
}

// AppliedLog fetches a node's applied log.
func (c *Cluster) AppliedLog(id uint64) ([]node.AppliedEntry, error) {
	cl := &http.Client{Timeout: 30 * time.Second}
	resp, err := cl.Get("http://" + c.clientAddr(id) + "/v1/log")
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	var l []node.AppliedEntry
	return l, json.NewDecoder(resp.Body).Decode(&l)
}

// WaitConverged waits until all nodes report the same applied index and state hash.
func (c *Cluster) WaitConverged(ctx context.Context) (node.Status, error) {
	for ctx.Err() == nil {
		var ref node.Status
		ok := true
		for i, id := range c.IDs() {
			st, err := c.Status(id)
			if err != nil || st.Applied == 0 || st.Applied < st.Commit {
				ok = false
				break
			}
			if i == 0 {
				ref = st
			} else if st.Applied != ref.Applied || st.StateHash != ref.StateHash {
				ok = false
				break
			}
		}
		if ok {
			return ref, nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return node.Status{}, ctx.Err()
}

// Stop kills everything.
func (c *Cluster) Stop() {
	for _, id := range c.IDs() {
		c.Kill(id)
	}
	for _, p := range c.proxies {
		p.Close()
	}
	for _, f := range c.LogFiles {
		_ = f.Close()
	}
}
