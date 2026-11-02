// Package client talks to a raftex cluster: it discovers the leader (following leader hints),
// and retries every command with the *same* (ClientID, Seq) until it gets a definite answer, so a
// command is applied exactly once even across leader failover.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/eshaan2418/raftex/internal/exchange"
)

// Client is safe for use by one goroutine at a time (Seq is per client).
type Client struct {
	id      uint64
	seq     uint64
	addrs   []string
	leader  string
	http    *http.Client
	Retries atomic.Uint64
}

// New creates a client with a unique id.
func New(id uint64, addrs []string) *Client {
	return &Client{id: id, addrs: addrs, leader: addrs[0],
		http: &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 4}}}
}

// ID returns the client id.
func (c *Client) ID() uint64 { return c.id }

// Do submits cmd (ClientID/Seq are filled in) and retries until a definite result or ctx ends.
// Returns the result and the Seq used.
func (c *Client) Do(ctx context.Context, cmd exchange.Command) (exchange.Result, uint64, error) {
	c.seq++
	cmd.ClientID, cmd.Seq = c.id, c.seq
	body, _ := json.Marshal(cmd)
	backoff := 10 * time.Millisecond
	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			return exchange.Result{}, cmd.Seq, ctx.Err()
		}
		res, hint, err := c.try(ctx, c.leader, body)
		if err == nil {
			return res, cmd.Seq, nil
		}
		c.Retries.Add(1)
		if hint != "" {
			c.leader = hint
			continue
		}
		// Unknown leader or node down: try another node after a short backoff.
		c.leader = c.addrs[rand.Intn(len(c.addrs))]
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return exchange.Result{}, cmd.Seq, ctx.Err()
		}
		if backoff < 200*time.Millisecond {
			backoff *= 2
		}
	}
}

func (c *Client) try(ctx context.Context, addr string, body []byte) (exchange.Result, string, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/v1/command", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return exchange.Result{}, "", err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		var r exchange.Result
		if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
			return exchange.Result{}, "", err
		}
		return r, "", nil
	case http.StatusMisdirectedRequest:
		_, _ = io.Copy(io.Discard, resp.Body)
		return exchange.Result{}, resp.Header.Get("X-Raftex-Leader"), errors.New("not leader")
	default:
		b, _ := io.ReadAll(resp.Body)
		return exchange.Result{}, "", fmt.Errorf("status %d: %s", resp.StatusCode, bytes.TrimSpace(b))
	}
}
