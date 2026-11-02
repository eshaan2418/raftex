// Command raftexd runs one replica of the raftex exchange cluster.
//
//	raftexd -id 1 -raft 127.0.0.1:7001 -client 127.0.0.1:8001 \
//	        -peers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 \
//	        -clients 1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003 -data ./data
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/eshaan2418/raftex/internal/node"
)

func parseMap(s string) map[uint64]string {
	m := map[uint64]string{}
	for _, kv := range strings.Split(s, ",") {
		if kv == "" {
			continue
		}
		p := strings.SplitN(kv, "=", 2)
		id, err := strconv.ParseUint(p[0], 10, 64)
		if err != nil || len(p) != 2 {
			fmt.Fprintln(os.Stderr, "bad map entry", kv)
			os.Exit(2)
		}
		m[id] = p[1]
	}
	return m
}

func main() {
	id := flag.Uint64("id", 1, "node id")
	raftListen := flag.String("raft", "127.0.0.1:7001", "raft listen address")
	clientAddr := flag.String("client", "127.0.0.1:8001", "client API listen address")
	peers := flag.String("peers", "", "id=addr,... raft addresses this node dials (may be proxies)")
	clients := flag.String("clients", "", "id=addr,... client API addresses of all nodes (leader hints)")
	data := flag.String("data", "data", "data directory")
	tick := flag.Duration("tick", 10*time.Millisecond, "raft tick")
	election := flag.Int("election-ticks", 15, "base election timeout in ticks")
	nosync := flag.Bool("nosync", false, "skip fsync (benchmarks only)")
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})).With("node", *id)
	if err := os.MkdirAll(*data, 0o755); err != nil {
		log.Error("mkdir", "err", err)
		os.Exit(1)
	}
	cfg := node.Config{ID: *id, RaftAddrs: parseMap(*peers), RaftListen: *raftListen,
		ClientAddr: *clientAddr, ClientAddrs: parseMap(*clients), DataDir: *data, Tick: *tick,
		ElectionTicks: *election, NoSync: *nosync, Log: log}
	if err := run(cfg, log); err != nil {
		os.Exit(1)
	}
}

// run starts and serves the node until SIGINT/SIGTERM. It is split out of main so the deferred
// context cancel runs before main calls os.Exit.
func run(cfg node.Config, log *slog.Logger) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	n, err := node.Start(ctx, cfg)
	if err != nil {
		log.Error("start", "err", err)
		return err
	}
	if err := n.Serve(ctx); err != nil {
		log.Error("serve", "err", err)
		return err
	}
	return nil
}
