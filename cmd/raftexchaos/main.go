// Command raftexchaos runs a 3-node raftex cluster as local processes behind fault-injecting
// proxies, drives it with concurrent clients, injects faults, and checks safety.
//
//	raftexchaos -mode chaos -duration 60s -clients 8 -fault-every 4s -out report.json
//	raftexchaos -mode failover -duration 60s -fault-every 3s
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/eshaan2418/raftex/internal/chaos"
)

func main() {
	mode := flag.String("mode", "chaos", "chaos | failover | load")
	bin := flag.String("bin", "bin/raftexd", "raftexd binary")
	dur := flag.Duration("duration", 60*time.Second, "workload duration")
	clients := flag.Int("clients", 8, "concurrent clients")
	every := flag.Duration("fault-every", 4*time.Second, "fault interval")
	seed := flag.Int64("seed", 1, "random seed")
	base := flag.Int("base", 17000, "port base")
	nosync := flag.Bool("nosync", false, "nodes skip fsync")
	out := flag.String("out", "", "write JSON report")
	dir := flag.String("dir", "", "data directory (default: temp)")
	flag.Parse()
	// A closed stdout pipe must not kill us before the deferred cleanup stops the node processes.
	signal.Ignore(syscall.SIGPIPE)
	d := *dir
	if d == "" {
		var err error
		d, err = os.MkdirTemp("", "raftex-chaos-")
		if err != nil {
			panic(err)
		}
		defer os.RemoveAll(d)
	}
	absBin, _ := filepath.Abs(*bin)
	c := &chaos.Cluster{Bin: absBin, Dir: d, N: 3, Base: *base, NoSync: *nosync}
	if err := c.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "start:", err)
		os.Exit(1)
	}
	defer c.Stop()
	rep := chaos.Run(context.Background(), c, chaos.Options{Mode: *mode, Duration: *dur, Clients: *clients, FaultEvery: *every, Seed: *seed})
	b, _ := json.MarshalIndent(rep, "", "  ")
	if *out != "" {
		_ = os.WriteFile(*out, b, 0o644)
	}
	fmt.Println(string(b))
	if !rep.ReplicasAgree || rep.LostAcked > 0 || rep.DoubleApplied > 0 || rep.ResultMismatch > 0 ||
		rep.RealTimeViol > 0 || rep.Porcupine == "NOT linearizable" {
		os.Exit(3)
	}
}
