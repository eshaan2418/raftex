package chaos

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// End-to-end: build raftexd, run 3 processes behind proxies, inject every fault kind (kill -9 and
// isolate leader, isolate/kill follower, link delay) under concurrent clients, then check replica
// agreement, no lost/duplicated acknowledged orders, result consistency, real-time order and
// Porcupine linearizability.
func TestChaosEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-process chaos test")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "raftexd")
	b := exec.Command("go", "build", "-o", bin, "github.com/eshaan2418/raftex/cmd/raftexd")
	b.Stderr = os.Stderr
	if err := b.Run(); err != nil {
		t.Fatal(err)
	}
	c := &Cluster{Bin: bin, Dir: dir, N: 3, Base: 19000}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	defer c.Stop()
	rep := Run(context.Background(), c, Options{Mode: "chaos", Duration: 12 * time.Second, Clients: 6, FaultEvery: 2 * time.Second, Seed: 3})
	t.Logf("ops=%d faults=%d retries=%d dups_absorbed=%d porcupine=%s", rep.Ops, len(rep.Faults), rep.Retries, rep.DupApplications, rep.Porcupine)
	if rep.Ops == 0 || len(rep.Faults) == 0 {
		t.Fatalf("no work done: %+v", rep)
	}
	if !rep.ReplicasAgree || rep.LostAcked != 0 || rep.DoubleApplied != 0 || rep.ResultMismatch != 0 || rep.RealTimeViol != 0 {
		t.Fatalf("safety check failed: %+v", rep)
	}
	if rep.Porcupine != "linearizable" {
		t.Fatalf("porcupine: %s", rep.Porcupine)
	}
}
