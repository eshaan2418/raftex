package chaos

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/eshaan2418/raftex/internal/client"
	"github.com/eshaan2418/raftex/internal/exchange"
	"github.com/eshaan2418/raftex/internal/node"
)

// Op is one completed client operation.
type Op struct {
	Client uint64           `json:"client"`
	Seq    uint64           `json:"seq"`
	Cmd    exchange.Command `json:"cmd"`
	Call   int64            `json:"call_ns"`
	Return int64            `json:"return_ns"`
	Res    exchange.Result  `json:"result"`
}

// Fault is one injected fault.
type Fault struct {
	At     int64  `json:"at_ns"`
	Kind   string `json:"kind"`
	Target uint64 `json:"target"`
	// Failover measurements (leader kills/isolation only).
	ElectionMs       float64 `json:"election_ms,omitempty"`
	UnavailabilityMs float64 `json:"unavailability_ms,omitempty"`
}

// Report is the outcome of a run.
type Report struct {
	Mode            string     `json:"mode"`
	DurationS       float64    `json:"duration_s"`
	Clients         int        `json:"clients"`
	Ops             int        `json:"ops_completed"`
	OpsPerSec       float64    `json:"ops_per_sec"`
	Faults          []Fault    `json:"faults"`
	Retries         uint64     `json:"client_retries"`
	LogLen          int        `json:"applied_log_entries"`
	DupApplications int        `json:"duplicate_log_entries_absorbed"`
	FinalHash       string     `json:"final_state_hash"`
	ReplicasAgree   bool       `json:"replicas_agree"`
	LostAcked       int        `json:"lost_acked_ops"`
	DoubleApplied   int        `json:"double_applied_ops"`
	ResultMismatch  int        `json:"result_mismatches"`
	RealTimeViol    int        `json:"real_time_order_violations"`
	Porcupine       string     `json:"porcupine"`
	PorcupineMs     float64    `json:"porcupine_ms"`
	LatencyMs       [6]float64 `json:"latency_ms_p50_p90_p99_p999_max_mean"`
	Errors          []string   `json:"errors,omitempty"`
}

// Options for Run.
type Options struct {
	Mode       string        // "chaos" (mixed faults) or "failover" (repeated leader kills)
	Duration   time.Duration // workload duration
	Clients    int
	FaultEvery time.Duration
	Seed       int64
}

// Run drives the cluster with clients while injecting faults, then heals and checks.
func Run(ctx context.Context, c *Cluster, o Options) Report {
	rep := Report{Mode: o.Mode, Clients: o.Clients}
	rnd := rand.New(rand.NewSource(o.Seed))
	t0 := time.Now()
	since := func() int64 { return time.Since(t0).Nanoseconds() }
	// Wait for an initial leader.
	for i := 0; i < 500; i++ {
		if l, _ := c.Leader(); l != 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	var mu sync.Mutex
	var ops []Op
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var retries uint64
	for k := 0; k < o.Clients; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			cl := client.New(uint64(1000+k), c.ClientAddrs())
			r := rand.New(rand.NewSource(o.Seed*131 + int64(k)))
			acct := uint32(1 + k%4)
			var resting []uint64
			for {
				select {
				case <-stop:
					mu.Lock()
					retries += cl.Retries.Load()
					mu.Unlock()
					return
				default:
				}
				var cmd exchange.Command
				if len(resting) > 0 && r.Intn(10) < 3 {
					i := r.Intn(len(resting))
					cmd = exchange.Command{Op: exchange.OpCancel, Account: acct, OrderID: resting[i]}
					resting = append(resting[:i], resting[i+1:]...)
				} else {
					side := exchange.Side(r.Intn(2))
					px := int64(1000 + r.Intn(11) - 5)
					cmd = exchange.Command{Op: exchange.OpNew, Account: acct, Side: side, Price: px,
						Qty: int64(1 + r.Intn(10)), IOC: r.Intn(4) == 0}
				}
				call := since()
				// Each op retries until definite; the generous deadline only bounds a broken run.
				octx, cancel := context.WithTimeout(ctx, 60*time.Second)
				res, seq, err := cl.Do(octx, cmd)
				cancel()
				ret := since()
				if err != nil {
					mu.Lock()
					rep.Errors = append(rep.Errors, fmt.Sprintf("client %d seq %d: %v", cl.ID(), seq, err))
					mu.Unlock()
					continue
				}
				cmd.ClientID, cmd.Seq = cl.ID(), seq
				for _, e := range res.Events {
					if e.Kind == exchange.EvRested {
						resting = append(resting, e.OrderID)
					}
				}
				mu.Lock()
				ops = append(ops, Op{Client: cl.ID(), Seq: seq, Cmd: cmd, Call: call, Return: ret, Res: res})
				mu.Unlock()
			}
		}(k)
	}

	// Nemesis ("load" mode: none).
	deadline := time.After(o.Duration)
	if o.Mode == "load" {
		o.FaultEvery = o.Duration * 10
	}
	tick := time.NewTicker(o.FaultEvery)
nemesis:
	for {
		select {
		case <-deadline:
			break nemesis
		case <-tick.C:
		}
		lead, _ := c.Leader()
		kind := "kill_leader"
		if o.Mode == "chaos" {
			kind = []string{"kill_leader", "isolate_leader", "isolate_follower", "kill_follower", "delay_link"}[rnd.Intn(5)]
		}
		f := Fault{At: since(), Kind: kind}
		pickFollower := func() uint64 {
			for _, id := range c.IDs() {
				if id != lead && c.Running(id) {
					return id
				}
			}
			return 0
		}
		switch kind {
		case "kill_leader":
			if lead == 0 {
				continue
			}
			f.Target = lead
			c.Kill(lead)
			f.ElectionMs = waitNewLeader(c, lead, t0, f.At)
			time.Sleep(1500 * time.Millisecond)
			_ = c.StartNode(lead)
		case "isolate_leader":
			if lead == 0 {
				continue
			}
			f.Target = lead
			c.Isolate(lead, true)
			f.ElectionMs = waitNewLeader(c, lead, t0, f.At)
			time.Sleep(1500 * time.Millisecond)
			c.Isolate(lead, false)
		case "isolate_follower":
			f.Target = pickFollower()
			c.Isolate(f.Target, true)
			time.Sleep(2 * time.Second)
			c.Isolate(f.Target, false)
		case "kill_follower":
			f.Target = pickFollower()
			c.Kill(f.Target)
			time.Sleep(2 * time.Second)
			_ = c.StartNode(f.Target)
		case "delay_link":
			ids := c.IDs()
			a, b := ids[rnd.Intn(len(ids))], ids[rnd.Intn(len(ids))]
			if a == b {
				continue
			}
			f.Target = a*10 + b
			c.Delay(a, b, 50*time.Millisecond)
			time.Sleep(2 * time.Second)
			c.Delay(a, b, 0)
		}
		mu.Lock()
		rep.Faults = append(rep.Faults, f)
		mu.Unlock()
	}
	tick.Stop()
	close(stop)
	wg.Wait()
	rep.DurationS = time.Since(t0).Seconds()
	rep.Retries = retries

	// Heal and converge.
	c.Heal()
	for _, id := range c.IDs() {
		if !c.Running(id) {
			_ = c.StartNode(id)
		}
	}
	cctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	final, err := c.WaitConverged(cctx)
	cancel()
	if err != nil {
		rep.Errors = append(rep.Errors, "replicas did not converge: "+err.Error())
	}
	rep.FinalHash = final.StateHash
	sort.Slice(ops, func(i, j int) bool { return ops[i].Call < ops[j].Call })
	rep.Ops = len(ops)
	rep.OpsPerSec = float64(len(ops)) / o.Duration.Seconds()
	measureUnavailability(&rep, ops)
	latencies(&rep, ops)
	check(&rep, c, ops)
	return rep
}

func waitNewLeader(c *Cluster, old uint64, t0 time.Time, at int64) float64 {
	for i := 0; i < 2000; i++ {
		if l, _ := c.Leader(); l != 0 && l != old {
			return float64(time.Since(t0).Nanoseconds()-at) / 1e6
		}
		time.Sleep(5 * time.Millisecond)
	}
	return -1
}

// measureUnavailability: for each leader fault, the gap between the last op completion before the
// fault and the first op completion after it (what clients experience).
func measureUnavailability(rep *Report, ops []Op) {
	rets := make([]int64, len(ops))
	for i, op := range ops {
		rets[i] = op.Return
	}
	sort.Slice(rets, func(i, j int) bool { return rets[i] < rets[j] })
	for i := range rep.Faults {
		f := &rep.Faults[i]
		if f.Kind != "kill_leader" && f.Kind != "isolate_leader" {
			continue
		}
		k := sort.Search(len(rets), func(j int) bool { return rets[j] > f.At })
		if k == 0 || k >= len(rets) {
			continue
		}
		f.UnavailabilityMs = float64(rets[k]-rets[k-1]) / 1e6
	}
}

func latencies(rep *Report, ops []Op) {
	if len(ops) == 0 {
		return
	}
	d := make([]float64, len(ops))
	var sum float64
	for i, op := range ops {
		d[i] = float64(op.Return-op.Call) / 1e6
		sum += d[i]
	}
	sort.Float64s(d)
	q := func(p float64) float64 { return d[int(p*float64(len(d)-1))] }
	rep.LatencyMs = [6]float64{q(0.5), q(0.9), q(0.99), q(0.999), d[len(d)-1], sum / float64(len(d))}
}

// check verifies replica agreement, no lost or duplicated acked ops, result consistency, real-time
// order (with the log as linearization witness), and runs Porcupine as an independent checker.
func check(rep *Report, c *Cluster, ops []Op) {
	logs := map[uint64][]node.AppliedEntry{}
	for _, id := range c.IDs() {
		l, err := c.AppliedLog(id)
		if err != nil {
			rep.Errors = append(rep.Errors, fmt.Sprintf("log from %d: %v", id, err))
			continue
		}
		logs[id] = l
	}
	rep.ReplicasAgree = true
	var ref []node.AppliedEntry
	for _, id := range c.IDs() {
		if ref == nil {
			ref = logs[id]
			continue
		}
		l := logs[id]
		if len(l) != len(ref) {
			rep.ReplicasAgree = false
			rep.Errors = append(rep.Errors, fmt.Sprintf("node %d log length %d != %d", id, len(l), len(ref)))
			continue
		}
		for i := range l {
			a, _ := json.Marshal(l[i])
			b, _ := json.Marshal(ref[i])
			if string(a) != string(b) {
				rep.ReplicasAgree = false
				rep.Errors = append(rep.Errors, fmt.Sprintf("node %d diverges at log position %d", id, i))
				break
			}
		}
	}
	rep.LogLen = len(ref)
	type key struct{ c, s uint64 }
	first := map[key]node.AppliedEntry{} // first (non-duplicate) application of each (client, seq)
	applications := map[key]int{}
	for _, e := range ref {
		var cmd exchange.Command
		_ = json.Unmarshal(e.Cmd, &cmd)
		k := key{cmd.ClientID, cmd.Seq}
		if e.Result.Duplicate {
			rep.DupApplications++
			continue
		}
		applications[k]++
		if _, ok := first[k]; !ok {
			first[k] = e
		}
	}
	for _, n := range applications {
		if n > 1 {
			rep.DoubleApplied++
		}
	}
	indexOf := map[key]uint64{}
	for _, op := range ops {
		k := key{op.Client, op.Seq}
		e, ok := first[k]
		if !ok {
			rep.LostAcked++
			continue
		}
		indexOf[k] = e.Index
		got := op.Res
		got.Duplicate = false
		a, _ := json.Marshal(got)
		b, _ := json.Marshal(e.Result)
		if string(a) != string(b) {
			rep.ResultMismatch++
		}
	}
	// Real-time order: if A returned before B was called, A must precede B in the log.
	byReturn := append([]Op(nil), ops...)
	sort.Slice(byReturn, func(i, j int) bool { return byReturn[i].Return < byReturn[j].Return })
	var maxIdx uint64
	j := 0
	for _, b := range ops { // ops sorted by call
		for j < len(byReturn) && byReturn[j].Return < b.Call {
			if idx := indexOf[key{byReturn[j].Client, byReturn[j].Seq}]; idx > maxIdx {
				maxIdx = idx
			}
			j++
		}
		if idx, ok := indexOf[key{b.Client, b.Seq}]; ok && idx < maxIdx {
			rep.RealTimeViol++
		}
	}
	// Porcupine (independent): the model is the exchange itself; an op's output fixes its log index
	// (order ids are log indexes), so a valid linearization must also respect index order.
	model := porcupine.Model{
		Init: func() interface{} { return exchange.New() },
		Step: func(state, input, output interface{}) (bool, interface{}) {
			m := state.(*exchange.Machine)
			in := input.(exchange.Command)
			out := output.(exchange.Result)
			if out.Index <= m.Applied() {
				return false, state
			}
			nm := m.Clone()
			data, _ := json.Marshal(in)
			r := nm.Apply(out.Index, data)
			out.Duplicate = false
			a, _ := json.Marshal(r)
			b, _ := json.Marshal(out)
			return string(a) == string(b), nm
		},
		Equal: func(a, b interface{}) bool {
			x, y := a.(*exchange.Machine), b.(*exchange.Machine)
			return x.Applied() == y.Applied() && x.Hash() == y.Hash()
		},
	}
	hist := make([]porcupine.Operation, 0, len(ops))
	for _, op := range ops {
		hist = append(hist, porcupine.Operation{ClientId: int(op.Client), Input: op.Cmd, Call: op.Call, Output: op.Res, Return: op.Return})
	}
	t := time.Now()
	res := porcupine.CheckOperationsTimeout(model, hist, 120*time.Second)
	rep.PorcupineMs = float64(time.Since(t).Microseconds()) / 1e3
	rep.Porcupine = map[porcupine.CheckResult]string{porcupine.Ok: "linearizable", porcupine.Illegal: "NOT linearizable", porcupine.Unknown: "timeout"}[res]
}
