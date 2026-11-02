# raftex design

## 1. Shape of the system

```
 clients (retry with same ClientID/Seq)
    │ HTTP JSON (any node; followers answer 421 + leader hint)
    ▼
 ┌────────────────────────── node (one process) ───────────────────────────┐
 │ client API ──proposal chan──▶ run loop (single goroutine)                │
 │                              ├─ raft.Node   (pure state machine)          │
 │                              ├─ WAL.Save    (fsync per Ready: group commit)│
 │                              ├─ transport.Send (after persist)            │
 │                              └─ exchange.Machine.Apply (committed entries) │
 └───────────────────────────────────────────────────────────────────────────┘
        ▲  raft messages over TCP (gob stream per peer), through fault proxies in tests
```

The run loop is the only goroutine that touches Raft, the WAL and the state machine, so none of
them need locks. Proposals arriving together are drained into one Ready: one WAL write and one
fsync cover all of them, so throughput grows with concurrency (group commit).

## 2. Raft core (`internal/raft`)

- **Pure and deterministic.** No I/O, no clock, injected randomness. The caller loops
  `Step`/`Tick`/`Propose` → `Ready` → persist → send → apply → `Advance`. The same code runs in the
  simulator and in production.
- **Election:** randomized timeouts in [T, 2T). With **Pre-Vote**, a node first asks whether it
  could win (without bumping its term). A partitioned node therefore cannot inflate its term and
  depose a healthy leader when it rejoins (tested). **Check-quorum:** a leader that has not heard
  from a majority within an election timeout steps down, so clients stop talking to a
  minority-side leader (tested). Leader stickiness: a node ignores vote requests while it has
  heard from a live leader within the timeout.
- **Replication:** pipelined. `next` advances optimistically when an append is sent. A rejection
  carries a hint (the first index of the follower's conflicting term, or its log end), so the
  leader skips a whole term per round trip. Heartbeat responses carry the follower's last index,
  which re-drives replication after lost appends.
- **Commitment:** a leader commits only entries from its own term by counting replicas (§5.4.2,
  "Figure 8"), and appends a no-op on election so earlier entries commit promptly.
  `TestFigure8NoCommitOfOldTermByCounting` reproduces the scenario, and fails if the rule is
  removed.
- **Persistence ordering:** the hard state and new entries in a Ready are fsynced before its
  messages are sent. A vote or an append acknowledgement is therefore never sent for state that
  could be lost.

## 3. WAL (`internal/wal`)
CRC32-C-framed records (hard state, entry, truncate) are appended to one file, with one fsync per
Ready. Recovery replays them to rebuild hard state and log, applying truncations, and stops at the
first torn or corrupt record. It is fuzzed: arbitrary bytes never panic and never produce a
non-contiguous log. There are no snapshots, so the log grows without bound; snapshots are on the
roadmap.

## 4. Exchange state machine (`internal/exchange`)
A deterministic price-time engine (limit GTC/IOC, market IOC, cancel with ownership). An order's
id is the **Raft log index** of its command, so ids agree on every replica without coordination.
Every result is folded into a running FNV hash, and replicas that applied the same prefix report
the same hash. Restart rebuilds the state by replaying the committed log from the WAL.

### Exactly-once across failover
A client sends `(ClientID, Seq)` and retries the same pair until it gets a definite answer. The
machine stores each client's last `(seq, result)`:
- if a committed command repeats a seq that has already been applied, it returns the cached
  result and changes nothing (`Duplicate: true`);
- an older seq is rejected as stale.

This is necessary because a leader can append and replicate an entry, then crash before replying.
The entry is committed, and the client's retry would otherwise apply it twice. The chaos runs
report how many such duplicate entries were absorbed.

## 5. Answering clients correctly
A proposer records `(index, term, data)` for each waiting client. When the entry at that index
commits, the client gets success **only if** the term and data match what was proposed: a new
leader may have placed a different entry at that index. If the node loses leadership, its
waiters are failed immediately so clients retry at the new leader. Dedup makes the retry safe
even if the original entry later commits.

## 6. Verification layers

| layer | scope | what it proves |
|---|---|---|
| unit (`raft_test.go`) | single node | vote rules, Figure 8, truncation, reject hints |
| simulation (`sim_test.go`) | 3/5 in-memory nodes, 200 seeds, drops/dups/reordering/partitions/crashes, half without Pre-Vote | election safety, log matching, leader completeness, state-machine safety each step; liveness after healing |
| mutation checks | removing the log-up-to-date vote check or the current-term commit rule | each is caught (sim / Figure-8 test) |
| WAL | fuzz + torn-tail/corruption tests | recovery is consistent on arbitrary bytes |
| chaos (`internal/chaos`) | 3 real processes, kill -9, isolation, link delay via TCP proxies | replicas agree; no lost or double-applied acked order; results match the log; real-time order respected; Porcupine linearizable |

The real-time check uses the log as the linearization witness: if op A returned before op B was
invoked, A's log index must be lower. Porcupine checks independently, with the exchange itself as
the sequential model. An op's output contains its log index (order ids are indexes), so the model
also enforces index order.

## 7. What is not here
Membership changes (joint consensus), snapshots and log compaction, follower reads with read
index or leases, request batching on the client side, TLS between nodes.
