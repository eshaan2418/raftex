# Performance log

Before: `bench/results/20261102-005238/`. After: `bench/results/20261102-011736/`. Same harness
(3 local processes behind TCP proxies, HTTP/JSON clients, 15 s per load point). The laptop was
shared with an unrelated workload; load averages are recorded in each `machine.txt`.

## 1. fsync cost on macOS
`File.Sync` in Go on darwin issues `F_FULLFSYNC`, a flush through the drive cache: about 3.6 ms per
call (measured with a 200-iteration probe while developing; it varies with machine load). That is
the floor for a durable commit, and the reason there are two columns everywhere.

## 2. Two changes on the commit path
1. **No fsync for commit-index-only changes.** Raft does not require the commit index to be
   durable. Previously every commit advance (on the leader, and on followers via heartbeats)
   triggered an fsync.
2. **Leader replicates in parallel with its own fsync** (Raft thesis §10.2.1). Appends are sent
   before the WAL write. The leader counts itself toward a majority only once `Advance` confirms
   its fsync (`persisted`), so safety is unchanged. A targeted test fails if the leader counts
   unpersisted entries. Leader and follower fsyncs now overlap instead of running in series.

| config | before ops/s | after ops/s | before p50 | after p50 |
|---|---|---|---|---|
| fsync, 4 clients | 103 | 171 (+66%) | 36.0 ms | 24.0 ms |
| fsync, 16 clients | 371 | 603 (+63%) | 41.9 ms | 26.9 ms |
| fsync, 64 clients | 1,520 | 2,338 (+54%) | 41.0 ms | 27.0 ms |
| no fsync, 4 clients | 8,101 | 15,857 (+96%) | 0.48 ms | 0.25 ms |
| no fsync, 64 clients | 26,433 | 29,254 (+11%) | 2.39 ms | 2.19 ms |

Throughput scales almost linearly with clients while latency stays flat: group commit, where one
fsync covers every proposal that arrived during the previous one.

## 3. Where the remaining time goes
Without fsync, at 64 clients the cluster is CPU-bound in HTTP + JSON handling and gob message
encoding, all three processes on one laptop. With fsync, latency (~24-27 ms p50) is several
F_FULLFSYNC calls under contention plus queueing behind the previous group commit.

## 4. Not done
- Binary pipelined client protocol, to replace HTTP/JSON per order.
- Message batching across ticks, and a custom binary codec instead of gob.
- Snapshots + log compaction (restart time currently grows with history).
- Linux numbers with `fdatasync` on NVMe.
