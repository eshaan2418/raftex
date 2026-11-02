# raftex results (20261102-005238)

## Throughput and latency (no faults)

| config | clients | committed ops/s | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | log-witness checks / Porcupine |
|---|---|---|---|---|---|---|---|---|
| load_fsync_c16 | 16 | 371 | 41.91 | 58.98 | 70.76 | 76.10 | 76 | pass / porcupine: timeout |
| load_fsync_c4 | 4 | 103 | 36.04 | 53.09 | 66.98 | 74.08 | 75 | pass / porcupine: linearizable |
| load_fsync_c64 | 64 | 1,520 | 40.97 | 54.80 | 64.04 | 69.14 | 72 | pass / porcupine: timeout |
| load_nosync_c16 | 16 | 22,299 | 0.69 | 0.90 | 1.31 | 2.56 | 15 | pass / porcupine: timeout |
| load_nosync_c4 | 4 | 8,101 | 0.48 | 0.60 | 0.89 | 1.72 | 5 | pass / porcupine: linearizable |
| load_nosync_c64 | 64 | 26,433 | 2.39 | 2.95 | 4.53 | 8.86 | 24 | pass / porcupine: timeout |

## Fault runs

| run | ops | faults | client retries | dup entries absorbed | lost acked | double applied | result mismatch | RT violations | Porcupine (ms) | replicas agree |
|---|---|---|---|---|---|---|---|---|---|---|
| chaos_seed1 | 26,755 | 19 | 942 | 88 | 0 | 0 | 0 | 0 | timeout (120005) | True |
| chaos_seed2 | 27,867 | 18 | 919 | 66 | 0 | 0 | 0 | 0 | timeout (120004) | True |
| chaos_seed3 | 29,563 | 16 | 487 | 33 | 0 | 0 | 0 | 0 | timeout (120006) | True |
| failover_fsync | 24,756 | 19 | 2538 | 173 | 0 | 0 | 0 | 0 | timeout (120006) | True |

## Failover (leader kill -9 / isolation; 43 events)

| metric | p50 | p90 | max |
|---|---|---|---|
| new leader elected (ms after fault) | 245 | 310 | 518 |
| client-observed unavailability (ms) | 344 | 362 | 662 |
