# raftex results (20261102-011736)

## Throughput and latency (no faults)

| config | clients | committed ops/s | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | log-witness checks / Porcupine |
|---|---|---|---|---|---|---|---|---|
| load_fsync_c16 | 16 | 603 | 26.91 | 35.93 | 39.14 | 46.59 | 47 | pass / porcupine: timeout |
| load_fsync_c4 | 4 | 171 | 23.95 | 33.07 | 39.97 | 45.16 | 48 | pass / porcupine: linearizable |
| load_fsync_c64 | 64 | 2,338 | 26.99 | 35.90 | 39.99 | 43.44 | 60 | pass / porcupine: timeout |
| load_nosync_c16 | 16 | 26,865 | 0.59 | 0.70 | 0.83 | 1.38 | 9 | pass / porcupine: timeout |
| load_nosync_c4 | 4 | 15,857 | 0.25 | 0.31 | 0.38 | 0.55 | 5 | pass / porcupine: linearizable |
| load_nosync_c64 | 64 | 29,254 | 2.19 | 2.48 | 2.80 | 4.96 | 9 | pass / porcupine: timeout |

## Fault runs

| run | ops | faults | client retries | dup entries absorbed | lost acked | double applied | result mismatch | RT violations | Porcupine (ms) | replicas agree |
|---|---|---|---|---|---|---|---|---|---|---|
| chaos_seed1 | 35,825 | 19 | 951 | 58 | 0 | 0 | 0 | 0 | timeout (120011) | True |
| chaos_seed2 | 28,594 | 18 | 882 | 64 | 0 | 0 | 0 | 0 | timeout (120005) | True |
| chaos_seed3 | 33,602 | 16 | 536 | 49 | 0 | 0 | 0 | 0 | timeout (120016) | True |
| failover_fsync | 32,823 | 19 | 2634 | 225 | 0 | 0 | 0 | 0 | timeout (120005) | True |

## Failover (leader kill -9 / isolation; 43 events)

| metric | p50 | p90 | max |
|---|---|---|---|
| new leader elected (ms after fault) | 245 | 414 | 659 |
| client-observed unavailability (ms) | 335 | 656 | 977 |
