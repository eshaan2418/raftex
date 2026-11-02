#!/usr/bin/env bash
# raftex benchmark + chaos suite. Raw JSON reports in bench/results/<stamp>/.
set -euo pipefail
go build -o bin/ ./cmd/...
OUT="bench/results/$(date +%Y%m%d-%H%M%S)"; mkdir -p "$OUT"
{ date -u +%FT%TZ; (sysctl -n machdep.cpu.brand_string 2>/dev/null || lscpu | grep 'Model name'); go version; uptime; } > "$OUT/machine.txt"
B=17400
run() { local name="$1"; shift; echo "[$name]" >&2; B=$((B+300)); bin/raftexchaos -base "$B" -out "$OUT/$name.json" "$@" > /dev/null || echo "[$name] CHECK FAILED (exit $?)" | tee -a "$OUT/FAILURES.txt"; }
for c in 4 16 64; do
  run "load_fsync_c$c"  -mode load -duration 15s -clients "$c"
  run "load_nosync_c$c" -mode load -duration 15s -clients "$c" -nosync
done
run failover_fsync -mode failover -duration 60s -clients 16 -fault-every 3s
for s in 1 2 3; do run "chaos_seed$s" -mode chaos -duration 60s -clients 16 -fault-every 3s -seed "$s"; done
python3 scripts/summarize.py "$OUT" | tee "$OUT/SUMMARY.md"
