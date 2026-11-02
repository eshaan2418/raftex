#!/usr/bin/env python3
import glob, json, os, statistics, sys
d = sys.argv[1]
R = {os.path.basename(f)[:-5]: json.load(open(f)) for f in sorted(glob.glob(os.path.join(d, "*.json")))}
print(f"# raftex results ({os.path.basename(d)})\n")
print("## Throughput and latency (no faults)\n")
print("| config | clients | committed ops/s | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | log-witness checks / Porcupine |")
print("|---|---|---|---|---|---|---|---|---|")
def ok(r):
    good = r["replicas_agree"] and r["lost_acked_ops"] == 0 and r["double_applied_ops"] == 0 and r["result_mismatches"] == 0 and r["real_time_order_violations"] == 0
    p = r["porcupine"]
    return ("pass" if good else "FAIL") + f" / porcupine: {p}"
for k, r in R.items():
    if not k.startswith("load"): continue
    l = r["latency_ms_p50_p90_p99_p999_max_mean"]
    print(f'| {k} | {r["clients"]} | {r["ops_per_sec"]:,.0f} | {l[0]:.2f} | {l[1]:.2f} | {l[2]:.2f} | {l[3]:.2f} | {l[4]:.0f} | {ok(r)} |')
print("\n## Fault runs\n")
print("| run | ops | faults | client retries | dup entries absorbed | lost acked | double applied | result mismatch | RT violations | Porcupine (ms) | replicas agree |")
print("|---|---|---|---|---|---|---|---|---|---|---|")
el, un = [], []
for k, r in R.items():
    if k.startswith("load"): continue
    print(f'| {k} | {r["ops_completed"]:,} | {len(r["faults"])} | {r["client_retries"]} | {r["duplicate_log_entries_absorbed"]} | {r["lost_acked_ops"]} | {r["double_applied_ops"]} | {r["result_mismatches"]} | {r["real_time_order_violations"]} | {r["porcupine"]} ({r["porcupine_ms"]:.0f}) | {r["replicas_agree"]} |')
    for f in r["faults"]:
        if f["kind"] in ("kill_leader", "isolate_leader"):
            if f.get("election_ms", -1) > 0: el.append(f["election_ms"])
            if f.get("unavailability_ms", 0) > 0: un.append(f["unavailability_ms"])
def pct(v, p): v = sorted(v); return v[int(p * (len(v) - 1))]
if el:
    print(f"\n## Failover (leader kill -9 / isolation; {len(el)} events)\n")
    print("| metric | p50 | p90 | max |\n|---|---|---|---|")
    print(f"| new leader elected (ms after fault) | {pct(el,.5):.0f} | {pct(el,.9):.0f} | {max(el):.0f} |")
    print(f"| client-observed unavailability (ms) | {pct(un,.5):.0f} | {pct(un,.9):.0f} | {max(un):.0f} |")
