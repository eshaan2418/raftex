import json, sys
d = json.load(sys.stdin)
for f in d["faults"]:
    print(f'  kill -9 leader node{f["target"]}: new leader in {f["election_ms"]:.0f} ms, clients blocked {f["unavailability_ms"]:.0f} ms')
print(f'  orders completed: {d["ops_completed"]:,}   client retries: {d["client_retries"]}   duplicate log entries absorbed: {d["duplicate_log_entries_absorbed"]}')
print(f'  replicas agree: {d["replicas_agree"]}   lost acked: {d["lost_acked_ops"]}   double-applied: {d["double_applied_ops"]}   Porcupine: {d["porcupine"]}')
