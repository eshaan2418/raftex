#!/usr/bin/env bash
# 3-node cluster under load; the leader is killed with SIGKILL repeatedly; clients keep trading;
# then every safety check runs.
set -euo pipefail
go build -o bin/ ./cmd/... >/dev/null
bin/raftexchaos -mode failover -duration 8s -clients 8 -fault-every 3s -base 18500 | python3 scripts/demo_fmt.py
