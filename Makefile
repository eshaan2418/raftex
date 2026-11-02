.PHONY: build test race sim chaos failover bench lint fuzz demo data clean
build:
	go build -o bin/ ./cmd/...
test:
	go test -short ./...
race:
	go test -race -short ./...
# Deterministic Raft simulation (200 seeds of crashes/partitions/drops/duplication).
sim:
	go test -count=1 -run TestSim -v ./internal/raft/
# Multi-process cluster under faults, with all safety checks (+ Porcupine).
chaos: build
	bin/raftexchaos -mode chaos -duration 60s -clients 16 -fault-every 3s
failover: build
	bin/raftexchaos -mode failover -duration 60s -clients 16 -fault-every 3s
bench:
	bash scripts/bench.sh
lint:
	go vet ./...
	@command -v golangci-lint >/dev/null && golangci-lint run ./... || echo "golangci-lint not installed; CI runs it"
fuzz:
	go test -run '^$$' -fuzz FuzzWALRecover -fuzztime $${FUZZTIME:-60s} ./internal/wal/
data:
	@echo "raftex needs no external data."
demo: build
	bash scripts/demo.sh
clean:
	rm -rf bin
