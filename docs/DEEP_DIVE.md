# raftex: deep-dive interview questions

### 1. Walk through an order from client to ack.
The client POSTs `{ClientID, Seq, op...}` to any node. A follower answers 421 with the leader's
address, and the client retries there with the same `(ClientID, Seq)`. The leader's HTTP handler
pushes a proposal onto the run loop's channel. The run loop calls `Propose`, which appends the
entry to the Raft log and queues `MsgApp`s, and it drains any other queued proposals into the
same batch. In `drain`, the leader sends the appends first, then writes and fsyncs its WAL. Each
follower appends, fsyncs and responds. When the leader has its own fsync done (Advance) and one
follower's ack (3 nodes), the entry commits. The leader applies it to the exchange state machine
(order id = log index), checks that the entry at that index is the one it proposed (same term,
same bytes), and replies with the result.

### 2. Why is the Raft core written without I/O?
Testability and determinism. The core is a function from (state, message/tick/proposal) to
(state, outputs), so the same code runs in a simulated network where I control delivery,
reordering, loss, duplication, partitions and crashes, and where every step can be checked
against Raft's safety properties. 200 seeds run in about five seconds. It also keeps durability
policy (when to fsync, how to batch) in the caller, where it belongs.

### 3. Prove to me your simulator catches bugs.
I mutated the implementation and checked that the tests fail:
- Removing the log-up-to-date check from voting is caught by the simulator (leader completeness
  violated), but only after half the seeds were run without Pre-Vote. Pre-Vote applies the same
  check and masked the bug.
- Removing the current-term commit rule (Figure 8) is too rare for random faults to hit, so a
  targeted unit test builds the scenario directly and catches it.
- Letting the leader count its unpersisted entries toward a majority is caught by a targeted test.

The lesson: random simulation finds broad classes of bugs, and specific known hazards deserve
directed tests.

### 4. Explain Figure 8 and your defence.
A leader from term 2 replicates entry E (term 2) to a minority and crashes. A term-3 leader
writes a different entry at that index and crashes. The term-2 node comes back as leader in term
4 and replicates E to a majority. If it committed E by counting replicas, the term-3 node could
still win a later election (its last log term, 3, beats 2) and overwrite E: a committed entry
lost. The rule is that a leader commits by counting replicas only entries from its **current**
term, and earlier entries commit indirectly. The leader appends a no-op on election so this
happens immediately.

### 5. What is Pre-Vote and why do you need it?
A node partitioned away keeps timing out and incrementing its term. When it reconnects, its
higher term forces the healthy leader to step down, a pointless election and an availability
hit. With Pre-Vote, a node first asks whether peers would vote for it, without changing any term.
Peers that have heard from a leader recently say no, so it never starts a real election.
`TestSimPreVotePreventsDisruption` shows the isolated node's term stays put and the leader
survives the rejoin.

### 6. What does check-quorum add?
A leader on the minority side of a partition would otherwise keep believing it is leader,
accepting proposals it can never commit and making clients wait. With check-quorum it steps down
after an election timeout without hearing from a majority, so clients fail over to the real
leader. It also enables leader stickiness: followers that recently heard from a leader ignore
vote requests.

### 7. How do you get exactly-once with retries?
Every command carries `(ClientID, Seq)`. The state machine stores each client's last seq and its
result. A committed duplicate (the client retried after the first attempt committed but its reply
was lost) returns the cached result without changing state. In the chaos runs, hundreds of such
duplicate log entries were absorbed with zero double-applications. Clients retry with the same
seq until they get a definite answer; a fresh seq would create a new order.

### 8. Can a client be told "success" for an order that isn't committed?
No. A waiter is resolved only when its index commits, and only if the committed entry has the
term and bytes that were proposed. If leadership changes, the slot might hold another leader's
entry; then the client gets "not leader" and retries, and dedup makes the retry safe. A leader
that loses leadership fails all its waiters immediately.

### 9. Why can the leader send appends before it fsyncs?
Commitment needs a majority of **durable** copies. If the leader replicated first but counted
itself immediately, leader plus one follower could reach "majority" while only the follower's
disk held the entry. A leader crash, a restart without the entry and a vote for a third node
lacking it would lose a committed entry. So the leader counts itself only after `Advance`
confirms its own fsync (the `persisted` index), and the follower acks are durable by
construction. The fsyncs on leader and followers then overlap instead of adding up. This is §10.2.1
of the Raft thesis, and how etcd behaves.

### 10. Why not fsync the commit index?
Raft does not need it to be durable: after a restart the node re-learns commit from the leader.
Fsyncing on every commit advance added one fsync per round trip on both leader and followers. Now
the record is written but rides along with the next fsync.

### 11. What did the linearizability checker actually check?
Two things:
- **Witness check (primary, O(n log n)).** All replicas hold the same applied log. Every
  acknowledged op appears exactly once, with the result the client saw. If op A returned before
  B was invoked, then A's log index is below B's. Since the log order is a sequential order
  consistent with every result, it is a linearization.
- **Porcupine (independent).** The exchange itself is the sequential model, and an op's output
  includes its order id (= log index). It passes on the CI chaos test and the low-concurrency runs,
  but times out (120 s) on 16-64-client histories of ~25k ops, because the search over concurrent
  ops grows quickly. I report the timeout as a timeout, not a pass.

### 12. What do you measure for failover, and what drives it?
For each leader kill -9 or isolation: the time until another node reports itself leader, and
the client-observed gap between the last ack before the fault and the first ack after it. Both
are dominated by the election timeout: 15 ticks of 10 ms base, randomized in [150, 300) ms, plus
one election round. A shorter timeout fails over faster but risks spurious elections under GC
pauses or network jitter. Production systems also use leader leases and faster failure detection
(e.g. kernel-level liveness via the load balancer).

### 13. Why is throughput with fsync so much lower than without?
macOS `File.Sync` issues `F_FULLFSYNC`, a real flush through the drive's write cache, measured at
about 3.6 ms. Every commit needs the leader's and a follower's fsync. Group commit is what scales:
more concurrent clients means more proposals per fsync, which is why ops/s rises with clients
while latency stays flat. On Linux with `fdatasync` on NVMe, or on battery-backed storage, the
fsync term shrinks by an order of magnitude. Without fsync the cluster does ~26k ops/s.

### 14. What breaks at 10× load?
- **The client protocol.** HTTP/JSON per order costs tens of µs of CPU, so a binary pipelined
  protocol would come first.
- **Unbounded log.** Without snapshots the log grows, and restart replays it in full. Snapshots
  of the exchange state plus log compaction fix both.
- **gob encoding** of Raft messages is convenient but allocation-heavy.
- **Single-group capacity.** One Raft group's throughput is bounded by the leader's fsync and
  network, so the next step is sharding instruments across Raft groups (one sequencer per shard).

### 15. Why not just use etcd's raft library?
For a production system, I would: it is battle-tested. Writing it from scratch was the point here:
I can defend every rule (why the no-op, why Pre-Vote, why the persisted-index rule) and show
tests that fail when each one is removed.

### 16. How would you add a hot standby with zero lost acks in a primary/backup design instead?
The primary sequences and multicasts each input to backups. It releases the ack to the client
only after at least one backup acks the input. On primary failure, the backup with the highest
acked sequence takes over. Fencing (STONITH or epochs) prevents a zombie primary from accepting
orders. Raft automates the election and fencing (terms are epochs) at the cost of a quorum.

### 17. Did the checkers ever catch anything real?
Yes, though not in raftex itself. An aborted demo run (its stdout pipe closed) killed the harness
before its cleanup, leaving three orphaned node processes on the demo ports. The next run's nodes
failed to bind, and its clients and proxies ended up talking to the orphans, which had stale
state. Porcupine reported **NOT linearizable**, and the duplicate-entry count was implausible.
The checkers did their job on a broken environment. The harness now refuses to start if any node
port is taken and ignores SIGPIPE so cleanup always runs.
