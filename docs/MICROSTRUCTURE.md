# Why exchanges replicate a sequencer

## The sequenced log is the exchange
A matching engine is deterministic: given the same ordered inputs, it produces the same trades.
The source of truth for an exchange is therefore the **sequenced input log**, not the order book.
The book, trades, market data and clearing records are all functions of it. Every serious venue
(equities, futures, FX, crypto) is built around a sequencer that assigns a total order to every
input and hands that order to the matching engine and to every downstream consumer.

## What "fault tolerant" must mean for an exchange
1. **No acknowledged order is lost.** Once a participant sees an ack (or a fill), that order is
   part of history. Losing it on failover means positions and books that disagree with what
   participants were told.
2. **No order is applied twice.** A participant that retries after a timeout must not end up
   with twice the position. Exchanges solve this with client order ids (FIX `ClOrdID`) and
   per-session sequence numbers; raftex uses `(ClientID, Seq)` with result caching.
3. **One timeline.** Two sequencers must never both accept orders (split brain), because the
   two histories cannot be merged. Consensus gives a single leader per term, and check-quorum
   makes a leader cut off from the majority stop accepting orders.
4. **Bounded failover time.** While the sequencer is unavailable the market is effectively
   halted. Real venues fail over in milliseconds to seconds, usually to a hot standby running
   the same log.

## How real venues do it
- **Primary/backup with a replicated log**: the primary sequences, and backups receive every
  message (often over reliable multicast) and ack before the primary releases the ack. LMAX,
  Nasdaq INET-derived systems and many crypto venues use variants of this.
- **Consensus (Raft/Paxos)**: the same guarantees with automatic leader election; used by newer
  venues, crypto exchanges and many internal order-management systems. The cost is a quorum round
  trip and an fsync on the critical path.
- **Determinism everywhere**: every replica runs the same engine on the same log, so a backup's
  state is identical and failover needs no state transfer. raftex's state hash checks exactly
  this.

## Latency budget
A replicated order costs at least: one network round trip leader → follower, plus a durable
write on a majority. In a colocated datacenter that is ~10-50 µs of network, plus NVMe write
latency or battery-backed RAM. On this laptop the dominant term is macOS `F_FULLFSYNC` (about
3.6 ms per call, measured), which is why the benchmarks report both fsync and no-fsync numbers.
Group commit (one fsync for every proposal that arrived together) turns that fixed cost into
throughput, and the latency stays roughly constant as concurrency rises.
