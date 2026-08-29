# Coordination Primitives Design

Leader election, leases, and fencing tokens for an active/standby controller.

## Design Goals

A control plane that dispatches commands to external devices needs two properties from its log. A command is durable in a quorum before the dispatch, and a deposed leader cannot dispatch at all. The second property is the hard one. A leader that loses its lease does not know it lost the lease, so it cannot fence itself. Something outside the leader must refuse its writes.

Teams build this today from a compacted topic and a transactional-id convention. The common fault is to treat the lease deadline as the safety boundary. A lease is a statement about two clocks. A paused process, a slow disk, or a clock step lets two nodes each believe they hold the lease. If the lease is the only guard, both write.

This subsystem exists to make the safety boundary a property the cluster enforces, and to leave the lease with the job it can actually do.

## Architecture Overview

The leadership epoch is the producer epoch that the transaction coordinator mints for `transactional.id = <role>`.

That one decision supplies every property the API promises:

- The coordinator writes the epoch to `__transaction_state`, which is replicated, so the epoch is quorum-minted.
- The coordinator advances the epoch on every `InitProducerId` call and never reuses one, so the epoch is monotonic.
- The broker rejects a write that carries a superseded epoch. It answers `INVALID_PRODUCER_EPOCH` or `PRODUCER_FENCED`. The cluster fences a deposed leader, and the leader does not have to fence itself.
- `DescribeTransactions` reports the current `producer_id` and `producer_epoch` for a transactional id. Any process can call it. A third party verifies a writer's authority with one request and no membership.

Per-role state lives in the compacted topic `__coordination_state`. A role's records all go to one partition, so they are totally ordered. The topic carries two record kinds and the key discriminates them: a registration, and a lease.

## Key Design Decisions

### The epoch is the safety mechanism and the lease is not

The epoch is enforced by the broker. Under any clock skew, any network partition, and any pause, two holders cannot both write, because the broker rejects the older epoch.

The lease is a liveness and anti-flap device only. It decides when a standby should challenge. A wrong lease makes the failover early or late. A wrong lease never makes two writers authoritative.

Read that inversion carefully against a hand-rolled design, where the lease deadline decides who may write. This design never asks the lease that question.

The practical consequence for a caller is that a leader does not have to prove its lease is live before each write. The write itself carries the proof, and the broker checks it.

### The lease record is written inside a transaction under the role's own epoch

A lease record authenticates itself. The broker fences any writer that does not hold the current epoch. A lease record that reached the log is proof that its author held the epoch.

Registration records are ordinary appends, because a candidate holds no epoch and must not take one to announce itself. A registration carries no authority, and the design does not let it claim any.

Readers use `read_committed`, so an aborted lease write is invisible.

### The fencing token is a pair and the comparison is lexicographic

The producer epoch is an `i16` and it wraps. Kafka handles exhaustion by allocating a fresh producer id and resetting the epoch to zero. An implementation that compares the epoch alone accepts a stale writer after about 32000 leadership changes.

The token is `(producer_id, producer_epoch)` and the ordering compares the producer id first. Kafka allocates producer ids from a monotonic block allocator, so the pair stays monotonic where the epoch alone does not.

### Rank comes from the log, not from configuration

The succession order is the order in which candidates registered. A registration record's offset in the partition is its join sequence. Log compaction keeps the offset of a record it retains. A reader that walks the partition in offset order gets the registration order.

A recovered node re-registers, so it gets a higher offset and lands at the tail. That is the no-failback rule, and it needs no counter and no coordinator.

A configured rank would produce the opposite behaviour. The recovered node would carry its old rank, preempt the node that replaced it, and cause a second failover that nothing needed.

### The challenge is staggered by rank, and the stagger is not what makes it correct

A standby of rank `n` challenges at `deadline + n * challenge_stagger`. The live standby closest to the front wins, and the standbys behind it never enter the race.

The stagger is an optimisation. `InitProducerId` is atomic at the coordinator, so a simultaneous challenge by every standby still produces exactly one winner. The losers learn that they lost on their first fenced write. The stagger saves the epoch churn that such a race causes. It does not supply the safety.

## Integration

The client builds on three existing layers and adds no wire message.

`krabka-client-producer` mints the epoch. A producer built with `transactional_id = <role>` calls `init_transactions`, which sends `InitProducerId` to the coordinator and stores the returned identity. The same producer is then bound to that epoch. The leadership handle gives it to the caller, so the broker fences every write the caller makes.

`krabka-client-admin` answers the third-party question with `describe_transaction`.

`krabka-client-core` supplies the coordinator lookup and the committed-read fetch that the state reader uses.

A role needs two producers. The transactional producer writes lease records under the role's epoch. A second, plain producer appends registrations, because Kafka requires every send from a transactional producer to sit inside a transaction.

## Kafka Compliance

The design adds no message and changes no wire format. It composes `FindCoordinator`, `InitProducerId` (KIP-98), `DescribeTransactions` (KIP-664), `Produce`, and `Fetch`. It runs against Apache Kafka as well as against a krabka broker.

`transaction_timeout_ms` on `InitProducerId` carries the lease duration. The coordinator then aborts a dead holder's open transaction without waiting for a client, which bounds how long an interrupted command sequence stays open.

The topic needs `cleanup.policy=compact`. The client writes with `acks=all`. An operator should set `min.insync.replicas` to at least 2. The claim "durable in a quorum before the dispatch" rests on the topic configuration, not on this client.

## Clock Assumptions

The lease deadline is a wall-clock instant that the holder writes and a challenger compares against its own clock. Skew between the two shifts the failover time by the size of the skew. It does not affect safety, for the reason the first decision above gives.

An operator who needs a bound on failover time needs a bound on skew. Proposal 8 in the krabka feature proposals treats clock confidence as a first-class signal for exactly this reason.

## Testing

The record codec carries property tests for the encode and decode round trip. It also carries golden byte vectors that pin the frozen layout. `krabka-streams-java` and `krabka-streams-go` assert the same vectors. A change to one implementation that the other does not follow fails a test rather than reaching a cluster.

The decoder reads bytes that arrive from the network. A property test drives it with arbitrary input. The test asserts that the decoder returns an error and does not panic. Table-driven tests cover a truncated buffer, trailing bytes, a negative string length, an unknown version, and an unknown kind.

The succession rules carry an exhaustive model check written with `stateright`. This repository already uses that tool for the producer failover and the consumer lock order. The model runs several candidates against a coordinator that mints strictly increasing epochs, and it adds crashes and recoveries. It asserts the two properties the design rests on. At most one member holds an unfenced epoch for a role, and a recovered member never preempts the current holder.

The end-to-end tests that need a live broker follow the convention of this repository. They are marked `#[ignore]` and they run with `--include-ignored` where Docker is available.
