//! Exhaustive stateright model of the classic consumer's async-Mutex lock
//! protocol.
//!
//! The model settles whether the `poll()` ↔ coordinator-task lock sequence can
//! deadlock through a lock-order cycle, or is provably deadlock-free.
//!
//! ## Why this model exists
//!
//! A WAL consumer, the logs-compactor, hangs at cold start with an
//! idle-runtime / lost-wakeup signature. There are two hypotheses: (a) a
//! lock-order deadlock between the poll loop and the background coordinator
//! task, or (b) a lost wakeup that is *not* a lock cycle. This model formally
//! rules one of them in. If the protocol is deadlock-free under exhaustive
//! search, the investigation should redirect to the lost-wakeup path.
//!
//! ## Fidelity (the krabka stateright program's cardinal rule)
//!
//! Every modeled lock edge comes from the real source. The citations below use
//! **function + lock variable**. Bare line numbers drift whenever someone adds
//! code above a lock site, so this file anchors to stable names that an auditor
//! can `grep`. Nothing here is invented. The model abstracts away *values* such
//! as offsets, partitions, and RPC payloads, and it abstracts away the network.
//! It keeps only what a deadlock can depend on: **which task holds which
//! `tokio::sync::Mutex` and where each task is suspended.**
//!
//! ## The nine shared `tokio::sync::Mutex`es
//!
//! `Consumer` declares them in `consumer.rs`, in the `Consumer` mutex fields,
//! and shares them into `CoordinatorState` with `Arc::clone`:
//!
//! | id | field          | abbrev |
//! |----|----------------|--------|
//! | 0  | `assigned`     | A      |
//! | 1  | `next_offsets` | N      |
//! | 2  | `positions`    | P      |
//! | 3  | `topic_ids`    | T      |
//! | 4  | `commit_identity` | CI  |
//! | 5  | `commit_serialization` | CS |
//! | 6  | `end_offsets`  | E      |
//! | 7  | `AutoCommit::polled` | AP |
//! | 8  | `AutoCommit::next_due` | ND |
//!
//! ## Modeled lock-holding regions (sequences where >1 guard is alive at once,
//! plus single-lock regions for completeness). Citations are to the real code.
//!
//! `commit_serialization` is intentionally held across commit RPCs and rebalance
//! waits. The coordinator takes it only in `commit_before_join`, while it holds
//! no other lock. All other guards are dropped before RPCs. The model abstracts
//! network waits and keeps every nested mutex acquisition, because only those
//! acquisitions can form lock-order cycles.
//!
//! A commit that holds `commit_serialization` can wait for a rebalance
//! (`commit_pending_offsets`, the deferred response). That is a wait for a
//! notification, not for a lock, so this model does not show it. The commit
//! marks that wait with `AutoCommit::park`, and `commit_before_join` then stops
//! its wait for the lock (`commit_turn`). The rebalance timeout also bounds that
//! wait.
//!
//! ### seek task (`seek.rs`)
//! - `seek_to_position` and `request_offset_reset`: **A → N → P** held
//!   together, all released. Region edges: A→N, N→P.
//!   `position` (`partition_state.rs`) takes A alone, then N→P, then the
//!   regions of `update_fetch_positions` that the poll task lists below.
//!   `current_lag` (`queries.rs`) holds A→N, then takes E alone.
//!   `pause`, `resume` and `paused` take CI alone. The paused set is a
//!   `std::sync::Mutex` that no region holds while it takes another lock.
//!
//! ### poll task (`poll.rs`, `validate.rs`, `commit.rs`)
//! - `maybe_auto_commit_async` (commit.rs): CI, N and P each alone
//!   (`consumed_positions`), then AP alone, then ND. While it holds ND it tries
//!   CS with `try_lock_owned`, which never waits, so the model has no ND → CS
//!   acquire. It hands the CS guard to the spawned auto commit task, which
//!   takes **CS → ND** after a retriable failure.
//! - `refresh_leader_epochs` (validate.rs): **P alone** (after the metadata
//!   `.await`), released; then **T alone** (the tracked `topic_ids` update).
//! - `resolve_reset_sentinels` (poll.rs): **N alone** for the sentinel
//!   snapshot, released; then **P alone** in `list_offsets` for the leader
//!   routes, released before the `ListOffsets` `.await`; then **N alone** to
//!   apply the offsets. No region takes a second lock.
//! - `validate_positions` (validate.rs): **N→P** snapshot held together,
//!   released before the RPC; then **P alone** in the post-RPC apply.
//!   `apply_truncation` (poll.rs) then holds N and takes AP
//!   (`AutoCommit::reset_polled`): **N→AP**.
//! - `poll` fetch-build (poll.rs, `group_fetches`): **CI alone** for the
//!   ownership of the paused partitions, then the `by_leader` snapshot:
//!   **N→P** held together, released before the Fetch `.await`.
//! - `poll` post-fetch loop (poll.rs): A `assigned.clone()` (released) → **N
//!   held across the whole processing loop**, and inside it P is acquired
//!   *second* at each per-partition site, that is **N→P every time** ("offsets
//!   is already locked, positions acquired second"). VERIFIED: there is **no
//!   P→N inversion** on the post-fetch path. N released before the metadata
//!   refresh `.await`. Updating the fetched high watermark adds **N→E**.
//!   Before N is released, `AutoCommit::reset_polled` adds **N→AP**.
//!   After N is released, `recover_out_of_range` takes **P alone** for the
//!   leader routes, and then **N→AP** to apply the log starts.
//! - `at_log_end` (consumer.rs): **A→N→E**.
//!
//! ### coordinator task (`coordinator.rs`)
//! - `rejoin`: A alone for assignment snapshots. `publish_assignment` holds
//!   **A→CI** while it atomically publishes assignment and commit identity. It
//!   also runs **N→P** scopes (the eager and
//!   cooperative `next_offsets`→`positions` prunes). A is never held while N or
//!   P is acquired.
//! - `run` after `UNKNOWN_MEMBER_ID`: **CI alone** while clearing identity.
//! - `install_generation` (before a cooperative revoke callback): **CI alone**.
//! - `commit_before_join` (at the start of `join_and_sync`): **CS**, and under
//!   it CI alone, then AP alone. At the end of `join_and_sync`,
//!   `restart_interval` takes ND alone.
//! - `prime_offsets`: T alone (`topic_ids.clone()`), then **N→P**
//!   (`next_offsets`→`positions`).
//! - `join_and_sync` (leader branch): **T alone** (`topic_ids` merge).
//!
//! ### commit task (`commit.rs`)
//! - Every commit holds **CS** for its complete operation. Synchronous commits
//!   initially nest **CI→N** under CS. The asynchronous snapshot takes CI and N
//!   separately under CS. Retry snapshots take CI alone under CS, followed by
//!   P, AP (`AutoCommit::record_sent`) and T alone under CS. The asynchronous
//!   snapshot also takes AP alone under CS. This serializes concurrent commits
//!   without blocking coordinator publication, because the coordinator takes
//!   CS only in `commit_before_join`, before it publishes anything.
//! - `auto_commit_on_close` holds CS, takes CI, N and P each alone under it,
//!   and then runs the synchronous commit regions.
//!
//! ## The lock hierarchy these regions imply
//!
//! Collecting every "hold L1 while acquiring L2" edge actually observed:
//!   N → P, N → E, A → N, A → CI, CS → CI, CI → N, CS → N, CS → P,
//!   CS → T, CS → ND, CS → AP, N → AP.
//! The resulting partial order is acyclic: `A < CI < N < P`,
//! `CS < CI < N < P`, `CS < T`, `CS < ND`, `CS < AP`, and
//! `N < AP`. No region takes a lock while it holds AP.
//! This is acyclic ⇒ the prediction is **deadlock-free**, and the model proves
//! it exhaustively across all task interleavings.

use std::time::Duration;

use stateright::{Checker, Model, Property};

/// Lock identifiers. The order here is incidental. The model assumes no
/// hierarchy and discovers cycles purely from the acquire/release sequences.
const A: u8 = 0; // assigned
const N: u8 = 1; // next_offsets
const P: u8 = 2; // positions
const T: u8 = 3; // topic_ids
const CI: u8 = 4; // commit_identity
const CS: u8 = 5; // commit_serialization
const E: u8 = 6; // end_offsets
const AP: u8 = 7; // AutoCommit::polled
const ND: u8 = 8; // AutoCommit::next_due
const NUM_LOCKS: usize = 9;

/// A single lock operation in a task's program. `Acquire` is a suspension
/// point, a `.lock().await`. `Release` drops a guard at the end of a scope or
/// through an explicit `drop`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Op {
    Acquire(u8),
    Release(u8),
}

use Op::{Acquire, Release};

/// One concurrently-running future. Its `program` is the exact ordered sequence
/// of lock acquisitions and releases from the real code. See the module docs
/// for the function + lock-variable anchor of every step. `pc` is the program
/// counter into it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Task {
    /// Human-readable label of the code region this program models.
    name: &'static str,
    program: Vec<Op>,
    pc: usize,
}

impl Task {
    fn new(name: &'static str, program: Vec<Op>) -> Self {
        Task {
            name,
            program,
            pc: 0,
        }
    }

    fn done(&self) -> bool {
        self.pc >= self.program.len()
    }

    fn next_op(&self) -> Option<Op> {
        self.program.get(self.pc).copied()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    /// `holder[l] == Some(task_idx)` when lock `l` is held by that task.
    holder: [Option<usize>; NUM_LOCKS],
    tasks: Vec<Task>,
}

/// The action the scheduler takes: advance task `idx` by executing its next op.
///
/// The model emits this only when that op is *enabled*, which means a Release,
/// or an Acquire of a lock that is free or already self-held. A blocked Acquire
/// is never enabled, so a task suspended on a contended `.lock().await` cannot
/// step.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Step {
    idx: usize,
}

/// Build the poll task's program, the full sequence of multi-lock regions that
/// a single `poll()` call walks, in source order.
///
/// This function acquires each region's locks in the real nesting order and
/// releases them at the modeled scope end.
///
/// The region boundaries are faithful to the real source and are anchored by
/// function. So are the RPC `.await`s between them, where all guards are
/// already dropped:
///   1. `maybe_auto_commit_async` (commit.rs): CI, N, P, AP, ND (each alone)
///   2. `refresh_leader_epochs` (validate.rs): P  then  T  (each alone)
///   3. `resolve_reset_sentinels` (poll.rs): N, P, N  (each alone)
///   4. `validate_positions` (validate.rs): N, P  then  P  (N→P snapshot, then P alone)
///   5. `poll` fetch-build (poll.rs)      : N, P      (N→P snapshot)
///   6. `poll` post-fetch loop (poll.rs)  : N  then (N,P)…  (N held, P second)
fn poll_program() -> Vec<Op> {
    vec![
        // --- maybe_auto_commit_async (commit.rs): identity, offsets and
        //     positions snapshots, each alone. ---
        Acquire(CI),
        Release(CI),
        Acquire(N),
        Release(N),
        Acquire(P),
        Release(P),
        // polled positions, then the interval deadline, each alone.
        Acquire(AP),
        Release(AP),
        // ND. The `try_lock_owned` on CS under it never waits. The CS guard
        // moves to the spawned task, which `auto_commit_task_program` models.
        Acquire(ND),
        Release(ND),
        // --- refresh_leader_epochs (validate.rs): P alone, then T alone
        //     (`topic_ids` update after `positions` is dropped). ---
        Acquire(P),
        Release(P),
        Acquire(T),
        Release(T),
        // --- resolve_reset_sentinels (poll.rs): N alone (sentinel
        //     snapshot), P alone (`list_offsets` routes), N alone (apply). ---
        Acquire(N),
        Release(N),
        Acquire(P),
        Release(P),
        Acquire(N),
        Release(N),
        // --- validate_positions (validate.rs): N→P snapshot … ---
        Acquire(N),
        Acquire(P),
        Release(P),
        Release(N),
        // … then P alone post-RPC (validate_positions apply).
        Acquire(P),
        Release(P),
        // apply_truncation (poll.rs): N, and under it AP
        // (AutoCommit::reset_polled).
        Acquire(N),
        Acquire(AP),
        Release(AP),
        Release(N),
        // --- poll fetch-build (poll.rs `group_fetches`): CI alone for the
        //     paused partitions, then the `by_leader` snapshot: N→P, dropped
        //     before the Fetch. ---
        Acquire(CI),
        Release(CI),
        Acquire(N),
        Acquire(P),
        Release(P),
        Release(N),
        // --- poll post-fetch (poll.rs): A snapshot (`assigned.clone()`),
        //     released at stmt end. ---
        Acquire(A),
        Release(A),
        // N held across the processing loop; inside it P is acquired SECOND at
        // each per-partition site — N→P, never P→N. Model one representative
        // nested P acquire/release inside the held-N region.
        Acquire(N),
        Acquire(P),
        Release(P),
        Acquire(E),
        Release(E),
        // AutoCommit::reset_polled before the offsets guard drops.
        Acquire(AP),
        Release(AP),
        Release(N),
        // recover_out_of_range (poll.rs), after N is released: P alone
        // (`list_offsets` routes), then N and under it AP (apply the log
        // starts, AutoCommit::reset_polled).
        Acquire(P),
        Release(P),
        Acquire(N),
        Acquire(AP),
        Release(AP),
        Release(N),
    ]
}

/// `seek_to_position` (seek.rs): `assigned`, `next_offsets` and `positions`
/// held together.
fn seek_program() -> Vec<Op> {
    vec![
        Acquire(A),
        Acquire(N),
        Acquire(P),
        Release(P),
        Release(N),
        Release(A),
    ]
}

fn at_log_end_program() -> Vec<Op> {
    vec![
        Acquire(A),
        Acquire(N),
        Acquire(E),
        Release(E),
        Release(N),
        Release(A),
    ]
}

/// The coordinator task's `rejoin` program. A is only ever held alone. The N→P
/// regions do the offset and position prune. This models the cooperative
/// phase-1 path, the most lock-dense one, which also subsumes the eager path's
/// edges.
///
/// Sequence (coordinator.rs `rejoin`, cooperative-revoke path):
///   A alone (`rejoin`: `assigned.clone()` owned snapshot)
///   [`commit_before_join` → CI alone ; AP alone]
///   [`join_and_sync` → T alone (`topic_ids` merge) ; ND alone (`restart_interval`)]
///   A→CI (`publish_assignment`: phase-1 assignment + commit identity)
///   N,P prune (`rejoin`: phase-1 `next_offsets`→`positions` remove)
///   A alone (`rejoin`: `owned_after_revoke` `assigned.clone()` snapshot)
///   [`prime_offsets` → T alone (`topic_ids.clone()`) ; N,P (`next_offsets`→`positions`)]
///   A→CI (`publish_assignment`: phase-2 assignment + commit identity)
fn coordinator_program() -> Vec<Op> {
    vec![
        // rejoin: owned snapshot (`assigned.clone()`)
        Acquire(A),
        Release(A),
        // commit_before_join: the commit lock, and under it the identity
        // snapshot, then the polled positions.
        Acquire(CS),
        Acquire(CI),
        Release(CI),
        Acquire(AP),
        Release(AP),
        Release(CS),
        // join_and_sync → topic_ids merge (leader branch, T alone)
        Acquire(T),
        Release(T),
        // join_and_sync: restart_interval (ND alone)
        Acquire(ND),
        Release(ND),
        // publish_assignment: assigned → commit_identity
        Acquire(A),
        Acquire(CI),
        Release(CI),
        Release(A),
        // rejoin: phase-1 prune next_offsets + positions, N→P
        Acquire(N),
        Acquire(P),
        Release(P),
        Release(N),
        // rejoin: owned_after_revoke snapshot (`assigned.clone()`)
        Acquire(A),
        Release(A),
        // prime_offsets: topic_ids snapshot (`topic_ids.clone()`)
        Acquire(T),
        Release(T),
        // prime_offsets: N→P (`next_offsets`→`positions`)
        Acquire(N),
        Acquire(P),
        Release(P),
        Release(N),
        // publish_assignment: assigned → commit_identity
        Acquire(A),
        Acquire(CI),
        Release(CI),
        Release(A),
        // run: UNKNOWN_MEMBER_ID clears commit_identity alone.
        Acquire(CI),
        Release(CI),
    ]
}

/// A synchronous commit task. CS spans the whole operation. The initial
/// validation/snapshot holds CI→N. Each attempt then snapshots CI, P, and T in
/// separate regions, and the deferred-response path snapshots CI once more.
fn commit_program() -> Vec<Op> {
    vec![
        Acquire(CS),
        // commit_sync / commit_offsets_sync initial ownership + offset snapshot.
        Acquire(CI),
        Acquire(N),
        Release(N),
        Release(CI),
        // commit_pending_offsets identity snapshot.
        Acquire(CI),
        Release(CI),
        // positions snapshot.
        Acquire(P),
        Release(P),
        // AutoCommit::record_sent raises the polled positions.
        Acquire(AP),
        Release(AP),
        // topic_ids snapshot.
        Acquire(T),
        Release(T),
        // Deferred response ownership/identity snapshot.
        Acquire(CI),
        Release(CI),
        Release(CS),
    ]
}

/// The asynchronous commit task. CS spans snapshot, RPC, and coordinator
/// re-find. `snapshot_commit_topics` clones CI before acquiring N, so these are
/// separate regions; P and T are likewise acquired alone under CS.
fn async_commit_program() -> Vec<Op> {
    vec![
        Acquire(CS),
        Acquire(CI),
        Release(CI),
        Acquire(N),
        Release(N),
        Acquire(P),
        Release(P),
        // snapshot_commit_topics: AutoCommit::record_sent.
        Acquire(AP),
        Release(AP),
        Acquire(T),
        Release(T),
        Release(CS),
    ]
}

/// The task that `maybe_auto_commit_async` spawns. It holds CS for the RPC and
/// sets ND after a retriable failure.
fn auto_commit_task_program() -> Vec<Op> {
    vec![Acquire(CS), Acquire(ND), Release(ND), Release(CS)]
}

/// `auto_commit_on_close`. CS spans the snapshot and the synchronous commit.
fn close_auto_commit_program() -> Vec<Op> {
    vec![
        Acquire(CS),
        // consumed_positions: CI, N and P each alone.
        Acquire(CI),
        Release(CI),
        Acquire(N),
        Release(N),
        Acquire(P),
        Release(P),
        // commit_pending_offsets identity and positions snapshots, then
        // AutoCommit::record_sent.
        Acquire(CI),
        Release(CI),
        Acquire(P),
        Release(P),
        Acquire(AP),
        Release(AP),
        Release(CS),
    ]
}

#[derive(Clone, Debug)]
struct LockOrderModel {
    programs: Vec<(&'static str, Vec<Op>)>,
}

impl Model for LockOrderModel {
    type State = State;
    type Action = Step;

    fn init_states(&self) -> Vec<Self::State> {
        let tasks = self
            .programs
            .iter()
            .map(|(name, prog)| Task::new(name, prog.clone()))
            .collect();
        vec![State {
            holder: [None; NUM_LOCKS],
            tasks,
        }]
    }

    fn actions(&self, s: &Self::State, acts: &mut Vec<Self::Action>) {
        for (idx, task) in s.tasks.iter().enumerate() {
            let Some(op) = task.next_op() else { continue };
            let enabled = match op {
                Op::Release(_) => true,
                Op::Acquire(l) => match s.holder[l as usize] {
                    // Free, or (impossible here) already self-held → can proceed.
                    None => true,
                    Some(h) => h == idx,
                    // Held by another task → BLOCKED: not enabled. This is the
                    // task suspended at a contended `.lock().await`.
                },
            };
            if enabled {
                acts.push(Step { idx });
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        let idx = action.idx;
        let op = s.tasks[idx].next_op()?;
        match op {
            Op::Acquire(l) => {
                match s.holder[l as usize] {
                    None => s.holder[l as usize] = Some(idx),
                    Some(h) if h == idx => {} // self-held no-op (not reachable here)
                    Some(_) => return None,   // blocked: not a legal step
                }
            }
            Op::Release(l) => {
                // Faithful: a task only releases a lock it holds.
                if s.holder[l as usize] != Some(idx) {
                    return None;
                }
                s.holder[l as usize] = None;
            }
        }
        s.tasks[idx].pc += 1;
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // THE property: no deadlock. A deadlock is a reachable state with at
            // least one unfinished task where EVERY unfinished task is blocked
            // (its next op is an Acquire of a lock held by a *different* task) —
            // a global wait cycle. We assert it can never happen.
            Property::always("no_deadlock", |_, s: &State| !is_deadlocked(s)),
            // Liveness sanity: the all-done terminal is reachable (proves the
            // programs can run to completion, so "no_deadlock" isn't vacuous).
            Property::sometimes("all_tasks_complete", |_, s: &State| {
                s.tasks.iter().all(Task::done)
            }),
            // Sanity: contention actually occurs (some lock is held while
            // another task wants it) — proves the interleaving is non-trivial.
            Property::sometimes("contention_observed", |_, s: &State| any_task_blocked(s)),
        ]
    }
}

/// A task is *blocked* iff its next op is an `Acquire` of a lock that a
/// different task currently holds. The task is then suspended at a contended
/// `.lock().await`.
fn task_blocked(s: &State, idx: usize) -> bool {
    match s.tasks[idx].next_op() {
        Some(Op::Acquire(l)) => matches!(s.holder[l as usize], Some(h) if h != idx),
        _ => false,
    }
}

fn any_task_blocked(s: &State) -> bool {
    (0..s.tasks.len()).any(|i| task_blocked(s, i))
}

/// Deadlock = at least one unfinished task, and every unfinished task is
/// blocked. If any unfinished task can still step, the system makes progress.
fn is_deadlocked(s: &State) -> bool {
    let unfinished: Vec<usize> = (0..s.tasks.len()).filter(|&i| !s.tasks[i].done()).collect();
    !unfinished.is_empty() && unfinished.iter().all(|&i| task_blocked(s, i))
}

const MAX_STATES: usize = 5_000_000;
const MAX_DEPTH: usize = 256;
const CHECK_TIMEOUT: Duration = Duration::from_mins(1);

fn run_model(programs: Vec<(&'static str, Vec<Op>)>) -> stateright::CheckerBuilder<LockOrderModel> {
    LockOrderModel { programs }
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .timeout(CHECK_TIMEOUT)
}

#[cfg(test)]
mod tests {

    use super::*;

    /// MAIN RESULT: the real classic-consumer lock protocol is DEADLOCK-FREE
    /// under exhaustive interleaving. The protocol here is the poll task, the
    /// coordinator task, and concurrent sync/async commit tasks, each running
    /// its extracted acquire/release sequence. The two commit programs make
    /// contention on `commit_serialization` reachable.
    #[test]
    fn classic_consumer_lock_protocol_is_deadlock_free() {
        let checker = run_model(vec![
            ("poll", poll_program()),
            ("at-log-end", at_log_end_program()),
            ("coordinator", coordinator_program()),
            ("commit-sync", commit_program()),
            ("commit-async", async_commit_program()),
        ])
        .spawn_bfs()
        .join();
        eprintln!(
            "[lock_order] unique={} generated={} depth={}",
            checker.unique_state_count(),
            checker.state_count(),
            checker.max_depth(),
        );
        // The bound must NOT have been hit, or "deadlock-free" would be a
        // statement about a truncated space, not the whole one.
        assert2::assert!(checker.state_count() < MAX_STATES);
        assert2::assert!(checker.max_depth() < MAX_DEPTH);
        checker.assert_properties();
    }

    /// The auto commit tasks race the poll task, the coordinator task and an
    /// asynchronous commit. The spawned auto commit and the auto commit of
    /// `close` hold `commit_serialization`, and the coordinator reads the
    /// polled positions. The protocol is still deadlock-free.
    #[test]
    fn auto_commit_tasks_are_deadlock_free() {
        let checker = run_model(vec![
            ("poll", poll_program()),
            ("coordinator", coordinator_program()),
            ("commit-async", async_commit_program()),
            ("auto-commit", auto_commit_task_program()),
            ("close-auto-commit", close_auto_commit_program()),
        ])
        .spawn_bfs()
        .join();
        eprintln!(
            "[lock_order/auto-commit] unique={} generated={} depth={}",
            checker.unique_state_count(),
            checker.state_count(),
            checker.max_depth(),
        );
        assert2::assert!(checker.state_count() < MAX_STATES);
        assert2::assert!(checker.max_depth() < MAX_DEPTH);
        checker.assert_properties();
    }

    /// A `seek` races the poll task, the coordinator task and a synchronous
    /// commit. The protocol is still deadlock-free.
    #[test]
    fn seek_is_deadlock_free() {
        let checker = run_model(vec![
            ("poll", poll_program()),
            ("seek", seek_program()),
            ("coordinator", coordinator_program()),
            ("commit-sync", commit_program()),
        ])
        .spawn_bfs()
        .join();
        eprintln!(
            "[lock_order/seek] unique={} generated={} depth={}",
            checker.unique_state_count(),
            checker.state_count(),
            checker.max_depth(),
        );
        assert2::assert!(checker.state_count() < MAX_STATES);
        assert2::assert!(checker.max_depth() < MAX_DEPTH);
        checker.assert_properties();
    }

    /// Two poll tasks race the coordinator, for example in a buggy double-poll.
    /// The protocol is still deadlock-free, because every region respects the
    /// same N<P order.
    #[test]
    fn two_pollers_and_coordinator_are_deadlock_free() {
        let checker = run_model(vec![
            ("poll-a", poll_program()),
            ("poll-b", poll_program()),
            ("coordinator", coordinator_program()),
        ])
        .spawn_bfs()
        .join();
        eprintln!(
            "[lock_order/2poll] unique={} generated={} depth={}",
            checker.unique_state_count(),
            checker.state_count(),
            checker.max_depth(),
        );
        assert2::assert!(checker.state_count() < MAX_STATES);
        assert2::assert!(checker.max_depth() < MAX_DEPTH);
        checker.assert_properties();
    }

    /// NEGATIVE CONTROL / falsification check: inject the hypothesized P→N
    /// inversion into a second task and confirm that the model DOES find the
    /// deadlock. The investigation feared that inversion on the post-fetch path.
    /// This proves that `no_deadlock` is a real, falsifiable property and not a
    /// model that can never fail. It also shows exactly the cycle that the real
    /// code avoids.
    #[test]
    fn injected_inversion_is_detected_as_deadlock() {
        // Task X: hold N, then acquire P  (the real order, N→P).
        let n_then_p = vec![Acquire(N), Acquire(P), Release(P), Release(N)];
        // Task Y: hold P, then acquire N  (the INVERSION, P→N — does NOT exist
        // in the real code; injected here only to validate the checker).
        let p_then_n = vec![Acquire(P), Acquire(N), Release(N), Release(P)];
        let checker = run_model(vec![("np", n_then_p), ("pn", p_then_n)])
            .spawn_bfs()
            .join();
        // The interleaving N-holds-N, P-holds-P, each then blocked on the other
        // is a genuine cycle; the checker must surface a `no_deadlock`
        // counterexample.
        assert2::assert!(checker.discoveries().contains_key("no_deadlock"));
    }

    /// Unit-level sanity for the deadlock predicate itself.
    #[test]
    fn deadlock_predicate_recognizes_a_mutual_wait_cycle() {
        // Two tasks, each holding one lock and next wanting the other's.
        let mut s = State {
            holder: [None; NUM_LOCKS],
            tasks: vec![
                Task::new("x", vec![Acquire(N), Acquire(P)]),
                Task::new("y", vec![Acquire(P), Acquire(N)]),
            ],
        };
        // x holds N (pc past its Acquire(N)); y holds P.
        s.holder[N as usize] = Some(0);
        s.tasks[0].pc = 1; // next op = Acquire(P)
        s.holder[P as usize] = Some(1);
        s.tasks[1].pc = 1; // next op = Acquire(N)
        assert2::assert!(is_deadlocked(&s));

        // Progress case: x's next op is a Release (it can step), so the system
        // is NOT deadlocked even though y is currently blocked.
        let mut live = State {
            holder: [None; NUM_LOCKS],
            tasks: vec![
                Task::new("x", vec![Acquire(N), Release(N), Acquire(P)]),
                Task::new("y", vec![Acquire(P), Acquire(N)]),
            ],
        };
        live.holder[N as usize] = Some(0);
        live.tasks[0].pc = 1; // x next op = Release(N): steppable
        live.holder[P as usize] = Some(1);
        live.tasks[1].pc = 1; // y next op = Acquire(N): blocked
        assert2::assert!(!is_deadlocked(&live));

        // Terminal-but-complete: all tasks done ⇒ not a deadlock (programs are
        // balanced, so a done task holds no locks).
        let all_done = State {
            holder: [None; NUM_LOCKS],
            tasks: vec![Task::new("x", vec![Acquire(N), Release(N)])],
        };
        let mut done = all_done;
        done.tasks[0].pc = 2; // done
        assert2::assert!(!is_deadlocked(&done));
    }
}
