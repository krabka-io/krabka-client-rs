//! An exhaustive model of role succession, checked with `stateright`.
//!
//! The model runs [`super::evaluate`] itself. It does not restate the rules. On
//! every step it builds a [`RoleState`] from the model state, asks `evaluate`
//! what each member does, and then applies that decision against a coordinator
//! and a broker fence.
//!
//! The cluster in the model has three candidates, a transaction coordinator
//! that mints strictly increasing epochs, and a broker that rejects a write
//! from a member whose epoch is not the newest minted one. A candidate can
//! crash at any point. A crashed candidate loses its producer session. Its
//! registration stays until the coordinator expires it, so the model covers a
//! short pause that keeps the rank and a long outage that loses it.
//!
//! Docker is not usable in this environment, so the broker-backed tests cannot
//! run here. This model is the correctness evidence in their place.

use std::time::Duration;

use krabka_units::secs;
use stateright::{Checker, Model, Property};

use super::{Decision, RoleState, RosterEntry, evaluate};
use crate::{
    lease::LeaseConfig,
    record::{FencingToken, Lease, MemberId},
};

/// The candidates of the modelled role.
const MEMBERS: usize = 3;
/// One model tick, in milliseconds. The lease extents are whole ticks.
const TICK_MILLIS: i64 = 1_000;
/// The lease duration, in ticks.
const LEASE_TICKS: u8 = 2;
/// The renew interval, in ticks.
const RENEW_TICKS: u8 = 1;
/// The challenge stagger, in ticks.
const STAGGER_TICKS: u8 = 1;
/// The producer id that the modelled coordinator hands out.
const PRODUCER_ID: i64 = 1;

/// The last tick the model reaches. Two lease generations fit inside it.
const MAX_TICK: u8 = 4;
/// The number of epochs the modelled coordinator mints.
const MAX_EPOCH: u8 = 3;
/// The last registration offset the model appends. Every candidate registers,
/// and every crashed candidate rejoins at the tail.
const MAX_OFFSET: u8 = 4;
/// The number of crashes the model injects.
const MAX_CRASHES: u8 = 2;

/// The caps that prove the search was exhaustive. The model reaches depth 22
/// and generates about 3.5 million states, so neither cap truncates the search.
/// The test asserts that, because a truncated search proves nothing.
const MAX_DEPTH: usize = 40;
const MAX_STATES: usize = 8_000_000;
const CHECK_TIMEOUT: Duration = Duration::from_mins(5);

/// One candidate of the role.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Candidate {
    /// The process runs.
    up: bool,
    /// The offset of the registration record, and `None` when the candidate is
    /// not in the roster.
    offset: Option<u8>,
    /// The tick the candidate registered on.
    registered_at: u8,
    /// The epoch the coordinator last minted for this candidate, and `None`
    /// when it holds no producer session.
    epoch: Option<u8>,
    /// The candidate minted an epoch and has not written the lease yet.
    pending_lease_write: bool,
}

impl Candidate {
    const fn down() -> Self {
        Self {
            up: false,
            offset: None,
            registered_at: 0,
            epoch: None,
            pending_lease_write: false,
        }
    }

    const fn start() -> Self {
        Self {
            up: true,
            ..Self::down()
        }
    }

    const fn registered(self) -> bool {
        self.up && self.offset.is_some()
    }
}

/// The lease record of the role, as the model stores it.
///
/// The model does not store the grant instant. `evaluate` reads the member and
/// the deadline of a lease and nothing else, so a separate grant instant would
/// only add states that no decision can tell apart.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct LeaseState {
    holder: u8,
    epoch: u8,
    deadline: u8,
}

/// The whole modelled cluster.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct State {
    /// The current tick. Every process reads the same clock, because clock skew
    /// only shifts the failover time and this model checks safety.
    now: u8,
    /// The offset the next registration record takes.
    next_offset: u8,
    /// The highest epoch the coordinator minted. Zero means it minted none.
    minted: u8,
    /// The number of crashes the model injected.
    crashes: u8,
    candidates: [Candidate; MEMBERS],
    lease: Option<LeaseState>,
    /// The highest epoch that ever reached the lease record.
    lease_epoch_water: u8,
    /// The broker rejected at least one write for a stale epoch.
    fenced_write_seen: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Act {
    /// Every clock moves one tick forward.
    Tick,
    /// The candidate appends a registration record.
    Register(u8),
    /// The candidate calls `InitProducerId` and the coordinator mints an epoch.
    Mint(u8),
    /// The candidate writes the lease record under the epoch it minted.
    PublishLease(u8),
    /// The holder writes a renewed lease record.
    Renew(u8),
    /// The process of the candidate stops.
    Crash(u8),
    /// The coordinator expires the session of a stopped candidate and drops its
    /// registration.
    ExpireSession(u8),
    /// The process of the candidate starts again.
    Recover(u8),
}

/// The succession model.
struct SuccessionModel {
    ids: [MemberId; MEMBERS],
    config: LeaseConfig,
}

/// A tick as an epoch-millisecond instant.
fn millis(ticks: u8) -> i64 {
    i64::from(ticks) * TICK_MILLIS
}

impl SuccessionModel {
    fn new() -> Self {
        let ids = [
            MemberId::new("m0").expect("the member id is well formed"),
            MemberId::new("m1").expect("the member id is well formed"),
            MemberId::new("m2").expect("the member id is well formed"),
        ];
        let config = LeaseConfig::new(
            secs(u32::from(LEASE_TICKS)),
            secs(u32::from(RENEW_TICKS)),
            secs(u32::from(STAGGER_TICKS)),
        )
        .expect("the model extents work together");
        Self { ids, config }
    }

    /// Builds the role state that a reader of the partition would build.
    fn role_state(&self, state: &State) -> RoleState {
        let mut roster: Vec<RosterEntry> = state
            .candidates
            .iter()
            .enumerate()
            .filter_map(|(index, candidate)| {
                candidate.offset.map(|offset| RosterEntry {
                    member: self.ids[index].clone(),
                    offset: i64::from(offset),
                    registered_at: millis(candidate.registered_at),
                })
            })
            .collect();
        roster.sort_unstable_by_key(|entry| entry.offset);
        RoleState {
            roster,
            lease: state.lease.map(|lease| self.lease_record(lease)),
        }
    }

    fn lease_record(&self, lease: LeaseState) -> Lease {
        Lease {
            member: self.ids[usize::from(lease.holder)].clone(),
            token: FencingToken::new(PRODUCER_ID, i16::from(lease.epoch))
                .expect("the modelled epoch is not negative"),
            granted_at: millis(lease.deadline.saturating_sub(LEASE_TICKS)),
            deadline: millis(lease.deadline),
        }
    }

    /// What `evaluate` tells candidate `index` to do right now.
    fn outcome(&self, role_state: &RoleState, state: &State, index: usize) -> Decision {
        evaluate(
            role_state,
            &self.ids[index],
            millis(state.now),
            &self.config,
        )
    }

    /// The decision of every candidate that runs and holds a registration.
    fn outcomes(&self, state: &State) -> [Option<Decision>; MEMBERS] {
        let role_state = self.role_state(state);
        let mut outcomes = [None; MEMBERS];
        for (index, slot) in outcomes.iter_mut().enumerate() {
            if state.candidates[index].registered() {
                *slot = Some(self.outcome(&role_state, state, index));
            }
        }
        outcomes
    }

    /// Reports whether the lease is live at the current tick.
    fn lease_is_live(state: &State) -> bool {
        state.lease.is_some_and(|lease| state.now < lease.deadline)
    }
}

/// Clears the state that no decision can read any more.
///
/// `evaluate` reads the registration instant of a member only while the role
/// has no lease. The model never clears a lease, so the instants are dead from
/// the first grant on. The reset folds the states that differ only in a dead
/// instant into one, and it changes no outcome.
fn normalise(state: &mut State) {
    if state.lease.is_some() {
        for candidate in &mut state.candidates {
            candidate.registered_at = 0;
        }
    }
}

impl Model for SuccessionModel {
    type State = State;
    type Action = Act;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            now: 0,
            next_offset: 0,
            minted: 0,
            crashes: 0,
            candidates: [Candidate::start(); MEMBERS],
            lease: None,
            lease_epoch_water: 0,
            fenced_write_seen: false,
        }]
    }

    fn actions(&self, state: &Self::State, acts: &mut Vec<Self::Action>) {
        if state.now < MAX_TICK {
            acts.push(Act::Tick);
        }
        let role_state = self.role_state(state);
        for index in 0..MEMBERS {
            let candidate = state.candidates[index];
            let id = u8::try_from(index).expect("the member count fits in a u8");
            if !candidate.up {
                acts.push(Act::Recover(id));
                if candidate.offset.is_some() {
                    acts.push(Act::ExpireSession(id));
                }
                continue;
            }
            if state.crashes < MAX_CRASHES {
                acts.push(Act::Crash(id));
            }
            if candidate.pending_lease_write {
                acts.push(Act::PublishLease(id));
                continue;
            }
            if candidate.offset.is_none() {
                if state.next_offset <= MAX_OFFSET {
                    acts.push(Act::Register(id));
                }
                continue;
            }
            match self.outcome(&role_state, state, index) {
                Decision::Challenge => {
                    if state.minted < MAX_EPOCH {
                        acts.push(Act::Mint(id));
                    }
                }
                Decision::Hold => {
                    if candidate.epoch.is_some() {
                        acts.push(Act::Renew(id));
                    }
                }
                Decision::Wait { .. } | Decision::NotRegistered => {}
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = *last;
        match action {
            Act::Tick => {
                if state.now >= MAX_TICK {
                    return None;
                }
                state.now += 1;
            }
            Act::Register(id) => {
                let index = usize::from(id);
                let candidate = &mut state.candidates[index];
                if !candidate.up || candidate.offset.is_some() || last.next_offset > MAX_OFFSET {
                    return None;
                }
                // The offset only grows, so a re-registration lands at the tail
                // of the roster. That is the no-failback rule.
                candidate.offset = Some(last.next_offset);
                candidate.registered_at = last.now;
                state.next_offset += 1;
            }
            Act::Mint(id) => {
                let index = usize::from(id);
                if !last.candidates[index].registered()
                    || last.candidates[index].pending_lease_write
                    || last.minted >= MAX_EPOCH
                {
                    return None;
                }
                let role_state = self.role_state(last);
                if self.outcome(&role_state, last, index) != Decision::Challenge {
                    return None;
                }
                // The coordinator is the single minter, and the epoch it hands
                // out is strictly greater than every epoch it minted before.
                state.minted = last.minted + 1;
                state.candidates[index].epoch = Some(state.minted);
                state.candidates[index].pending_lease_write = true;
            }
            Act::PublishLease(id) => {
                let index = usize::from(id);
                if !last.candidates[index].up || !last.candidates[index].pending_lease_write {
                    return None;
                }
                state.candidates[index].pending_lease_write = false;
                if last.candidates[index].epoch == Some(last.minted) {
                    state.lease = Some(LeaseState {
                        holder: id,
                        epoch: last.minted,
                        deadline: last.now + LEASE_TICKS,
                    });
                    state.lease_epoch_water = last.minted;
                } else {
                    // The broker rejects the write, and the member learns it
                    // lost the role. It drops its producer session.
                    state.candidates[index].epoch = None;
                    state.fenced_write_seen = true;
                }
            }
            Act::Renew(id) => {
                let index = usize::from(id);
                if !last.candidates[index].registered()
                    || last.candidates[index].pending_lease_write
                    || last.candidates[index].epoch.is_none()
                {
                    return None;
                }
                let role_state = self.role_state(last);
                if self.outcome(&role_state, last, index) != Decision::Hold {
                    return None;
                }
                if last.candidates[index].epoch == Some(last.minted) {
                    let lease = state.lease.as_mut()?;
                    lease.deadline = last.now + LEASE_TICKS;
                } else {
                    state.candidates[index].epoch = None;
                    state.fenced_write_seen = true;
                }
            }
            Act::Crash(id) => {
                let index = usize::from(id);
                if !last.candidates[index].up || last.crashes >= MAX_CRASHES {
                    return None;
                }
                let registered_at = last.candidates[index].registered_at;
                let offset = last.candidates[index].offset;
                state.candidates[index] = Candidate {
                    offset,
                    registered_at,
                    ..Candidate::down()
                };
                state.crashes += 1;
            }
            Act::ExpireSession(id) => {
                let index = usize::from(id);
                if last.candidates[index].up || last.candidates[index].offset.is_none() {
                    return None;
                }
                state.candidates[index].offset = None;
            }
            Act::Recover(id) => {
                let index = usize::from(id);
                if last.candidates[index].up {
                    return None;
                }
                state.candidates[index].up = true;
            }
        }
        normalise(&mut state);
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Safety. The epoch is the authority, so at most one member can
            // write under the newest minted epoch.
            Property::always("at_most_one_unfenced_epoch", |_, state: &State| {
                state.minted == 0
                    || state
                        .candidates
                        .iter()
                        .filter(|candidate| candidate.epoch == Some(state.minted))
                        .count()
                        <= 1
            }),
            // Safety. At most one member both believes it holds the role and
            // owns an epoch the broker still accepts.
            Property::always(
                "at_most_one_live_authority",
                |model: &SuccessionModel, state: &State| {
                    model
                        .outcomes(state)
                        .into_iter()
                        .enumerate()
                        .filter(|(index, outcome)| {
                            *outcome == Some(Decision::Hold)
                                && state.candidates[*index].epoch == Some(state.minted)
                                && state.minted > 0
                        })
                        .count()
                        <= 1
                },
            ),
            // A `Hold` outcome always names a live lease of this member.
            Property::always(
                "hold_needs_a_live_lease_of_the_member",
                |model: &SuccessionModel, state: &State| {
                    model
                        .outcomes(state)
                        .into_iter()
                        .enumerate()
                        .all(|(index, outcome)| {
                            outcome != Some(Decision::Hold)
                                || state.lease.is_some_and(|lease| {
                                    usize::from(lease.holder) == index && state.now < lease.deadline
                                })
                        })
                },
            ),
            // Anti-flap. Nobody challenges while the lease of the holder is
            // live, so a short pause never moves the role.
            Property::always(
                "no_challenge_while_the_lease_is_live",
                |model: &SuccessionModel, state: &State| {
                    !SuccessionModel::lease_is_live(state)
                        || model
                            .outcomes(state)
                            .into_iter()
                            .all(|outcome| outcome != Some(Decision::Challenge))
                },
            ),
            // No failback. A member that joined later never outranks a member
            // that joined earlier. The holder is out of the rank order.
            Property::always(
                "a_later_joiner_never_outranks_an_earlier_one",
                |model: &SuccessionModel, state: &State| {
                    let role_state = model.role_state(state);
                    let holder = role_state.holder().cloned();
                    let contenders: Vec<&RosterEntry> = role_state
                        .roster
                        .iter()
                        .filter(|entry| Some(&entry.member) != holder.as_ref())
                        .collect();
                    contenders.iter().all(|early| {
                        contenders.iter().all(|late| {
                            early.offset >= late.offset
                                || role_state.rank_of(&early.member)
                                    < role_state.rank_of(&late.member)
                        })
                    })
                },
            ),
            // Every registered member that runs gets an actionable outcome.
            Property::always(
                "a_registered_member_always_has_a_rank",
                |model: &SuccessionModel, state: &State| {
                    model
                        .outcomes(state)
                        .into_iter()
                        .all(|outcome| outcome != Some(Decision::NotRegistered))
                },
            ),
            // The epoch of the lease record only grows, because the broker
            // rejects a write from an older epoch.
            Property::always("the_lease_epoch_only_grows", |_, state: &State| {
                state
                    .lease
                    .is_none_or(|lease| lease.epoch == state.lease_epoch_water)
                    && state.lease_epoch_water <= state.minted
            }),
            // No member ever carries an epoch past the minted high-water mark.
            Property::always("no_epoch_passes_the_minted_mark", |_, state: &State| {
                state
                    .candidates
                    .iter()
                    .all(|candidate| candidate.epoch.is_none_or(|epoch| epoch <= state.minted))
            }),
            // Reachability. The interesting states are not cut off by a bound.
            Property::sometimes("a_failover_completes", |_, state: &State| {
                state.lease_epoch_water >= 2
            }),
            Property::sometimes("the_broker_fences_a_write", |_, state: &State| {
                state.fenced_write_seen
            }),
            Property::sometimes("a_member_carries_a_stale_epoch", |_, state: &State| {
                state.candidates.iter().any(|candidate| {
                    candidate
                        .epoch
                        .is_some_and(|epoch| epoch < state.minted && candidate.up)
                })
            }),
            Property::sometimes(
                "a_recovered_member_ranks_last_behind_its_replacement",
                |model: &SuccessionModel, state: &State| {
                    if !SuccessionModel::lease_is_live(state) {
                        return false;
                    }
                    let role_state = model.role_state(state);
                    let Some(holder) = role_state.holder().cloned() else {
                        return false;
                    };
                    let Some(holder_entry) = role_state.entry(&holder) else {
                        return false;
                    };
                    let contenders = role_state.roster.len().saturating_sub(1);
                    contenders >= 2
                        && role_state.roster.iter().any(|entry| {
                            entry.offset > holder_entry.offset
                                && role_state.rank_of(&entry.member) == Some(contenders - 1)
                        })
                },
            ),
            Property::sometimes(
                "two_standbys_wait_for_different_instants",
                |model: &SuccessionModel, state: &State| {
                    let waits: Vec<i64> = model
                        .outcomes(state)
                        .into_iter()
                        .filter_map(|outcome| match outcome {
                            Some(Decision::Wait { until_millis }) => Some(until_millis),
                            _ => None,
                        })
                        .collect();
                    waits.len() >= 2 && waits.iter().any(|wait| *wait != waits[0])
                },
            ),
            Property::sometimes("every_candidate_registers", |_, state: &State| {
                state
                    .candidates
                    .iter()
                    .all(|candidate| candidate.offset.is_some())
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.now <= MAX_TICK
            && state.minted <= MAX_EPOCH
            && state.next_offset <= MAX_OFFSET + 1
            && state.crashes <= MAX_CRASHES
            && state.lease_epoch_water <= state.minted
    }
}

#[test]
fn succession_never_gives_two_members_a_live_epoch() {
    let checker = SuccessionModel::new()
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .timeout(CHECK_TIMEOUT)
        .spawn_bfs()
        .join();
    eprintln!(
        "[succession] unique={} generated={} depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert2::assert!(checker.max_depth() < MAX_DEPTH);
    assert2::assert!(checker.state_count() < MAX_STATES);
    checker.assert_properties();
}
