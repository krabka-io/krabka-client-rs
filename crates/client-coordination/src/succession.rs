//! The succession rules of one role: ordered candidates, and no failback.
//!
//! # The epoch is the safety mechanism
//!
//! Kafka's transaction coordinator mints the leadership epoch when a member
//! calls `InitProducerId` for the `transactional.id` of a role. The quorum
//! picks the value, the value only grows, and every broker rejects a write that
//! carries an older one. That fence is the whole of the safety story. The rules
//! in this module decide *when* a member calls `InitProducerId`. They never
//! decide who is authoritative, because the coordinator decides that.
//!
//! A wrong decision here makes a failover early or late, and it costs epoch
//! churn. It never makes two members authoritative for one role.
//!
//! # Rank comes from the log, not from configuration
//!
//! A candidate appends a registration record to the coordination topic. The
//! offset of that record in the partition is the join sequence of the
//! candidate. Log compaction keeps the offset of every record it retains, so a
//! reader that walks the partition in offset order sees the registrations in
//! registration order. [`RoleStateBuilder`] folds that walk into a
//! [`RoleState`], and the roster it builds is in offset order.
//!
//! A recovered node re-registers. The new record gets a higher offset, so the
//! node lands at the tail of the roster and takes the last rank. That is the
//! no-failback rule. A rank from a configuration file would put the recovered
//! node back at the front, and it would then preempt the member that replaced
//! it. The cluster would pay for a second failover that it does not need.
//!
//! # The stagger is an optimisation, not the safety property
//!
//! A challenger of rank `n` waits `n` challenge staggers past the deadline of
//! the lease. This saves epoch churn, and it does nothing else.
//! `InitProducerId` is atomic at the coordinator, so a simultaneous challenge
//! by every standby still mints one epoch per call and still leaves exactly one
//! member with the newest epoch. The losers keep an older epoch, and each one
//! learns that it lost on its first guarded write, which the broker fences.
//! Set the stagger to make that outcome rare. Never set it to make the outcome
//! safe, because it already is.

use std::collections::HashMap;

use crate::{
    lease::LeaseConfig,
    record::{self, CoordinationKey, CoordinationRecord, MemberId, RecordKind, Role},
};

/// One member of the roster of a role.
///
/// `offset` is the offset of the registration record in the coordination
/// partition, and it is the join sequence of the member. `registered_at` is the
/// instant the member wrote that record, in milliseconds since the Unix epoch.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RosterEntry {
    /// The member that registered.
    pub member: MemberId,
    /// The offset of the registration record of the member.
    pub offset: i64,
    /// The time the member registered, in milliseconds since the Unix epoch.
    pub registered_at: i64,
}

/// The state that a reader builds from the records of one role.
///
/// `roster` is in offset order, which is registration order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RoleState {
    /// The candidates of the role, in registration order.
    pub roster: Vec<RosterEntry>,
    /// The lease of the role, and `None` when no member holds it.
    pub lease: Option<record::Lease>,
}

impl RoleState {
    /// Folds the records of one role into a role state.
    ///
    /// The caller reads the coordination partition in offset order and passes
    /// each record with its offset. The fold also gives the right answer for a
    /// different order, because it keeps the record of the highest offset for
    /// every key.
    #[must_use]
    pub fn from_records<I>(role: Role, records: I) -> Self
    where
        I: IntoIterator<Item = (i64, CoordinationKey, CoordinationRecord)>,
    {
        let mut builder = RoleStateBuilder::new(role);
        for (offset, key, record) in records {
            builder.apply(offset, key, record);
        }
        builder.build()
    }

    /// The member the lease names, and `None` when the role has no lease.
    ///
    /// The holder of an expired lease is still the holder. Ask
    /// [`crate::lease::LeaseTiming::is_live_at`] whether the lease is live.
    #[must_use]
    pub fn holder(&self) -> Option<&MemberId> {
        self.lease.as_ref().map(|lease| &lease.member)
    }

    /// The roster entry of `member`, and `None` when it did not register.
    #[must_use]
    pub fn entry(&self, member: &MemberId) -> Option<&RosterEntry> {
        self.roster.iter().find(|entry| entry.member == *member)
    }

    /// The challenge rank of `member`, and `None` when it did not register.
    ///
    /// The rank is the index of `member` in the roster after the removal of the
    /// current holder, so the first standby takes rank 0. The holder itself
    /// keeps rank 0. A holder only reaches the rank test when its own lease has
    /// expired, and it still owns the newest epoch at that point, so it
    /// reclaims the role for less churn than a failover costs.
    #[must_use]
    pub fn rank_of(&self, member: &MemberId) -> Option<usize> {
        self.entry(member)?;
        let holder = self.holder();
        if holder == Some(member) {
            return Some(0);
        }
        self.roster
            .iter()
            .filter(|entry| Some(&entry.member) != holder)
            .position(|entry| entry.member == *member)
    }
}

/// The slot that one member keeps in a fold.
///
/// `registered_at` is `None` after a tombstone. The slot stays in the map, so a
/// registration of a lower offset that arrives later cannot revive the member.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MemberSlot {
    offset: i64,
    registered_at: Option<i64>,
}

/// The slot that the lease keeps in a fold.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LeaseSlot {
    offset: i64,
    lease: Option<record::Lease>,
}

/// Folds the records of one role into a [`RoleState`].
///
/// The builder keeps the record of the highest offset for every key, which is
/// what log compaction keeps. A later registration for a member replaces the
/// earlier one and moves the member to the tail of the roster. A registration
/// tombstone removes the member. A lease tombstone clears the lease.
///
/// The builder ignores a record of another role, so a caller folds a partition
/// that holds several roles without a filter of its own.
#[derive(Debug, Clone)]
pub struct RoleStateBuilder {
    role: Role,
    members: HashMap<MemberId, MemberSlot>,
    lease: Option<LeaseSlot>,
}

impl RoleStateBuilder {
    /// Builds an empty fold for `role`.
    #[must_use]
    pub fn new(role: Role) -> Self {
        Self {
            role,
            members: HashMap::new(),
            lease: None,
        }
    }

    /// The role that this fold collects.
    #[must_use]
    pub const fn role(&self) -> &Role {
        &self.role
    }

    /// Applies one record of the coordination partition.
    ///
    /// The key carries the identity, because the key is the compaction key. The
    /// builder takes the member from `key` and not from the registration value,
    /// and it takes the record kind from `key` too. It ignores a value whose
    /// kind does not match the kind of the key. `decode_value` reads the kind
    /// from the key, so a decoder cannot produce such a pair.
    pub fn apply(&mut self, offset: i64, key: CoordinationKey, record: CoordinationRecord) {
        if key.role != self.role {
            return;
        }
        match (key.kind, record) {
            (RecordKind::Registration, CoordinationRecord::Registration(registration)) => {
                if let Some(member) = key.member {
                    self.put_member(offset, member, Some(registration.registered_at));
                }
            }
            (RecordKind::Registration, CoordinationRecord::Tombstone) => {
                if let Some(member) = key.member {
                    self.put_member(offset, member, None);
                }
            }
            (RecordKind::Lease, CoordinationRecord::Lease(lease)) => {
                self.put_lease(offset, Some(lease));
            }
            (RecordKind::Lease, CoordinationRecord::Tombstone) => self.put_lease(offset, None),
            (RecordKind::Registration, CoordinationRecord::Lease(_))
            | (RecordKind::Lease, CoordinationRecord::Registration(_)) => {}
        }
    }

    /// Builds the role state, with the roster in offset order.
    #[must_use]
    pub fn build(self) -> RoleState {
        let mut roster: Vec<RosterEntry> = self
            .members
            .into_iter()
            .filter_map(|(member, slot)| {
                slot.registered_at.map(|registered_at| RosterEntry {
                    member,
                    offset: slot.offset,
                    registered_at,
                })
            })
            .collect();
        roster.sort_unstable_by_key(|entry| entry.offset);
        RoleState {
            roster,
            lease: self.lease.and_then(|slot| slot.lease),
        }
    }

    fn put_member(&mut self, offset: i64, member: MemberId, registered_at: Option<i64>) {
        let slot = MemberSlot {
            offset,
            registered_at,
        };
        match self.members.get_mut(&member) {
            Some(held) if held.offset >= offset => {}
            Some(held) => *held = slot,
            None => {
                self.members.insert(member, slot);
            }
        }
    }

    fn put_lease(&mut self, offset: i64, lease: Option<record::Lease>) {
        if self
            .lease
            .as_ref()
            .is_some_and(|held| held.offset >= offset)
        {
            return;
        }
        self.lease = Some(LeaseSlot { offset, lease });
    }
}

/// What one member does about a role right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Decision {
    /// This member holds the role, and its lease is live. It renews the lease
    /// and it does not call `InitProducerId`.
    Hold,
    /// This member may call `InitProducerId` now.
    Challenge,
    /// This member waits, and then evaluates the role state again.
    Wait {
        /// The instant of the next evaluation, in milliseconds since the Unix
        /// epoch.
        until_millis: i64,
    },
    /// This member is not in the roster of the role, so it has no rank. It
    /// registers first, reads the partition again, and evaluates again.
    NotRegistered,
}

/// Decides what `me` does about the role of `state` at `now_millis`.
///
/// The rules are:
///
/// 1. A member that is not in the roster gets [`Decision::NotRegistered`]. It
///    has no rank, because rank comes from the registration record.
/// 2. A member that the lease names, while that lease is live, gets
///    [`Decision::Hold`].
/// 3. A challenger of rank `n` gets [`Decision::Challenge`] from
///    `deadline + n * challenge_stagger` on. With no lease, the anchor is the
///    registration instant of the member instead of a deadline, so rank 0
///    challenges at once and rank `n` challenges `n` staggers later.
/// 4. Every other member gets [`Decision::Wait`], and the instant it carries
///    is the earliest instant at which this answer changes for an unchanged
///    role state.
///
/// The anchor of rule 3 always comes from the role state and never from
/// `now_millis`. A member that has no lease to wait for anchors on its own
/// registration record. An anchor of "now plus `n` staggers" would move forward
/// on every evaluation, and a standby of rank 1 or more would then wait for
/// ever while rank 0 is dead.
///
/// The member of an expired lease keeps rank 0, so it reclaims its own role at
/// its own deadline. See [`RoleState::rank_of`].
///
/// A caller re-evaluates when it reads a new record, and at the latest at the
/// instant that [`Decision::Wait`] names.
#[must_use]
pub fn evaluate(
    state: &RoleState,
    me: &MemberId,
    now_millis: i64,
    config: &LeaseConfig,
) -> Decision {
    let (Some(entry), Some(rank)) = (state.entry(me), state.rank_of(me)) else {
        return Decision::NotRegistered;
    };
    let challenge_at_millis = match state.lease.as_ref() {
        Some(lease) => {
            let timing = config.timing(lease);
            if lease.member == *me && timing.is_live_at(now_millis) {
                return Decision::Hold;
            }
            timing.challenge_at_millis(rank)
        }
        None => entry
            .registered_at
            .saturating_add(config.challenge_delay_millis(rank)),
    };
    if now_millis >= challenge_at_millis {
        Decision::Challenge
    } else {
        Decision::Wait {
            until_millis: challenge_at_millis,
        }
    }
}

#[cfg(test)]
mod model;

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_units::secs;

    use super::*;
    use crate::record::{FencingToken, Registration};

    // One record of the coordination partition, with the offset it sits at.
    type LogRecord = (i64, CoordinationKey, CoordinationRecord);

    fn role() -> Role {
        Role::new("controller").expect("the role name is well formed")
    }

    fn other_role() -> Role {
        Role::new("compactor").expect("the role name is well formed")
    }

    fn member(name: &str) -> MemberId {
        MemberId::new(name).expect("the member id is well formed")
    }

    fn token(epoch: i16) -> FencingToken {
        FencingToken::new(11, epoch).expect("the token is not negative")
    }

    fn config() -> LeaseConfig {
        LeaseConfig::new(secs(30), secs(10), secs(5)).expect("the extents work together")
    }

    fn registration(offset: i64, role: &Role, name: &str, registered_at: i64) -> LogRecord {
        (
            offset,
            CoordinationKey::registration(role.clone(), member(name)),
            CoordinationRecord::Registration(Registration {
                member: member(name),
                registered_at,
            }),
        )
    }

    fn deregistration(offset: i64, role: &Role, name: &str) -> LogRecord {
        (
            offset,
            CoordinationKey::registration(role.clone(), member(name)),
            CoordinationRecord::Tombstone,
        )
    }

    fn lease_record(
        offset: i64,
        role: &Role,
        name: &str,
        granted_at: i64,
        deadline: i64,
    ) -> LogRecord {
        (
            offset,
            CoordinationKey::lease(role.clone()),
            CoordinationRecord::Lease(record::Lease {
                member: member(name),
                token: token(1),
                granted_at,
                deadline,
            }),
        )
    }

    fn lease_tombstone(offset: i64, role: &Role) -> LogRecord {
        (
            offset,
            CoordinationKey::lease(role.clone()),
            CoordinationRecord::Tombstone,
        )
    }

    fn entry(name: &str, offset: i64, registered_at: i64) -> RosterEntry {
        RosterEntry {
            member: member(name),
            offset,
            registered_at,
        }
    }

    fn state_with(roster: Vec<RosterEntry>, lease: Option<record::Lease>) -> RoleState {
        RoleState { roster, lease }
    }

    fn live_lease(name: &str) -> record::Lease {
        record::Lease {
            member: member(name),
            token: token(1),
            granted_at: 0,
            deadline: 30_000,
        }
    }

    #[test]
    fn the_roster_follows_offset_order_and_not_arrival_order() {
        let role = role();
        let records = vec![
            registration(30, &role, "node-3", 300),
            registration(10, &role, "node-1", 100),
            registration(20, &role, "node-2", 200),
        ];

        let state = RoleState::from_records(role, records);

        check!(
            state
                == state_with(
                    vec![
                        entry("node-1", 10, 100),
                        entry("node-2", 20, 200),
                        entry("node-3", 30, 300),
                    ],
                    None,
                )
        );
    }

    #[test]
    fn a_later_registration_moves_a_member_to_the_tail() {
        let role = role();
        let records = vec![
            registration(10, &role, "node-1", 100),
            registration(20, &role, "node-2", 200),
            registration(30, &role, "node-3", 300),
            // node-1 recovers and rejoins. It must not preempt its replacement.
            registration(40, &role, "node-1", 400),
        ];

        let state = RoleState::from_records(role, records);

        check!(
            state
                == state_with(
                    vec![
                        entry("node-2", 20, 200),
                        entry("node-3", 30, 300),
                        entry("node-1", 40, 400),
                    ],
                    None,
                )
        );
        check!(state.rank_of(&member("node-1")) == Some(2));
    }

    #[test]
    fn a_tombstone_removes_a_member_and_a_later_record_brings_it_back() {
        let role = role();
        let cases: Vec<(&str, Vec<LogRecord>, RoleState)> = vec![
            (
                "tombstone removes",
                vec![
                    registration(10, &role, "node-1", 100),
                    registration(20, &role, "node-2", 200),
                    deregistration(30, &role, "node-1"),
                ],
                state_with(vec![entry("node-2", 20, 200)], None),
            ),
            (
                "registration after a tombstone rejoins at the tail",
                vec![
                    registration(10, &role, "node-1", 100),
                    registration(20, &role, "node-2", 200),
                    deregistration(30, &role, "node-1"),
                    registration(40, &role, "node-1", 400),
                ],
                state_with(
                    vec![entry("node-2", 20, 200), entry("node-1", 40, 400)],
                    None,
                ),
            ),
            (
                "a tombstone of a lower offset does not revive a member",
                vec![
                    registration(10, &role, "node-1", 100),
                    registration(40, &role, "node-1", 400),
                    deregistration(20, &role, "node-1"),
                ],
                state_with(vec![entry("node-1", 40, 400)], None),
            ),
        ];

        for (name, records, expected) in cases {
            let state = RoleState::from_records(role.clone(), records);

            check!(state == expected, "{name}");
        }
    }

    #[test]
    fn the_lease_of_the_highest_offset_wins_and_a_tombstone_clears_it() {
        let role = role();
        let cases: Vec<(&str, Vec<LogRecord>, Option<record::Lease>)> = vec![
            (
                "the last lease wins",
                vec![
                    lease_record(10, &role, "node-1", 0, 30_000),
                    lease_record(20, &role, "node-2", 10_000, 40_000),
                ],
                Some(record::Lease {
                    member: member("node-2"),
                    token: token(1),
                    granted_at: 10_000,
                    deadline: 40_000,
                }),
            ),
            (
                "a tombstone clears the lease",
                vec![
                    lease_record(10, &role, "node-1", 0, 30_000),
                    lease_tombstone(20, &role),
                ],
                None,
            ),
            (
                "a lease of a lower offset does not overwrite a later one",
                vec![
                    lease_record(20, &role, "node-2", 10_000, 40_000),
                    lease_record(10, &role, "node-1", 0, 30_000),
                ],
                Some(record::Lease {
                    member: member("node-2"),
                    token: token(1),
                    granted_at: 10_000,
                    deadline: 40_000,
                }),
            ),
        ];

        for (name, records, expected) in cases {
            let state = RoleState::from_records(role.clone(), records);

            check!(state.lease == expected, "{name}");
        }
    }

    #[test]
    fn the_builder_keeps_the_role_it_collects() {
        let role = role();
        let mut builder = RoleStateBuilder::new(role.clone());
        let (offset, key, record) = registration(10, &role, "node-1", 100);
        builder.apply(offset, key, record);

        check!(builder.role() == &role);
        check!(builder.build() == state_with(vec![entry("node-1", 10, 100)], None));
    }

    #[test]
    fn the_builder_ignores_the_records_of_another_role() {
        let role = role();
        let other = other_role();
        let records = vec![
            registration(10, &role, "node-1", 100),
            registration(20, &other, "node-9", 200),
            lease_record(30, &other, "node-9", 200, 40_000),
        ];

        let state = RoleState::from_records(role, records);

        check!(state == state_with(vec![entry("node-1", 10, 100)], None));
    }

    #[test]
    fn the_holder_is_out_of_the_rank_order_and_keeps_rank_zero() {
        let state = state_with(
            vec![
                entry("node-1", 10, 100),
                entry("node-2", 20, 200),
                entry("node-3", 30, 300),
            ],
            Some(live_lease("node-2")),
        );

        check!(state.holder() == Some(&member("node-2")));
        check!(state.rank_of(&member("node-2")) == Some(0));
        check!(state.rank_of(&member("node-1")) == Some(0));
        check!(state.rank_of(&member("node-3")) == Some(1));
        check!(state.rank_of(&member("node-9")) == None);
    }

    #[test]
    fn a_member_outside_the_roster_gets_no_rank_and_no_challenge() {
        let state = state_with(vec![entry("node-1", 10, 100)], Some(live_lease("node-1")));

        check!(evaluate(&state, &member("node-9"), 0, &config()) == Decision::NotRegistered);
    }

    #[test]
    fn the_live_holder_holds_and_every_standby_waits_its_stagger() {
        let state = state_with(
            vec![
                entry("node-1", 10, 100),
                entry("node-2", 20, 200),
                entry("node-3", 30, 300),
            ],
            Some(live_lease("node-1")),
        );
        let config = config();

        let cases = [
            ("node-1", Decision::Hold),
            (
                "node-2",
                Decision::Wait {
                    until_millis: 30_000,
                },
            ),
            (
                "node-3",
                Decision::Wait {
                    until_millis: 35_000,
                },
            ),
        ];

        for (name, expected) in cases {
            check!(
                evaluate(&state, &member(name), 1_000, &config) == expected,
                "{name}"
            );
        }
    }

    #[test]
    fn the_ranks_challenge_one_stagger_apart_after_the_deadline() {
        let state = state_with(
            vec![
                entry("node-1", 10, 100),
                entry("node-2", 20, 200),
                entry("node-3", 30, 300),
            ],
            Some(live_lease("node-1")),
        );
        let config = config();

        let cases = [
            // The deadline is 30 000. Rank 0 is node-2 and rank 1 is node-3.
            (
                29_999,
                "node-2",
                Decision::Wait {
                    until_millis: 30_000,
                },
            ),
            (30_000, "node-2", Decision::Challenge),
            (
                30_000,
                "node-3",
                Decision::Wait {
                    until_millis: 35_000,
                },
            ),
            (
                34_999,
                "node-3",
                Decision::Wait {
                    until_millis: 35_000,
                },
            ),
            (35_000, "node-3", Decision::Challenge),
            // The deposed holder keeps rank 0 and reclaims at its own deadline.
            (29_999, "node-1", Decision::Hold),
            (30_000, "node-1", Decision::Challenge),
        ];

        for (now_millis, name, expected) in cases {
            check!(
                evaluate(&state, &member(name), now_millis, &config) == expected,
                "{name} at {now_millis}"
            );
        }
    }

    #[test]
    fn with_no_lease_the_registration_instant_anchors_the_stagger() {
        let state = state_with(
            vec![
                entry("node-1", 10, 1_000),
                entry("node-2", 20, 2_000),
                entry("node-3", 30, 3_000),
            ],
            None,
        );
        let config = config();

        let cases = [
            (1_000, "node-1", Decision::Challenge),
            (
                1_000,
                "node-2",
                Decision::Wait {
                    until_millis: 7_000,
                },
            ),
            (7_000, "node-2", Decision::Challenge),
            (
                1_000,
                "node-3",
                Decision::Wait {
                    until_millis: 13_000,
                },
            ),
            (13_000, "node-3", Decision::Challenge),
        ];

        for (now_millis, name, expected) in cases {
            check!(
                evaluate(&state, &member(name), now_millis, &config) == expected,
                "{name} at {now_millis}"
            );
        }
    }

    #[test]
    fn a_recovered_member_defers_to_the_member_that_replaced_it() {
        let role = role();
        let config = config();
        let records = vec![
            registration(10, &role, "node-1", 0),
            registration(20, &role, "node-2", 0),
            lease_record(30, &role, "node-1", 0, 30_000),
            // node-1 dies, node-2 takes the role, and then node-1 comes back.
            deregistration(40, &role, "node-1"),
            lease_record(50, &role, "node-2", 30_000, 60_000),
            registration(60, &role, "node-1", 31_000),
        ];

        let state = RoleState::from_records(role, records);

        check!(state.holder() == Some(&member("node-2")));
        check!(state.rank_of(&member("node-1")) == Some(0));
        // The recovered member is behind the holder in the roster, and the live
        // lease keeps it waiting for the whole deadline.
        assert!(let Some(recovered) = state.entry(&member("node-1")));
        check!(recovered.offset == 60);
        check!(
            evaluate(&state, &member("node-1"), 31_000, &config)
                == Decision::Wait {
                    until_millis: 60_000
                }
        );
        check!(evaluate(&state, &member("node-2"), 31_000, &config) == Decision::Hold);
    }

    #[test]
    fn a_third_member_that_rejoins_ranks_behind_the_other_standby() {
        let role = role();
        let config = config();
        let records = vec![
            registration(10, &role, "node-1", 0),
            registration(20, &role, "node-2", 0),
            registration(30, &role, "node-3", 0),
            lease_record(40, &role, "node-1", 0, 30_000),
            deregistration(50, &role, "node-2"),
            registration(60, &role, "node-2", 5_000),
        ];

        let state = RoleState::from_records(role, records);

        // node-3 registered before the rejoin of node-2, so node-3 leads.
        check!(state.rank_of(&member("node-3")) == Some(0));
        check!(state.rank_of(&member("node-2")) == Some(1));
        check!(evaluate(&state, &member("node-3"), 30_000, &config) == Decision::Challenge);
        check!(
            evaluate(&state, &member("node-2"), 30_000, &config)
                == Decision::Wait {
                    until_millis: 35_000
                }
        );
    }

    #[test]
    fn a_lone_member_with_no_lease_challenges_at_once() {
        let state = state_with(vec![entry("node-1", 10, 1_000)], None);

        check!(evaluate(&state, &member("node-1"), 1_000, &config()) == Decision::Challenge);
        check!(evaluate(&state, &member("node-1"), 9_000, &config()) == Decision::Challenge);
    }

    #[test]
    fn an_empty_role_state_has_no_holder_and_no_ranks() {
        let state = RoleState::default();

        check!(state.holder() == None);
        check!(state.rank_of(&member("node-1")) == None);
        check!(evaluate(&state, &member("node-1"), 0, &config()) == Decision::NotRegistered);
    }
}
