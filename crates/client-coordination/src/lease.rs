//! The lease clock and the anti-flap policy around a lease record.
//!
//! # The lease adds no safety
//!
//! The leadership epoch is the safety mechanism, and the broker enforces it.
//! Kafka's transaction coordinator mints one producer epoch for the
//! `transactional.id` of a role. The quorum picks the value, and every broker
//! rejects a write that carries an older epoch. The lease adds no safety of its
//! own. It is a liveness and anti-flap device. It tells a standby when to stop
//! waiting for a quiet holder. A wrong lease makes a failover early or late. It
//! never makes two members authoritative.
//!
//! Read the deadline of a lease as a hint and not as a guarantee. A holder past
//! its deadline still owns the newest epoch until a challenger mints a newer
//! one. A challenger that reads a live lease still wins the role at once if it
//! calls `InitProducerId` anyway. The lease only stops it from trying.
//!
//! # Clock skew
//!
//! The holder and each challenger read their own clock. A skew between two
//! clocks shifts the failover time by the size of the skew, and no more. A
//! challenger with a fast clock sees the deadline pass early, so it challenges
//! early. A challenger with a slow clock challenges late.
//!
//! Neither case breaks safety, because no clock takes part in the fence. The
//! challenger still calls `InitProducerId`, the coordinator still mints exactly
//! one epoch per call, and the broker still rejects every write that carries
//! the older epoch. Skew costs one early failover, one late failover, or some
//! epoch churn. It cannot give two members a live epoch for the same role.
//!
//! # Instants and extents
//!
//! [`LeaseConfig`] holds extents, so each field is a [`Time`]. Every deadline
//! and every "now" value is a coordinate on the epoch-millisecond line, so each
//! one is a plain `i64`. The two kinds never mix in one type.

use std::{
    sync::atomic::{AtomicI64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use krabka_units::{Time, convert::TimeExt, secs};

use crate::{
    error::CoordinationError,
    record::{self, FencingToken, MemberId},
};

/// The default extent of a lease.
pub const DEFAULT_LEASE_DURATION: Time = secs(30);

/// The default gap between two renewals by the holder.
pub const DEFAULT_RENEW_INTERVAL: Time = secs(10);

/// The default extra delay that one rank of succession adds.
pub const DEFAULT_CHALLENGE_STAGGER: Time = secs(5);

/// The renew interval that the documentation recommends, as a part of the lease
/// duration.
const RECOMMENDED_RENEW_FRACTION: f64 = 3.0;

/// The lease timings of one role.
///
/// The three extents are independent of the record layout, so one process can
/// hold several roles with different timings. Build a value with
/// [`LeaseConfig::new`], which rejects a set of extents that cannot work, or
/// take the defaults with `LeaseConfig::default`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LeaseConfig {
    duration: Time,
    renew_interval: Time,
    challenge_stagger: Time,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            duration: DEFAULT_LEASE_DURATION,
            renew_interval: DEFAULT_RENEW_INTERVAL,
            challenge_stagger: DEFAULT_CHALLENGE_STAGGER,
        }
    }
}

impl LeaseConfig {
    /// Builds a lease policy from three extents.
    ///
    /// A caller should set `renew_interval` to at most a third of `duration`.
    /// The holder then keeps two more attempts before the deadline. The
    /// constructor accepts a larger value and does not reject it. Ask
    /// [`LeaseConfig::renews_with_margin`] which side of that bound a value is
    /// on.
    ///
    /// # Errors
    ///
    /// Returns [`CoordinationError::InvalidConfig`] when `duration`,
    /// `renew_interval`, or `challenge_stagger` is zero, negative, infinite, or
    /// `NaN`. Returns the same error when `renew_interval` is equal to or
    /// larger than `duration`, because the holder then never renews in time.
    pub fn new(
        duration: Time,
        renew_interval: Time,
        challenge_stagger: Time,
    ) -> Result<Self, CoordinationError> {
        check_extent("lease duration", duration)?;
        check_extent("renew interval", renew_interval)?;
        check_extent("challenge stagger", challenge_stagger)?;
        if renew_interval >= duration {
            return Err(CoordinationError::InvalidConfig(format!(
                "renew interval of {} ms is not shorter than the lease duration of {} ms",
                renew_interval.millis_i64(),
                duration.millis_i64()
            )));
        }
        Ok(Self {
            duration,
            renew_interval,
            challenge_stagger,
        })
    }

    /// The extent of a lease that a member takes now.
    #[must_use]
    pub const fn duration(&self) -> Time {
        self.duration
    }

    /// The gap between two renewals by the holder.
    #[must_use]
    pub const fn renew_interval(&self) -> Time {
        self.renew_interval
    }

    /// The extra delay that one rank of succession adds.
    #[must_use]
    pub const fn challenge_stagger(&self) -> Time {
        self.challenge_stagger
    }

    /// Reports whether the renew interval leaves the holder two spare attempts.
    ///
    /// The renew interval that this crate recommends is at most a third of the
    /// lease duration. A configuration outside that bound still works, and the
    /// holder then loses the role after fewer missed renewals.
    #[must_use]
    pub fn renews_with_margin(&self) -> bool {
        self.renew_interval.secs_f64() * RECOMMENDED_RENEW_FRACTION <= self.duration.secs_f64()
    }

    /// The extra delay in milliseconds that a challenger of rank `rank` takes.
    ///
    /// The value saturates at [`i64::MAX`], so a very large rank does not wrap
    /// the instant that a caller adds it to.
    #[must_use]
    pub fn challenge_delay_millis(&self, rank: usize) -> i64 {
        let rank = i64::try_from(rank).unwrap_or(i64::MAX);
        rank.saturating_mul(self.challenge_stagger.millis_i64())
    }

    /// The lease record that a member writes after it mints `token`.
    ///
    /// A renewal writes the same record with the same token and a later
    /// `now_millis`, so this one function covers a first grant and a renewal.
    #[must_use]
    pub fn grant(&self, member: MemberId, token: FencingToken, now_millis: i64) -> record::Lease {
        record::Lease {
            member,
            token,
            granted_at: now_millis,
            deadline: now_millis.saturating_add(self.duration.millis_i64()),
        }
    }

    /// The clock questions that this policy answers about `lease`.
    #[must_use]
    pub const fn timing<'lease>(&self, lease: &'lease record::Lease) -> LeaseTiming<'lease> {
        LeaseTiming {
            lease,
            config: *self,
        }
    }
}

/// Rejects an extent that is not a positive, finite length of time.
fn check_extent(field: &str, value: Time) -> Result<(), CoordinationError> {
    let seconds = value.secs_f64();
    if !seconds.is_finite() || seconds <= 0.0 {
        return Err(CoordinationError::InvalidConfig(format!(
            "{field} must be a positive finite extent, got {seconds} s"
        )));
    }
    Ok(())
}

/// The lease clock of one lease record under one policy.
///
/// Every method takes and returns epoch milliseconds, because a deadline is a
/// coordinate and not an extent. [`LeaseTiming::remaining_at`] is the one
/// exception, and it returns the extent that is left.
#[derive(Debug, Clone, Copy)]
pub struct LeaseTiming<'lease> {
    lease: &'lease record::Lease,
    config: LeaseConfig,
}

impl<'lease> LeaseTiming<'lease> {
    /// Binds a lease record to a policy.
    #[must_use]
    pub const fn new(lease: &'lease record::Lease, config: LeaseConfig) -> Self {
        Self { lease, config }
    }

    /// The lease record behind this clock.
    #[must_use]
    pub const fn lease(&self) -> &'lease record::Lease {
        self.lease
    }

    /// The instant the lease expires, in milliseconds since the Unix epoch.
    #[must_use]
    pub const fn expires_at_millis(&self) -> i64 {
        self.lease.deadline
    }

    /// Reports whether the lease is live at `now_millis`.
    ///
    /// The lease is live before its deadline and expired from the deadline on.
    /// The rank 0 challenger may challenge at exactly the deadline, so the two
    /// tests leave no gap and no overlap.
    #[must_use]
    pub const fn is_live_at(&self, now_millis: i64) -> bool {
        now_millis < self.lease.deadline
    }

    /// The extent that is left before the deadline, and zero after it.
    #[must_use]
    pub fn remaining_at(&self, now_millis: i64) -> Time {
        let left = self.lease.deadline.saturating_sub(now_millis).max(0);
        Time::from_millis(left)
    }

    /// The instant at which the holder should write its next renewal.
    ///
    /// The value is the grant instant plus the renew interval. It never passes
    /// the deadline, so a holder that follows it always writes before it loses
    /// the lease.
    #[must_use]
    pub fn renew_at_millis(&self) -> i64 {
        self.lease
            .granted_at
            .saturating_add(self.config.renew_interval().millis_i64())
            .min(self.lease.deadline)
    }

    /// Reports whether the holder should write a renewal at `now_millis`.
    #[must_use]
    pub fn renew_due_at(&self, now_millis: i64) -> bool {
        now_millis >= self.renew_at_millis()
    }

    /// The instant at which a challenger of rank `rank` may challenge.
    ///
    /// Rank 0 challenges at the deadline. Each later rank adds one challenge
    /// stagger. The stagger saves epoch churn and gives no safety.
    #[must_use]
    pub fn challenge_at_millis(&self, rank: usize) -> i64 {
        self.lease
            .deadline
            .saturating_add(self.config.challenge_delay_millis(rank))
    }
}

/// A source of the current time.
///
/// The succession rules and the lease clock take an instant as a parameter, so
/// they need no clock at all. This trait is the seam for the code around them
/// that reads a real clock. A test supplies [`TestClock`] and drives time by
/// hand, with no sleep.
pub trait Clock: Send + Sync {
    /// The current time, in milliseconds since the Unix epoch.
    fn now_millis(&self) -> i64;
}

/// The clock of the host, read through [`SystemTime`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_millis(&self) -> i64 {
        signed_millis(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|before| before.duration()),
        )
    }
}

/// Gives the epoch millis of a duration measured from the epoch.
///
/// `Ok` carries a duration after the epoch and `Err` carries the distance a
/// host clock sits before it. A host clock set before 1970 gives a negative
/// instant, and the sign is what keeps the ordering of two instants right.
/// The function saturates rather than wrapping, because a lease deadline that
/// wrapped would read as live.
fn signed_millis(since_epoch: Result<Duration, Duration>) -> i64 {
    match since_epoch {
        Ok(after) => i64::try_from(after.as_millis()).unwrap_or(i64::MAX),
        Err(before) => -i64::try_from(before.as_millis()).unwrap_or(i64::MAX),
    }
}

/// A clock that a test moves by hand.
///
/// The clock takes `&self` for every operation, so a test shares one clock
/// between the holder and the challengers and moves them all together.
#[derive(Debug)]
pub struct TestClock {
    now_millis: AtomicI64,
}

impl TestClock {
    /// Builds a clock that reads `now_millis`.
    #[must_use]
    pub const fn new(now_millis: i64) -> Self {
        Self {
            now_millis: AtomicI64::new(now_millis),
        }
    }

    /// Moves the clock to `now_millis`.
    pub fn set(&self, now_millis: i64) {
        self.now_millis.store(now_millis, Ordering::Relaxed);
    }

    /// Moves the clock forward by `step`.
    pub fn advance(&self, step: Time) {
        self.now_millis
            .fetch_add(step.millis_i64(), Ordering::Relaxed);
    }
}

impl Clock for TestClock {
    fn now_millis(&self) -> i64 {
        self.now_millis.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    /// The epoch millis of an instant keep their sign. A host clock set
    /// before 1970 gives a negative instant, and dropping the sign would put
    /// that instant far in the future, where an expired lease reads as live.
    #[test]
    fn an_instant_before_the_epoch_stays_negative() {
        check!(signed_millis(Ok(Duration::from_secs(1_700_000_000))) == 1_700_000_000_000);
        check!(signed_millis(Ok(Duration::ZERO)) == 0);
        check!(signed_millis(Err(Duration::from_secs(2))) == -2_000);
        check!(signed_millis(Err(Duration::ZERO)) == 0);
        // A duration past the range of an i64 saturates rather than wrapping.
        check!(signed_millis(Ok(Duration::MAX)) == i64::MAX);
        check!(signed_millis(Err(Duration::MAX)) == -i64::MAX);
    }

    use assert2::{assert, check};
    use krabka_units::{millis, secs};

    use super::*;

    fn member(name: &str) -> MemberId {
        MemberId::new(name).expect("the member id is well formed")
    }

    fn token() -> FencingToken {
        FencingToken::new(7, 3).expect("the token is not negative")
    }

    fn lease_at(granted_at: i64, deadline: i64) -> record::Lease {
        record::Lease {
            member: member("node-1"),
            token: token(),
            granted_at,
            deadline,
        }
    }

    #[test]
    fn default_config_holds_the_documented_extents() {
        let config = LeaseConfig::default();

        check!(config.duration() == secs(30));
        check!(config.renew_interval() == secs(10));
        check!(config.challenge_stagger() == secs(5));
        check!(config.renews_with_margin());
    }

    #[test]
    fn new_accepts_the_default_extents() {
        let built = LeaseConfig::new(secs(30), secs(10), secs(5));

        assert!(let Ok(config) = built);
        check!(config == LeaseConfig::default());
    }

    #[test]
    fn new_rejects_extents_that_cannot_work() {
        let cases = [
            ("zero duration", secs(0), secs(1), secs(1)),
            ("zero renew interval", secs(30), secs(0), secs(5)),
            ("zero stagger", secs(30), secs(10), secs(0)),
            ("negative duration", Time::from_millis(-1), secs(1), secs(1)),
            (
                "negative renew interval",
                secs(30),
                Time::from_millis(-1),
                secs(5),
            ),
            (
                "negative stagger",
                secs(30),
                secs(10),
                Time::from_millis(-1),
            ),
            ("renew equals duration", secs(30), secs(30), secs(5)),
            ("renew past duration", secs(30), secs(31), secs(5)),
            (
                "infinite duration",
                Time::from_secs_f64(f64::INFINITY),
                secs(1),
                secs(1),
            ),
            (
                "not a number",
                Time::from_secs_f64(f64::NAN),
                secs(1),
                secs(1),
            ),
        ];

        for (name, duration, renew_interval, challenge_stagger) in cases {
            let built = LeaseConfig::new(duration, renew_interval, challenge_stagger);

            assert!(let Err(error) = built, "{name} was accepted");
            assert!(let CoordinationError::InvalidConfig(_) = error, "{name}");
        }
    }

    #[test]
    fn renews_with_margin_reports_the_recommended_bound() {
        let cases = [
            (secs(30), secs(10), true),
            (secs(30), secs(9), true),
            (secs(30), secs(11), false),
            (secs(30), secs(29), false),
        ];

        for (duration, renew_interval, expected) in cases {
            assert!(let Ok(config) = LeaseConfig::new(duration, renew_interval, secs(1)));

            check!(config.renews_with_margin() == expected);
        }
    }

    #[test]
    fn grant_puts_the_deadline_one_duration_after_now() {
        let config = LeaseConfig::default();

        let granted = config.grant(member("node-1"), token(), 1_000);

        check!(
            granted
                == record::Lease {
                    member: member("node-1"),
                    token: token(),
                    granted_at: 1_000,
                    deadline: 31_000,
                }
        );
    }

    #[test]
    fn grant_saturates_rather_than_wrapping_the_deadline() {
        let config = LeaseConfig::default();

        let granted = config.grant(member("node-1"), token(), i64::MAX);

        check!(granted.deadline == i64::MAX);
    }

    #[test]
    fn a_lease_is_live_before_its_deadline_and_expired_from_it() {
        let config = LeaseConfig::default();
        let lease = lease_at(1_000, 31_000);
        let timing = config.timing(&lease);

        let cases = [
            (0, true),
            (1_000, true),
            (30_999, true),
            (31_000, false),
            (31_001, false),
        ];

        for (now_millis, expected) in cases {
            check!(timing.is_live_at(now_millis) == expected, "at {now_millis}");
        }
    }

    #[test]
    fn a_timing_binds_one_lease_to_one_policy() {
        let config = LeaseConfig::default();
        let lease = lease_at(1_000, 31_000);

        let timing = LeaseTiming::new(&lease, config);

        check!(timing.lease() == &lease);
        check!(timing.expires_at_millis() == 31_000);
        check!(timing.renew_at_millis() == config.timing(&lease).renew_at_millis());
    }

    #[test]
    fn remaining_falls_to_zero_at_the_deadline() {
        let config = LeaseConfig::default();
        let lease = lease_at(1_000, 31_000);
        let timing = config.timing(&lease);

        check!(timing.remaining_at(1_000) == secs(30));
        check!(timing.remaining_at(30_500) == millis(500));
        check!(timing.remaining_at(31_000) == Time::ZERO);
        check!(timing.remaining_at(99_000) == Time::ZERO);
    }

    #[test]
    fn the_holder_renews_one_interval_after_the_grant() {
        let config = LeaseConfig::default();
        let lease = lease_at(1_000, 31_000);
        let timing = config.timing(&lease);

        check!(timing.renew_at_millis() == 11_000);
        check!(!timing.renew_due_at(10_999));
        check!(timing.renew_due_at(11_000));
        check!(timing.expires_at_millis() == 31_000);
    }

    #[test]
    fn the_renew_instant_never_passes_the_deadline() {
        let config = LeaseConfig::default();
        // A writer with a different policy can leave a deadline that is closer
        // than one renew interval. The holder still renews before it expires.
        let lease = lease_at(1_000, 3_000);
        let timing = config.timing(&lease);

        check!(timing.renew_at_millis() == 3_000);
    }

    #[test]
    fn each_rank_adds_one_stagger_after_the_deadline() {
        let config = LeaseConfig::default();
        let lease = lease_at(1_000, 31_000);
        let timing = config.timing(&lease);

        let cases = [(0, 31_000), (1, 36_000), (2, 41_000), (3, 46_000)];

        for (rank, expected) in cases {
            check!(timing.challenge_at_millis(rank) == expected, "rank {rank}");
        }
    }

    #[test]
    fn a_huge_rank_saturates_the_challenge_instant() {
        let config = LeaseConfig::default();
        let lease = lease_at(1_000, 31_000);
        let timing = config.timing(&lease);

        check!(config.challenge_delay_millis(usize::MAX) == i64::MAX);
        check!(timing.challenge_at_millis(usize::MAX) == i64::MAX);
    }

    #[test]
    fn the_test_clock_moves_only_when_a_test_moves_it() {
        let clock = TestClock::new(1_000);

        check!(clock.now_millis() == 1_000);
        clock.advance(secs(30));
        check!(clock.now_millis() == 31_000);
        clock.set(-500);
        check!(clock.now_millis() == -500);
    }

    #[test]
    fn the_system_clock_reads_a_plausible_wall_time() {
        // 2020-01-01T00:00:00Z. The host clock is later than that.
        let year_2020_millis = 1_577_836_800_000;

        check!(SystemClock.now_millis() > year_2020_millis);
    }
}
