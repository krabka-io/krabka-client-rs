//! Kafka's exponential backoff with random jitter.

use std::{
    hash::{BuildHasher, RandomState},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use krabka_units::{Time, convert::TimeExt as _};

/// Kafka's `ExponentialBackoff`: `initial * multiplier^attempts`, times a
/// random factor in `[1 - jitter, 1 + jitter)`, and never above `max`.
///
/// Kafka uses it with multiplier 2 and jitter 0.2 for `reconnect.backoff.ms`,
/// `socket.connection.setup.timeout.ms` and `retry.backoff.ms`
/// (`ClusterConnectionStates`, `CommonClientConfigs`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ExponentialBackoff {
    initial_ms: f64,
    multiplier: f64,
    max_ms: f64,
    jitter: f64,
    exp_max: f64,
}

/// The jitter of Kafka's connection and retry backoffs.
pub const KAFKA_BACKOFF_JITTER: f64 = 0.2;

/// The multiplier of Kafka's connection and retry backoffs.
pub const KAFKA_BACKOFF_MULTIPLIER: u32 = 2;

impl ExponentialBackoff {
    /// Build a backoff. As in Kafka, an `initial` above `max` becomes `max`,
    /// and a `max` at or below `initial` turns the growth off. A `jitter`
    /// outside `0.0..=1.0` is clamped to that range.
    #[must_use]
    pub fn new(initial: Time, multiplier: u32, max: Time, jitter: f64) -> Self {
        let jitter = jitter.clamp(0.0, 1.0);
        let max_ms = max.secs_f64() * 1000.0;
        let initial_ms = (initial.secs_f64() * 1000.0).min(max_ms);
        let multiplier = f64::from(multiplier);
        let exp_max = if max_ms > initial_ms {
            (max_ms / initial_ms.max(1.0)).ln() / multiplier.ln()
        } else {
            0.0
        };
        Self {
            initial_ms,
            multiplier,
            max_ms,
            jitter,
            exp_max,
        }
    }

    /// Kafka's connection and retry backoff: multiplier 2 and jitter 0.2.
    #[must_use]
    pub fn kafka(initial: Time, max: Time) -> Self {
        Self::new(initial, KAFKA_BACKOFF_MULTIPLIER, max, KAFKA_BACKOFF_JITTER)
    }

    /// The backoff after `attempts` failures, with a random jitter.
    #[must_use]
    pub fn backoff(&self, attempts: u32) -> Duration {
        self.backoff_with(attempts, random_unit())
    }

    /// The backoff after `attempts` failures for a random value `unit` in
    /// `[0, 1)`. `unit` 0.5 gives the backoff without jitter.
    #[must_use]
    pub fn backoff_with(&self, attempts: u32, unit: f64) -> Duration {
        if self.exp_max == 0.0 {
            return millis_duration(self.initial_ms);
        }
        let exp = f64::from(attempts).min(self.exp_max);
        let term = self.initial_ms * self.multiplier.powf(exp);
        let factor = if self.jitter < f64::MIN_POSITIVE {
            1.0
        } else {
            (1.0 - self.jitter) + unit * 2.0 * self.jitter
        };
        // Kafka truncates the jittered value to whole milliseconds.
        millis_duration((factor * term).trunc().min(self.max_ms))
    }
}

fn millis_duration(milliseconds: f64) -> Duration {
    Duration::from_secs_f64(milliseconds.max(0.0) / 1000.0)
}

/// A random value in `0..bound`, or 0 for a `bound` of 0.
pub(crate) fn random_below(bound: usize) -> usize {
    let bound = u128::try_from(bound).unwrap_or(u128::MAX);
    usize::try_from((u128::from(random_bits()) * bound) >> 32).unwrap_or(0)
}

/// A random value in `[0, 1)`.
fn random_unit() -> f64 {
    f64::from(random_bits()) / 4_294_967_296.0
}

/// 32 random bits. The jitter needs no strong randomness, so a hash of a
/// counter with a random process key is enough.
fn random_bits() -> u32 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    static KEY: std::sync::OnceLock<RandomState> = std::sync::OnceLock::new();
    let hash = KEY
        .get_or_init(RandomState::new)
        .hash_one(COUNTER.fetch_add(1, Ordering::Relaxed));
    u32::try_from(hash >> 32).unwrap_or(u32::MAX)
}
#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_units::{millis, secs};

    use super::*;

    /// `ExponentialBackoffTest` in Kafka: the value doubles per attempt, stays
    /// within the jitter bounds, and never passes the maximum.
    #[test]
    fn backoff_grows_and_caps_as_kafka_does() {
        let reconnect = ExponentialBackoff::kafka(millis(50), secs(1));
        let setup = ExponentialBackoff::kafka(secs(10), secs(30));
        let flat = ExponentialBackoff::kafka(millis(100), millis(100));
        let above_max = ExponentialBackoff::kafka(secs(2), secs(1));
        let ms = Duration::from_millis;
        for (name, backoff, attempts, unit, expected) in [
            ("reconnect, first", reconnect, 0, 0.5, ms(50)),
            ("reconnect, second", reconnect, 1, 0.5, ms(100)),
            ("reconnect, fourth", reconnect, 3, 0.5, ms(400)),
            ("reconnect, low jitter", reconnect, 3, 0.0, ms(320)),
            ("reconnect, capped", reconnect, 10, 0.5, ms(1000)),
            (
                "reconnect, jitter below the cap",
                reconnect,
                4,
                0.99,
                ms(956),
            ),
            (
                "reconnect, capped after jitter",
                reconnect,
                5,
                0.99,
                ms(1000),
            ),
            ("setup timeout, first", setup, 0, 0.5, ms(10_000)),
            ("setup timeout, second", setup, 1, 0.5, ms(20_000)),
            ("setup timeout, capped", setup, 2, 0.5, ms(30_000)),
            ("no growth when max equals initial", flat, 5, 0.9, ms(100)),
            ("initial above max becomes max", above_max, 0, 0.9, ms(1000)),
        ] {
            check!(backoff.backoff_with(attempts, unit) == expected, "{name}");
        }
    }

    #[test]
    fn random_backoff_stays_within_the_jitter_bounds() {
        let backoff = ExponentialBackoff::kafka(millis(50), secs(1));
        for _ in 0..1000 {
            let value = backoff.backoff(2);
            check!(value >= Duration::from_millis(160));
            check!(value < Duration::from_millis(240));
        }
    }
}
