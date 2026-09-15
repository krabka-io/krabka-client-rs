//! `ApiVersionTable`: broker-advertised version ranges per API key, the
//! finalized features of the cluster, plus client-side negotiation.

use std::collections::HashMap;

use crate::{error::ClientError, request::ProtocolRequest};

/// The finalized features that a broker reported in its `ApiVersions` answer
/// (version 3 and later).
///
/// Kafka's `NodeApiVersions` keeps the same data: the epoch of the finalized
/// features, and the `max_version_level` of each finalized feature. A broker
/// that answers an older `ApiVersions` version reports no features and the
/// epoch -1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizedFeatures {
    epoch: i64,
    max_version_levels: HashMap<String, i16>,
}

impl Default for FinalizedFeatures {
    fn default() -> Self {
        Self {
            epoch: -1,
            max_version_levels: HashMap::new(),
        }
    }
}

impl FinalizedFeatures {
    /// Build from the finalized features epoch and a sequence of
    /// `(name, max_version_level)` pairs.
    #[must_use]
    pub fn new(epoch: i64, levels: impl IntoIterator<Item = (String, i16)>) -> Self {
        Self {
            epoch,
            max_version_levels: levels.into_iter().collect(),
        }
    }

    /// The epoch of the finalized features, or -1 when the broker reported
    /// none.
    #[must_use]
    pub const fn epoch(&self) -> i64 {
        self.epoch
    }

    /// The finalized `max_version_level` of the feature `name`, for example
    /// `transaction.version`, or `None` when the feature is not finalized.
    #[must_use]
    pub fn max_version_level(&self, name: &str) -> Option<i16> {
        self.max_version_levels.get(name).copied()
    }
}

#[derive(Debug, Clone, Default)]
pub struct ApiVersionTable {
    by_key: HashMap<i16, (i16, i16)>,
    finalized_features: FinalizedFeatures,
}

impl ApiVersionTable {
    /// Build from a sequence of `(api_key, broker_min, broker_max)` tuples.
    /// Used when seeding the table from a decoded `ApiVersionsResponse`.
    #[must_use]
    pub fn from_entries(entries: impl IntoIterator<Item = (i16, i16, i16)>) -> Self {
        let mut by_key = HashMap::new();
        for (k, lo, hi) in entries {
            by_key.insert(k, (lo, hi));
        }
        Self {
            by_key,
            finalized_features: FinalizedFeatures::default(),
        }
    }

    /// Give the table the finalized features that the broker reported.
    #[must_use]
    pub fn with_finalized_features(mut self, finalized_features: FinalizedFeatures) -> Self {
        self.finalized_features = finalized_features;
        self
    }

    /// The finalized features that the broker reported.
    #[must_use]
    pub const fn finalized_features(&self) -> &FinalizedFeatures {
        &self.finalized_features
    }

    /// Highest version both sides support for `R`, or
    /// [`ClientError::IncompatibleVersion`] if the ranges do not overlap.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub fn negotiate<R: ProtocolRequest>(&self) -> Result<i16, ClientError> {
        let api_key = R::API_KEY;
        let client_min = R::MIN_VERSION;
        let client_max = R::MAX_VERSION;
        let (broker_min, broker_max) = self.by_key.get(&api_key).copied().unwrap_or((0, 0));
        let chosen = client_max.min(broker_max);
        if chosen < client_min || chosen < broker_min {
            return Err(ClientError::IncompatibleVersion {
                api_key,
                broker_min,
                broker_max,
                client_min,
                client_max,
            });
        }
        Ok(chosen)
    }

    /// Return the broker-advertised `(min, max)` version range for `api_key`,
    /// or `None` if the broker did not advertise it.
    #[must_use]
    pub fn broker_range(&self, api_key: i16) -> Option<(i16, i16)> {
        self.by_key.get(&api_key).copied()
    }

    /// Returns `true` if the table contains no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::owned::api_versions_request::ApiVersionsRequest;

    use super::*;

    // `ApiVersionsRequest` acts as a sample `ProtocolRequest`. We only
    // need the trait's constants here; the impl comes from codegen.

    /// A table without features reports the epoch -1 and no feature, as a
    /// broker that answers `ApiVersions` below v3.
    #[test]
    fn finalized_features_report_the_epoch_and_levels_of_the_answer() {
        let features = FinalizedFeatures::new(
            9,
            [
                ("transaction.version".to_owned(), 2),
                ("group.version".to_owned(), 1),
            ],
        );
        let table = ApiVersionTable::from_entries([]).with_finalized_features(features.clone());
        let read = |features: &FinalizedFeatures| {
            (
                features.epoch(),
                features.max_version_level("transaction.version"),
                features.max_version_level("group.version"),
                features.max_version_level("metadata.version"),
            )
        };
        assert!(read(table.finalized_features()) == (9, Some(2), Some(1), None));
        assert!(*table.finalized_features() == features);
        assert!(read(ApiVersionTable::default().finalized_features()) == (-1, None, None, None));
    }

    #[test]
    fn negotiate_takes_min_of_max() {
        let t = ApiVersionTable::from_entries([(
            ApiVersionsRequest::API_KEY,
            0,
            ApiVersionsRequest::MAX_VERSION,
        )]);
        // Sanity: client max wins if broker max is higher.
        let _ = t.negotiate::<ApiVersionsRequest>().unwrap();
    }

    #[test]
    fn negotiate_errors_when_disjoint() {
        let t = ApiVersionTable::from_entries([(ApiVersionsRequest::API_KEY, 99, 100)]);
        assert!(matches!(
            t.negotiate::<ApiVersionsRequest>(),
            Err(ClientError::IncompatibleVersion { .. })
        ));
    }

    #[test]
    fn negotiate_picks_lowest_supported_when_broker_caps_low() {
        let t = ApiVersionTable::from_entries([(ApiVersionsRequest::API_KEY, 0, 0)]);
        // Both sides support 0; that's what's chosen.
        assert!(t.negotiate::<ApiVersionsRequest>().unwrap() == 0);
    }

    #[test]
    fn broker_range_reports_exact_advertised_bounds() {
        let t = ApiVersionTable::from_entries([(ApiVersionsRequest::API_KEY, 2, 4)]);

        assert!(t.broker_range(ApiVersionsRequest::API_KEY) == Some((2, 4)));
        assert!(t.broker_range(ApiVersionsRequest::API_KEY + 1).is_none());
    }

    #[test]
    fn is_empty_reflects_whether_any_versions_were_advertised() {
        assert!(ApiVersionTable::default().is_empty());
        assert!(!ApiVersionTable::from_entries([(ApiVersionsRequest::API_KEY, 0, 1)]).is_empty());
    }
}
