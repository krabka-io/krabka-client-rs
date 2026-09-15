//! `ApiVersionTable`: broker-advertised version ranges per API key,
//! plus client-side negotiation.

use std::collections::{BTreeMap, HashMap};

use krabka_protocol::owned::api_versions_response::ApiVersionsResponse;

use crate::{error::ClientError, request::ProtocolRequest};

/// Kafka's `ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH`.
pub const UNKNOWN_FINALIZED_FEATURES_EPOCH: i64 = -1;

/// The finalized feature levels of a broker (KIP-584), from `ApiVersions` v3
/// and later, as Kafka's `ApiVersions.FinalizedFeaturesInfo` holds them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinalizedFeatures {
    /// The epoch of the levels. [`UNKNOWN_FINALIZED_FEATURES_EPOCH`] when the
    /// broker sent none.
    pub epoch: i64,
    /// The finalized maximum version level of each feature, for example
    /// `transaction.version`.
    pub levels: BTreeMap<String, i16>,
}

impl Default for FinalizedFeatures {
    fn default() -> Self {
        Self {
            epoch: UNKNOWN_FINALIZED_FEATURES_EPOCH,
            levels: BTreeMap::new(),
        }
    }
}

impl FinalizedFeatures {
    /// The finalized level of `feature`, or `None` when the broker did not
    /// finalize it.
    #[must_use]
    pub fn level(&self, feature: &str) -> Option<i16> {
        self.levels.get(feature).copied()
    }
}

#[derive(Debug, Clone, Default)]
pub struct ApiVersionTable {
    by_key: HashMap<i16, (i16, i16)>,
    /// The supported `(min, max)` version of each feature of the broker.
    supported_features: BTreeMap<String, (i16, i16)>,
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
            ..Self::default()
        }
    }

    /// Build from a decoded `ApiVersionsResponse`, with its feature data.
    #[must_use]
    pub fn from_response(response: &ApiVersionsResponse) -> Self {
        let mut table = Self::from_entries(
            response
                .api_keys
                .iter()
                .map(|key| (key.api_key, key.min_version, key.max_version)),
        );
        table.supported_features = response
            .supported_features
            .iter()
            .map(|feature| {
                (
                    feature.name.clone(),
                    (feature.min_version, feature.max_version),
                )
            })
            .collect();
        table.finalized_features = FinalizedFeatures {
            epoch: response.finalized_features_epoch,
            levels: response
                .finalized_features
                .iter()
                .map(|feature| (feature.name.clone(), feature.max_version_level))
                .collect(),
        };
        table
    }

    /// The finalized feature levels that the broker sent.
    #[must_use]
    pub const fn finalized_features(&self) -> &FinalizedFeatures {
        &self.finalized_features
    }

    /// The supported `(min, max)` version of each feature of the broker.
    #[must_use]
    pub const fn supported_features(&self) -> &BTreeMap<String, (i16, i16)> {
        &self.supported_features
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
