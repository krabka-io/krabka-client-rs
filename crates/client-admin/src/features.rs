//! Cluster feature administration, as Kafka's `Admin.describeFeatures` and
//! `Admin.updateFeatures`.

use std::{collections::BTreeMap, time::Duration};

use krabka_protocol::owned::{
    api_versions_request::ApiVersionsRequest,
    api_versions_response::ApiVersionsResponse,
    update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest},
    update_features_response::UpdateFeaturesResponse,
};
use krabka_units::{Time, convert::TimeExt as _};

use crate::{
    AdminClient, AdminError, KafkaError, MetadataVersionUpdate, NOT_CONTROLLER, kafka_error_if,
    kafka_error_name,
    log_dirs::{NodeTarget, call_node},
    retry::{ControllerRetry, CoordinatorRetry, RetryPolicy},
};

const METADATA_VERSION_FEATURE: &str = "metadata.version";

/// `UNKNOWN_SERVER_ERROR`: Kafka's code for a plain `ApiException`.
const UNKNOWN_SERVER_ERROR: i16 = -1;

/// The lowest `ApiVersions` version whose response carries features.
const FEATURES_API_VERSIONS_MIN_VERSION: i16 = 3;

/// The lowest `UpdateFeatures` version with `UpgradeType` and `ValidateOnly`.
/// Kafka's generated `UpdateFeaturesRequestData` refuses to write either
/// field at a non-default value at v0, where neither exists.
const UPGRADE_TYPE_MIN_VERSION: i16 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureRange {
    pub name: String,
    pub min_version: i16,
    pub max_version: i16,
}

/// The features that one node reports, as Kafka's `FeatureMetadata`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureMetadata {
    pub supported: Vec<FeatureRange>,
    pub finalized: Vec<FeatureRange>,
    /// The epoch of the finalized features. `None` when the node reports a
    /// negative epoch, as Kafka's `finalizedFeaturesEpoch` is empty then.
    pub finalized_features_epoch: Option<i64>,
}

/// What kind of change a [`FeatureUpdate`] asks for, as Kafka's
/// `FeatureUpdate.UpgradeType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UpgradeType {
    /// Raise the finalized level only.
    Upgrade,
    /// Lower the finalized level, or delete the feature, only where no
    /// metadata is lost.
    SafeDowngrade,
    /// Lower the finalized level, or delete the feature, even where metadata
    /// is lost.
    UnsafeDowngrade,
}

impl UpgradeType {
    /// The `UpgradeType` code of the wire request: 1, 2 or 3.
    #[must_use]
    pub const fn code(self) -> i8 {
        match self {
            Self::Upgrade => 1,
            Self::SafeDowngrade => 2,
            Self::UnsafeDowngrade => 3,
        }
    }
}

/// One finalized-feature change, as Kafka's `FeatureUpdate`. The feature name
/// is the key of the map that [`AdminClient::update_features`] takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeatureUpdate {
    max_version_level: i16,
    upgrade_type: UpgradeType,
}

impl FeatureUpdate {
    /// A change of the feature to `max_version_level`. Level 0 deletes the
    /// feature and needs a downgrade type.
    ///
    /// # Errors
    /// Returns [`AdminError::InvalidArgument`] for level 0 with
    /// [`UpgradeType::Upgrade`] and for a negative level, as Kafka's
    /// constructor throws `IllegalArgumentException`.
    pub fn new(max_version_level: i16, upgrade_type: UpgradeType) -> Result<Self, AdminError> {
        if max_version_level == 0 && upgrade_type == UpgradeType::Upgrade {
            return Err(AdminError::InvalidArgument(format!(
                "The upgradeType flag should be set to SAFE_DOWNGRADE or UNSAFE_DOWNGRADE when \
                 the provided maxVersionLevel:{max_version_level} is < 1."
            )));
        }
        if max_version_level < 0 {
            return Err(AdminError::InvalidArgument(
                "Cannot specify a negative version level.".into(),
            ));
        }
        Ok(Self {
            max_version_level,
            upgrade_type,
        })
    }

    /// The new maximum finalized level.
    #[must_use]
    pub const fn max_version_level(&self) -> i16 {
        self.max_version_level
    }

    /// The kind of change.
    #[must_use]
    pub const fn upgrade_type(&self) -> UpgradeType {
        self.upgrade_type
    }
}

/// Options of [`AdminClient::update_features`], as Kafka's
/// `UpdateFeaturesOptions`.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct UpdateFeaturesOptions {
    /// The deadline of the call, including retries. Each request carries the
    /// time that remains as its `timeout_ms`. `None` uses the client's
    /// `default.api.timeout.ms`.
    pub timeout: Option<Time>,
    /// Ask the controller to validate the updates and apply none. Needs
    /// `UpdateFeatures` v1.
    pub validate_only: bool,
}

/// Options of [`AdminClient::describe_features`], as Kafka's
/// `DescribeFeaturesOptions`.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct DescribeFeaturesOptions {
    /// The deadline of the call, including retries. `None` uses the client's
    /// `default.api.timeout.ms`.
    pub timeout: Option<Time>,
    /// The node to ask. `None` asks the node of the client's connection, as
    /// Kafka's `LeastLoadedBrokerOrActiveKController` picks one. A node id
    /// goes to that broker, or to that controller with a controller
    /// bootstrap, as Kafka's `ConstantNodeIdProvider` does.
    pub node_id: Option<i32>,
}

/// The result of each feature of an [`AdminClient::update_features`] call,
/// as Kafka's `UpdateFeaturesResult.values`.
pub type UpdateFeaturesResults = BTreeMap<String, Result<(), KafkaError>>;

/// `base` with the call deadline `timeout`, when set.
fn call_policy(timeout: Option<Time>, base: RetryPolicy) -> RetryPolicy {
    match timeout {
        Some(timeout) => RetryPolicy {
            timeout: Duration::from_millis(u64::try_from(timeout.millis_i64()).unwrap_or(0)),
            ..base
        },
        None => base,
    }
}

/// The `UpdateFeatures` request of `updates`, as `KafkaAdminClient`'s
/// `updateFeatures` builds it. It sets `UpgradeType` and never the v0
/// `AllowDowngrade`, which stays `false`.
fn update_request(
    updates: &BTreeMap<String, FeatureUpdate>,
    options: UpdateFeaturesOptions,
    timeout_ms: i32,
) -> UpdateFeaturesRequest {
    UpdateFeaturesRequest {
        timeout_ms,
        feature_updates: updates
            .iter()
            .map(|(feature, update)| FeatureUpdateKey {
                feature: feature.clone(),
                max_version_level: update.max_version_level,
                upgrade_type: update.upgrade_type.code(),
                ..Default::default()
            })
            .collect(),
        validate_only: options.validate_only,
        ..Default::default()
    }
}

/// The lowest `UpdateFeatures` version that can carry `updates` and
/// `options`. Kafka's v0 serializer throws `UnsupportedVersionException` for
/// a non-default `UpgradeType` or `ValidateOnly`, so a downgrade or a
/// validate-only call needs v1.
fn update_min_version(
    updates: &BTreeMap<String, FeatureUpdate>,
    options: UpdateFeaturesOptions,
) -> i16 {
    let needs_v1 = options.validate_only
        || updates
            .values()
            .any(|update| update.upgrade_type != UpgradeType::Upgrade);
    if needs_v1 {
        UPGRADE_TYPE_MIN_VERSION
    } else {
        0
    }
}

impl AdminClient {
    /// Returns the supported and cluster-finalized feature ranges of one node,
    /// as Kafka's `describeFeatures`.
    ///
    /// # Errors
    /// Returns a transport, protocol, or broker error. With a node id, a node
    /// that the metadata does not name, or that does not answer, is retried
    /// until the deadline and then gives `REQUEST_TIMED_OUT` (7).
    pub async fn describe_features(
        &self,
        options: DescribeFeaturesOptions,
    ) -> Result<FeatureMetadata, AdminError> {
        let request = ApiVersionsRequest {
            client_software_name: "krabka-client-rs".into(),
            client_software_version: env!("CARGO_PKG_VERSION").into(),
            ..Default::default()
        };
        let policy = call_policy(options.timeout, self.retry);
        let response = if let Some(node_id) = options.node_id {
            call_node(
                &self.conn,
                NodeTarget {
                    node_id,
                    supports_controllers: true,
                    min_version: Some(FEATURES_API_VERSIONS_MIN_VERSION),
                },
                &self.options,
                request,
                CoordinatorRetry::new(policy),
            )
            .await
            .map_err(|error| AdminError::Broker {
                api: "ApiVersions",
                code: error.code,
                name: error.name,
                message: error.message,
            })?
        } else {
            ControllerRetry::new("ApiVersions", policy)
                .bounded(
                    self.conn
                        .send_at_least(request, FEATURES_API_VERSIONS_MIN_VERSION),
                )
                .await?
        };
        parse_feature_metadata(response)
    }

    /// Applies finalized-feature updates through `UpdateFeatures` on the
    /// active controller, as Kafka's `updateFeatures`.
    ///
    /// Every feature of `updates` gets a result. A top-level error of the
    /// controller fails every feature with it. A v2 response with no
    /// per-feature results and no error succeeds every feature, and a
    /// feature that a v0/v1 response leaves out fails with
    /// `UNKNOWN_SERVER_ERROR`, as Kafka does.
    ///
    /// # Errors
    /// Returns [`AdminError::InvalidArgument`], and sends nothing, for no
    /// updates or a blank feature name, as Kafka throws
    /// `IllegalArgumentException`. Returns
    /// [`krabka_client_core::ClientError::IncompatibleVersion`] before
    /// sending when a downgrade or [`UpdateFeaturesOptions::validate_only`]
    /// meets a controller with only v0, as Kafka's request serializer throws
    /// `UnsupportedVersionException`. A `NOT_CONTROLLER` answer is retried
    /// after controller discovery until the deadline, then gives
    /// `REQUEST_TIMED_OUT` (7). Transport errors are returned as they are.
    pub async fn update_features(
        &mut self,
        updates: &BTreeMap<String, FeatureUpdate>,
        options: UpdateFeaturesOptions,
    ) -> Result<UpdateFeaturesResults, AdminError> {
        if updates.is_empty() {
            return Err(AdminError::InvalidArgument(
                "Feature updates can not be null or empty.".into(),
            ));
        }
        if updates.keys().any(|feature| feature.trim().is_empty()) {
            return Err(AdminError::InvalidArgument(
                "Provided feature can not be empty.".into(),
            ));
        }
        let min_version = update_min_version(updates, options);
        let mut retry =
            ControllerRetry::new("UpdateFeatures", call_policy(options.timeout, self.retry));
        loop {
            let request = update_request(updates, options, retry.remaining_millis());
            let response = retry
                .bounded(self.conn.send_at_least(request, min_version))
                .await?;
            if response.error_code != NOT_CONTROLLER {
                return Ok(update_results(updates, response));
            }
            retry.after_not_controller(self).await?;
        }
    }

    /// Finalize `metadata.version` at `level` through `UpdateFeatures`.
    ///
    /// # Errors
    /// Returns a transport, protocol, or broker error when Kafka rejects the
    /// requested level.
    pub async fn update_metadata_version(
        &mut self,
        level: i16,
        upgrade_type: UpgradeType,
        timeout: Time,
    ) -> Result<MetadataVersionUpdate, AdminError> {
        let updates = BTreeMap::from([(
            METADATA_VERSION_FEATURE.to_owned(),
            FeatureUpdate::new(level, upgrade_type)?,
        )]);
        let results = self
            .update_features(
                &updates,
                UpdateFeaturesOptions {
                    timeout: Some(timeout),
                    validate_only: false,
                },
            )
            .await?;
        if let Some(Err(error)) = results.into_values().next() {
            return Err(AdminError::Broker {
                api: "UpdateFeatures",
                code: error.code,
                name: error.name,
                message: error.message,
            });
        }
        Ok(MetadataVersionUpdate { level })
    }
}

fn parse_feature_metadata(response: ApiVersionsResponse) -> Result<FeatureMetadata, AdminError> {
    if response.error_code != 0 {
        return Err(AdminError::Broker {
            api: "ApiVersions",
            code: response.error_code,
            name: kafka_error_name(response.error_code),
            message: None,
        });
    }
    Ok(FeatureMetadata {
        supported: response
            .supported_features
            .into_iter()
            .map(|feature| FeatureRange {
                name: feature.name,
                min_version: feature.min_version,
                max_version: feature.max_version,
            })
            .collect(),
        finalized: response
            .finalized_features
            .into_iter()
            .map(|feature| FeatureRange {
                name: feature.name,
                min_version: feature.min_version_level,
                max_version: feature.max_version_level,
            })
            .collect(),
        finalized_features_epoch: (response.finalized_features_epoch >= 0)
            .then_some(response.finalized_features_epoch),
    })
}

/// The result of each feature of `updates` from `response`, as
/// `KafkaAdminClient`'s `updateFeatures` completes its futures.
fn update_results(
    updates: &BTreeMap<String, FeatureUpdate>,
    response: UpdateFeaturesResponse,
) -> UpdateFeaturesResults {
    if let Some(error) = kafka_error_if(response.error_code, response.error_message) {
        return updates
            .keys()
            .map(|feature| (feature.clone(), Err(error.clone())))
            .collect();
    }
    if response.results.is_empty() {
        return updates
            .keys()
            .map(|feature| (feature.clone(), Ok(())))
            .collect();
    }
    let mut results = BTreeMap::new();
    for result in response.results {
        if !updates.contains_key(&result.feature) {
            tracing::warn!(
                feature = %result.feature,
                "server response mentioned unknown feature"
            );
            continue;
        }
        let outcome = match kafka_error_if(result.error_code, result.error_message) {
            Some(error) => Err(error),
            None => Ok(()),
        };
        results.insert(result.feature, outcome);
    }
    for feature in updates.keys() {
        results.entry(feature.clone()).or_insert_with(|| {
            Err(KafkaError {
                code: UNKNOWN_SERVER_ERROR,
                name: kafka_error_name(UNKNOWN_SERVER_ERROR),
                message: Some(format!(
                    "The controller response did not contain a result for feature {feature}"
                )),
            })
        });
    }
    results
}

#[cfg(test)]
mod tests {
    use std::{
        net::SocketAddr,
        sync::{Arc, Mutex},
    };

    use krabka_client_core::{ClientError, MockBroker, MockReply};
    use krabka_protocol::owned::{
        api_versions_request,
        api_versions_response::{ApiVersion, FinalizedFeatureKey, SupportedFeatureKey},
        metadata_request,
        metadata_response::{MetadataResponse, MetadataResponseBroker},
        update_features_request,
        update_features_response::UpdatableFeatureResult,
    };

    use super::*;
    use crate::partition_leaders::test_support::{decode_request, encode_response, fast_admin};

    fn update(level: i16, upgrade_type: UpgradeType) -> FeatureUpdate {
        FeatureUpdate::new(level, upgrade_type).expect("valid update")
    }

    fn key(feature: &str, max_version_level: i16, upgrade_type: i8) -> FeatureUpdateKey {
        FeatureUpdateKey {
            feature: feature.into(),
            max_version_level,
            upgrade_type,
            ..Default::default()
        }
    }

    fn error(code: i16, message: Option<&str>) -> KafkaError {
        KafkaError {
            code,
            name: kafka_error_name(code),
            message: message.map(Into::into),
        }
    }

    #[test]
    fn feature_update_refuses_what_kafka_refuses() {
        for (name, level, upgrade_type, ok) in [
            ("upgrade to 1", 1, UpgradeType::Upgrade, true),
            ("safe delete", 0, UpgradeType::SafeDowngrade, true),
            ("unsafe delete", 0, UpgradeType::UnsafeDowngrade, true),
            ("upgrade to 0", 0, UpgradeType::Upgrade, false),
            ("negative level", -1, UpgradeType::SafeDowngrade, false),
        ] {
            let result = FeatureUpdate::new(level, upgrade_type);
            match (ok, result) {
                (true, Ok(update)) => assert2::assert!(
                    (update.max_version_level(), update.upgrade_type()) == (level, upgrade_type),
                    "case {name}"
                ),
                (false, Err(AdminError::InvalidArgument(_))) => {}
                (_, other) => panic!("case {name}: {other:?}"),
            }
        }
    }

    #[test]
    fn upgrade_type_codes_match_kafka() {
        assert2::assert!(
            [
                UpgradeType::Upgrade.code(),
                UpgradeType::SafeDowngrade.code(),
                UpgradeType::UnsafeDowngrade.code(),
            ] == [1, 2, 3]
        );
    }

    #[test]
    fn describe_features_preserves_supported_finalized_and_epoch() {
        for (name, epoch, expected_epoch) in
            [("finalized", 7, Some(7)), ("no finalized epoch", -1, None)]
        {
            let metadata = parse_feature_metadata(ApiVersionsResponse {
                supported_features: vec![SupportedFeatureKey {
                    name: "kraft.version".into(),
                    min_version: 0,
                    max_version: 1,
                    ..Default::default()
                }],
                finalized_features: vec![FinalizedFeatureKey {
                    name: "kraft.version".into(),
                    min_version_level: 1,
                    max_version_level: 1,
                    ..Default::default()
                }],
                finalized_features_epoch: epoch,
                ..Default::default()
            })
            .unwrap();
            assert2::assert!(
                metadata
                    == FeatureMetadata {
                        supported: vec![FeatureRange {
                            name: "kraft.version".into(),
                            min_version: 0,
                            max_version: 1,
                        }],
                        finalized: vec![FeatureRange {
                            name: "kraft.version".into(),
                            min_version: 1,
                            max_version: 1,
                        }],
                        finalized_features_epoch: expected_epoch,
                    },
                "case {name}"
            );
        }
    }

    #[test]
    fn update_results_complete_every_feature_as_kafka_does() {
        let updates = BTreeMap::from([
            ("group.version".to_owned(), update(1, UpgradeType::Upgrade)),
            (
                "metadata.version".to_owned(),
                update(20, UpgradeType::Upgrade),
            ),
        ]);
        let row = |feature: &str, error_code, message: Option<&str>| UpdatableFeatureResult {
            feature: feature.into(),
            error_code,
            error_message: message.map(Into::into),
            ..Default::default()
        };
        let missing = "The controller response did not contain a result for feature \
                       metadata.version";
        for (name, response, expected) in [
            (
                "v2 success has no rows",
                UpdateFeaturesResponse::default(),
                [Ok(()), Ok(())],
            ),
            (
                "top-level error fails every feature",
                UpdateFeaturesResponse {
                    error_code: 95,
                    error_message: Some("bad".into()),
                    ..Default::default()
                },
                [Err(error(95, Some("bad"))), Err(error(95, Some("bad")))],
            ),
            (
                "per-feature rows, one missing, one unknown",
                UpdateFeaturesResponse {
                    results: vec![
                        row("group.version", 95, Some("no")),
                        row("other.version", 0, None),
                    ],
                    ..Default::default()
                },
                [
                    Err(error(95, Some("no"))),
                    Err(error(UNKNOWN_SERVER_ERROR, Some(missing))),
                ],
            ),
        ] {
            let [group, metadata] = expected;
            assert2::assert!(
                update_results(&updates, response)
                    == BTreeMap::from([
                        ("group.version".to_owned(), group),
                        ("metadata.version".to_owned(), metadata),
                    ]),
                "case {name}"
            );
        }
    }

    /// A broker that advertises `UpdateFeatures` v0 up to `max_version`,
    /// records each `UpdateFeatures` request with its version, and answers
    /// with no error.
    async fn update_features_broker(
        max_version: i16,
        seen: Arc<Mutex<Vec<(i16, UpdateFeaturesRequest)>>>,
    ) -> MockBroker {
        MockBroker::start(move |api_key, version, _, body| match api_key {
            api_versions_request::API_KEY => {
                Some(crate::partition_leaders::test_support::api_versions(&[(
                    update_features_request::API_KEY,
                    0,
                    max_version,
                )]))
            }
            update_features_request::API_KEY => {
                let mut request: UpdateFeaturesRequest = decode_request(body, version, true);
                // The deadline that remains differs between runs.
                request.timeout_ms = 0;
                seen.lock().expect("seen lock").push((version, request));
                Some(encode_response(
                    &UpdateFeaturesResponse::default(),
                    version,
                    true,
                ))
            }
            _ => None,
        })
        .await
    }

    /// The whole `UpdateFeatures` request that reaches the controller, for
    /// each version the controller advertises, each upgrade type and each
    /// `validate_only`. A downgrade or a validate-only call is refused
    /// before sending against a v0-only controller, as Kafka's v0 serializer
    /// throws `UnsupportedVersionException` for a non-default `UpgradeType`
    /// or `ValidateOnly`. `AllowDowngrade` is never set, as Kafka never sets
    /// it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn update_features_sends_kafkas_request_per_version() {
        let upgrade_types = [
            UpgradeType::Upgrade,
            UpgradeType::SafeDowngrade,
            UpgradeType::UnsafeDowngrade,
        ];
        for max_version in 0..=2 {
            for upgrade_type in upgrade_types {
                for validate_only in [false, true] {
                    let name = format!("v{max_version} {upgrade_type:?} validate={validate_only}");
                    let seen = Arc::new(Mutex::new(Vec::new()));
                    let broker = update_features_broker(max_version, Arc::clone(&seen)).await;
                    let mut admin = AdminClient::connect(&[broker.addr.to_string()])
                        .await
                        .expect("admin connects");
                    let level = if upgrade_type == UpgradeType::Upgrade {
                        7
                    } else {
                        0
                    };
                    let updates = BTreeMap::from([
                        ("group.version".to_owned(), update(1, upgrade_type)),
                        ("share.version".to_owned(), update(level, upgrade_type)),
                    ]);

                    let result = admin
                        .update_features(
                            &updates,
                            UpdateFeaturesOptions {
                                timeout: None,
                                validate_only,
                            },
                        )
                        .await;
                    broker.stop();

                    let sendable = max_version >= 1
                        || (upgrade_type == UpgradeType::Upgrade && !validate_only);
                    if !sendable {
                        assert2::assert!(
                            matches!(
                                result,
                                Err(AdminError::Transport(ClientError::IncompatibleVersion {
                                    api_key: update_features_request::API_KEY,
                                    client_min: 1,
                                    ..
                                }))
                            ),
                            "case {name}: {result:?}"
                        );
                        assert2::assert!(seen.lock().unwrap().is_empty(), "case {name}");
                        continue;
                    }
                    let code = upgrade_type.code();
                    // v0 carries neither field, so it decodes as the defaults.
                    let (wire_code, wire_validate) = if max_version == 0 {
                        (1, false)
                    } else {
                        (code, validate_only)
                    };
                    assert2::assert!(
                        result.expect("update succeeds")
                            == BTreeMap::from([
                                ("group.version".to_owned(), Ok(())),
                                ("share.version".to_owned(), Ok(())),
                            ]),
                        "case {name}"
                    );
                    assert2::assert!(
                        *seen.lock().unwrap()
                            == vec![(
                                max_version,
                                UpdateFeaturesRequest {
                                    timeout_ms: 0,
                                    feature_updates: vec![
                                        key("group.version", 1, wire_code),
                                        key("share.version", level, wire_code),
                                    ],
                                    validate_only: wire_validate,
                                    ..Default::default()
                                }
                            )],
                        "case {name}"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn update_features_refuses_empty_or_blank_features_before_sending() {
        let broker = MockBroker::start(|api_key, _, _, _| {
            (api_key == api_versions_request::API_KEY).then(|| {
                crate::partition_leaders::test_support::api_versions(&[(
                    update_features_request::API_KEY,
                    0,
                    2,
                )])
            })
        })
        .await;
        let mut admin = AdminClient::connect(&[broker.addr.to_string()])
            .await
            .expect("admin connects");
        for (name, updates) in [
            ("empty", BTreeMap::new()),
            (
                "blank",
                BTreeMap::from([(" ".to_owned(), update(1, UpgradeType::Upgrade))]),
            ),
        ] {
            let result = admin
                .update_features(&updates, UpdateFeaturesOptions::default())
                .await;
            assert2::assert!(
                matches!(result, Err(AdminError::InvalidArgument(_))),
                "case {name}"
            );
        }
        broker.stop();
    }

    /// A broker `node_id` that answers `ApiVersions` with `supported` as its
    /// one supported feature, and `Metadata` with `nodes`.
    async fn feature_node(
        supported: &'static str,
        nodes: Arc<Mutex<Vec<(i32, SocketAddr)>>>,
    ) -> MockBroker {
        MockBroker::start_with_replies(move |api_key, version, _, _| match api_key {
            api_versions_request::API_KEY => MockReply::Respond(encode_response(
                &ApiVersionsResponse {
                    api_keys: vec![
                        ApiVersion {
                            api_key: api_versions_request::API_KEY,
                            min_version: 0,
                            max_version: 4,
                            ..Default::default()
                        },
                        ApiVersion {
                            api_key: metadata_request::API_KEY,
                            min_version: 12,
                            max_version: 12,
                            ..Default::default()
                        },
                    ],
                    supported_features: vec![SupportedFeatureKey {
                        name: supported.into(),
                        min_version: 0,
                        max_version: 1,
                        ..Default::default()
                    }],
                    finalized_features_epoch: 3,
                    ..Default::default()
                },
                version,
                false,
            )),
            metadata_request::API_KEY => MockReply::Respond(encode_response(
                &MetadataResponse {
                    controller_id: 1,
                    brokers: nodes
                        .lock()
                        .expect("nodes lock")
                        .iter()
                        .map(|(node_id, addr)| MetadataResponseBroker {
                            node_id: *node_id,
                            host: addr.ip().to_string(),
                            port: i32::from(addr.port()),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                },
                version,
                true,
            )),
            _ => MockReply::Silent,
        })
        .await
    }

    /// `describe_features` asks the node of the client's connection, or the
    /// node that `node_id` names, as Kafka's `describeFeatures` with a
    /// `DescribeFeaturesOptions.nodeId` does for `kafka-features describe
    /// --node-id`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn describe_features_asks_the_named_node() {
        let nodes = Arc::new(Mutex::new(Vec::new()));
        let first = feature_node("on.node.1", Arc::clone(&nodes)).await;
        let second = feature_node("on.node.2", Arc::clone(&nodes)).await;
        *nodes.lock().unwrap() = vec![(1, first.addr), (2, second.addr)];
        let admin = fast_admin(first.addr, krabka_units::secs(5)).await;

        for (name, node_id, feature) in [
            ("any node", None, "on.node.1"),
            ("node 1", Some(1), "on.node.1"),
            ("node 2", Some(2), "on.node.2"),
        ] {
            let metadata = admin
                .describe_features(DescribeFeaturesOptions {
                    timeout: None,
                    node_id,
                })
                .await
                .expect("describe succeeds");
            assert2::assert!(
                metadata
                    == FeatureMetadata {
                        supported: vec![FeatureRange {
                            name: feature.into(),
                            min_version: 0,
                            max_version: 1,
                        }],
                        finalized: Vec::new(),
                        finalized_features_epoch: Some(3),
                    },
                "case {name}"
            );
        }

        let unknown = admin
            .describe_features(DescribeFeaturesOptions {
                timeout: Some(krabka_units::millis(50)),
                node_id: Some(9),
            })
            .await;
        assert2::assert!(
            matches!(
                unknown,
                Err(AdminError::Broker {
                    code: crate::retry::REQUEST_TIMED_OUT,
                    ..
                })
            ),
            "{unknown:?}"
        );
        first.stop();
        second.stop();
    }

    #[tokio::test]
    async fn update_metadata_version_surfaces_the_feature_error() {
        let broker = MockBroker::start(|api_key, version, _, _| match api_key {
            api_versions_request::API_KEY => {
                Some(crate::partition_leaders::test_support::api_versions(&[(
                    update_features_request::API_KEY,
                    0,
                    1,
                )]))
            }
            update_features_request::API_KEY => Some(encode_response(
                &UpdateFeaturesResponse {
                    results: vec![UpdatableFeatureResult {
                        feature: METADATA_VERSION_FEATURE.into(),
                        error_code: 95,
                        error_message: Some("not supported by every broker".into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                version,
                true,
            )),
            _ => None,
        })
        .await;
        let mut admin = AdminClient::connect(&[broker.addr.to_string()])
            .await
            .expect("admin connects");
        let result = admin
            .update_metadata_version(15, UpgradeType::SafeDowngrade, krabka_units::secs(5))
            .await;
        broker.stop();
        assert2::assert!(
            matches!(
                result,
                Err(AdminError::Broker {
                    api: "UpdateFeatures",
                    code: 95,
                    ..
                })
            ),
            "{result:?}"
        );
    }
}
