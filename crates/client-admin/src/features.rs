//! Cluster feature administration.

use krabka_protocol::owned::{
    api_versions_request::ApiVersionsRequest,
    api_versions_response::ApiVersionsResponse,
    update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest},
    update_features_response::UpdateFeaturesResponse,
};
use krabka_units::{Time, convert::TimeExt as _};

use crate::{
    AdminClient, AdminError, KafkaError, MetadataVersionUpdate, kafka_error_if, kafka_error_name,
};

const METADATA_VERSION_FEATURE: &str = "metadata.version";
const UPGRADE: i8 = 1;
const SAFE_DOWNGRADE: i8 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureRange {
    pub name: String,
    pub min_version: i16,
    pub max_version: i16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureMetadata {
    pub supported: Vec<FeatureRange>,
    pub finalized: Vec<FeatureRange>,
    pub finalized_features_epoch: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureUpdate {
    pub name: String,
    pub max_version_level: i16,
    pub safe_downgrade: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureUpdateOutcome {
    pub name: String,
    pub error: Option<KafkaError>,
}

fn metadata_update_error(
    response: &krabka_protocol::owned::update_features_response::UpdateFeaturesResponse,
) -> Option<AdminError> {
    if response.error_code != 0 {
        return Some(AdminError::Broker {
            api: "UpdateFeatures",
            code: response.error_code,
            name: kafka_error_name(response.error_code),
            message: response.error_message.clone(),
        });
    }
    response
        .results
        .iter()
        .find(|result| result.feature == METADATA_VERSION_FEATURE && result.error_code != 0)
        .map(|error| AdminError::Broker {
            api: "UpdateFeatures",
            code: error.error_code,
            name: kafka_error_name(error.error_code),
            message: error.error_message.clone(),
        })
}

fn update_request(updates: &[FeatureUpdate], timeout: Time) -> UpdateFeaturesRequest {
    UpdateFeaturesRequest {
        timeout_ms: timeout.millis_i32(),
        feature_updates: updates
            .iter()
            .map(|update| FeatureUpdateKey {
                feature: update.name.clone(),
                max_version_level: update.max_version_level,
                allow_downgrade: update.safe_downgrade,
                upgrade_type: if update.safe_downgrade {
                    SAFE_DOWNGRADE
                } else {
                    UPGRADE
                },
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

impl AdminClient {
    /// Returns the broker's supported and cluster-finalized feature ranges.
    ///
    /// # Errors
    /// Returns a transport, protocol, or broker error.
    pub async fn describe_features(&mut self) -> Result<FeatureMetadata, AdminError> {
        let response = self
            .conn
            .send_at_least(
                ApiVersionsRequest {
                    client_software_name: "krabka-client-rs".into(),
                    client_software_version: env!("CARGO_PKG_VERSION").into(),
                    ..Default::default()
                },
                3,
            )
            .await?;
        parse_feature_metadata(response)
    }

    /// Applies arbitrary finalized-feature updates through `UpdateFeatures`.
    ///
    /// # Errors
    /// Returns a transport, protocol, or top-level broker error. Per-feature
    /// broker errors remain attached to their outcomes.
    pub async fn update_features(
        &mut self,
        updates: &[FeatureUpdate],
        timeout: Time,
    ) -> Result<Vec<FeatureUpdateOutcome>, AdminError> {
        let first = self.conn.send(update_request(updates, timeout)).await?;
        if first.error_code != crate::NOT_CONTROLLER {
            return parse_update_features(first);
        }
        self.refresh_controller_connection().await?;
        let second = self.conn.send(update_request(updates, timeout)).await?;
        if second.error_code == crate::NOT_CONTROLLER {
            return Err(AdminError::NotControllerExhausted);
        }
        parse_update_features(second)
    }

    /// Finalize `metadata.version` through `UpdateFeatures`.
    ///
    /// # Errors
    /// Returns a transport, protocol, or broker error when Kafka rejects the
    /// requested level.
    pub async fn update_metadata_version(
        &mut self,
        level: i16,
        safe_downgrade: bool,
        timeout: Time,
    ) -> Result<MetadataVersionUpdate, AdminError> {
        let outcomes = self
            .update_features(
                &[FeatureUpdate {
                    name: METADATA_VERSION_FEATURE.into(),
                    max_version_level: level,
                    safe_downgrade,
                }],
                timeout,
            )
            .await?;
        if let Some(error) = outcomes.into_iter().find_map(|outcome| outcome.error) {
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
        finalized_features_epoch: response.finalized_features_epoch,
    })
}

fn parse_update_features(
    response: UpdateFeaturesResponse,
) -> Result<Vec<FeatureUpdateOutcome>, AdminError> {
    if let Some(error) = metadata_update_error(&response).filter(|_| response.error_code != 0) {
        return Err(error);
    }
    Ok(response
        .results
        .into_iter()
        .map(|result| FeatureUpdateOutcome {
            name: result.feature,
            error: kafka_error_if(result.error_code, result.error_message),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use krabka_protocol::{
        Decode, Encode,
        owned::api_versions_response::{FinalizedFeatureKey, SupportedFeatureKey},
    };

    use super::*;

    #[test]
    fn describe_features_preserves_supported_finalized_and_epoch() {
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
            finalized_features_epoch: 7,
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
                    finalized_features_epoch: 7,
                }
        );
    }

    #[test]
    fn metadata_update_selects_safe_downgrade() {
        let request = update_request(
            &[FeatureUpdate {
                name: METADATA_VERSION_FEATURE.into(),
                max_version_level: 15,
                safe_downgrade: true,
            }],
            krabka_units::secs(30),
        );
        let update = &request.feature_updates[0];

        assert2::assert!(request.timeout_ms == 30_000);
        assert2::assert!(update.allow_downgrade);
        assert2::assert!(update.upgrade_type == 2);
        assert2::assert!(update.max_version_level == 15);
    }

    #[test]
    fn metadata_safe_downgrade_survives_v0_encoding() {
        let request = update_request(
            &[FeatureUpdate {
                name: METADATA_VERSION_FEATURE.into(),
                max_version_level: 15,
                safe_downgrade: true,
            }],
            krabka_units::secs(30),
        );
        let mut bytes = BytesMut::new();
        request.encode(&mut bytes, 0).unwrap();

        let mut encoded = bytes.freeze();
        let decoded = UpdateFeaturesRequest::decode(&mut encoded, 0).unwrap();
        assert2::assert!(decoded.feature_updates[0].allow_downgrade);
    }

    #[test]
    fn metadata_update_surfaces_row_error_from_v0_or_v1() {
        use krabka_protocol::owned::update_features_response::{
            UpdatableFeatureResult, UpdateFeaturesResponse,
        };

        let response = UpdateFeaturesResponse {
            results: vec![UpdatableFeatureResult {
                feature: METADATA_VERSION_FEATURE.into(),
                error_code: 95,
                error_message: Some("not supported by every broker".into()),
                ..Default::default()
            }],
            ..Default::default()
        };

        assert2::assert!(matches!(
            metadata_update_error(&response),
            Some(AdminError::Broker { code: 95, .. })
        ));
    }

    #[test]
    fn metadata_update_recognizes_not_controller_for_retry() {
        let response = krabka_protocol::owned::update_features_response::UpdateFeaturesResponse {
            error_code: crate::NOT_CONTROLLER,
            ..Default::default()
        };

        assert2::assert!(matches!(
            metadata_update_error(&response),
            Some(AdminError::Broker {
                code: crate::NOT_CONTROLLER,
                ..
            })
        ));
    }
}
