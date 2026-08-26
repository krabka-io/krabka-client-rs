//! Shared `FindCoordinator` (`api_key=10`) request and response helpers.
//!
//! Kafka answers `FindCoordinator` in two wire shapes. Versions 0 to 3 carry
//! one answer in the top-level `node_id`, `host`, and `port` fields. Version 4
//! added the `coordinators` array of KIP-699, which holds one row per key.
//!
//! [`build_find_coordinator`] sets the fields of both shapes. The codegen
//! encodes only the set that is valid for the negotiated version, so one
//! request works at any version. [`coordinator_endpoint`] reads the answer back
//! from either shape.
//!
//! These helpers do not retry. Each client crate keeps its own retry policy on
//! top of them.

use crabka_protocol::owned::{
    find_coordinator_request::FindCoordinatorRequest,
    find_coordinator_response::FindCoordinatorResponse,
};

use crate::{connection::Connection, error::ClientError};

/// The key space that a `FindCoordinator` lookup asks about.
///
/// Kafka puts this on the wire as the `key_type` field, an `i8`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CoordinatorKeyType {
    /// The key is a consumer group id. Kafka sends `0`.
    Group,
    /// The key is a transactional id. Kafka sends `1`.
    Transaction,
}

impl CoordinatorKeyType {
    /// Give the `i8` that Kafka puts in the `key_type` field for this variant.
    #[must_use]
    pub fn as_wire(self) -> i8 {
        match self {
            Self::Group => 0,
            Self::Transaction => 1,
        }
    }
}

/// The broker that coordinates one consumer group or one transactional id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatorEndpoint {
    /// Broker id of the coordinator.
    pub node_id: i32,
    /// Host name that the coordinator advertises.
    pub host: String,
    /// Port that the coordinator advertises.
    pub port: i32,
}

impl CoordinatorEndpoint {
    /// Join the host and the port into the `host:port` form that the connect
    /// path takes.
    #[must_use]
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// Build a `FindCoordinator` request for `key` that works at any negotiated
/// version.
///
/// This function sets the `key` and `key_type` fields of versions 0 to 3, and
/// the `coordinator_keys` array of version 4 and later. The codegen drops the
/// set that the negotiated version does not define.
#[must_use]
pub fn build_find_coordinator(key: &str, key_type: CoordinatorKeyType) -> FindCoordinatorRequest {
    FindCoordinatorRequest {
        key: key.to_owned(),
        key_type: key_type.as_wire(),
        coordinator_keys: vec![key.to_owned()],
        ..Default::default()
    }
}

/// Read the coordinator endpoint for `key` out of a `FindCoordinator` response.
///
/// This function prefers the `coordinators` row whose `key` field matches
/// `key`, which version 4 and later return. It falls back to the top-level
/// `node_id`, `host`, and `port` fields, which versions 0 to 3 return.
///
/// # Errors
///
/// Returns [`ClientError::Server`] with the broker's code when the matched row
/// or the top-level response carries a non-zero `error_code`. Returns
/// [`ClientError::NoCoordinator`] when neither shape gives a host name.
pub fn coordinator_endpoint(
    key: &str,
    response: FindCoordinatorResponse,
) -> Result<CoordinatorEndpoint, ClientError> {
    if let Some(coordinator) = response
        .coordinators
        .into_iter()
        .find(|coordinator| coordinator.key == key)
    {
        if coordinator.error_code != 0 {
            return Err(ClientError::Server {
                error_code: coordinator.error_code,
            });
        }
        if !coordinator.host.is_empty() {
            return Ok(CoordinatorEndpoint {
                node_id: coordinator.node_id,
                host: coordinator.host,
                port: coordinator.port,
            });
        }
    }

    if response.error_code != 0 {
        return Err(ClientError::Server {
            error_code: response.error_code,
        });
    }
    if response.host.is_empty() {
        return Err(ClientError::NoCoordinator {
            key: key.to_owned(),
        });
    }
    Ok(CoordinatorEndpoint {
        node_id: response.node_id,
        host: response.host,
        port: response.port,
    })
}

/// Ask the broker behind `conn` which broker coordinates `key`.
///
/// Any broker answers `FindCoordinator`, so `conn` does not have to be the
/// coordinator itself. This function sends one request and reads the answer
/// with [`coordinator_endpoint`]. It does not retry, so a caller that handles
/// the cold-coordinator codes should wrap it.
///
/// # Errors
///
/// Returns [`ClientError::IncompatibleVersion`], [`ClientError::Disconnected`],
/// or [`ClientError::Timeout`] when the send fails. Returns
/// [`ClientError::Server`] when the broker answers with a non-zero error code,
/// and [`ClientError::NoCoordinator`] when the answer holds no host name.
pub async fn find_coordinator(
    conn: &Connection,
    key: &str,
    key_type: CoordinatorKeyType,
) -> Result<CoordinatorEndpoint, ClientError> {
    let response = conn.send(build_find_coordinator(key, key_type)).await?;
    coordinator_endpoint(key, response)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_protocol::{UnknownTaggedFields, owned::find_coordinator_response::Coordinator};

    use super::*;

    #[test]
    fn key_types_use_the_wire_codes_that_kafka_defines() {
        let cases = [
            (CoordinatorKeyType::Group, 0_i8),
            (CoordinatorKeyType::Transaction, 1_i8),
        ];

        for (key_type, wire) in cases {
            assert!(key_type.as_wire() == wire, "{key_type:?}");
        }
    }

    #[test]
    fn the_request_carries_both_the_legacy_key_and_the_batched_key_list() {
        let cases = [
            (CoordinatorKeyType::Group, "orders", 0_i8),
            (CoordinatorKeyType::Transaction, "payments", 1_i8),
        ];

        for (key_type, key, wire) in cases {
            let request = build_find_coordinator(key, key_type);

            assert!(
                request
                    == FindCoordinatorRequest {
                        key: key.to_owned(),
                        key_type: wire,
                        coordinator_keys: vec![key.to_owned()],
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    }
            );
        }
    }

    #[test]
    fn the_batched_row_for_the_asked_key_wins_over_the_other_rows() {
        let response = FindCoordinatorResponse {
            coordinators: vec![
                Coordinator {
                    key: "other".to_owned(),
                    node_id: 7,
                    host: "wrong".to_owned(),
                    port: 1,
                    ..Default::default()
                },
                Coordinator {
                    key: "payments".to_owned(),
                    node_id: 3,
                    host: "coordinator".to_owned(),
                    port: 9092,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let endpoint = coordinator_endpoint("payments", response).expect("matching row");

        assert!(
            endpoint
                == CoordinatorEndpoint {
                    node_id: 3,
                    host: "coordinator".to_owned(),
                    port: 9092,
                }
        );
        assert!(endpoint.address() == "coordinator:9092");
    }

    #[test]
    fn an_empty_batched_array_falls_back_to_the_top_level_fields() {
        let response = FindCoordinatorResponse {
            node_id: 2,
            host: "legacy".to_owned(),
            port: 9093,
            ..Default::default()
        };

        let endpoint = coordinator_endpoint("orders", response).expect("top-level answer");

        assert!(
            endpoint
                == CoordinatorEndpoint {
                    node_id: 2,
                    host: "legacy".to_owned(),
                    port: 9093,
                }
        );
    }

    #[test]
    fn a_batched_array_without_the_asked_key_falls_back_to_the_top_level_fields() {
        let response = FindCoordinatorResponse {
            node_id: 2,
            host: "legacy".to_owned(),
            port: 9093,
            coordinators: vec![Coordinator {
                key: "other".to_owned(),
                host: "wrong".to_owned(),
                port: 1,
                ..Default::default()
            }],
            ..Default::default()
        };

        let endpoint = coordinator_endpoint("orders", response).expect("top-level answer");

        assert!(
            endpoint
                == CoordinatorEndpoint {
                    node_id: 2,
                    host: "legacy".to_owned(),
                    port: 9093,
                }
        );
    }

    #[test]
    fn an_error_code_on_the_matched_row_becomes_a_server_error() {
        let response = FindCoordinatorResponse {
            host: "legacy".to_owned(),
            port: 9093,
            coordinators: vec![Coordinator {
                key: "payments".to_owned(),
                error_code: 15,
                error_message: Some("coordinator not available".to_owned()),
                ..Default::default()
            }],
            ..Default::default()
        };

        let error = coordinator_endpoint("payments", response).expect_err("row error code");

        assert!(matches!(error, ClientError::Server { error_code: 15 }));
    }

    #[test]
    fn a_top_level_error_code_becomes_a_server_error() {
        let response = FindCoordinatorResponse {
            error_code: 16,
            error_message: Some("not coordinator".to_owned()),
            ..Default::default()
        };

        let error = coordinator_endpoint("orders", response).expect_err("top-level error code");

        assert!(matches!(error, ClientError::Server { error_code: 16 }));
    }

    #[test]
    fn a_response_with_no_host_in_either_shape_reports_no_coordinator() {
        let error = coordinator_endpoint("orders", FindCoordinatorResponse::default())
            .expect_err("no entry for the key");

        assert!(matches!(&error, ClientError::NoCoordinator { key } if key == "orders"));
        assert!(error.to_string() == r#"FindCoordinator returned no entry for key "orders""#);
    }

    #[test]
    fn the_address_joins_the_host_and_the_port_with_a_colon() {
        let endpoint = CoordinatorEndpoint {
            node_id: 1,
            host: "broker-1.example".to_owned(),
            port: 9092,
        };

        assert!(endpoint.address() == "broker-1.example:9092");
    }
}
