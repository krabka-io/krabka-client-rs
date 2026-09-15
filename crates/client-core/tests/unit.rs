//! MockBroker-based unit tests for `Connection`.
//!
//! These tests start an in-process mock Kafka broker and verify that
//! `Connection::connect`, `Connection::send`, and the timeout path all
//! behave correctly without any JVM dependency.
//!
//! Run with: `cargo test -p krabka-client-core --features mock --test unit`

use assert2::{assert, check};
use bytes::BytesMut;
use krabka_client_core::{
    ClientError, Connection, ConnectionOptions, FinalizedFeatures, MockBroker,
};
// Use the raw constants so we don't need `ProtocolRequest` in scope.
use krabka_protocol::owned::api_versions_request;
use krabka_protocol::{
    Encode,
    owned::{
        api_versions_response::{ApiVersion, ApiVersionsResponse},
        metadata_request as metadata_request_mod,
        metadata_request::MetadataRequest,
        metadata_response::MetadataResponse,
    },
};

// ── helpers ──────────────────────────────────────────────────────────────────

/// Encode an `ApiVersionsResponse` at version 0 that advertises:
/// - `api_key` 18 (`ApiVersions`): min 0, max 3
/// - `api_key` 3 (`Metadata`): min 0, max 12
///
/// The mock returns this as the response body. It comes after the
/// correlation-id, which `MockBroker` prepends automatically.
fn api_versions_response_v0() -> Vec<u8> {
    let resp = ApiVersionsResponse {
        error_code: 0,
        api_keys: vec![
            ApiVersion {
                api_key: api_versions_request::API_KEY,
                min_version: 0,
                max_version: 3,
                ..Default::default()
            },
            ApiVersion {
                api_key: metadata_request_mod::API_KEY,
                min_version: 0,
                max_version: 12,
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    resp.encode(&mut buf, 0).unwrap();
    buf.to_vec()
}

/// Encode a response body at the given version, with the correct
/// `ResponseHeader` prefix.
///
/// Kafka's `ResponseHeader`:
/// - v0 (non-flexible): only `correlation_id` (already prepended by `MockBroker`).
///   No additional bytes.
/// - v1 (flexible, i.e., version >= `FLEXIBLE_MIN`): one additional byte,
///   the UVARINT-encoded empty tagged-fields count (0x00).
///
/// Exception: `ApiVersionsResponse` always uses `ResponseHeader` v0 even when
/// the request version is flexible. The tests here handle `MetadataResponse`,
/// which follows the normal rule.
fn metadata_response_at(version: i16) -> Vec<u8> {
    metadata_response_with_throttle(version, 0)
}

/// Encode a `MetadataResponse` at the given version with a custom
/// `throttle_time_ms`, preceded by the correct `ResponseHeader` prefix.
fn metadata_response_with_throttle(version: i16, throttle_time_ms: i32) -> Vec<u8> {
    use krabka_protocol::owned::metadata_response::FLEXIBLE_MIN;
    let resp = MetadataResponse {
        throttle_time_ms,
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    // Prepend ResponseHeader v1 tagged-fields byte for flexible versions.
    if version >= FLEXIBLE_MIN {
        buf.extend_from_slice(&[0x00u8]); // empty tagged fields
    }
    resp.encode(&mut buf, version).unwrap();
    buf.to_vec()
}

// ── tests ────────────────────────────────────────────────────────────────────

/// `Connection::connect` successfully negotiates API versions via the mock.
#[tokio::test]
async fn connect_negotiates_api_versions() {
    let mock = MockBroker::start(|api_key, _version, _corr_id, _body| {
        // Only the bootstrap ApiVersions call is expected here.
        assert!(api_key == api_versions_request::API_KEY);
        Some(api_versions_response_v0())
    })
    .await;

    let conn = Connection::connect(mock.addr, ConnectionOptions::default())
        .await
        .unwrap();

    check!(!conn.versions().is_empty());
    // The mock advertised api_key 18 min=0 max=3.
    check!(conn.versions().broker_range(api_versions_request::API_KEY) == Some((0, 3)));
    // The mock advertised api_key 3 min=0 max=12.
    check!(conn.versions().broker_range(metadata_request_mod::API_KEY) == Some((0, 12)));

    conn.close();
    mock.stop();
}

/// How the broker of one negotiation row answers `ApiVersions`.
#[derive(Debug, Clone, Copy)]
enum ApiVersionsBroker {
    /// Answers every version up to this one with transaction.version 2, and
    /// `UNSUPPORTED_VERSION` in a v0 body above it, naming this maximum.
    Supports(i16),
    /// Answers `UNSUPPORTED_VERSION` in a v0 body with no `api_keys` above
    /// version 0, as a broker before Kafka 2.4 does.
    SupportsOnlyV0,
    /// Answers every request with this error code in a v0 body.
    Fails(i16),
    /// Answers `UNSUPPORTED_VERSION` in a v0 body that names this maximum,
    /// whatever the request version is.
    RefusesNaming(i16),
}

/// The request that the mock broker got.
#[derive(Debug, PartialEq, Eq)]
struct SeenApiVersions {
    version: i16,
    client_software_name: String,
}

/// What one negotiation gave.
#[derive(Debug, PartialEq, Eq)]
struct Negotiated {
    requests: Vec<SeenApiVersions>,
    /// The advertised metadata range and the finalized features, or the error.
    outcome: Result<(Option<(i16, i16)>, FinalizedFeatures), String>,
}

fn api_versions_answer(broker: ApiVersionsBroker, version: i16) -> Vec<u8> {
    use krabka_protocol::owned::api_versions_response::FinalizedFeatureKey;
    let metadata = ApiVersion {
        api_key: metadata_request_mod::API_KEY,
        min_version: 0,
        max_version: 12,
        ..Default::default()
    };
    let (response, encode_at) = match broker {
        ApiVersionsBroker::Supports(max) if version <= max => (
            ApiVersionsResponse {
                api_keys: vec![metadata],
                finalized_features_epoch: 7,
                finalized_features: vec![FinalizedFeatureKey {
                    name: "transaction.version".into(),
                    max_version_level: 2,
                    min_version_level: 2,
                    ..Default::default()
                }],
                ..Default::default()
            },
            version,
        ),
        ApiVersionsBroker::Supports(max) | ApiVersionsBroker::RefusesNaming(max) => (
            ApiVersionsResponse {
                error_code: 35,
                api_keys: vec![ApiVersion {
                    api_key: api_versions_request::API_KEY,
                    min_version: 0,
                    max_version: max,
                    ..Default::default()
                }],
                ..Default::default()
            },
            0,
        ),
        ApiVersionsBroker::SupportsOnlyV0 if version == 0 => (
            ApiVersionsResponse {
                api_keys: vec![metadata],
                ..Default::default()
            },
            0,
        ),
        ApiVersionsBroker::SupportsOnlyV0 => (
            ApiVersionsResponse {
                error_code: 35,
                ..Default::default()
            },
            0,
        ),
        ApiVersionsBroker::Fails(error_code) => (
            ApiVersionsResponse {
                error_code,
                ..Default::default()
            },
            0,
        ),
    };
    let mut buf = BytesMut::new();
    response.encode(&mut buf, encode_at).unwrap();
    buf.to_vec()
}

/// Kafka's `NetworkClient` sends `ApiVersions` at the latest version. A broker
/// that does not support it answers `UNSUPPORTED_VERSION` in a v0 body with
/// its own range, and the client asks again at the maximum of that range, or
/// at version 0 without a range (`handleApiVersionsResponse`). Kafka's
/// `NodeApiVersions` keeps the finalized features of the answer.
#[tokio::test]
async fn connect_negotiates_the_api_versions_version_and_keeps_finalized_features() {
    use std::sync::{Arc, Mutex};

    use krabka_protocol::{Decode, owned::api_versions_request::ApiVersionsRequest};

    let features = FinalizedFeatures::new(7, [("transaction.version".to_owned(), 2)]);
    let seen = |versions: &[i16]| {
        versions
            .iter()
            .map(|&version| SeenApiVersions {
                version,
                client_software_name: if version >= 3 {
                    "krabka-client-rs".to_owned()
                } else {
                    String::new()
                },
            })
            .collect::<Vec<_>>()
    };
    let cases = [
        (
            "broker supports v5",
            ApiVersionsBroker::Supports(5),
            Negotiated {
                requests: seen(&[5]),
                outcome: Ok((Some((0, 12)), features.clone())),
            },
        ),
        (
            "broker supports up to v3",
            ApiVersionsBroker::Supports(3),
            Negotiated {
                requests: seen(&[5, 3]),
                outcome: Ok((Some((0, 12)), features.clone())),
            },
        ),
        (
            "broker supports up to v2",
            ApiVersionsBroker::Supports(2),
            Negotiated {
                requests: seen(&[5, 2]),
                outcome: Ok((Some((0, 12)), FinalizedFeatures::default())),
            },
        ),
        (
            "broker names no range",
            ApiVersionsBroker::SupportsOnlyV0,
            Negotiated {
                requests: seen(&[5, 0]),
                outcome: Ok((Some((0, 12)), FinalizedFeatures::default())),
            },
        ),
        (
            "unsupported at every version",
            ApiVersionsBroker::Fails(35),
            Negotiated {
                requests: seen(&[5, 0]),
                outcome: Err("protocol error from server: 35".to_owned()),
            },
        ),
        (
            "unsupported naming the refused version",
            ApiVersionsBroker::RefusesNaming(5),
            Negotiated {
                requests: seen(&[5]),
                outcome: Err("protocol error from server: 35".to_owned()),
            },
        ),
        (
            "other error code",
            ApiVersionsBroker::Fails(58),
            Negotiated {
                requests: seen(&[5]),
                outcome: Err("protocol error from server: 58".to_owned()),
            },
        ),
    ];
    for (name, broker, expected) in cases {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let handler_requests = Arc::clone(&requests);
        let mock = MockBroker::start(move |api_key, version, _corr_id, body| {
            assert!(api_key == api_versions_request::API_KEY);
            // The body starts with the client id of the request header, and a
            // flexible version adds a tagged-fields byte after it.
            let client_id_len = usize::try_from(i16::from_be_bytes([body[0], body[1]])).unwrap();
            let flexible = usize::from(version >= api_versions_request::FLEXIBLE_MIN);
            let mut request = &body[2 + client_id_len + flexible..];
            let request = ApiVersionsRequest::decode(&mut request, version).unwrap();
            handler_requests.lock().unwrap().push(SeenApiVersions {
                version,
                client_software_name: request.client_software_name,
            });
            Some(api_versions_answer(broker, version))
        })
        .await;

        let outcome = Connection::connect(mock.addr, ConnectionOptions::default())
            .await
            .map(|conn| {
                let versions = conn.versions();
                let answer = (
                    versions.broker_range(metadata_request_mod::API_KEY),
                    versions.finalized_features().clone(),
                );
                conn.close();
                answer
            })
            .map_err(|error| error.to_string());
        mock.stop();
        let actual = Negotiated {
            requests: std::mem::take(&mut *requests.lock().unwrap()),
            outcome,
        };
        check!(actual == expected, "{name}");
    }
}

/// When the mock never responds to `ApiVersions`, `Connection::connect` returns
/// `ClientError::Timeout` once the request timeout elapses.
///
/// Design note: the handler returns `None`, the `Option<Vec<u8>>` sentinel in
/// `MockBroker`'s API, so the broker silently drops the request and does not
/// send even an empty frame. An empty length-delimited frame would still be a
/// valid frame with 0 body bytes. The client would try to decode it and would
/// then report a codec error rather than a timeout. `None` correctly
/// simulates a hung or unreachable broker.
#[tokio::test]
async fn timeout_when_handler_silent() {
    let mock = MockBroker::start(|_api_key, _version, _corr_id, _body| {
        // Return None to drop the request; the client's request timeout fires.
        None
    })
    .await;

    let opts = ConnectionOptions {
        request_timeout: krabka_units::millis(200),
        ..ConnectionOptions::default()
    };

    match Connection::connect(mock.addr, opts).await {
        Err(ClientError::Timeout(_)) => { /* expected */ }
        Err(other) => panic!("expected Timeout, got: {other:?}"),
        Ok(_conn) => panic!("connect should have timed out but succeeded"),
    }

    mock.stop();
}

/// A full round-trip: connect (`ApiVersions` handshake) then send a
/// `MetadataRequest` and receive a `MetadataResponse`.
///
/// The mock encodes the `MetadataResponse` at the same version as the
/// request, which is the version negotiated by the client.
#[tokio::test]
async fn round_trip_metadata_request() {
    let mock = MockBroker::start(|api_key, version, _corr_id, _body| {
        if api_key == api_versions_request::API_KEY {
            return Some(api_versions_response_v0());
        }
        if api_key == metadata_request_mod::API_KEY {
            // Encode at the negotiated version so flexible framing matches.
            return Some(metadata_response_at(version));
        }
        None
    })
    .await;

    let conn = Connection::connect(mock.addr, ConnectionOptions::default())
        .await
        .unwrap();

    // send() should succeed; smoke-test passes on no error.
    let _resp = conn.send(MetadataRequest::default()).await.unwrap();

    conn.close();
    mock.stop();
}

/// Three concurrent `send` calls on the same connection are all dispatched and
/// routed back to the correct caller via correlation IDs.
///
/// The mock increments an atomic counter per Metadata request and stamps the
/// `throttle_time_ms` field with the counter value. After `tokio::join!` all
/// three futures, the three `throttle_time_ms` values should be 0, 1, 2 in
/// some order.
#[tokio::test]
async fn concurrent_sends_get_correct_responses() {
    use std::sync::{
        Arc,
        atomic::{AtomicI32, Ordering},
    };

    let counter = Arc::new(AtomicI32::new(0));
    let counter_for_mock = Arc::clone(&counter);

    let mock = MockBroker::start(move |api_key, version, _corr_id, _body| {
        if api_key == api_versions_request::API_KEY {
            return Some(api_versions_response_v0());
        }
        if api_key == metadata_request_mod::API_KEY {
            let n = counter_for_mock.fetch_add(1, Ordering::Relaxed);
            return Some(metadata_response_with_throttle(version, n));
        }
        None
    })
    .await;

    let conn = Connection::connect(mock.addr, ConnectionOptions::default())
        .await
        .unwrap();

    // Send three requests concurrently; the connection multiplexes them via
    // correlation IDs and routes each response back to its caller.
    let (r1, r2, r3) = tokio::join!(
        conn.send(MetadataRequest::default()),
        conn.send(MetadataRequest::default()),
        conn.send(MetadataRequest::default()),
    );

    let r1 = r1.unwrap();
    let r2 = r2.unwrap();
    let r3 = r3.unwrap();

    // All three requests received distinct responses stamped 0, 1, 2.
    let mut seen = [
        r1.throttle_time_ms,
        r2.throttle_time_ms,
        r3.throttle_time_ms,
    ];
    seen.sort_unstable();
    assert!(
        seen == [0, 1, 2],
        "each concurrent send must get a distinct response"
    );

    conn.close();
    mock.stop();
}

/// `Client::refresh_metadata` calls the bootstrap broker, decodes the broker
/// list, and populates the pool's address registry.
///
/// The mock serves two synthetic brokers with ids 1 and 2. After
/// `refresh_metadata` the test verifies that the response decoded correctly
/// and holds 2 brokers. The test cannot connect to those addresses, because
/// they are fake ports the mock does not listen on, but it does exercise the
/// registry population.
#[tokio::test]
async fn client_refresh_metadata_populates_pool() {
    use krabka_protocol::owned::metadata_response::{
        FLEXIBLE_MIN, MetadataResponse, MetadataResponseBroker,
    };

    let mock = MockBroker::start(move |api_key, version, _corr_id, _body| {
        if api_key == api_versions_request::API_KEY {
            return Some(api_versions_response_v0());
        }
        if api_key == metadata_request_mod::API_KEY {
            let resp = MetadataResponse {
                brokers: vec![
                    MetadataResponseBroker {
                        node_id: 1,
                        host: "127.0.0.1".into(),
                        port: 9092,
                        ..Default::default()
                    },
                    MetadataResponseBroker {
                        node_id: 2,
                        host: "127.0.0.1".into(),
                        port: 9093,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            };
            // Encode the response with the correct ResponseHeader prefix.
            let mut buf = BytesMut::new();
            if version >= FLEXIBLE_MIN {
                buf.extend_from_slice(&[0x00u8]);
            }
            resp.encode(&mut buf, version).unwrap();
            return Some(buf.to_vec());
        }
        None
    })
    .await;

    let client = krabka_client_core::Client::builder()
        .bootstrap(mock.addr.to_string())
        .build()
        .await
        .unwrap();

    let metadata = client.refresh_metadata().await.unwrap();
    assert!(
        metadata.brokers.len() == 2,
        "expected 2 brokers in metadata"
    );

    // After refresh the pool knows broker 1 and 2's addresses. We can't
    // actually connect to those ports (the mock isn't listening there), but
    // the metadata response decoded correctly and has the right shape.
    client.close();
    mock.stop();
}
