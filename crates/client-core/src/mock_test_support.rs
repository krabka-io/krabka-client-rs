//! Shared protocol-response builders for mock-client tests, and a capture of
//! the tracing events a call records.

use std::sync::{Arc, Mutex, PoisonError};

use bytes::BytesMut;
use krabka_protocol::{
    Encode, ProtocolError,
    owned::{
        api_versions_request,
        api_versions_response::{ApiVersion, ApiVersionsResponse},
        find_coordinator_request, metadata_request,
        offset_commit_request::OffsetCommitRequest,
        offset_commit_response::{
            OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
        },
    },
};
use tracing::subscriber::set_default;
use tracing_subscriber::layer::Context;

/// Advertises API versions plus one request API at its supported maximum.
///
/// # Errors
/// Returns the codec error when the response does not encode.
pub fn api_versions_with(api_key: i16, max_version: i16) -> Result<Vec<u8>, ProtocolError> {
    encode_api_versions(vec![
        api_version(api_versions_request::API_KEY, 0, 3),
        api_version(api_key, 0, max_version),
    ])
}

/// Builds an API-versions response from API-key/max-version pairs.
#[must_use]
pub fn api_versions_response(
    api_keys: impl IntoIterator<Item = (i16, i16)>,
) -> ApiVersionsResponse {
    ApiVersionsResponse {
        api_keys: api_keys
            .into_iter()
            .map(|(api_key, max_version)| api_version(api_key, 0, max_version))
            .collect(),
        ..Default::default()
    }
}

/// Builds an API-versions response from API-key/min/max-version triples.
#[must_use]
pub fn api_versions_response_ranges(
    api_keys: impl IntoIterator<Item = (i16, i16, i16)>,
) -> ApiVersionsResponse {
    ApiVersionsResponse {
        api_keys: api_keys
            .into_iter()
            .map(|(api_key, min_version, max_version)| {
                api_version(api_key, min_version, max_version)
            })
            .collect(),
        ..Default::default()
    }
}

/// Advertises one group RPC plus the API-versions, coordinator lookup, and
/// metadata requests needed by consumer tests.
///
/// # Errors
/// Returns the codec error when the response does not encode.
pub fn group_api_versions(
    api_key: i16,
    version_range: (i16, i16),
) -> Result<Vec<u8>, ProtocolError> {
    encode_api_versions(vec![
        api_version(api_versions_request::API_KEY, 0, 3),
        api_version(api_key, version_range.0, version_range.1),
        api_version(find_coordinator_request::API_KEY, 0, 0),
        api_version(metadata_request::API_KEY, 0, 8),
    ])
}

fn api_version(api_key: i16, min_version: i16, max_version: i16) -> ApiVersion {
    ApiVersion {
        api_key,
        min_version,
        max_version,
        ..Default::default()
    }
}

fn encode_api_versions(api_keys: Vec<ApiVersion>) -> Result<Vec<u8>, ProtocolError> {
    let response = ApiVersionsResponse {
        api_keys,
        ..Default::default()
    };
    let mut buffer = BytesMut::new();
    response.encode(&mut buffer, 0)?;
    Ok(buffer.to_vec())
}

/// Builds an `OffsetCommit` response with one result for every requested partition.
#[must_use]
pub fn offset_commit_response(
    request: &OffsetCommitRequest,
    error_code: impl Fn(&str) -> i16,
) -> OffsetCommitResponse {
    OffsetCommitResponse {
        topics: request
            .topics
            .iter()
            .map(|topic| OffsetCommitResponseTopic {
                name: topic.name.clone(),
                partitions: topic
                    .partitions
                    .iter()
                    .map(|partition| OffsetCommitResponsePartition {
                        partition_index: partition.partition_index,
                        error_code: error_code(&topic.name),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// The level of every event recorded while it is the default subscriber.
#[derive(Clone, Default)]
struct EventLevels(Arc<Mutex<Vec<tracing::Level>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for EventLevels {
    fn on_event(&self, event: &tracing::Event<'_>, _context: Context<'_, S>) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(*event.metadata().level());
    }
}

/// Await `call` with a capturing subscriber as this thread's default, and give
/// back its output and the level of every event it recorded.
///
/// The subscriber is the thread's default, so `call` must not move to another
/// worker thread: run it on a current-thread runtime.
pub async fn recorded_levels<T>(call: impl Future<Output = T>) -> (T, Vec<tracing::Level>) {
    use tracing_subscriber::layer::SubscriberExt as _;

    let levels = EventLevels::default();
    let _default = set_default(tracing_subscriber::registry().with(levels.clone()));
    let output = call.await;
    let recorded = levels
        .0
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    (output, recorded)
}
