//! Transport seam for the background sender.
//!
//! The sender needs only a narrow slice of [`krabka_client_core::Client`]. It
//! ships a single-partition `ProduceRequest` to a partition leader, or to the
//! bootstrap connection when the leader is unknown. It evicts a dead broker
//! connection, asks whether a broker id is dialable, and refreshes cluster
//! metadata. [`ProduceTransport`] captures exactly that surface, so tests can
//! drive the sender against an in-process broker model,
//! [`MockTransport`](#tests), with no socket. That makes idempotent-sequencing
//! hangs reproducible.
//!
//! [`ClientTransport`] is the thin production adapter over a real `Client`.

use async_trait::async_trait;
use bytes::BufMut;
use krabka_client_core::{Client, ClientError};
use krabka_protocol::{
    Encode, ProtocolError, ProtocolRequest,
    owned::{
        metadata_response::MetadataResponse,
        produce_request::{self, ProduceRequest},
        produce_response::ProduceResponse,
    },
    primitives::uuid::Uuid,
};

/// The broker-facing operations the sender performs.
///
/// `send_produce` folds the difference between `Client::broker(id).send` and
/// the bootstrap `Client::send` into one call. `leader = Some(id)` routes to
/// that broker, and `None` uses the bootstrap connection. This mirrors the
/// sender's existing `BOOTSTRAP_LEADER` fallback, and it keeps the trait a
/// clean, testable seam.
///
/// The trait uses `async_trait` for dyn-compatibility, so `SenderConfig` can
/// hold a `Box<dyn ProduceTransport>` without leaking generics across the whole
/// crate.
#[async_trait]
pub(crate) trait ProduceTransport: Send + Sync {
    /// Send a single-partition `ProduceRequest` to `leader`, which is a broker
    /// id, or to the bootstrap connection when `leader` is `None`.
    async fn send_produce(
        &self,
        leader: Option<i32>,
        req: ProduceRequest,
    ) -> Result<ProduceResponse, ClientError>;

    /// Enqueue Produce `acks=0`, for which the broker sends no response.
    async fn send_produce_no_response(
        &self,
        leader: Option<i32>,
        req: ProduceRequest,
    ) -> Result<(), ClientError> {
        self.send_produce(leader, req).await.map(drop)
    }

    /// Drop any pooled connection to `broker_id` so the next send reconnects.
    /// No-op for the bootstrap connection.
    fn evict_broker(&self, broker_id: i32);

    /// Whether the transport has a dialable address for `broker_id`.
    fn knows_broker(&self, broker_id: i32) -> bool;

    /// Refresh cluster metadata, refill the broker-address registry, and
    /// return the typed response, so the sender can update its leader map.
    async fn refresh_metadata(&self) -> Result<MetadataResponse, ClientError>;
}

/// The last Produce version that carries topic names.
const PRODUCE_TOPIC_NAME_MAX_VERSION: i16 = 12;

/// A Produce request that negotiates v12 at most, because v13 names each topic
/// by id only.
///
/// The producer has no topic id for a topic that its metadata cache does not
/// hold yet. A v13 request with a zero id names no topic, so the broker cannot
/// resolve it and answers `UNKNOWN_TOPIC_ID` with an empty name and a zero id.
/// v12 carries the topic name. Apache Kafka's producer does not send such a
/// request: `KafkaProducer.waitOnMetadata` blocks `send` until the metadata
/// holds the topic, so `Sender.topicIdsForBatches` finds each id. This cap
/// gives the same wire result without the block. Kafka's
/// `TransactionManager.txnOffsetCommitHandler` caps `TxnOffsetCommit` in the
/// same way: it uses `forTopicNames` when an id is missing.
#[derive(Clone, Debug, PartialEq)]
struct TopicNameProduce(ProduceRequest);

impl Encode for TopicNameProduce {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        self.0.encode(buf, version)
    }

    fn encoded_len(&self, version: i16) -> usize {
        self.0.encoded_len(version)
    }
}

impl ProtocolRequest for TopicNameProduce {
    const API_KEY: i16 = produce_request::API_KEY;
    const MIN_VERSION: i16 = produce_request::MIN_VERSION;
    const MAX_VERSION: i16 = PRODUCE_TOPIC_NAME_MAX_VERSION;
    const FLEXIBLE_MIN: i16 = produce_request::FLEXIBLE_MIN;
    type Response = ProduceResponse;
}

/// Tell if a topic in `req` has no topic id, so the request must name topics.
fn needs_topic_names(req: &ProduceRequest) -> bool {
    req.topic_data
        .iter()
        .any(|topic| topic.topic_id == Uuid::ZERO)
}

/// Production [`ProduceTransport`] backed by a real [`Client`].
pub(crate) struct ClientTransport {
    client: Client,
}

impl ClientTransport {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ProduceTransport for ClientTransport {
    #[tracing::instrument(level = "debug", skip_all, fields(leader = ?leader), err)]
    async fn send_produce(
        &self,
        leader: Option<i32>,
        req: ProduceRequest,
    ) -> Result<ProduceResponse, ClientError> {
        match (leader, needs_topic_names(&req)) {
            (Some(id), true) => self.client.broker(id).send(TopicNameProduce(req)).await,
            (Some(id), false) => self.client.broker(id).send(req).await,
            (None, true) => self.client.send(TopicNameProduce(req)).await,
            (None, false) => self.client.send(req).await,
        }
    }

    async fn send_produce_no_response(
        &self,
        leader: Option<i32>,
        req: ProduceRequest,
    ) -> Result<(), ClientError> {
        match (leader, needs_topic_names(&req)) {
            (Some(id), true) => {
                self.client
                    .broker(id)
                    .send_no_response(TopicNameProduce(req))
                    .await
            }
            (Some(id), false) => self.client.broker(id).send_no_response(req).await,
            (None, true) => self.client.send_no_response(TopicNameProduce(req)).await,
            (None, false) => self.client.send_no_response(req).await,
        }
    }

    fn evict_broker(&self, broker_id: i32) {
        self.client.evict_broker(broker_id);
    }

    fn knows_broker(&self, broker_id: i32) -> bool {
        self.client.knows_broker(broker_id)
    }

    #[tracing::instrument(level = "debug", skip_all, err)]
    async fn refresh_metadata(&self) -> Result<MetadataResponse, ClientError> {
        self.client.refresh_metadata().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicU16, AtomicUsize, Ordering},
    };

    use bytes::BytesMut;
    use krabka_client_core::{Client, MockBroker};
    use krabka_protocol::{
        Decode, Encode,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            metadata_request,
            metadata_response::{
                FLEXIBLE_MIN as META_FLEXIBLE_MIN, MetadataResponse, MetadataResponseBroker,
            },
            produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
            produce_response::{
                self, FLEXIBLE_MIN as PROD_FLEXIBLE_MIN, PartitionProduceResponse, ProduceResponse,
                TopicProduceResponse,
            },
        },
    };

    use super::*;

    /// `ApiVersionsResponse` (header v0) advertising `ApiVersions`, `Metadata`,
    /// and Produce so the client can negotiate all three against the mock.
    fn api_versions_v0() -> Vec<u8> {
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
                    api_key: metadata_request::API_KEY,
                    min_version: 0,
                    max_version: 12,
                    ..Default::default()
                },
                ApiVersion {
                    api_key: produce_response::API_KEY,
                    min_version: 0,
                    max_version: 9,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf, 0).unwrap();
        buf.to_vec()
    }

    /// `MetadataResponse` advertising one broker (id 1) at the mock's own port,
    /// encoded at `version` with the correct `ResponseHeader` prefix.
    fn metadata_v(version: i16, port: u16) -> Vec<u8> {
        let resp = MetadataResponse {
            brokers: vec![MetadataResponseBroker {
                node_id: 1,
                host: "127.0.0.1".into(),
                port: i32::from(port),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        if version >= META_FLEXIBLE_MIN {
            buf.extend_from_slice(&[0x00u8]); // empty tagged fields
        }
        resp.encode(&mut buf, version).unwrap();
        buf.to_vec()
    }

    /// A non-default `ProduceResponse` with one topic and partition, encoded
    /// at `version` with the correct `ResponseHeader` prefix.
    fn produce_v(version: i16) -> Vec<u8> {
        let resp = ProduceResponse {
            responses: vec![TopicProduceResponse {
                name: "t".into(),
                partition_responses: vec![PartitionProduceResponse {
                    index: 0,
                    error_code: 0,
                    base_offset: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        if version >= PROD_FLEXIBLE_MIN {
            buf.extend_from_slice(&[0x00u8]); // empty tagged fields
        }
        resp.encode(&mut buf, version).unwrap();
        buf.to_vec()
    }

    /// `ClientTransport` forwards each operation to the underlying `Client`. A
    /// real in-process broker confirms that the delegations are live.
    /// `refresh_metadata` and `send_produce` return the broker's data rather
    /// than a default, `knows_broker` reflects the pool, and `evict_broker`
    /// drops the cached connection so that the next send re-handshakes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn client_transport_delegates_to_client() {
        // One ApiVersions handshake happens per new TCP connection, so this
        // counts connections established to the mock.
        let handshakes = Arc::new(AtomicUsize::new(0));
        let port = Arc::new(AtomicU16::new(0));
        let h_handshakes = handshakes.clone();
        let h_port = port.clone();

        let mock = MockBroker::start(move |api_key, version, _corr, _body| {
            if api_key == api_versions_request::API_KEY {
                h_handshakes.fetch_add(1, Ordering::SeqCst);
                return Some(api_versions_v0());
            }
            if api_key == metadata_request::API_KEY {
                return Some(metadata_v(version, h_port.load(Ordering::SeqCst)));
            }
            if api_key == produce_response::API_KEY {
                return Some(produce_v(version));
            }
            None
        })
        .await;
        port.store(mock.addr.port(), Ordering::SeqCst);

        let client = Client::builder()
            .bootstrap(mock.addr.to_string())
            .build()
            .await
            .expect("client connects to the mock");
        let transport = ClientTransport::new(client);

        // refresh_metadata returns the live broker list (a default would be empty).
        let md = transport
            .refresh_metadata()
            .await
            .expect("refresh_metadata");
        assert2::assert!(!md.brokers.is_empty());

        // knows_broker reflects the pool: broker 1 is registered, 999 is not.
        assert2::assert!(transport.knows_broker(1));
        assert2::assert!(!transport.knows_broker(999));

        // send_produce returns the broker's real response (a default is empty) and
        // caches a connection to broker 1.
        let resp = transport
            .send_produce(Some(1), ProduceRequest::default())
            .await
            .expect("send_produce to broker 1");
        assert2::assert!(!resp.responses.is_empty());

        // evict_broker drops the cached connection, so the next send must open a
        // fresh one — observable as another handshake.
        let before = handshakes.load(Ordering::SeqCst);
        transport.evict_broker(1);
        let _ = transport
            .send_produce(Some(1), ProduceRequest::default())
            .await
            .expect("send_produce after evict reconnects");
        let after = handshakes.load(Ordering::SeqCst);
        assert2::assert!(after > before);

        mock.stop();
    }

    /// A Produce request as the broker decoded it, with the version it got.
    type SeenProduce = (i16, ProduceRequest);

    /// Start a mock broker (id 1) that advertises Produce up to v13 and records
    /// each Produce it decodes.
    async fn produce_recording_broker(
        seen: Arc<std::sync::Mutex<Vec<SeenProduce>>>,
    ) -> (MockBroker, ClientTransport) {
        const CLIENT_ID: &str = "p";
        let port = Arc::new(AtomicU16::new(0));
        let h_port = Arc::clone(&port);
        let mock = MockBroker::start(move |api_key, version, _corr, body| {
            if api_key == api_versions_request::API_KEY {
                let mut buf = BytesMut::new();
                ApiVersionsResponse {
                    api_keys: vec![
                        ApiVersion {
                            api_key: metadata_request::API_KEY,
                            min_version: 0,
                            max_version: 12,
                            ..Default::default()
                        },
                        ApiVersion {
                            api_key: produce_response::API_KEY,
                            min_version: 3,
                            max_version: 13,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }
                .encode(&mut buf, 0)
                .unwrap();
                return Some(buf.to_vec());
            }
            if api_key == metadata_request::API_KEY {
                return Some(metadata_v(version, h_port.load(Ordering::SeqCst)));
            }
            if api_key == produce_response::API_KEY {
                let flexible = version >= produce_request::FLEXIBLE_MIN;
                let mut request_body = &body[2 + CLIENT_ID.len() + usize::from(flexible)..];
                let decoded =
                    ProduceRequest::decode(&mut request_body, version).expect("decode Produce");
                seen.lock().unwrap().push((version, decoded));
                return Some(produce_v(version));
            }
            None
        })
        .await;
        port.store(mock.addr.port(), Ordering::SeqCst);
        let client = Client::builder()
            .bootstrap(mock.addr.to_string())
            .client_id(CLIENT_ID)
            .build()
            .await
            .expect("client connects to the mock");
        let transport = ClientTransport::new(client);
        transport
            .refresh_metadata()
            .await
            .expect("refresh_metadata registers broker 1");
        (mock, transport)
    }

    fn topic(name: &str, topic_id: Uuid) -> TopicProduceData {
        TopicProduceData {
            name: name.into(),
            topic_id,
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: None,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// A request whose topics all have ids goes out at v13, which names topics
    /// by id only. A request with a topic that has no id goes out at v12, which
    /// names topics. Each row compares the version and the whole request that
    /// the broker decoded.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn produce_without_a_topic_id_names_the_topic_at_v12() {
        let known = Uuid([7u8; 16]);
        let other = Uuid([8u8; 16]);
        let request = |topics| ProduceRequest {
            acks: -1,
            timeout_ms: 1_000,
            topic_data: topics,
            ..Default::default()
        };
        let cases = [
            (
                "every id known",
                vec![topic("a", known), topic("b", other)],
                (13, request(vec![topic("", known), topic("", other)])),
            ),
            (
                "one id unknown",
                vec![topic("a", known), topic("b", Uuid::ZERO)],
                (
                    12,
                    request(vec![topic("a", Uuid::ZERO), topic("b", Uuid::ZERO)]),
                ),
            ),
            (
                "no id known",
                vec![topic("a", Uuid::ZERO)],
                (12, request(vec![topic("a", Uuid::ZERO)])),
            ),
        ];
        for (name, topics, expected) in cases {
            for leader in [Some(1), None] {
                let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
                let (mock, transport) = produce_recording_broker(Arc::clone(&seen)).await;
                transport
                    .send_produce(leader, request(topics.clone()))
                    .await
                    .expect("send_produce");
                let sent = seen.lock().unwrap().clone();
                assert2::assert!(sent == vec![expected.clone()], "{name}, leader {leader:?}");
                mock.stop();
            }
        }
    }

    #[test]
    fn a_request_needs_topic_names_when_any_topic_id_is_zero() {
        let known = Uuid([7u8; 16]);
        let cases = [
            ("no topics", vec![], false),
            ("every id known", vec![topic("a", known)], false),
            (
                "one id zero",
                vec![topic("a", known), topic("b", Uuid::ZERO)],
                true,
            ),
        ];
        for (name, topics, expected) in cases {
            let request = ProduceRequest {
                topic_data: topics,
                ..Default::default()
            };
            assert2::assert!(needs_topic_names(&request) == expected, "{name}");
        }
    }
}
