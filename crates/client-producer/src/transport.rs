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

/// The last Produce version before transaction version 2.
const PRODUCE_TRANSACTION_V1_MAX_VERSION: i16 = 11;

/// A Produce request that negotiates `MAX` at most.
///
/// [`produce_cap`] gives the cap of one request.
#[derive(Clone, Debug, PartialEq)]
struct CappedProduce<const MAX: i16>(ProduceRequest);

impl<const MAX: i16> From<ProduceRequest> for CappedProduce<MAX> {
    fn from(req: ProduceRequest) -> Self {
        Self(req)
    }
}

impl<const MAX: i16> Encode for CappedProduce<MAX> {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        self.0.encode(buf, version)
    }

    fn encoded_len(&self, version: i16) -> usize {
        self.0.encoded_len(version)
    }
}

impl<const MAX: i16> ProtocolRequest for CappedProduce<MAX> {
    const API_KEY: i16 = produce_request::API_KEY;
    const MIN_VERSION: i16 = produce_request::MIN_VERSION;
    const MAX_VERSION: i16 = MAX;
    const FLEXIBLE_MIN: i16 = produce_request::FLEXIBLE_MIN;
    type Response = ProduceResponse;
}

/// A Produce request that names its topics, because v13 names each topic by id
/// only.
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
type TopicNameProduce = CappedProduce<PRODUCE_TOPIC_NAME_MAX_VERSION>;

/// A Produce request of a transaction that follows transaction version 1.
///
/// This producer sends `AddPartitionsToTxn` for each partition of a
/// transaction, which is the transaction version 1 protocol. With transaction
/// version 2 (KIP-890) the broker adds the partition itself when it sees a
/// transactional Produce at v12 or higher, and the producer sends no
/// `AddPartitionsToTxn`. The version of the Produce request tells the broker
/// which protocol the producer follows, so a v12 or higher request from this
/// producer would make the broker use the wrong one.
///
/// Apache Kafka's producer makes the same choice from the finalized feature
/// `transaction.version`: `Sender.sendProduceRequest` passes
/// `useTransactionV1Version = !transactionManager.isTransactionV2Enabled()`,
/// and `ProduceRequest.builder` then caps the version at
/// `LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2` (11). This producer does not
/// implement transaction version 2, so the cap holds for every transactional
/// request.
type TransactionV1Produce = CappedProduce<PRODUCE_TRANSACTION_V1_MAX_VERSION>;

/// The Produce version cap of one request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProduceCap {
    /// The request carries a transactional id. It follows transaction version
    /// 1, so it stops at v11.
    TransactionV1,
    /// A topic of the request has no topic id, so the request stops at v12,
    /// which names each topic.
    TopicNames,
    /// Every topic has an id, so the request can use the latest version.
    Latest,
}

/// Give the version cap of `req`. A transactional request takes the lowest
/// cap: it names its topics too, because v11 is below v12.
fn produce_cap(req: &ProduceRequest) -> ProduceCap {
    if req
        .transactional_id
        .as_ref()
        .is_some_and(|id| !id.is_empty())
    {
        ProduceCap::TransactionV1
    } else if req
        .topic_data
        .iter()
        .any(|topic| topic.topic_id == Uuid::ZERO)
    {
        ProduceCap::TopicNames
    } else {
        ProduceCap::Latest
    }
}

/// Production [`ProduceTransport`] backed by a real [`Client`].
pub(crate) struct ClientTransport {
    client: Client,
}

impl ClientTransport {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Send `req` to `leader`, which is a broker id, or to the bootstrap
    /// connection when `leader` is `None`.
    async fn send_capped<R>(
        &self,
        leader: Option<i32>,
        req: R,
    ) -> Result<ProduceResponse, ClientError>
    where
        R: ProtocolRequest<Response = ProduceResponse> + Encode + Send + Sync,
    {
        match leader {
            Some(id) => self.client.broker(id).send(req).await,
            None => self.client.send(req).await,
        }
    }

    /// Enqueue `req`, for which the broker sends no response.
    async fn send_capped_no_response<R>(
        &self,
        leader: Option<i32>,
        req: R,
    ) -> Result<(), ClientError>
    where
        R: ProtocolRequest<Response = ProduceResponse> + Encode + Send + Sync,
    {
        match leader {
            Some(id) => self.client.broker(id).send_no_response(req).await,
            None => self.client.send_no_response(req).await,
        }
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
        match produce_cap(&req) {
            ProduceCap::TransactionV1 => {
                self.send_capped(leader, TransactionV1Produce::from(req))
                    .await
            }
            ProduceCap::TopicNames => self.send_capped(leader, TopicNameProduce::from(req)).await,
            ProduceCap::Latest => self.send_capped(leader, req).await,
        }
    }

    async fn send_produce_no_response(
        &self,
        leader: Option<i32>,
        req: ProduceRequest,
    ) -> Result<(), ClientError> {
        match produce_cap(&req) {
            ProduceCap::TransactionV1 => {
                self.send_capped_no_response(leader, TransactionV1Produce::from(req))
                    .await
            }
            ProduceCap::TopicNames => {
                self.send_capped_no_response(leader, TopicNameProduce::from(req))
                    .await
            }
            ProduceCap::Latest => self.send_capped_no_response(leader, req).await,
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
    /// names topics. A request of a transaction goes out at v11, the last
    /// version before transaction version 2. Each row compares the version and
    /// the whole request that the broker decoded.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn produce_version_follows_the_cap_of_the_request() {
        let known = Uuid([7u8; 16]);
        let other = Uuid([8u8; 16]);
        let request = |transactional_id: Option<&str>, topics| ProduceRequest {
            transactional_id: transactional_id.map(str::to_owned),
            acks: -1,
            timeout_ms: 1_000,
            topic_data: topics,
            ..Default::default()
        };
        let cases = [
            (
                "every id known",
                None,
                vec![topic("a", known), topic("b", other)],
                (13, request(None, vec![topic("", known), topic("", other)])),
            ),
            (
                "one id unknown",
                None,
                vec![topic("a", known), topic("b", Uuid::ZERO)],
                (
                    12,
                    request(None, vec![topic("a", Uuid::ZERO), topic("b", Uuid::ZERO)]),
                ),
            ),
            (
                "no id known",
                None,
                vec![topic("a", Uuid::ZERO)],
                (12, request(None, vec![topic("a", Uuid::ZERO)])),
            ),
            (
                "a transaction with every id known",
                Some("tx-1"),
                vec![topic("a", known)],
                (11, request(Some("tx-1"), vec![topic("a", Uuid::ZERO)])),
            ),
            (
                "a transaction without a topic id",
                Some("tx-1"),
                vec![topic("a", Uuid::ZERO)],
                (11, request(Some("tx-1"), vec![topic("a", Uuid::ZERO)])),
            ),
        ];
        for (name, transactional_id, topics, expected) in cases {
            for leader in [Some(1), None] {
                let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
                let (mock, transport) = produce_recording_broker(Arc::clone(&seen)).await;
                transport
                    .send_produce(leader, request(transactional_id, topics.clone()))
                    .await
                    .expect("send_produce");
                let sent = seen.lock().unwrap().clone();
                assert2::assert!(sent == vec![expected.clone()], "{name}, leader {leader:?}");
                mock.stop();
            }
        }
    }

    /// The cap of a Produce request: a transaction stops at v11, a topic
    /// without an id stops at v12, and every other request uses the latest
    /// version.
    #[test]
    fn a_produce_request_takes_the_cap_of_its_contents() {
        let known = Uuid([7u8; 16]);
        let cases = [
            ("no topics", None, vec![], ProduceCap::Latest),
            (
                "every id known",
                None,
                vec![topic("a", known)],
                ProduceCap::Latest,
            ),
            (
                "one id zero",
                None,
                vec![topic("a", known), topic("b", Uuid::ZERO)],
                ProduceCap::TopicNames,
            ),
            (
                "transactional",
                Some("tx-1"),
                vec![topic("a", known)],
                ProduceCap::TransactionV1,
            ),
            (
                "transactional without a topic id",
                Some("tx-1"),
                vec![topic("a", Uuid::ZERO)],
                ProduceCap::TransactionV1,
            ),
            (
                "empty transactional id",
                Some(""),
                vec![topic("a", known)],
                ProduceCap::Latest,
            ),
        ];
        for (name, transactional_id, topics, expected) in cases {
            let request = ProduceRequest {
                transactional_id: transactional_id.map(str::to_owned),
                topic_data: topics,
                ..Default::default()
            };
            assert2::assert!(produce_cap(&request) == expected, "{name}");
        }
    }
}
