//! Transaction administration.

use bytes::BufMut;
use krabka_client_core::{CoordinatorKeyType, build_find_coordinator};
use krabka_protocol::{
    Encode, ProtocolError, ProtocolRequest,
    owned::{
        describe_transactions_request::DescribeTransactionsRequest,
        describe_transactions_response::{DescribeTransactionsResponse, TransactionState},
        find_coordinator_response::FindCoordinatorResponse,
        init_producer_id_request::{self, InitProducerIdRequest},
        init_producer_id_response::InitProducerIdResponse,
    },
};
use krabka_units::{Time, convert::TimeExt as _};

use crate::{AdminClient, AdminError, kafka_error_name};

/// The transaction coordinator's view of one transactional ID.
///
/// A third party reads [`producer_id`](Self::producer_id) and
/// [`producer_epoch`](Self::producer_epoch) to verify the authority of a
/// writer. The coordinator raises the epoch each time it fences the previous
/// producer generation, so a writer that presents an older epoch no longer
/// holds authority over the transactional ID.
#[derive(Debug, Clone, PartialEq)]
pub struct TransactionDescription {
    /// The transactional ID that the caller asked about.
    pub transactional_id: String,
    /// The coordinator's state name, such as `Ongoing` or `CompleteCommit`.
    pub state: String,
    /// The transaction timeout that the producer registered.
    pub timeout: Time,
    /// The start of the current transaction, in Kafka epoch milliseconds. An
    /// instant is a coordinate, so it stays a raw integer.
    pub start_time_ms: i64,
    /// The producer ID that the coordinator holds for this transactional ID.
    pub producer_id: i64,
    /// The producer epoch that the coordinator holds for this transactional
    /// ID.
    pub producer_epoch: i16,
}

fn coordinator_address(
    transactional_id: &str,
    response: FindCoordinatorResponse,
) -> Result<String, AdminError> {
    if let Some(coordinator) = response
        .coordinators
        .into_iter()
        .find(|coordinator| coordinator.key == transactional_id)
    {
        if coordinator.error_code != 0 {
            return Err(AdminError::Broker {
                api: "FindCoordinator",
                code: coordinator.error_code,
                name: kafka_error_name(coordinator.error_code),
                message: coordinator.error_message,
            });
        }
        return Ok(format!("{}:{}", coordinator.host, coordinator.port));
    }

    if response.error_code != 0 {
        return Err(AdminError::Broker {
            api: "FindCoordinator",
            code: response.error_code,
            name: kafka_error_name(response.error_code),
            message: response.error_message,
        });
    }
    if response.host.is_empty() {
        return Err(AdminError::Protocol(format!(
            "FindCoordinator returned no entry for transactional id {transactional_id:?}"
        )));
    }
    Ok(format!("{}:{}", response.host, response.port))
}

/// An `InitProducerId` request limited to the released versions, v0 to v5.
///
/// `InitProducerIdRequest.json` marks v6 as its unstable latest version
/// (`"latestVersionUnstable": true`, KIP-939), and krabka-protocol's
/// generated `MAX_VERSION` includes v6. Apache Kafka's
/// `FenceProducersHandler.buildSingleRequest` uses
/// `new InitProducerIdRequest.Builder(data)`. That builder calls
/// `AbstractRequest.Builder(ApiKeys.INIT_PRODUCER_ID)`, which allows
/// `apiKey.latestVersion(false)`, the latest released version. This type
/// gives the same cap to version negotiation.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ReleasedInitProducerId(InitProducerIdRequest);

impl Encode for ReleasedInitProducerId {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        self.0.encode(buf, version)
    }

    fn encoded_len(&self, version: i16) -> usize {
        self.0.encoded_len(version)
    }
}

impl ProtocolRequest for ReleasedInitProducerId {
    const API_KEY: i16 = init_producer_id_request::API_KEY;
    const MIN_VERSION: i16 = init_producer_id_request::MIN_VERSION;
    /// The latest released `InitProducerId` version.
    const MAX_VERSION: i16 = 5;
    const FLEXIBLE_MIN: i16 = init_producer_id_request::FLEXIBLE_MIN;
    type Response = InitProducerIdResponse;
}

fn force_terminate_request(
    transactional_id: &str,
    transaction_timeout_ms: i32,
) -> ReleasedInitProducerId {
    ReleasedInitProducerId(InitProducerIdRequest {
        transactional_id: Some(transactional_id.to_owned()),
        transaction_timeout_ms,
        producer_id: -1,
        producer_epoch: -1,
        enable2_pc: false,
        keep_prepared_txn: false,
        ..Default::default()
    })
}

fn describe_transactions_request(transactional_id: &str) -> DescribeTransactionsRequest {
    DescribeTransactionsRequest {
        transactional_ids: vec![transactional_id.to_owned()],
        ..Default::default()
    }
}

/// Converts one coordinator row into the domain struct, or surfaces the error
/// code that the coordinator attached to that row.
fn described_transaction(state: TransactionState) -> Result<TransactionDescription, AdminError> {
    if state.error_code != 0 {
        return Err(AdminError::Broker {
            api: "DescribeTransactions",
            code: state.error_code,
            name: kafka_error_name(state.error_code),
            message: None,
        });
    }
    Ok(TransactionDescription {
        transactional_id: state.transactional_id,
        state: state.transaction_state,
        timeout: Time::from_millis(i64::from(state.transaction_timeout_ms)),
        start_time_ms: state.transaction_start_time_ms,
        producer_id: state.producer_id,
        producer_epoch: state.producer_epoch,
    })
}

/// Picks the row for `transactional_id` out of a `DescribeTransactions`
/// response and maps it to the domain struct.
fn transaction_description(
    transactional_id: &str,
    response: DescribeTransactionsResponse,
) -> Result<TransactionDescription, AdminError> {
    response
        .transaction_states
        .into_iter()
        .find(|state| state.transactional_id == transactional_id)
        .ok_or_else(|| {
            AdminError::Protocol(format!(
                "DescribeTransactions returned no state for transactional id {transactional_id:?}"
            ))
        })
        .and_then(described_transaction)
}

impl AdminClient {
    /// Fences the current producer generation and aborts any ongoing
    /// transaction for `transactional_id`.
    ///
    /// This is Kafka's `forceTerminateTransaction` operation: it discovers the
    /// transaction coordinator and sends `InitProducerId` with no producer
    /// identity and `keepPreparedTxn=false`. It is safe to call when no
    /// transaction is open; the coordinator still advances the producer
    /// generation so stale writers are fenced.
    ///
    /// The client negotiates `InitProducerId` v5 or lower, as Kafka's
    /// `FenceProducersHandler` does. v6 is an unstable version.
    ///
    /// # Errors
    ///
    /// Returns [`AdminError::Protocol`] for an empty transactional ID, or the
    /// coordinator lookup, connection, transport, and broker errors returned
    /// by Kafka. Returns [`ClientError::IncompatibleVersion`] when the
    /// coordinator does not support `InitProducerId` v5 or lower.
    ///
    /// [`ClientError::IncompatibleVersion`]: krabka_client_core::ClientError::IncompatibleVersion
    pub async fn force_terminate_transaction(
        &self,
        transactional_id: &str,
    ) -> Result<(), AdminError> {
        if transactional_id.is_empty() {
            return Err(AdminError::Protocol(
                "transactional id must not be empty".to_owned(),
            ));
        }

        let response = self
            .conn
            .send(build_find_coordinator(
                transactional_id,
                CoordinatorKeyType::Transaction,
            ))
            .await?;
        let coordinator = coordinator_address(transactional_id, response)?;
        let connection = Self::connect_one(&coordinator, self.options.clone()).await?;
        let response = connection
            .send(force_terminate_request(
                transactional_id,
                self.options.request_timeout.millis_i32(),
            ))
            .await?;
        if response.error_code != 0 {
            return Err(AdminError::Broker {
                api: "InitProducerId",
                code: response.error_code,
                name: kafka_error_name(response.error_code),
                message: None,
            });
        }
        Ok(())
    }

    /// Reads the transaction coordinator's current state for one transactional
    /// ID.
    ///
    /// This is Kafka's `describeTransactions` operation (KIP-664). The client
    /// discovers the transaction coordinator for `transactional_id` and sends
    /// `DescribeTransactions` to that coordinator. An external system calls
    /// this to verify the authority of a writer. It joins no group and holds
    /// no producer state. The returned producer ID and producer epoch are the
    /// only generation from which the coordinator accepts writes.
    ///
    /// # Errors
    ///
    /// Returns [`AdminError::Protocol`] for an empty transactional ID, and for
    /// a response that carries no row for `transactional_id`. Returns
    /// [`AdminError::Broker`] when the coordinator lookup fails, or when the
    /// coordinator attaches an error code to the row, such as
    /// `TRANSACTIONAL_ID_NOT_FOUND`,
    /// `TRANSACTIONAL_ID_AUTHORIZATION_FAILED`, or `NOT_COORDINATOR`. Returns
    /// [`AdminError::Transport`] when the connection to the coordinator fails.
    pub async fn describe_transaction(
        &self,
        transactional_id: &str,
    ) -> Result<TransactionDescription, AdminError> {
        if transactional_id.is_empty() {
            return Err(AdminError::Protocol(
                "transactional id must not be empty".to_owned(),
            ));
        }

        let response = self
            .conn
            .send(build_find_coordinator(
                transactional_id,
                CoordinatorKeyType::Transaction,
            ))
            .await?;
        let coordinator = coordinator_address(transactional_id, response)?;
        let connection = Self::connect_one(&coordinator, self.options.clone()).await?;
        let response = connection
            .send(describe_transactions_request(transactional_id))
            .await?;
        transaction_description(transactional_id, response)
    }

    /// Reads the transaction coordinator's current state for each ID in
    /// `transactional_ids`.
    ///
    /// Two transactional IDs can live on two different coordinators, so the
    /// client describes them one at a time. Each ID costs one
    /// `FindCoordinator` round trip, one new connection to the coordinator
    /// that the lookup names, and one `DescribeTransactions` round trip. The
    /// cost grows linearly with the number of IDs. Callers that describe a
    /// large set should expect that cost. The results keep the order of
    /// `transactional_ids`.
    ///
    /// # Errors
    ///
    /// Stops at the first ID that fails and returns its error. Returns
    /// [`AdminError::Protocol`] for an empty transactional ID, and for a
    /// response that carries no row for the ID. Returns
    /// [`AdminError::Broker`] when the coordinator lookup fails, or when the
    /// coordinator attaches an error code to the row. Returns
    /// [`AdminError::Transport`] when the connection to a coordinator fails.
    pub async fn describe_transactions(
        &self,
        transactional_ids: &[&str],
    ) -> Result<Vec<TransactionDescription>, AdminError> {
        let mut descriptions = Vec::with_capacity(transactional_ids.len());
        for transactional_id in transactional_ids {
            descriptions.push(self.describe_transaction(transactional_id).await?);
        }
        Ok(descriptions)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use assert2::check;
    use bytes::{Buf, BytesMut};
    use krabka_client_core::{ClientError, MockBroker};
    use krabka_protocol::{
        Decode,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            find_coordinator_request,
            find_coordinator_response::Coordinator,
            metadata_request,
            metadata_response::MetadataResponse,
        },
    };

    use super::*;

    fn state_row(transactional_id: &str, error_code: i16) -> TransactionState {
        TransactionState {
            error_code,
            transactional_id: transactional_id.to_owned(),
            ..Default::default()
        }
    }

    /// The `InitProducerId` request that fences `payments` with no producer
    /// identity and no kept prepared transaction.
    fn fence_payments() -> InitProducerIdRequest {
        InitProducerIdRequest {
            transactional_id: Some("payments".to_owned()),
            transaction_timeout_ms: 30_000,
            producer_id: -1,
            producer_epoch: -1,
            enable2_pc: false,
            keep_prepared_txn: false,
            ..Default::default()
        }
    }

    #[test]
    fn force_termination_request_fences_without_preserving_transaction() {
        let request = force_terminate_request("payments", 30_000);

        assert2::assert!(request == ReleasedInitProducerId(fence_payments()));
    }

    /// The body of `response` at `version`, behind an empty tagged-field
    /// byte when `flexible` is set.
    fn encode_response(response: &impl Encode, version: i16, flexible: bool) -> Vec<u8> {
        let mut bytes = BytesMut::new();
        if flexible {
            bytes.extend_from_slice(&[0]);
        }
        response
            .encode(&mut bytes, version)
            .expect("response encodes");
        bytes.to_vec()
    }

    /// An `ApiVersions` response that advertises `InitProducerId` in
    /// `init_producer_id_range`.
    fn api_versions(init_producer_id_range: (i16, i16)) -> Vec<u8> {
        let api_version = |api_key, min_version, max_version| ApiVersion {
            api_key,
            min_version,
            max_version,
            ..Default::default()
        };
        encode_response(
            &ApiVersionsResponse {
                api_keys: vec![
                    api_version(api_versions_request::API_KEY, 0, 0),
                    api_version(find_coordinator_request::API_KEY, 0, 0),
                    api_version(metadata_request::API_KEY, 13, 13),
                    api_version(
                        init_producer_id_request::API_KEY,
                        init_producer_id_range.0,
                        init_producer_id_range.1,
                    ),
                ],
                ..Default::default()
            },
            0,
            false,
        )
    }

    /// The negotiated `InitProducerId` version and the result of
    /// `force_terminate_transaction`, with a version error as its ranges.
    type FenceResult = Result<(), (i16, i16, i16, i16, i16)>;

    /// Apache Kafka's `FenceProducersHandler` builds `InitProducerId` with
    /// `new InitProducerIdRequest.Builder(data)`, which allows only released
    /// versions. v6 is unstable, so the request stops at v5.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn force_terminate_transaction_sends_a_released_init_producer_id_version() {
        let sent_request = |version| vec![(version, fence_payments())];
        for (name, init_producer_id_range, expected_requests, expected_result) in [
            ("coordinator stops at v1", (0, 1), sent_request(1), Ok(())),
            ("coordinator stops at v4", (0, 4), sent_request(4), Ok(())),
            ("coordinator stops at v5", (0, 5), sent_request(5), Ok(())),
            (
                "coordinator supports unstable v6",
                (0, 6),
                sent_request(5),
                Ok(()),
            ),
            (
                "coordinator supports only unstable v6",
                (6, 6),
                Vec::new(),
                Err((init_producer_id_request::API_KEY, 6, 6, 0, 5)),
            ),
        ] {
            let requests = Arc::new(Mutex::new(Vec::new()));
            let requests_in_mock = Arc::clone(&requests);
            let coordinator =
                MockBroker::start(move |api_key, version, _, mut body| match api_key {
                    api_versions_request::API_KEY => Some(api_versions(init_producer_id_range)),
                    init_producer_id_request::API_KEY => {
                        let client_id_len = body.get_i16();
                        body.advance(usize::try_from(client_id_len).expect("client id length"));
                        let flexible = version >= init_producer_id_request::FLEXIBLE_MIN;
                        if flexible {
                            body.advance(1);
                        }
                        let request = InitProducerIdRequest::decode(&mut body, version)
                            .expect("init producer id request decodes");
                        requests_in_mock
                            .lock()
                            .expect("requests lock")
                            .push((version, request));
                        Some(encode_response(
                            &InitProducerIdResponse {
                                producer_id: 4242,
                                producer_epoch: 1,
                                ..Default::default()
                            },
                            version,
                            flexible,
                        ))
                    }
                    _ => None,
                })
                .await;
            let coordinator_addr = coordinator.addr;
            let bootstrap = MockBroker::start(move |api_key, version, _, _| match api_key {
                api_versions_request::API_KEY => Some(api_versions(init_producer_id_range)),
                find_coordinator_request::API_KEY => Some(encode_response(
                    &FindCoordinatorResponse {
                        node_id: 2,
                        host: coordinator_addr.ip().to_string(),
                        port: i32::from(coordinator_addr.port()),
                        ..Default::default()
                    },
                    version,
                    false,
                )),
                metadata_request::API_KEY => {
                    Some(encode_response(&MetadataResponse::default(), version, true))
                }
                _ => None,
            })
            .await;
            let admin = AdminClient::connect(&[bootstrap.addr.to_string()])
                .await
                .expect("admin connects");

            let result: FenceResult =
                admin
                    .force_terminate_transaction("payments")
                    .await
                    .map_err(|error| match error {
                        AdminError::Transport(ClientError::IncompatibleVersion {
                            api_key,
                            broker_min,
                            broker_max,
                            client_min,
                            client_max,
                        }) => (api_key, broker_min, broker_max, client_min, client_max),
                        other => panic!("case {name}: unexpected error {other:?}"),
                    });

            bootstrap.stop();
            coordinator.stop();
            let requests = requests.lock().expect("requests lock").clone();
            assert2::assert!(
                (requests, result) == (expected_requests, expected_result),
                "case {name}"
            );
        }
    }

    #[test]
    fn coordinator_lookup_selects_the_matching_batched_entry() {
        let response = FindCoordinatorResponse {
            coordinators: vec![
                Coordinator {
                    key: "other".to_owned(),
                    host: "wrong".to_owned(),
                    port: 1,
                    ..Default::default()
                },
                Coordinator {
                    key: "payments".to_owned(),
                    host: "coordinator".to_owned(),
                    port: 9092,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let address = coordinator_address("payments", response).expect("matching coordinator");
        assert2::assert!(address == "coordinator:9092");
    }

    #[test]
    fn describe_request_asks_for_exactly_one_transactional_id() {
        let request = describe_transactions_request("payments");

        assert2::assert!(
            request
                == DescribeTransactionsRequest {
                    transactional_ids: vec!["payments".to_owned()],
                    ..Default::default()
                }
        );
    }

    #[test]
    fn description_maps_the_matching_row_to_the_domain_struct() {
        let response = DescribeTransactionsResponse {
            transaction_states: vec![
                TransactionState {
                    transactional_id: "other".to_owned(),
                    producer_id: 11,
                    producer_epoch: 1,
                    ..Default::default()
                },
                TransactionState {
                    transactional_id: "payments".to_owned(),
                    transaction_state: "Ongoing".to_owned(),
                    transaction_timeout_ms: 60_000,
                    transaction_start_time_ms: 1_700_000_000_123,
                    producer_id: 4242,
                    producer_epoch: 7,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let described = transaction_description("payments", response).expect("matching row");

        assert2::assert!(
            described
                == TransactionDescription {
                    transactional_id: "payments".to_owned(),
                    state: "Ongoing".to_owned(),
                    timeout: Time::from_millis(60_000),
                    start_time_ms: 1_700_000_000_123,
                    producer_id: 4242,
                    producer_epoch: 7,
                }
        );
    }

    #[test]
    fn description_surfaces_the_error_code_of_the_matching_row() {
        for (_case, code, want_name) in [
            ("unknown id", 105, "TRANSACTIONAL_ID_NOT_FOUND"),
            (
                "not authorized",
                53,
                "TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
            ),
            ("moved coordinator", 16, "NOT_COORDINATOR"),
            ("loading", 14, "COORDINATOR_LOAD_IN_PROGRESS"),
        ] {
            let response = DescribeTransactionsResponse {
                transaction_states: vec![state_row("other", 0), state_row("payments", code)],
                ..Default::default()
            };

            let error = transaction_description("payments", response).expect_err("broker error");
            match error {
                AdminError::Broker {
                    api,
                    code: got,
                    name,
                    message,
                } => {
                    check!(
                        (api, got, name, message)
                            == ("DescribeTransactions", code, want_name, None)
                    );
                }
                other => panic!("expected AdminError::Broker, got {other:?}"),
            }
        }
    }

    #[test]
    fn description_rejects_a_response_that_omits_the_requested_id() {
        let response = DescribeTransactionsResponse {
            transaction_states: vec![state_row("other", 0)],
            ..Default::default()
        };

        let error = transaction_description("payments", response).expect_err("missing row");
        assert2::assert!(matches!(error, AdminError::Protocol(_)));
    }
}
