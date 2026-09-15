//! Transaction administration.

use bytes::BufMut;
use krabka_client_core::{Connection, CoordinatorKeyType, build_find_coordinator};
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

use crate::{
    AdminClient, AdminError, kafka_error_name,
    retry::{
        CoordinatorRetry, KAFKA_ADMIN_RETRY, RetryAction, RetryPolicy, connection_failure_action,
    },
};

/// `COORDINATOR_LOAD_IN_PROGRESS`: the coordinator is loading its state.
const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
/// `COORDINATOR_NOT_AVAILABLE`: no broker coordinates the transactional ID now.
const COORDINATOR_NOT_AVAILABLE: i16 = 15;
/// `NOT_COORDINATOR`: the broker does not coordinate the transactional ID.
const NOT_COORDINATOR: i16 = 16;
/// `CONCURRENT_TRANSACTIONS`: the coordinator is completing a transaction.
const CONCURRENT_TRANSACTIONS: i16 = 51;

/// Map the result of one `DescribeTransactions` attempt to a retry action, as
/// Kafka's `DescribeTransactionsHandler.handleError` does. 14 retries on the
/// same coordinator. 15 and 16 unmap the transactional ID, so the next attempt
/// finds the coordinator again. Every other result is final.
fn describe_retry_action(
    result: Result<TransactionDescription, AdminError>,
) -> RetryAction<TransactionDescription> {
    match result {
        Err(AdminError::Broker {
            code: COORDINATOR_LOAD_IN_PROGRESS,
            ..
        }) => RetryAction::SameCoordinator(result),
        Err(AdminError::Broker {
            code: COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR,
            ..
        }) => RetryAction::FindCoordinator(result),
        result => RetryAction::Done(result),
    }
}

/// Map the error code of one `InitProducerId` answer to a retry action, as
/// Kafka's `FenceProducersHandler.handleError` does. 14 and 51 retry on the
/// same coordinator. 15 and 16 unmap the transactional ID, so the next attempt
/// finds the coordinator again. Every other code is final.
fn fence_retry_action(error_code: i16) -> RetryAction<()> {
    if error_code == 0 {
        return RetryAction::Done(Ok(()));
    }
    let result = Err(AdminError::Broker {
        api: "InitProducerId",
        code: error_code,
        name: kafka_error_name(error_code),
        message: None,
    });
    match error_code {
        COORDINATOR_LOAD_IN_PROGRESS | CONCURRENT_TRANSACTIONS => {
            RetryAction::SameCoordinator(result)
        }
        COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR => RetryAction::FindCoordinator(result),
        _ => RetryAction::Done(result),
    }
}

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
    /// The cap is a released version, so it is also the stable maximum.
    const LATEST_STABLE_VERSION: i16 = Self::MAX_VERSION;
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
    /// The call retries as Apache Kafka's `FenceProducersHandler.handleError` does:
    ///
    /// - `COORDINATOR_LOAD_IN_PROGRESS` (14) and `CONCURRENT_TRANSACTIONS` (51): send the request again to the same coordinator.
    /// - `COORDINATOR_NOT_AVAILABLE` (15) and `NOT_COORDINATOR` (16): find the
    ///   coordinator again, then send the request again.
    ///
    /// A `FindCoordinator` answer of 14 or 15, and a failed or lost connection
    /// to the coordinator (including a TLS or SASL handshake that stops
    /// before the broker gives a verdict), also make the call find the
    /// coordinator again. The call waits between attempts with Kafka's
    /// backoff, and it stops when Kafka's default `default.api.timeout.ms`
    /// (60 s) elapses. A call whose retries are unresolved at the deadline, or
    /// whose attempt is still running, fails with a timeout error, as Kafka
    /// gives a `TimeoutException`.
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

        self.force_terminate_transaction_with_retry(transactional_id, KAFKA_ADMIN_RETRY)
            .await
    }

    async fn force_terminate_transaction_with_retry(
        &self,
        transactional_id: &str,
        retry: RetryPolicy,
    ) -> Result<(), AdminError> {
        let mut coordinator = None;
        let mut retry = CoordinatorRetry::new(retry);
        loop {
            let action = retry
                .run(self.force_terminate_attempt(
                    transactional_id,
                    &mut coordinator,
                    retry.find_coordinator(),
                ))
                .await;
            if let Some(result) = retry.next(action).await {
                return result;
            }
        }
    }

    /// One attempt of `force_terminate_transaction`.
    async fn force_terminate_attempt(
        &self,
        transactional_id: &str,
        coordinator: &mut Option<Connection>,
        find_coordinator: bool,
    ) -> RetryAction<()> {
        let connection = match self
            .transaction_coordinator(transactional_id, coordinator, find_coordinator)
            .await
        {
            Ok(connection) => connection,
            Err(action) => return action,
        };
        let request =
            force_terminate_request(transactional_id, self.options.request_timeout.millis_i32());
        match connection.send(request).await {
            Ok(response) => fence_retry_action(response.error_code),
            Err(error) => connection_failure_action(error.into()),
        }
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
    /// The call retries as Apache Kafka's `DescribeTransactionsHandler.handleError` does:
    ///
    /// - `COORDINATOR_LOAD_IN_PROGRESS` (14): send the request again to the same coordinator.
    /// - `COORDINATOR_NOT_AVAILABLE` (15) and `NOT_COORDINATOR` (16): find the
    ///   coordinator again, then send the request again.
    ///
    /// A `FindCoordinator` answer of 14 or 15, and a failed or lost connection
    /// to the coordinator (including a TLS or SASL handshake that stops
    /// before the broker gives a verdict), also make the call find the
    /// coordinator again. The call waits between attempts with Kafka's
    /// backoff, and it stops when Kafka's default `default.api.timeout.ms`
    /// (60 s) elapses. A call whose retries are unresolved at the deadline, or
    /// whose attempt is still running, fails with a timeout error, as Kafka
    /// gives a `TimeoutException`.
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

        self.describe_transaction_with_retry(transactional_id, KAFKA_ADMIN_RETRY)
            .await
    }

    async fn describe_transaction_with_retry(
        &self,
        transactional_id: &str,
        retry: RetryPolicy,
    ) -> Result<TransactionDescription, AdminError> {
        let mut coordinator = None;
        let mut retry = CoordinatorRetry::new(retry);
        loop {
            let action = retry
                .run(self.describe_transaction_attempt(
                    transactional_id,
                    &mut coordinator,
                    retry.find_coordinator(),
                ))
                .await;
            if let Some(result) = retry.next(action).await {
                return result;
            }
        }
    }

    /// One attempt of `describe_transaction`.
    async fn describe_transaction_attempt(
        &self,
        transactional_id: &str,
        coordinator: &mut Option<Connection>,
        find_coordinator: bool,
    ) -> RetryAction<TransactionDescription> {
        let connection = match self
            .transaction_coordinator(transactional_id, coordinator, find_coordinator)
            .await
        {
            Ok(connection) => connection,
            Err(action) => return action,
        };
        match connection
            .send(describe_transactions_request(transactional_id))
            .await
        {
            Ok(response) => {
                describe_retry_action(transaction_description(transactional_id, response))
            }
            Err(error) => connection_failure_action(error.into()),
        }
    }

    /// Returns the connection to the transaction coordinator of
    /// `transactional_id`. When `find_coordinator` is set, or when no
    /// connection is open, the attempt first sends `FindCoordinator` and
    /// connects to the coordinator that it names.
    ///
    /// A `FindCoordinator` answer of `COORDINATOR_LOAD_IN_PROGRESS` (14) or
    /// `COORDINATOR_NOT_AVAILABLE` (15) and a failed connection to the
    /// coordinator give [`RetryAction::FindCoordinator`], as Kafka's
    /// `CoordinatorStrategy.handleError` and `AdminApiDriver.onFailure` do.
    async fn transaction_coordinator<'c, T>(
        &self,
        transactional_id: &str,
        coordinator: &'c mut Option<Connection>,
        find_coordinator: bool,
    ) -> Result<&'c Connection, RetryAction<T>> {
        if find_coordinator {
            *coordinator = None;
        }
        if let Some(connection) = coordinator {
            return Ok(connection);
        }
        let response = self
            .conn
            .send(build_find_coordinator(
                transactional_id,
                CoordinatorKeyType::Transaction,
            ))
            .await
            .map_err(connection_failure_action)?;
        let address = match coordinator_address(transactional_id, response) {
            Ok(address) => address,
            Err(
                error @ AdminError::Broker {
                    code: COORDINATOR_LOAD_IN_PROGRESS | COORDINATOR_NOT_AVAILABLE,
                    ..
                },
            ) => return Err(RetryAction::FindCoordinator(Err(error))),
            Err(error) => return Err(RetryAction::Done(Err(error))),
        };
        match Self::connect_one(&address, self.options.clone()).await {
            Ok(connection) => Ok(coordinator.insert(connection)),
            Err(error) => Err(connection_failure_action(error)),
        }
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
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    use assert2::check;
    use bytes::{Buf, BytesMut};
    use krabka_client_core::{ClientError, MockBroker, MockReply};
    use krabka_protocol::{
        Decode,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            describe_transactions_request, find_coordinator_request,
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

    /// The transaction admin call of a retry case.
    #[derive(Clone, Copy, Debug)]
    enum TransactionCall {
        Describe,
        ForceTerminate,
    }

    /// The error codes that the mock brokers answer, one per request. The last
    /// code repeats.
    #[derive(Default)]
    struct CoordinatorScript {
        find_coordinator: Vec<i16>,
        coordinator: Vec<i16>,
        find_coordinator_requests: usize,
        coordinator_requests: usize,
        /// The first `unreachable_answers` `FindCoordinator` answers name
        /// `unreachable`, an address that refuses connections.
        unreachable_answers: usize,
        unreachable: Option<std::net::SocketAddr>,
        /// The coordinator does not answer the transaction request.
        silent: bool,
        /// The first `closed_find_coordinators` `FindCoordinator` requests
        /// close the connection with no answer.
        closed_find_coordinators: usize,
    }

    /// How the coordinator of a retry case behaves.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum CoordinatorBehavior {
        /// It answers from the script.
        Normal,
        /// The first `FindCoordinator` answer names an address that refuses
        /// connections.
        UnreachableOnce,
        /// It does not answer the transaction request.
        Silent,
        /// The bootstrap broker closes the connection on the first
        /// `FindCoordinator`.
        FindCoordinatorClosedOnce,
    }

    /// An address on which no listener accepts connections.
    async fn refused_address() -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a port");
        listener.local_addr().expect("local address")
    }

    fn next_code(codes: &[i16], requests: &mut usize) -> i16 {
        let code = codes[(*requests).min(codes.len() - 1)];
        *requests += 1;
        code
    }

    /// An `ApiVersions` response for the retry mocks.
    fn retry_api_versions() -> Vec<u8> {
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
                    api_version(init_producer_id_request::API_KEY, 0, 0),
                    api_version(describe_transactions_request::API_KEY, 0, 0),
                ],
                ..Default::default()
            },
            0,
            false,
        )
    }

    /// A mock broker that answers `FindCoordinator` with `coordinator`, and
    /// `DescribeTransactions` and `InitProducerId` with the codes of `script`.
    async fn scripted_transaction_broker(
        script: Arc<Mutex<CoordinatorScript>>,
        coordinator: Arc<Mutex<Option<std::net::SocketAddr>>>,
    ) -> MockBroker {
        MockBroker::start_with_replies(move |api_key, version, _, _| {
            let mut script = script.lock().expect("script lock");
            let script = &mut *script;
            if api_key == find_coordinator_request::API_KEY && script.closed_find_coordinators > 0 {
                script.closed_find_coordinators -= 1;
                script.find_coordinator_requests += 1;
                return MockReply::Close;
            }
            let reply = match api_key {
                api_versions_request::API_KEY => Some(retry_api_versions()),
                find_coordinator_request::API_KEY => {
                    let error_code = next_code(
                        &script.find_coordinator,
                        &mut script.find_coordinator_requests,
                    );
                    let addr = match script.unreachable {
                        Some(unreachable)
                            if script.find_coordinator_requests <= script.unreachable_answers =>
                        {
                            unreachable
                        }
                        _ => coordinator
                            .lock()
                            .expect("coordinator lock")
                            .expect("coordinator address"),
                    };
                    Some(encode_response(
                        &FindCoordinatorResponse {
                            error_code,
                            node_id: 2,
                            host: addr.ip().to_string(),
                            port: i32::from(addr.port()),
                            ..Default::default()
                        },
                        version,
                        false,
                    ))
                }
                describe_transactions_request::API_KEY => {
                    let error_code =
                        next_code(&script.coordinator, &mut script.coordinator_requests);
                    if script.silent {
                        return MockReply::Silent;
                    }
                    Some(encode_response(
                        &DescribeTransactionsResponse {
                            transaction_states: vec![TransactionState {
                                error_code,
                                transactional_id: "payments".to_owned(),
                                transaction_state: "Ongoing".to_owned(),
                                producer_id: 4242,
                                producer_epoch: 7,
                                ..Default::default()
                            }],
                            ..Default::default()
                        },
                        version,
                        true,
                    ))
                }
                init_producer_id_request::API_KEY => {
                    let error_code =
                        next_code(&script.coordinator, &mut script.coordinator_requests);
                    if script.silent {
                        return MockReply::Silent;
                    }
                    Some(encode_response(
                        &InitProducerIdResponse {
                            error_code,
                            producer_id: 4242,
                            producer_epoch: 8,
                            ..Default::default()
                        },
                        version,
                        false,
                    ))
                }
                _ => None,
            };
            reply.map_or(MockReply::Silent, MockReply::Respond)
        })
        .await
    }

    /// A call that stops at its deadline gives a timeout. The retry tests give
    /// it Kafka's `REQUEST_TIMED_OUT` code.
    const TIMED_OUT: i16 = 7;
    /// A deadline that several attempts fit in.
    const LONG: Duration = Duration::from_secs(5);
    /// One attempt fits in `NOW`, and the backoff after it reaches the
    /// deadline, so the call makes no second attempt.
    const NOW: Duration = Duration::from_millis(500);
    /// A deadline shorter than the request timeout.
    const SHORT: Duration = Duration::from_millis(300);

    type TransactionOutcome = Result<(), (&'static str, i16)>;

    type TransactionCase = (
        &'static str,
        TransactionCall,
        Vec<i16>,
        Vec<i16>,
        (Duration, CoordinatorBehavior),
        (TransactionOutcome, usize, usize),
    );

    const OK: TransactionOutcome = Ok(());

    fn failed(api: &'static str, code: i16) -> TransactionOutcome {
        Err((api, code))
    }

    /// Run one retry case against a scripted bootstrap broker and
    /// coordinator.
    async fn run_transaction_case(case: TransactionCase) {
        use TransactionCall::{Describe, ForceTerminate};

        let (name, call, find_coordinator, coordinator, (timeout, behavior), expected) = case;
        let backoff = if timeout == LONG {
            Duration::from_millis(1)
        } else {
            timeout
        };
        let script = Arc::new(Mutex::new(CoordinatorScript {
            find_coordinator,
            coordinator,
            unreachable_answers: usize::from(behavior == CoordinatorBehavior::UnreachableOnce),
            unreachable: Some(refused_address().await),
            silent: behavior == CoordinatorBehavior::Silent,
            closed_find_coordinators: usize::from(
                behavior == CoordinatorBehavior::FindCoordinatorClosedOnce,
            ),
            ..CoordinatorScript::default()
        }));
        let coordinator_addr = Arc::new(Mutex::new(None));
        let coordinator =
            scripted_transaction_broker(Arc::clone(&script), Arc::clone(&coordinator_addr)).await;
        *coordinator_addr.lock().expect("coordinator lock") = Some(coordinator.addr);
        let bootstrap =
            scripted_transaction_broker(Arc::clone(&script), Arc::clone(&coordinator_addr)).await;
        let admin = AdminClient::connect(&[bootstrap.addr.to_string()])
            .await
            .expect("admin connects");
        let retry = RetryPolicy {
            timeout,
            initial_backoff: backoff,
            max_backoff: backoff,
            jitter: 0.0,
        };

        let started = tokio::time::Instant::now();
        let result = match call {
            Describe => admin
                .describe_transaction_with_retry("payments", retry)
                .await
                .map(|_| ()),
            ForceTerminate => {
                admin
                    .force_terminate_transaction_with_retry("payments", retry)
                    .await
            }
        }
        .map_err(|error| match error {
            AdminError::Broker { api, code, .. } => (api, code),
            AdminError::Transport(ClientError::Timeout(_)) => ("timeout", TIMED_OUT),
            other => panic!("case {name}: unexpected error {other:?}"),
        });

        bootstrap.stop();
        coordinator.stop();
        let script = script.lock().expect("script lock");
        // A call never runs far past its deadline.
        let within_deadline = started.elapsed() < timeout + Duration::from_secs(2);
        assert2::assert!(
            (
                result,
                script.find_coordinator_requests,
                script.coordinator_requests,
                within_deadline
            ) == (expected.0, expected.1, expected.2, true),
            "case {name}"
        );
    }

    /// Apache Kafka's `DescribeTransactionsHandler.handleError` retries
    /// `COORDINATOR_LOAD_IN_PROGRESS` (14) on the same coordinator.
    /// `FenceProducersHandler.handleError` retries 14 and
    /// `CONCURRENT_TRANSACTIONS` (51) on the same coordinator. Both unmap the
    /// transactional ID on `COORDINATOR_NOT_AVAILABLE` (15) and
    /// `NOT_COORDINATOR` (16), so the driver finds the coordinator again, and
    /// fail on every other code. `CoordinatorStrategy.handleError` retries a
    /// `FindCoordinator` answer of 14 or 15. The driver stops at the call
    /// timeout.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transaction_calls_retry_coordinator_errors_as_kafka_does() {
        use TransactionCall::{Describe, ForceTerminate};

        for case in [
            (
                "describe: no error",
                Describe,
                vec![0],
                vec![0],
                (LONG, CoordinatorBehavior::Normal),
                (OK, 1, 1),
            ),
            (
                "describe: coordinator load in progress retries on the same coordinator",
                Describe,
                vec![0],
                vec![14, 14, 0],
                (LONG, CoordinatorBehavior::Normal),
                (OK, 1, 3),
            ),
            (
                "describe: not coordinator finds the coordinator again",
                Describe,
                vec![0],
                vec![16, 0],
                (LONG, CoordinatorBehavior::Normal),
                (OK, 2, 2),
            ),
            (
                "describe: coordinator not available finds the coordinator again",
                Describe,
                vec![0],
                vec![15, 0],
                (LONG, CoordinatorBehavior::Normal),
                (OK, 2, 2),
            ),
            (
                "describe: concurrent transactions is final",
                Describe,
                vec![0],
                vec![51],
                (LONG, CoordinatorBehavior::Normal),
                (failed("DescribeTransactions", 51), 1, 1),
            ),
            (
                "describe: transactional id not found is final",
                Describe,
                vec![0],
                vec![105],
                (LONG, CoordinatorBehavior::Normal),
                (failed("DescribeTransactions", 105), 1, 1),
            ),
            (
                "describe: coordinator load in progress past the timeout times out",
                Describe,
                vec![0],
                vec![14],
                (NOW, CoordinatorBehavior::Normal),
                (failed("timeout", TIMED_OUT), 1, 1),
            ),
            (
                "describe: find coordinator not available, then the coordinator",
                Describe,
                vec![15, 0],
                vec![0],
                (LONG, CoordinatorBehavior::Normal),
                (OK, 2, 1),
            ),
            (
                "describe: find coordinator authorization failed is final",
                Describe,
                vec![53],
                vec![0],
                (LONG, CoordinatorBehavior::Normal),
                (failed("FindCoordinator", 53), 1, 0),
            ),
            (
                "fence: no error",
                ForceTerminate,
                vec![0],
                vec![0],
                (LONG, CoordinatorBehavior::Normal),
                (OK, 1, 1),
            ),
            (
                "fence: coordinator load in progress retries on the same coordinator",
                ForceTerminate,
                vec![0],
                vec![14, 0],
                (LONG, CoordinatorBehavior::Normal),
                (OK, 1, 2),
            ),
            (
                "fence: concurrent transactions retries on the same coordinator",
                ForceTerminate,
                vec![0],
                vec![51, 51, 0],
                (LONG, CoordinatorBehavior::Normal),
                (OK, 1, 3),
            ),
            (
                "fence: not coordinator finds the coordinator again",
                ForceTerminate,
                vec![0],
                vec![16, 0],
                (LONG, CoordinatorBehavior::Normal),
                (OK, 2, 2),
            ),
            (
                "fence: cluster authorization failed is final",
                ForceTerminate,
                vec![0],
                vec![31],
                (LONG, CoordinatorBehavior::Normal),
                (failed("InitProducerId", 31), 1, 1),
            ),
            (
                "fence: concurrent transactions past the timeout times out",
                ForceTerminate,
                vec![0],
                vec![51],
                (NOW, CoordinatorBehavior::Normal),
                (failed("timeout", TIMED_OUT), 1, 1),
            ),
            (
                "fence: find coordinator load in progress, then the coordinator",
                ForceTerminate,
                vec![14, 0],
                vec![0],
                (LONG, CoordinatorBehavior::Normal),
                (OK, 2, 1),
            ),
        ] {
            run_transaction_case(case).await;
        }
    }

    /// Kafka's `AdminApiDriver.onFailure` looks the coordinator up again
    /// after a disconnect, and `KafkaAdminClient` times out a call in flight
    /// at its deadline.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transaction_calls_find_the_coordinator_again_after_connection_failures() {
        use TransactionCall::{Describe, ForceTerminate};

        for case in [
            (
                "describe: a closed connection during FindCoordinator recovers",
                Describe,
                vec![0],
                vec![0],
                (LONG, CoordinatorBehavior::FindCoordinatorClosedOnce),
                (OK, 2, 1),
            ),
            (
                "fence: a closed connection during FindCoordinator recovers",
                ForceTerminate,
                vec![0],
                vec![0],
                (LONG, CoordinatorBehavior::FindCoordinatorClosedOnce),
                (OK, 2, 1),
            ),
            (
                "describe: an unreachable coordinator finds the coordinator again",
                Describe,
                vec![0],
                vec![0],
                (LONG, CoordinatorBehavior::UnreachableOnce),
                (OK, 2, 1),
            ),
            (
                "describe: a silent coordinator stops the attempt at the deadline",
                Describe,
                vec![0],
                vec![0],
                (SHORT, CoordinatorBehavior::Silent),
                (failed("timeout", TIMED_OUT), 1, 1),
            ),
            (
                "fence: an unreachable coordinator finds the coordinator again",
                ForceTerminate,
                vec![0],
                vec![0],
                (LONG, CoordinatorBehavior::UnreachableOnce),
                (OK, 2, 1),
            ),
            (
                "fence: a silent coordinator stops the attempt at the deadline",
                ForceTerminate,
                vec![0],
                vec![0],
                (SHORT, CoordinatorBehavior::Silent),
                (failed("timeout", TIMED_OUT), 1, 1),
            ),
        ] {
            run_transaction_case(case).await;
        }
    }

    fn assert_send<T: Send>(_: T) {}

    /// A caller can spawn every retrying admin call on a multi-thread
    /// runtime, so each future must be `Send`.
    #[test]
    fn retrying_admin_call_futures_are_send() {
        let _ = |admin: &mut AdminClient,
                 offsets: &std::collections::BTreeMap<(String, i32), i64>| {
            assert_send(admin.alter_consumer_group_offsets("workers", offsets));
            assert_send(admin.list_consumer_group_offsets("workers"));
            assert_send(admin.describe_transaction("payments"));
            assert_send(admin.force_terminate_transaction("payments"));
        };
    }
}
