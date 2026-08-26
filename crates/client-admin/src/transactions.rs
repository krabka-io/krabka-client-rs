//! Transaction administration.

use crabka_client_core::{CoordinatorKeyType, build_find_coordinator};
use crabka_protocol::owned::{
    describe_transactions_request::DescribeTransactionsRequest,
    describe_transactions_response::{DescribeTransactionsResponse, TransactionState},
    find_coordinator_response::FindCoordinatorResponse,
    init_producer_id_request::InitProducerIdRequest,
};
use crabka_units::{Time, convert::TimeExt as _};

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

fn force_terminate_request(
    transactional_id: &str,
    transaction_timeout_ms: i32,
) -> InitProducerIdRequest {
    InitProducerIdRequest {
        transactional_id: Some(transactional_id.to_owned()),
        transaction_timeout_ms,
        producer_id: -1,
        producer_epoch: -1,
        enable2_pc: false,
        keep_prepared_txn: false,
        ..Default::default()
    }
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
    /// # Errors
    ///
    /// Returns [`AdminError::Protocol`] for an empty transactional ID, or the
    /// coordinator lookup, connection, transport, and broker errors returned
    /// by Kafka.
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
    use assert2::check;
    use crabka_protocol::owned::find_coordinator_response::Coordinator;

    use super::*;

    fn state_row(transactional_id: &str, error_code: i16) -> TransactionState {
        TransactionState {
            error_code,
            transactional_id: transactional_id.to_owned(),
            ..Default::default()
        }
    }

    #[test]
    fn force_termination_request_fences_without_preserving_transaction() {
        let request = force_terminate_request("payments", 30_000);

        assert2::assert!(request.transactional_id.as_deref() == Some("payments"));
        assert2::assert!(request.transaction_timeout_ms == 30_000);
        assert2::assert!(request.producer_id == -1);
        assert2::assert!(request.producer_epoch == -1);
        assert2::assert!(!request.enable2_pc);
        assert2::assert!(!request.keep_prepared_txn);
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
