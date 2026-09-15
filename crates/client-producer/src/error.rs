use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ProducerError {
    #[error("client: {0}")]
    Client(#[from] krabka_client_core::ClientError),

    #[error("protocol: {0}")]
    Protocol(#[from] krabka_protocol::ProtocolError),

    #[error("broker error_code {0}")]
    Server(i16),

    #[error("fenced by newer producer instance")]
    FencedProducer,

    #[error("invalid config: {0}")]
    InvalidConfig(String),

    #[error("batch too large: {batch_size} > max")]
    BatchTooLarge { batch_size: usize },

    #[error("record too large: {record_size} > max_request_size")]
    RecordTooLarge { record_size: usize },

    #[error("send buffer full (max_block exceeded)")]
    BufferFull,

    #[error("producer closed")]
    Closed,

    #[error("flush timed out")]
    FlushTimeout,

    /// `send` waited `waited` (the configured `max_block`) for metadata that
    /// holds the topic, and the partition of the record when it names one.
    /// Kafka's `KafkaProducer.waitOnMetadata` throws `TimeoutException` with
    /// the same message.
    #[error("{}", metadata_timeout_message(topic, *partition, *partition_count, *waited))]
    MetadataTimeout {
        topic: String,
        partition: Option<i32>,
        partition_count: Option<i32>,
        waited: std::time::Duration,
    },

    /// The batch ran out of retries, or its routing budget ended, before the
    /// broker acknowledged it. Kafka raises `TimeoutException` for the same
    /// case (`Sender.sendProducerData` and `RecordAccumulator.expiredBatches`).
    #[error("the batch was not acknowledged before its retries ran out")]
    SendTimeout,

    #[error("compression: {0}")]
    Compression(#[from] krabka_compression::CompressionError),

    #[error("producer is not transactional (no transactional_id configured)")]
    NotTransactional,

    #[error("invalid transaction state: {0}")]
    InvalidTransactionState(&'static str),

    #[error("transaction was aborted by the broker (timeout or fence)")]
    TransactionAborted,

    #[error("concurrent transactions on the same transactional_id")]
    ConcurrentTransactions,

    #[error(
        "transaction outcome is unknown; call init_transactions before sending or beginning another transaction"
    )]
    RecoveryRequired,
}

/// Kafka's `KafkaProducer.getErrorMessage`.
fn metadata_timeout_message(
    topic: &str,
    partition: Option<i32>,
    partition_count: Option<i32>,
    waited: std::time::Duration,
) -> String {
    let waited_ms = waited.as_millis();
    match (partition, partition_count) {
        (Some(partition), Some(count)) => format!(
            "Partition {partition} of topic {topic} with partition count {count} is not present in metadata after {waited_ms} ms."
        ),
        _ => format!("Topic {topic} not present in metadata after {waited_ms} ms."),
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn display_messages() {
        for (_name, error, expected) in [
            (
                "fenced producer",
                ProducerError::FencedProducer,
                "fenced by newer producer instance",
            ),
            (
                "invalid config",
                ProducerError::InvalidConfig("idempotence requires acks=all".to_owned()),
                "invalid config: idempotence requires acks=all",
            ),
        ] {
            assert2::assert!(error.to_string() == expected);
        }
    }
}
