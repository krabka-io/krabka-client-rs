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

    /// The record names a negative partition. Kafka's `ProducerRecord`
    /// constructor throws `IllegalArgumentException` with the same message.
    #[error("Invalid partition: {0}. Partition number should always be non-negative or null.")]
    InvalidPartition(i32),

    /// The record carries a negative timestamp. Kafka's `ProducerRecord`
    /// constructor throws `IllegalArgumentException` with the same message.
    #[error("Invalid timestamp: {0}. Timestamp should always be non-negative or null.")]
    InvalidTimestamp(i64),

    #[error("batch too large: {batch_size} > max")]
    BatchTooLarge { batch_size: usize },

    /// The record is larger than a limit of the producer. Kafka's
    /// `KafkaProducer.ensureValidRecordSize` throws `RecordTooLargeException`
    /// with the same message for each limit.
    #[error("{}", record_too_large_message(*record_size, *limit))]
    RecordTooLarge {
        /// Kafka's upper bound of the serialized size of the record, in bytes.
        record_size: usize,
        /// The limit that the record is larger than.
        limit: RecordSizeLimit,
    },

    /// `send` waited `max_block` (the part of `max_block` that the metadata
    /// wait left) for buffer memory, and the memory did not become free.
    /// Kafka's `BufferPool.allocate` throws `BufferExhaustedException` with the
    /// same message.
    #[error(
        "Failed to allocate {size} bytes within the configured max blocking time {} ms. Total memory: {total} bytes. Available memory: {available} bytes. Poolable size: {poolable} bytes",
        max_block.as_millis()
    )]
    BufferExhausted {
        size: usize,
        max_block: std::time::Duration,
        total: usize,
        available: usize,
        poolable: usize,
    },

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

/// The producer limit that a record is larger than.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordSizeLimit {
    /// The `max_request_size` setting, in bytes. Kafka's `max.request.size`.
    MaxRequestSize(usize),
    /// The `buffer_memory` setting. Kafka's `buffer.memory`.
    BufferMemory,
}

/// Kafka's `KafkaProducer.ensureValidRecordSize` messages.
fn record_too_large_message(record_size: usize, limit: RecordSizeLimit) -> String {
    match limit {
        RecordSizeLimit::MaxRequestSize(max_request_size) => format!(
            "The message is {record_size} bytes when serialized which is larger than {max_request_size}, which is the value of the max_request_size configuration."
        ),
        RecordSizeLimit::BufferMemory => format!(
            "The message is {record_size} bytes when serialized which is larger than the total memory buffer you have configured with the buffer_memory configuration."
        ),
    }
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
