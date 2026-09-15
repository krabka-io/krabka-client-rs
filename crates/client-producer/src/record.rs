//! Public record types. `ProducerRecord` is what the caller sends,
//! `RecordMetadata` is what it gets back, and `Header` holds the per-record key
//! and value pairs.

use bytes::Bytes;

use crate::error::ProducerError;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProducerRecord {
    pub topic: String,
    /// If `Some(p)`, the producer bypasses the partitioner and uses partition
    /// `p`. `send` fails a negative `p` with
    /// [`ProducerError::InvalidPartition`].
    pub partition: Option<i32>,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<Header>,
    /// If `None`, the producer fills in the current wall-clock time at
    /// accumulator append time. `send` fails a negative value with
    /// [`ProducerError::InvalidTimestamp`].
    pub timestamp_ms: Option<i64>,
}

impl ProducerRecord {
    /// Check the record as Kafka's `ProducerRecord` constructor does: first
    /// the timestamp, then the partition.
    ///
    /// # Errors
    ///
    /// Returns [`ProducerError::InvalidTimestamp`] for a negative timestamp,
    /// and [`ProducerError::InvalidPartition`] for a negative partition.
    pub(crate) fn validate(&self) -> Result<(), ProducerError> {
        if let Some(timestamp) = self.timestamp_ms.filter(|timestamp| *timestamp < 0) {
            return Err(ProducerError::InvalidTimestamp(timestamp));
        }
        if let Some(partition) = self.partition.filter(|partition| *partition < 0) {
            return Err(ProducerError::InvalidPartition(partition));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub key: String,
    pub value: Option<Bytes>,
}

/// What the producer learned about a record that the broker acknowledged.
///
/// Kafka's `RecordMetadata` carries the same fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordMetadata {
    /// The topic of the record.
    pub topic: String,
    pub partition: i32,
    /// The offset of the record in the partition. It is -1 when the offset is
    /// not known, which is always the case with `acks=0`. Kafka's
    /// `RecordMetadata` constructor keeps a base offset of -1 and does not add
    /// the index of the record in its batch.
    pub offset: i64,
    /// The log append time from the broker when the topic uses
    /// `message.timestamp.type=LogAppendTime`, and else the create time of the
    /// record. Kafka's `FutureRecordMetadata.timestamp` makes the same choice.
    pub timestamp_ms: i64,
    /// The size of the key in bytes, or -1 for a null key.
    pub serialized_key_size: i32,
    /// The size of the value in bytes, or -1 for a null value.
    pub serialized_value_size: i32,
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn producer_record_default_is_empty() {
        let r = ProducerRecord::default();
        assert2::assert!(
            r == ProducerRecord {
                topic: String::new(),
                partition: None,
                key: None,
                value: None,
                headers: vec![],
                timestamp_ms: None,
            }
        );
    }
}
