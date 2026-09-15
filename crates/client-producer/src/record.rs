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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordMetadata {
    pub topic_index: usize, // index into the original topic list — useful for batching callers
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: i64,
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
