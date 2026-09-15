//! Idempotent producer client for Apache Kafka in Rust.
//!
//! It builds on [`krabka_client_core`] for transport, and adds full
//! idempotent-producer semantics: `InitProducerId` on connect, per-batch
//! `(producer_id, producer_epoch, base_sequence)`, and retries that re-frame
//! the same `RecordBatch` so the broker's dedup catches them.
//!
//! It also supports transactional, exactly-once production:
//! `init_transactions`, `begin_transaction`, which returns a [`Transaction`]
//! guard whose `commit`/`abort` finishes it, and `send_offsets_to_transaction`
//! for the consume-process-produce pattern of KIP-447.
//!
//! ## Quick start
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use bytes::Bytes;
//! use krabka_client_producer::{Acks, Compression, Producer, ProducerRecord};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let producer = Producer::builder()
//!     .bootstrap("localhost:9092")
//!     .compression(Compression::Lz4)
//!     .acks(Acks::All)
//!     .linger(Duration::from_millis(5))
//!     .build()
//!     .await?;
//!
//! let metadata = producer
//!     .send(ProducerRecord {
//!         topic: "my-topic".into(),
//!         value: Some(Bytes::from("hello")),
//!         ..Default::default()
//!     })
//!     .await
//!     .await??;
//!
//! producer.flush().await?;
//! producer.close().await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Capabilities and boundaries
//!
//! This crate owns producer-facing semantics: batching, compression,
//! idempotence, retries, per-record partition overrides, transactional RPCs,
//! and `send_offsets_to_transaction` for consume-process-produce flows. The
//! built-in partitioner follows Kafka's: keyed records hash to a partition,
//! and keyless records use an adaptive sticky partition (KIP-480, KIP-794). Set
//! `ProducerRecord::partition` to pin an individual record. Serialization is
//! deliberately owned by the caller: `key` and `value` are raw `Bytes`, so a
//! schema-registry or serde integration can sit on top without constraining the
//! producer API.

#![doc(html_root_url = "https://docs.rs/krabka-client-producer/0.4.0")]

mod accumulator;
mod buffer_pool;
mod builder;
#[cfg(test)]
mod client_failover_model;
mod compression;
mod error;
mod error_class;
mod metadata_age;
mod metadata_wait;
mod partitioner;
mod producer;
mod record;
mod sender;
mod transactional;
mod transport;
mod txn_retry;

pub use builder::{
    DEFAULT_PRODUCER_ACKS, DEFAULT_PRODUCER_BATCH_BYTES, DEFAULT_PRODUCER_BUFFER_MEMORY,
    DEFAULT_PRODUCER_COMPRESSION, DEFAULT_PRODUCER_DELIVERY_TIMEOUT,
    DEFAULT_PRODUCER_FLUSH_TIMEOUT, DEFAULT_PRODUCER_INIT_RETRY_TIMEOUT, DEFAULT_PRODUCER_LINGER,
    DEFAULT_PRODUCER_MAX_BLOCK, DEFAULT_PRODUCER_MAX_IN_FLIGHT, DEFAULT_PRODUCER_MAX_REQUEST_SIZE,
    DEFAULT_PRODUCER_PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE,
    DEFAULT_PRODUCER_PARTITIONER_AVAILABILITY_TIMEOUT, DEFAULT_PRODUCER_PARTITIONER_IGNORE_KEYS,
    DEFAULT_PRODUCER_REQUEST_TIMEOUT, DEFAULT_PRODUCER_RETRIES, DEFAULT_PRODUCER_RETRY_BACKOFF,
    DEFAULT_PRODUCER_RETRY_BACKOFF_MAX, DEFAULT_PRODUCER_TRANSACTION_TIMEOUT, ProducerFlushTimeout,
    ProducerRetryPolicy, ProducerThroughputPolicy,
};
pub use compression::Compression;
pub use error::{ProducerError, RecordSizeLimit};
pub use krabka_client_consumer::ConsumerGroupMetadata;
pub use metadata_age::{
    DEFAULT_PRODUCER_METADATA_MAX_AGE, DEFAULT_PRODUCER_METADATA_MAX_IDLE,
    MIN_PRODUCER_METADATA_MAX_IDLE,
};
pub use partitioner::partition_for_key;
pub use producer::{Acks, Producer};
pub use record::{Header, ProducerRecord, RecordMetadata};
pub use transactional::{
    EndTransactionError, OwnedTransaction, PreparedTransactionState,
    PreparedTransactionStateParseError, Transaction,
};
