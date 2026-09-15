//! Subscribe-style consumer client for Apache Kafka in Rust.
//!
//! This crate builds on `krabka-client-core` for transport. It adds the classic
//! consumer-group lifecycle (`JoinGroup` → `SyncGroup` → `Heartbeat` →
//! `Fetch` → `OffsetCommit` → `LeaveGroup`) and a built-in heartbeat
//! task.
//!
//! ## Quick start
//!
//! ```no_run
//! use krabka_client_consumer::{AutoOffsetReset, Consumer};
//! use krabka_units::millis;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let mut consumer = Consumer::builder()
//!     .bootstrap("localhost:9092")
//!     .group_id("my-group")
//!     .client_id("my-app")
//!     .auto_offset_reset(AutoOffsetReset::Earliest)
//!     .subscribe(["my-topic".to_string()])
//!     .build()
//!     .await?;
//!
//! loop {
//!     let records = consumer.poll(millis(500)).await?;
//!     for _r in records {
//!         // ... handle r ...
//!     }
//!     consumer.commit_sync().await?;
//! }
//! # }
//! ```
//!
//! ## Share-group consumption
//!
//! ```no_run
//! use krabka_client_consumer::{ShareAckMode, ShareAckType, ShareConsumer};
//! use krabka_units::secs;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let mut consumer = ShareConsumer::builder()
//!     .bootstrap("localhost:9092")
//!     .group_id("share-workers")
//!     .subscribe(["jobs".to_string()])
//!     .ack_mode(ShareAckMode::Explicit)
//!     .build()
//!     .await?;
//!
//! let records = consumer.poll(secs(1)).await?;
//! for record in &records {
//!     consumer.acknowledge(record, ShareAckType::Accept)?;
//! }
//! consumer.commit().await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Capabilities and boundaries
//!
//! This crate owns consumer-facing semantics: classic group membership,
//! assignment, fetch/poll, offset commit, cooperative shutdown, and KIP-932
//! share-group consumption. It does not duplicate admin-client surfaces such as
//! `DescribeGroups`/`ListGroups`. Manual partition fetches stay available
//! through the lower-level helpers in `krabka-client-core`. Transactional
//! consume-process-produce workflows use this crate's
//! [`ConsumerGroupMetadata`] together with `krabka-client-producer`'s
//! `send_offsets_to_transaction` support.
//!
//! ## Cargo features
//!
//! None for now.

#![doc(html_root_url = "https://docs.rs/krabka-client-consumer/0.4.0")]

mod assignor;
#[cfg(test)]
mod authentication_failure_tests;
mod builder;
mod commit;
mod consumer;
mod control;
mod coordinator;
mod error;
mod fetch_buffer;
mod fetch_session;
mod group_metadata;
#[cfg(test)]
mod lock_order_model;
mod offset_wire;
mod partition_state;
mod poll;
mod position;
mod queries;
mod rebalance_listener;
mod seek;
mod share;
mod validate;

pub use assignor::Assignor;
pub use builder::{AutoOffsetReset, IsolationLevel};
pub use commit::{OffsetAndMetadata, OffsetCommitCallback};
pub use consumer::{
    Consumer, ConsumerFetchMaxBytes, ConsumerFetchPartitionMaxBytes, ConsumerLeaveGroupTimeout,
    ConsumerRecord, ConsumerRetryPolicy, ConsumerSubscriptionMetadataRefreshInterval,
    DEFAULT_CONSUMER_DEFAULT_API_TIMEOUT, DEFAULT_CONSUMER_FETCH_MAX_WAIT,
    DEFAULT_CONSUMER_LEAVE_GROUP_TIMEOUT, DEFAULT_CONSUMER_MAX_POLL_INTERVAL,
    DEFAULT_CONSUMER_MAX_POLL_RECORDS, DEFAULT_CONSUMER_METADATA_MAX_AGE,
    DEFAULT_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL, GroupMembershipOperation, Header,
    TimestampType,
};
pub use control::{CloseOptions, DEFAULT_CONSUMER_CLOSE_TIMEOUT, WakeupHandle};
pub use error::ConsumerError;
pub use group_metadata::ConsumerGroupMetadata;
pub use queries::{Node, OffsetAndTimestamp, PartitionInfo};
pub use rebalance_listener::{ConsumerRebalanceListener, RebalanceListenerError};
pub use share::{
    DEFAULT_SHARE_CONSUMER_FETCH_MAX, DEFAULT_SHARE_CONSUMER_FETCH_MAX_RECORDS,
    DEFAULT_SHARE_CONSUMER_FETCH_MIN, DEFAULT_SHARE_CONSUMER_LEAVE_HEARTBEAT_TIMEOUT, ShareAckMode,
    ShareAckType, ShareAcquireMode, ShareConsumer, ShareConsumerFetchMaxBytes,
    ShareConsumerFetchMaxRecords, ShareConsumerFetchMinBytes, ShareConsumerLeaveHeartbeatTimeout,
    ShareConsumerRecord,
};
