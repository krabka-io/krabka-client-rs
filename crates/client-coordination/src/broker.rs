//! The [`CoordinationTransport`] that talks to a Kafka cluster.
//!
//! [`BrokerTransport`] composes `crabka-client-producer`,
//! `crabka-client-admin`, and `crabka-client-core`. It adds no wire message.
//! The points below are the parts of the composition that a reader cannot see
//! from the call sites, and each one is load-bearing.
//!
//! # One role uses two producers
//!
//! The transactional producer carries `transactional.id = <role>`, and it
//! writes the lease records of that role. A second, plain producer appends the
//! registration records of every role. Kafka requires every send of a
//! transactional producer to sit inside a transaction, and a candidate holds no
//! epoch to open a transaction with, so a registration cannot travel on the
//! transactional producer.
//!
//! # The transactional producer is the epoch
//!
//! [`BrokerTransport::acquire_epoch`] builds the transactional producer and
//! calls `init_transactions`. That call sends `InitProducerId` to the
//! transaction coordinator, and the coordinator mints the producer epoch of the
//! role. The transport keeps the producer, because the same instance stays
//! bound to the epoch that the coordinator minted for it, and every write it
//! makes carries that epoch.
//! [`BrokerTransport::bound_producer`] hands the producer to the caller, so a
//! leadership handle gives out a writer that the broker already fences.
//!
//! # A fence is the loss of the role
//!
//! [`BrokerTransport::write_lease`] maps `FencedProducer`, and the broker codes
//! 47 `INVALID_PRODUCER_EPOCH` and 90 `PRODUCER_FENCED`, onto
//! [`CoordinationError::Fenced`]. That mapping is the whole mechanism by which
//! a deposed leader learns that it lost the role. No clock takes part in it.
//!
//! [`BrokerTransport::register`] does not use the mapping. Its producer holds
//! no epoch for the role, so a fence on the plain producer says something about
//! the idempotent identity of that producer and says nothing about the role.
//!
//! # All records of one role go to one partition
//!
//! The succession rules rank candidates on the offset of their registration, so
//! the records of one role need a total order, and one partition gives that
//! order. Both producers pin the partition with
//! [`ProducerRecord::partition`](crabka_client_producer::ProducerRecord), and
//! they compute it from the role name.
//!
//! Pinning is a correctness requirement and not a preference. Kafka's default
//! partitioner hashes the record key. A registration key and a lease key of the
//! same role differ, because the registration key names a member and the lease
//! key does not. A partitioner that reads the key puts the two kinds in two
//! partitions, and the total order is gone.
//!
//! `krabka-streams-java` and `krabka-streams-go` write the same topic, so they
//! need the same partition for the same role. [`role_partition`] is the rule
//! the three implementations share. It calls
//! `crabka_client_producer::partition_for_key`, which is Kafka's own rule:
//! `murmur2` of the role name in UTF-8, masked with `Utils.toPositive`, then
//! the remainder of the partition count.
//!
//! # The read is committed
//!
//! [`BrokerTransport::read_role_records`] fetches at isolation level 1, so an
//! aborted lease write is invisible. It walks from the first offset of the
//! partition to the last stable offset, and it drops every record whose key
//! belongs to another role.

use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    sync::Arc,
};

use async_trait::async_trait;
use crabka_client_admin::{
    AdminClient, AdminError, CreateTopicOutcome, CreateTopicSpec, TransactionDescription,
};
use crabka_client_core::{
    BrokerInfo, BrokerPool, ClientDnsTimeout, ClientError, ClientSecurity, Connection,
    ConnectionOptions, DEFAULT_FETCH_RESPONSE_MAX, FetchMinBytes, FetchedRecord, IsolatedFetch,
    fetch_partition_with_isolation_progress,
};
use crabka_client_producer::{
    Acks, Producer, ProducerError, ProducerRecord, RecordMetadata, partition_for_key,
};
use crabka_protocol::{
    owned::{
        list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
        list_offsets_response::ListOffsetsResponse,
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        metadata_response::MetadataResponse,
    },
    primitives::uuid::Uuid as WireUuid,
};
use crabka_units::{ByteSize, Time, convert::TimeExt as _, mebibytes, millis, secs};
use tokio::sync::{Mutex, OnceCell};

use crate::{
    error::CoordinationError,
    record::{
        COORDINATION_STATE_TOPIC, CoordinationKey, CoordinationRecord, FencingToken, Lease,
        MemberId, Registration, Role, decode_key, decode_value, encode_key, encode_value,
    },
    transport::{CoordinationTransport, RoleRecords},
};

/// The number of partitions that the transport gives the coordination topic
/// when it creates it.
pub const DEFAULT_COORDINATION_PARTITIONS: i32 = 16;

/// The replication factor that the transport gives the coordination topic when
/// it creates it.
///
/// The design of the coordination client asks an operator for
/// `min.insync.replicas >= 2`, and three replicas leave room for one broker to
/// fail. A single-broker cluster rejects this factor, so a development cluster
/// needs a lower one on the builder.
pub const DEFAULT_COORDINATION_REPLICATION: i32 = 3;

/// The deadline that the transport puts on one request.
pub const DEFAULT_COORDINATION_REQUEST_TIMEOUT: Time = secs(30);

/// The time that one fetch of the coordination topic waits for records.
pub const DEFAULT_COORDINATION_FETCH_MAX_WAIT: Time = millis(500);

/// The byte limit that the transport puts on one partition fetch.
pub const DEFAULT_COORDINATION_FETCH_PARTITION_MAX: ByteSize = mebibytes(1);

/// The client id that the transport gives every connection it opens.
pub const DEFAULT_COORDINATION_CLIENT_ID: &str = "crabka-coordination";

/// Kafka's `UNKNOWN_TOPIC_OR_PARTITION`.
const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;

/// Kafka's `LEADER_NOT_AVAILABLE`.
const LEADER_NOT_AVAILABLE: i16 = 5;

/// Kafka's `TOPIC_ALREADY_EXISTS`. A second member that starts against the
/// same cluster gets this code, and it is the expected answer.
const TOPIC_ALREADY_EXISTS: i16 = 36;

/// Kafka's `INVALID_PRODUCER_EPOCH`. The broker answers this when a write
/// carries an epoch that a later holder superseded.
const INVALID_PRODUCER_EPOCH: i16 = 47;

/// Kafka's `PRODUCER_FENCED`. The transaction coordinator answers this when a
/// later holder superseded the producer of an open transaction.
const PRODUCER_FENCED: i16 = 90;

/// Kafka's `TRANSACTIONAL_ID_NOT_FOUND`. The coordinator answers this for a
/// role that no member has ever held.
const TRANSACTIONAL_ID_NOT_FOUND: i16 = 105;

/// The `isolation_level` of a committed read.
const READ_COMMITTED: i8 = 1;

/// The `ListOffsets` timestamp that asks for the first offset of a partition.
const LIST_OFFSETS_EARLIEST: i64 = -2;

/// The `ListOffsets` timestamp that asks for the offset past the last record.
/// Under [`READ_COMMITTED`] the broker answers with the last stable offset.
const LIST_OFFSETS_LATEST: i64 = -1;

/// The partition count and the topic id of the coordination topic.
///
/// The transport reads this once and keeps it. An operator that adds
/// partitions to the topic afterwards moves the partition of every role, so the
/// transport does not follow such a change. Restart the members after such a
/// change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TopicLayout {
    /// The number of partitions the topic has.
    partitions: i32,
    /// The topic id that a `Fetch` of version 13 and later carries.
    topic_id: WireUuid,
}

/// The transactional producer of one role and the epoch it holds.
struct Holder {
    /// The producer that `init_transactions` bound to `token`.
    producer: Arc<Producer>,
    /// The epoch the transaction coordinator minted for the role.
    token: FencingToken,
}

/// The [`CoordinationTransport`] that talks to a Kafka cluster.
///
/// Build one with [`BrokerTransport::builder`]. One transport serves every
/// role of one process. It opens the connections it needs on first use, and
/// [`BrokerTransport::close`] shuts them down.
///
/// The module documentation states the parts of the design that the call sites
/// do not show.
pub struct BrokerTransport {
    /// The bootstrap address list, as the caller wrote it.
    bootstrap: String,
    /// The client id of every connection and every producer.
    client_id: String,
    /// The TLS and SASL policy of every connection and every producer.
    security: Option<ClientSecurity>,
    /// The lease duration, which becomes the transaction timeout of a role.
    lease_duration: Time,
    /// The deadline of one request.
    request_timeout: Time,
    /// The time one fetch waits for records.
    fetch_max_wait: Time,
    /// The byte limit of one partition fetch.
    fetch_partition_max: ByteSize,
    /// The smallest response one fetch accepts.
    fetch_min: FetchMinBytes,
    /// The partition count the transport asks for when it creates the topic.
    topic_partitions: i32,
    /// The replication factor the transport asks for when it creates the topic.
    topic_replication: i32,
    /// The admin client. `create_topics` needs unique access, so a mutex owns
    /// it.
    admin: Mutex<AdminClient>,
    /// The connections to the brokers. `read_role_records` fetches from the
    /// leader of the partition of the role, so it needs a connection per
    /// broker id.
    pool: BrokerPool,
    /// The layout of the coordination topic, read on first use.
    layout: OnceCell<TopicLayout>,
    /// The plain producer that appends registrations, built on first use.
    registrar: OnceCell<Producer>,
    /// The transactional producer of every role this process holds an epoch
    /// for.
    holders: Mutex<HashMap<Role, Holder>>,
}

#[bon::bon]
impl BrokerTransport {
    /// Builds a transport against the cluster at `bootstrap`.
    ///
    /// `bootstrap` takes one `host:port`, or several separated by commas.
    /// `lease_duration` becomes the transaction timeout of every role, so the
    /// transaction coordinator aborts the open transaction of a dead holder
    /// after that extent.
    ///
    /// The call opens the admin connection. It does not create the topic and
    /// it does not build a producer. The first call that needs the topic
    /// creates it.
    ///
    /// # Errors
    ///
    /// Returns [`CoordinationError::InvalidConfig`] when `bootstrap` holds no
    /// address, when an address does not resolve, or when the partition count
    /// or the replication factor is not positive. Returns
    /// [`CoordinationError::Admin`] when no bootstrap address accepts the
    /// admin connection.
    #[builder(start_fn = builder, finish_fn = build)]
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(bootstrap = %bootstrap, client_id = %client_id),
        err,
    )]
    pub async fn start(
        #[builder(into)] bootstrap: String,
        #[builder(into, default = DEFAULT_COORDINATION_CLIENT_ID.to_owned())] client_id: String,
        #[builder(default = crate::lease::DEFAULT_LEASE_DURATION)] lease_duration: Time,
        #[builder(default = DEFAULT_COORDINATION_REQUEST_TIMEOUT)] request_timeout: Time,
        #[builder(default = DEFAULT_COORDINATION_FETCH_MAX_WAIT)] fetch_max_wait: Time,
        #[builder(default = DEFAULT_COORDINATION_FETCH_PARTITION_MAX)]
        fetch_partition_max: ByteSize,
        #[builder(default = DEFAULT_COORDINATION_PARTITIONS)] topic_partitions: i32,
        #[builder(default = DEFAULT_COORDINATION_REPLICATION)] topic_replication: i32,
        security: Option<ClientSecurity>,
    ) -> Result<Self, CoordinationError> {
        check_topic_shape(topic_partitions, topic_replication)?;
        let addresses = bootstrap_addresses(&bootstrap);
        if addresses.is_empty() {
            return Err(CoordinationError::InvalidConfig(format!(
                "the bootstrap list {bootstrap:?} holds no address"
            )));
        }
        let fetch_min = FetchMinBytes::new(1).map_err(CoordinationError::InvalidConfig)?;
        let options = ConnectionOptions {
            client_id: client_id.clone(),
            request_timeout,
            security: security.clone().map(Box::new),
            ..ConnectionOptions::default()
        };
        let resolved = resolve_addresses(&addresses, options.dns_timeout).await?;
        let admin = AdminClient::connect_secured(&addresses, security.clone()).await?;
        Ok(Self {
            bootstrap,
            client_id,
            security,
            lease_duration,
            request_timeout,
            fetch_max_wait,
            fetch_partition_max,
            fetch_min,
            topic_partitions,
            topic_replication,
            admin: Mutex::new(admin),
            pool: BrokerPool::new(resolved, options),
            layout: OnceCell::new(),
            registrar: OnceCell::new(),
            holders: Mutex::new(HashMap::new()),
        })
    }
}

impl BrokerTransport {
    /// The producer that [`CoordinationTransport::acquire_epoch`] bound to the
    /// epoch of `role`.
    ///
    /// The result is `None` when this process holds no epoch for `role`. A
    /// leadership handle gives the producer to the caller, and the broker
    /// fences every write the caller makes with it.
    pub async fn bound_producer(&self, role: &Role) -> Option<Arc<Producer>> {
        let holders = self.holders.lock().await;
        holders.get(role).map(|holder| Arc::clone(&holder.producer))
    }

    /// The epoch that [`CoordinationTransport::acquire_epoch`] minted for
    /// `role` in this process.
    ///
    /// The result is `None` when this process holds no epoch for `role`. The
    /// value is what this process believes. The cluster is the authority, and
    /// [`CoordinationTransport::describe`] asks it.
    pub async fn held_token(&self, role: &Role) -> Option<FencingToken> {
        let holders = self.holders.lock().await;
        holders.get(role).map(|holder| holder.token)
    }

    /// Closes every producer and every connection this transport opened.
    ///
    /// Each producer runs a sender task, and this call stops those tasks. Drop
    /// every leadership handle before this call. A producer that a handle still
    /// shares stays open, and this call reports it in a warning.
    pub async fn close(self) {
        for (role, holder) in self.holders.into_inner() {
            close_producer(&role, holder.producer).await;
        }
        let closed = match self.registrar.into_inner() {
            Some(registrar) => registrar.close().await,
            None => Ok(()),
        };
        if let Err(error) = closed {
            tracing::warn!(%error, "the registration producer did not close cleanly");
        }
        self.pool.close_all();
    }

    /// The partition that holds every record of `role`.
    ///
    /// This reads the layout of the topic on first use, and it creates the
    /// topic when the cluster does not have it.
    async fn partition_of(&self, role: &Role) -> Result<i32, CoordinationError> {
        let layout = self.topic_layout().await?;
        role_partition(role, layout.partitions)
    }

    /// The layout of the coordination topic, read once and kept.
    async fn topic_layout(&self) -> Result<TopicLayout, CoordinationError> {
        self.layout
            .get_or_try_init(|| Box::pin(self.resolve_topic()))
            .await
            .copied()
    }

    /// Creates the topic when it is absent, then reads its layout.
    async fn resolve_topic(&self) -> Result<TopicLayout, CoordinationError> {
        self.create_topic().await?;
        let metadata = self.read_metadata().await?;
        topic_layout(&metadata)
    }

    /// Creates the coordination topic with `cleanup.policy=compact`, and takes
    /// `TOPIC_ALREADY_EXISTS` for success.
    #[tracing::instrument(level = "info", skip_all, err)]
    async fn create_topic(&self) -> Result<(), CoordinationError> {
        let spec = coordination_topic_spec(self.topic_partitions, self.topic_replication);
        let outcomes = self
            .admin
            .lock()
            .await
            .create_topics(&[spec], self.request_timeout)
            .await?;
        accept_topic_creation(&outcomes)
    }

    /// Reads the metadata of the coordination topic from the bootstrap broker.
    async fn read_metadata(&self) -> Result<MetadataResponse, CoordinationError> {
        let connection = self.pool.bootstrap_connection().await?;
        let metadata = connection.send(coordination_metadata_request()).await?;
        Ok(metadata)
    }

    /// Opens a connection to the broker that leads `partition`.
    async fn leader_connection(
        &self,
        partition: i32,
    ) -> Result<Arc<Connection>, CoordinationError> {
        let metadata = self.read_metadata().await?;
        self.pool.refresh_brokers(&brokers_of(&metadata)).await;
        let leader = partition_leader(&metadata, partition)?;
        Ok(self.pool.get(leader).await?)
    }

    /// Asks the leader for one boundary offset of `partition`.
    async fn list_offset(
        &self,
        connection: &Connection,
        partition: i32,
        timestamp: i64,
    ) -> Result<i64, CoordinationError> {
        let response = connection
            .send(list_offsets_request(partition, timestamp))
            .await?;
        list_offsets_offset(&response, partition)
    }

    /// The plain producer that appends registrations, built on first use.
    async fn registrar(&self) -> Result<&Producer, CoordinationError> {
        self.registrar
            .get_or_try_init(|| Box::pin(self.build_registrar()))
            .await
    }

    /// Builds the plain producer. It holds no `transactional.id`, so it appends
    /// outside a transaction.
    async fn build_registrar(&self) -> Result<Producer, CoordinationError> {
        let producer = Producer::builder()
            .bootstrap(self.bootstrap.clone())
            .client_id(self.client_id.clone())
            .acks(Acks::All)
            .request_timeout(self.request_timeout.to_std())
            .maybe_security(self.security.clone())
            .build()
            .await?;
        Ok(producer)
    }

    /// Builds the transactional producer of `role`. Its `transactional.id` is
    /// the role name, and its transaction timeout is the lease duration.
    async fn build_role_producer(&self, role: &Role) -> Result<Producer, CoordinationError> {
        let producer = Producer::builder()
            .bootstrap(self.bootstrap.clone())
            .client_id(self.client_id.clone())
            .acks(Acks::All)
            .request_timeout(self.request_timeout.to_std())
            .transactional_id(role.as_str().to_owned())
            .transaction_timeout(self.lease_duration.to_std())
            .maybe_security(self.security.clone())
            .build()
            .await
            .map_err(|error| fenced_or(role, error))?;
        Ok(producer)
    }

    /// Reads the transactional producer and the epoch this process holds for
    /// `role`.
    async fn holder_of(&self, role: &Role) -> Option<(Arc<Producer>, FencingToken)> {
        let holders = self.holders.lock().await;
        holders
            .get(role)
            .map(|holder| (Arc::clone(&holder.producer), holder.token))
    }
}

#[async_trait]
impl CoordinationTransport for BrokerTransport {
    /// Mints the epoch of `role` and fences the member that held it.
    ///
    /// The call builds the transactional producer of the role and calls
    /// `init_transactions`, which sends `InitProducerId` to the transaction
    /// coordinator. The transport keeps that producer, because it stays bound
    /// to the epoch the coordinator minted.
    ///
    /// The call then reads the minted pair out of that producer with
    /// `Producer::transactional_identity`. It does not ask the coordinator
    /// again. A second request would report whatever the coordinator holds at
    /// that moment, so a competitor that mints in between would hand this
    /// member the token of the competitor. `Producer::producer_id` is a
    /// different pair again: it reports the build-time `InitProducerId`, which
    /// carries no `transactional.id`, and the coordinator never advances it.
    ///
    /// # Errors
    ///
    /// Returns [`CoordinationError::Fenced`] when a later member already took
    /// `role` while this call ran. Returns [`CoordinationError::Producer`] when
    /// the coordinator refuses the call or returns no identity, and
    /// [`CoordinationError::Client`] when a connection fails.
    #[tracing::instrument(level = "info", skip_all, fields(role = %role), err)]
    async fn acquire_epoch(&self, role: &Role) -> Result<FencingToken, CoordinationError> {
        self.topic_layout().await?;
        let producer = self.build_role_producer(role).await?;
        producer
            .init_transactions()
            .await
            .map_err(|error| fenced_or(role, error))?;
        // Read the minted pair back from the producer itself. A read through
        // DescribeTransactions would report whatever the coordinator holds at
        // that moment, so a competitor that mints between the two requests
        // would give this member the token of another member.
        let (producer_id, producer_epoch) =
            producer
                .transactional_identity()
                .await
                .ok_or(CoordinationError::Producer(
                    ProducerError::InvalidTransactionState(
                        "init_transactions returned without an identity",
                    ),
                ))?;
        let token = FencingToken::new(producer_id, producer_epoch)?;
        let replaced = self.holders.lock().await.insert(
            role.clone(),
            Holder {
                producer: Arc::new(producer),
                token,
            },
        );
        if let Some(replaced) = replaced {
            close_producer(role, replaced.producer).await;
        }
        Ok(token)
    }

    /// Reads the partition of `role` and returns its records in offset order.
    ///
    /// The scan runs at isolation level 1, so an aborted lease write is
    /// invisible. It starts at the first offset the partition still holds, and
    /// it stops when the cursor reaches the last stable offset that the cluster
    /// reported at the start of the scan. The scan does not wait for a record
    /// that a writer appends while it runs.
    ///
    /// The scan drops a record whose key does not decode, and a record whose
    /// key names another role. Both belong to a writer that this role does not
    /// own.
    ///
    /// # Errors
    ///
    /// Returns [`CoordinationError::Client`] when the metadata read, the offset
    /// lookup, or a fetch fails, and when the cluster reports no leader for the
    /// partition of `role`. Returns [`CoordinationError::Record`] when the
    /// value of a record of `role` does not decode. Returns
    /// [`CoordinationError::InvalidConfig`] when the topic reports no
    /// partition.
    #[tracing::instrument(level = "debug", skip_all, fields(role = %role), err)]
    async fn read_role_records(&self, role: &Role) -> Result<RoleRecords, CoordinationError> {
        let layout = self.topic_layout().await?;
        let partition = role_partition(role, layout.partitions)?;
        let connection = self.leader_connection(partition).await?;
        let start = self
            .list_offset(&connection, partition, LIST_OFFSETS_EARLIEST)
            .await?;
        let end = self
            .list_offset(&connection, partition, LIST_OFFSETS_LATEST)
            .await?;

        let mut offset = start;
        let mut records = RoleRecords::new();
        while offset < end {
            let page = fetch_partition_with_isolation_progress(
                &connection,
                IsolatedFetch {
                    topic: COORDINATION_STATE_TOPIC,
                    topic_id: layout.topic_id,
                    partition,
                    fetch_offset: offset,
                    max_wait: self.fetch_max_wait,
                    max: DEFAULT_FETCH_RESPONSE_MAX,
                    partition_max: self.fetch_partition_max,
                    fetch_min: self.fetch_min,
                    isolation_level: READ_COMMITTED,
                },
            )
            .await?;
            records.append(&mut select_role_records(role, &page.records)?);
            match next_scan_offset(offset, end, page.next_offset) {
                Some(next) => offset = next,
                None => break,
            }
        }
        Ok(records)
    }

    /// Appends the registration of `member` to the partition of `role`.
    ///
    /// The append travels on the plain producer and sits outside a
    /// transaction, because a candidate holds no epoch. The offset the broker
    /// gives it is the join sequence of the member.
    ///
    /// # Errors
    ///
    /// Returns [`CoordinationError::Producer`] when the append fails, and
    /// [`CoordinationError::Client`] when a connection fails. This method never
    /// reports [`CoordinationError::Fenced`], because its producer holds no
    /// epoch for the role.
    #[tracing::instrument(level = "info", skip_all, fields(role = %role, member = %member), err)]
    async fn register(&self, role: &Role, member: &MemberId) -> Result<(), CoordinationError> {
        let partition = self.partition_of(role).await?;
        let producer = self.registrar().await?;
        let record = registration_record(role, member, partition, current_millis());
        send_one(producer, record).await?;
        Ok(())
    }

    /// Writes the lease of `role` in a transaction under `token`.
    ///
    /// The write travels on the producer that
    /// [`CoordinationTransport::acquire_epoch`] bound to `token`. The broker
    /// checks the epoch of that producer, so a superseded holder cannot append
    /// a lease. That check is how a deposed holder learns that it lost the
    /// role.
    ///
    /// A failed send aborts the transaction, so the partial write stays
    /// invisible to a committed read.
    ///
    /// # Errors
    ///
    /// Returns [`CoordinationError::NotHeld`] when this process holds no epoch
    /// for `role`, and when `token` is not the epoch it holds. Returns
    /// [`CoordinationError::Fenced`] when the broker rejects the epoch, and
    /// [`CoordinationError::Producer`] for every other failure of the write.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(role = %role, token = %token, member = %lease.member),
        err,
    )]
    async fn write_lease(
        &self,
        role: &Role,
        token: FencingToken,
        lease: &Lease,
    ) -> Result<(), CoordinationError> {
        let partition = self.partition_of(role).await?;
        let bound = self
            .holder_of(role)
            .await
            .filter(|(_producer, held)| *held == token);
        let Some((producer, _held)) = bound else {
            return Err(CoordinationError::NotHeld {
                role: role.clone(),
                member: lease.member.clone(),
            });
        };

        let transaction = producer
            .begin_transaction()
            .await
            .map_err(|error| fenced_or(role, error))?;
        if let Err(error) = send_one(&producer, lease_record(role, lease, partition)).await {
            if let Err(failed) = transaction.abort().await {
                tracing::warn!(
                    role = %role,
                    error = %failed.source,
                    "the abort of a failed lease write did not complete",
                );
            }
            return Err(fenced_or(role, error));
        }
        transaction
            .commit()
            .await
            .map_err(|failed| fenced_or(role, failed.source))
    }

    /// Asks the transaction coordinator which epoch holds `role` now.
    ///
    /// The call sends `DescribeTransactions` (KIP-664). It joins no group and
    /// it takes no epoch, so any process makes it. The result is `None` when
    /// the coordinator answers `TRANSACTIONAL_ID_NOT_FOUND`, which says that no
    /// member has ever held `role`.
    ///
    /// # Errors
    ///
    /// Returns [`CoordinationError::Admin`] when the coordinator lookup fails,
    /// or when the coordinator reports a fault other than an unknown role.
    #[tracing::instrument(level = "debug", skip_all, fields(role = %role), err)]
    async fn describe(&self, role: &Role) -> Result<Option<FencingToken>, CoordinationError> {
        let admin = self.admin.lock().await;
        match admin.describe_transaction(role.as_str()).await {
            Ok(description) => Ok(token_from_description(&description)),
            Err(error) if reports_unknown_transactional_id(&error) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}

/// Splits a bootstrap list on commas and drops the empty entries.
fn bootstrap_addresses(bootstrap: &str) -> Vec<String> {
    bootstrap
        .split(',')
        .map(str::trim)
        .filter(|address| !address.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

/// Rejects a topic shape that Kafka does not accept.
fn check_topic_shape(partitions: i32, replication: i32) -> Result<(), CoordinationError> {
    if partitions <= 0 {
        return Err(CoordinationError::InvalidConfig(format!(
            "the coordination topic needs at least one partition, got {partitions}"
        )));
    }
    if replication <= 0 {
        return Err(CoordinationError::InvalidConfig(format!(
            "the coordination topic needs at least one replica, got {replication}"
        )));
    }
    Ok(())
}

/// Resolves every bootstrap address inside the DNS deadline.
async fn resolve_addresses(
    addresses: &[String],
    dns_timeout: ClientDnsTimeout,
) -> Result<Vec<SocketAddr>, CoordinationError> {
    let mut resolved = Vec::new();
    for address in addresses {
        let lookup = tokio::time::timeout(
            dns_timeout.time().to_std(),
            tokio::net::lookup_host(address),
        )
        .await
        .map_err(|_elapsed| {
            CoordinationError::InvalidConfig(format!("the DNS lookup of {address} timed out"))
        })?
        .map_err(|error| {
            CoordinationError::InvalidConfig(format!("the DNS lookup of {address} failed: {error}"))
        })?;
        resolved.extend(lookup);
    }
    if resolved.is_empty() {
        return Err(CoordinationError::InvalidConfig(format!(
            "no address of {addresses:?} resolved"
        )));
    }
    Ok(resolved)
}

/// The partition that holds every record of `role`.
///
/// The rule is Kafka's own key partitioning, which
/// `crabka_client_producer::partition_for_key` implements: `murmur2` of the
/// role name in UTF-8, masked with `Utils.toPositive`, then the remainder of
/// `partitions`. `krabka-streams-java` and `krabka-streams-go` write the same
/// topic, so they implement the same rule. A change here needs the same change
/// in the two ports.
///
/// Both producers of a role call this and pin the partition on the record. The
/// key of a registration and the key of a lease differ, so a partitioner that
/// hashes the key would split one role across two partitions and destroy the
/// total order the succession rules rank on.
///
/// # Errors
///
/// Returns [`CoordinationError::InvalidConfig`] when `partitions` is not
/// positive.
pub fn role_partition(role: &Role, partitions: i32) -> Result<i32, CoordinationError> {
    if partitions <= 0 {
        return Err(CoordinationError::InvalidConfig(format!(
            "topic {COORDINATION_STATE_TOPIC} reports {partitions} partitions"
        )));
    }
    Ok(partition_for_key(role.as_str().as_bytes(), partitions))
}

/// The spec that creates the coordination topic.
///
/// The topic is compacted, so it keeps the last record of every role and every
/// member and drops the rest.
fn coordination_topic_spec(partitions: i32, replication: i32) -> CreateTopicSpec {
    CreateTopicSpec {
        name: COORDINATION_STATE_TOPIC.to_owned(),
        partitions,
        replicas: replication,
        configs: BTreeMap::from([("cleanup.policy".to_owned(), "compact".to_owned())]),
    }
}

/// Takes `TOPIC_ALREADY_EXISTS` for success and reports every other code.
///
/// Every member of a cluster creates the topic on first use, so all but the
/// first get code 36.
///
/// # Errors
///
/// Returns [`CoordinationError::Admin`] with the code the broker reported.
fn accept_topic_creation(outcomes: &[CreateTopicOutcome]) -> Result<(), CoordinationError> {
    for outcome in outcomes {
        match &outcome.error {
            Some(error) if error.code != TOPIC_ALREADY_EXISTS => {
                return Err(CoordinationError::Admin(AdminError::Broker {
                    api: "CreateTopics",
                    code: error.code,
                    name: error.name,
                    message: error.message.clone(),
                }));
            }
            _ => {}
        }
    }
    Ok(())
}

/// The `Metadata` request that names the coordination topic.
///
/// It sets `allow_auto_topic_creation` to `false`, because
/// [`accept_topic_creation`] already created the topic with the configuration
/// the design needs.
fn coordination_metadata_request() -> MetadataRequest {
    MetadataRequest {
        topics: Some(vec![MetadataRequestTopic {
            name: Some(COORDINATION_STATE_TOPIC.to_owned()),
            ..Default::default()
        }]),
        allow_auto_topic_creation: false,
        ..Default::default()
    }
}

/// Reads the partition count and the topic id out of a `Metadata` response.
fn topic_layout(response: &MetadataResponse) -> Result<TopicLayout, CoordinationError> {
    let topic = coordination_topic(response).ok_or_else(|| server(UNKNOWN_TOPIC_OR_PARTITION))?;
    if topic.error_code != 0 {
        return Err(server(topic.error_code));
    }
    let partitions = i32::try_from(topic.partitions.len()).map_err(|_error| {
        CoordinationError::InvalidConfig(format!(
            "topic {COORDINATION_STATE_TOPIC} reports {} partitions",
            topic.partitions.len()
        ))
    })?;
    if partitions == 0 {
        return Err(server(UNKNOWN_TOPIC_OR_PARTITION));
    }
    Ok(TopicLayout {
        partitions,
        topic_id: topic.topic_id,
    })
}

/// Reads the leader of `partition` out of a `Metadata` response.
fn partition_leader(response: &MetadataResponse, partition: i32) -> Result<i32, CoordinationError> {
    let topic = coordination_topic(response).ok_or_else(|| server(UNKNOWN_TOPIC_OR_PARTITION))?;
    let entry = topic
        .partitions
        .iter()
        .find(|entry| entry.partition_index == partition)
        .ok_or_else(|| server(UNKNOWN_TOPIC_OR_PARTITION))?;
    if entry.error_code != 0 {
        return Err(server(entry.error_code));
    }
    if entry.leader_id < 0 {
        return Err(server(LEADER_NOT_AVAILABLE));
    }
    Ok(entry.leader_id)
}

/// Picks the coordination topic out of a `Metadata` response.
fn coordination_topic(
    response: &MetadataResponse,
) -> Option<&crabka_protocol::owned::metadata_response::MetadataResponseTopic> {
    response
        .topics
        .iter()
        .find(|topic| topic.name.as_deref() == Some(COORDINATION_STATE_TOPIC))
}

/// The broker list of a `Metadata` response, in the shape the pool takes.
fn brokers_of(response: &MetadataResponse) -> Vec<BrokerInfo> {
    response
        .brokers
        .iter()
        .map(|broker| BrokerInfo {
            id: broker.node_id,
            host: broker.host.clone(),
            port: broker.port,
            rack: broker.rack.clone(),
        })
        .collect()
}

/// The `ListOffsets` request for one boundary of one partition.
fn list_offsets_request(partition: i32, timestamp: i64) -> ListOffsetsRequest {
    ListOffsetsRequest {
        replica_id: -1,
        isolation_level: READ_COMMITTED,
        topics: vec![ListOffsetsTopic {
            name: COORDINATION_STATE_TOPIC.to_owned(),
            partitions: vec![ListOffsetsPartition {
                partition_index: partition,
                current_leader_epoch: -1,
                timestamp,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Reads the offset of `partition` out of a `ListOffsets` response.
fn list_offsets_offset(
    response: &ListOffsetsResponse,
    partition: i32,
) -> Result<i64, CoordinationError> {
    let entry = response
        .topics
        .iter()
        .filter(|topic| topic.name == COORDINATION_STATE_TOPIC)
        .flat_map(|topic| topic.partitions.iter())
        .find(|entry| entry.partition_index == partition)
        .ok_or_else(|| server(UNKNOWN_TOPIC_OR_PARTITION))?;
    if entry.error_code != 0 {
        return Err(server(entry.error_code));
    }
    Ok(entry.offset)
}

/// The offset the next fetch of a scan starts at, or `None` when the scan is
/// finished.
///
/// The scan stops when the broker reported no batch, when the cursor did not
/// move, and when the cursor reached the end the scan started with. The first
/// two guard the loop against a fetch that makes no progress.
fn next_scan_offset(current: i64, end: i64, next_offset: Option<i64>) -> Option<i64> {
    let next = next_offset?;
    if next <= current || next >= end {
        return None;
    }
    Some(next)
}

/// Decodes the records of `role` out of one fetch and drops the rest.
///
/// A key that does not decode belongs to a writer this crate does not know, and
/// a key of another role belongs to a role that shares the partition. The
/// function drops both. A value that does not decode under a key of `role` is a
/// fault in the state of this role, and the function reports it.
///
/// # Errors
///
/// Returns [`CoordinationError::Record`] when the value of a record of `role`
/// does not decode.
fn select_role_records(
    role: &Role,
    records: &[FetchedRecord],
) -> Result<RoleRecords, CoordinationError> {
    let mut selected = RoleRecords::new();
    for record in records {
        let Some(bytes) = record.key.as_deref() else {
            continue;
        };
        let Ok(key) = decode_key(bytes) else {
            continue;
        };
        if key.role != *role {
            continue;
        }
        let value = decode_value(key.kind, record.value.as_deref())?;
        selected.push((record.offset, key, value));
    }
    Ok(selected)
}

/// The registration record of `member`, pinned to the partition of `role`.
fn registration_record(
    role: &Role,
    member: &MemberId,
    partition: i32,
    registered_at: i64,
) -> ProducerRecord {
    coordination_record(
        &CoordinationKey::registration(role.clone(), member.clone()),
        &CoordinationRecord::Registration(Registration {
            member: member.clone(),
            registered_at,
        }),
        partition,
    )
}

/// The lease record of `role`, pinned to the partition of `role`.
fn lease_record(role: &Role, lease: &Lease, partition: i32) -> ProducerRecord {
    coordination_record(
        &CoordinationKey::lease(role.clone()),
        &CoordinationRecord::Lease(lease.clone()),
        partition,
    )
}

/// Encodes one record of the coordination topic and pins its partition.
fn coordination_record(
    key: &CoordinationKey,
    value: &CoordinationRecord,
    partition: i32,
) -> ProducerRecord {
    ProducerRecord {
        topic: COORDINATION_STATE_TOPIC.to_owned(),
        partition: Some(partition),
        key: Some(encode_key(key)),
        value: encode_value(value),
        headers: Vec::new(),
        timestamp_ms: None,
    }
}

/// Sends one record and waits for the acknowledgement of the broker.
async fn send_one(
    producer: &Producer,
    record: ProducerRecord,
) -> Result<RecordMetadata, ProducerError> {
    producer
        .send(record)
        .await
        .await
        .unwrap_or(Err(ProducerError::Closed))
}

/// Reports whether the broker fenced the writer.
///
/// The producer reports a fence as `FencedProducer`, and it passes the raw
/// broker codes through for the paths it does not translate. Kafka answers 47
/// `INVALID_PRODUCER_EPOCH` on a produce and on `EndTxn`, and 90
/// `PRODUCER_FENCED` when a later holder superseded the producer.
fn is_fenced(error: &ProducerError) -> bool {
    match error {
        ProducerError::FencedProducer => true,
        ProducerError::Server(code) => {
            matches!(*code, INVALID_PRODUCER_EPOCH | PRODUCER_FENCED)
        }
        ProducerError::Client(ClientError::Server { error_code }) => {
            matches!(*error_code, INVALID_PRODUCER_EPOCH | PRODUCER_FENCED)
        }
        _ => false,
    }
}

/// Maps a fence onto the loss of `role`, and passes every other fault through.
///
/// This is the mechanism by which a deposed leader learns that it lost the
/// role, so every guarded write goes through it.
fn fenced_or(role: &Role, error: ProducerError) -> CoordinationError {
    if is_fenced(&error) {
        return CoordinationError::Fenced { role: role.clone() };
    }
    CoordinationError::Producer(error)
}

/// The token that a `DescribeTransactions` row reports.
///
/// The result is `None` when the coordinator reports the `-1` pair, which says
/// that it holds no producer for the transactional id.
fn token_from_description(description: &TransactionDescription) -> Option<FencingToken> {
    FencingToken::new(description.producer_id, description.producer_epoch).ok()
}

/// Reports whether the coordinator answered that it never saw the role.
fn reports_unknown_transactional_id(error: &AdminError) -> bool {
    matches!(
        error,
        AdminError::Broker {
            code: TRANSACTIONAL_ID_NOT_FOUND,
            ..
        }
    )
}

/// A broker error code, in the shape the coordination error carries it.
fn server(error_code: i16) -> CoordinationError {
    CoordinationError::Client(ClientError::Server { error_code })
}

/// The current wall clock, in milliseconds since the Unix epoch.
///
/// A clock before the Unix epoch reports `0`. The lease deadline is an
/// anti-flap device, so a wrong instant moves the failover time and does not
/// affect safety.
fn current_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
        })
}

/// Closes the producer of a role, and reports a producer a handle still shares.
async fn close_producer(role: &Role, producer: Arc<Producer>) {
    let Some(producer) = Arc::into_inner(producer) else {
        tracing::warn!(
            role = %role,
            "a leadership handle still shares the producer of this role, so it stays open",
        );
        return;
    };
    if let Err(error) = producer.close().await {
        tracing::warn!(role = %role, %error, "the producer of a role did not close cleanly");
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use crabka_client_admin::KafkaError;
    use crabka_protocol::owned::{
        list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse},
        metadata_response::{
            MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
        },
    };
    use crabka_units::secs;

    use super::*;
    use crate::record::RecordKind;

    const GRANTED_AT: i64 = 1_700_000_000_000;
    const DEADLINE: i64 = 1_700_000_030_000;

    fn role(name: &str) -> Role {
        Role::new(name).expect("a valid role")
    }

    fn member(name: &str) -> MemberId {
        MemberId::new(name).expect("a valid member id")
    }

    fn token(producer_id: i64, producer_epoch: i16) -> FencingToken {
        FencingToken::new(producer_id, producer_epoch).expect("a valid token")
    }

    fn lease(member_name: &str) -> Lease {
        Lease {
            member: member(member_name),
            token: token(4242, 7),
            granted_at: GRANTED_AT,
            deadline: DEADLINE,
        }
    }

    fn fetched(offset: i64, key: Option<&[u8]>, value: Option<&[u8]>) -> FetchedRecord {
        FetchedRecord {
            offset,
            key: key.map(bytes::Bytes::copy_from_slice),
            value: value.map(bytes::Bytes::copy_from_slice),
            timestamp: GRANTED_AT,
            headers: Vec::new(),
        }
    }

    fn registration_bytes(role_name: &str, member_name: &str) -> (Vec<u8>, Vec<u8>) {
        let key = CoordinationKey::registration(role(role_name), member(member_name));
        let value = CoordinationRecord::Registration(Registration {
            member: member(member_name),
            registered_at: GRANTED_AT,
        });
        (
            encode_key(&key).to_vec(),
            encode_value(&value)
                .expect("a registration is not a tombstone")
                .to_vec(),
        )
    }

    fn metadata_topic(partitions: Vec<MetadataResponsePartition>) -> MetadataResponseTopic {
        MetadataResponseTopic {
            name: Some(COORDINATION_STATE_TOPIC.to_owned()),
            topic_id: WireUuid([9; 16]),
            partitions,
            ..Default::default()
        }
    }

    fn metadata_partition(index: i32, leader: i32) -> MetadataResponsePartition {
        MetadataResponsePartition {
            partition_index: index,
            leader_id: leader,
            ..Default::default()
        }
    }

    fn metadata(topics: Vec<MetadataResponseTopic>) -> MetadataResponse {
        MetadataResponse {
            topics,
            ..Default::default()
        }
    }

    fn offsets_response(partition: i32, error_code: i16, offset: i64) -> ListOffsetsResponse {
        ListOffsetsResponse {
            topics: vec![ListOffsetsTopicResponse {
                name: COORDINATION_STATE_TOPIC.to_owned(),
                partitions: vec![ListOffsetsPartitionResponse {
                    partition_index: partition,
                    error_code,
                    offset,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn broker_code(error: &CoordinationError) -> Option<i16> {
        match error {
            CoordinationError::Client(ClientError::Server { error_code }) => Some(*error_code),
            CoordinationError::Admin(AdminError::Broker { code, .. }) => Some(*code),
            _ => None,
        }
    }

    /// The partition rule is frozen, because three implementations write the
    /// same topic. A change to these values is a change to the wire contract.
    /// Each value is `toPositive(murmur2(role)) % partitions`, which is what
    /// Kafka's own key partitioning computes.
    #[test]
    fn a_role_maps_onto_a_frozen_partition() {
        let cases = [
            ("controller", 16, 12),
            ("dispatcher", 16, 10),
            ("role-a", 16, 10),
            ("role-b", 16, 12),
            ("controller", 1, 0),
        ];
        for (name, partitions, want) in cases {
            check!(
                role_partition(&role(name), partitions).unwrap() == want,
                "role {name} over {partitions} partitions"
            );
        }
    }

    /// Both producers of a role call the same rule, so the registration and
    /// the lease of one role land in one partition even though their keys
    /// differ.
    #[test]
    fn both_record_kinds_of_a_role_take_the_same_partition() {
        let name = role("controller");
        let partition = role_partition(&name, 16).unwrap();
        let registration = registration_record(&name, &member("node-1"), partition, GRANTED_AT);
        let lease = lease_record(&name, &lease("node-1"), partition);
        check!(registration.partition == lease.partition);
        check!(registration.partition == Some(partition));
        check!(registration.key != lease.key);
    }

    #[test]
    fn a_role_partition_stays_inside_the_partition_count() {
        for count in [1_i32, 2, 3, 7, 16, 50] {
            for name in ["a", "controller", "a-very-long-role-name-0123456789"] {
                let partition = role_partition(&role(name), count).unwrap();
                check!(partition >= 0, "role {name} over {count} partitions");
                check!(partition < count, "role {name} over {count} partitions");
            }
        }
    }

    #[test]
    fn a_role_partition_rejects_a_topic_with_no_partition() {
        for count in [0_i32, -1, i32::MIN] {
            assert!(let Err(CoordinationError::InvalidConfig(_)) =
                role_partition(&role("controller"), count));
        }
    }

    #[test]
    fn a_bootstrap_list_splits_on_commas_and_drops_the_empty_entries() {
        let cases: [(&str, Vec<String>); 5] = [
            ("localhost:9092", vec!["localhost:9092".to_owned()]),
            (
                "a:1, b:2 ,c:3",
                vec!["a:1".to_owned(), "b:2".to_owned(), "c:3".to_owned()],
            ),
            (",a:1,,", vec!["a:1".to_owned()]),
            ("", Vec::new()),
            ("  ,  ", Vec::new()),
        ];
        for (input, want) in cases {
            check!(bootstrap_addresses(input) == want, "bootstrap {input:?}");
        }
    }

    #[test]
    fn a_topic_shape_needs_a_partition_and_a_replica() {
        check!(check_topic_shape(16, 3).is_ok());
        check!(check_topic_shape(1, 1).is_ok());
        assert!(let Err(CoordinationError::InvalidConfig(_)) = check_topic_shape(0, 3));
        assert!(let Err(CoordinationError::InvalidConfig(_)) = check_topic_shape(16, 0));
        assert!(let Err(CoordinationError::InvalidConfig(_)) = check_topic_shape(-1, -1));
    }

    /// `CreateTopicSpec` carries no `PartialEq`, so this test compares the
    /// fields.
    #[test]
    fn the_topic_spec_asks_for_a_compacted_topic() {
        let spec = coordination_topic_spec(16, 3);
        check!(spec.name == COORDINATION_STATE_TOPIC);
        check!(spec.partitions == 16);
        check!(spec.replicas == 3);
        check!(
            spec.configs == BTreeMap::from([("cleanup.policy".to_owned(), "compact".to_owned())])
        );
    }

    /// Every member creates the topic on first use, so all but the first get
    /// `TOPIC_ALREADY_EXISTS`.
    #[test]
    fn topic_creation_takes_an_existing_topic_for_success() {
        let outcome = |code: Option<i16>| CreateTopicOutcome {
            name: COORDINATION_STATE_TOPIC.to_owned(),
            topic_id: None,
            error: code.map(|code| KafkaError {
                code,
                name: "TEST",
                message: None,
            }),
        };
        check!(accept_topic_creation(&[]).is_ok());
        check!(accept_topic_creation(&[outcome(None)]).is_ok());
        check!(accept_topic_creation(&[outcome(Some(TOPIC_ALREADY_EXISTS))]).is_ok());

        let refused = accept_topic_creation(&[outcome(Some(38))])
            .expect_err("an invalid replication factor is a fault");
        check!(broker_code(&refused) == Some(38));
    }

    #[test]
    fn the_metadata_request_names_the_topic_and_never_creates_it() {
        check!(
            coordination_metadata_request()
                == MetadataRequest {
                    topics: Some(vec![MetadataRequestTopic {
                        name: Some(COORDINATION_STATE_TOPIC.to_owned()),
                        ..Default::default()
                    }]),
                    allow_auto_topic_creation: false,
                    ..Default::default()
                }
        );
    }

    #[test]
    fn the_topic_layout_reads_the_partition_count_and_the_topic_id() {
        let response = metadata(vec![metadata_topic(vec![
            metadata_partition(0, 1),
            metadata_partition(1, 2),
        ])]);
        check!(
            topic_layout(&response).unwrap()
                == TopicLayout {
                    partitions: 2,
                    topic_id: WireUuid([9; 16]),
                }
        );
    }

    #[test]
    fn the_topic_layout_reports_a_topic_the_cluster_does_not_have() {
        let absent = metadata(Vec::new());
        check!(
            broker_code(&topic_layout(&absent).unwrap_err()) == Some(UNKNOWN_TOPIC_OR_PARTITION)
        );

        let empty = metadata(vec![metadata_topic(Vec::new())]);
        check!(broker_code(&topic_layout(&empty).unwrap_err()) == Some(UNKNOWN_TOPIC_OR_PARTITION));

        let refused = metadata(vec![MetadataResponseTopic {
            error_code: 29,
            ..metadata_topic(vec![metadata_partition(0, 1)])
        }]);
        check!(broker_code(&topic_layout(&refused).unwrap_err()) == Some(29));
    }

    #[test]
    fn the_partition_leader_comes_from_the_metadata_row_of_that_partition() {
        let response = metadata(vec![metadata_topic(vec![
            metadata_partition(0, 11),
            metadata_partition(1, 22),
        ])]);
        check!(partition_leader(&response, 0).unwrap() == 11);
        check!(partition_leader(&response, 1).unwrap() == 22);
        check!(
            broker_code(&partition_leader(&response, 9).unwrap_err())
                == Some(UNKNOWN_TOPIC_OR_PARTITION)
        );
    }

    #[test]
    fn a_partition_with_no_leader_is_a_broker_error() {
        let without_leader = metadata(vec![metadata_topic(vec![metadata_partition(0, -1)])]);
        check!(
            broker_code(&partition_leader(&without_leader, 0).unwrap_err())
                == Some(LEADER_NOT_AVAILABLE)
        );

        let refused = metadata(vec![metadata_topic(vec![MetadataResponsePartition {
            error_code: 9,
            ..metadata_partition(0, 1)
        }])]);
        check!(broker_code(&partition_leader(&refused, 0).unwrap_err()) == Some(9));
    }

    #[test]
    fn the_broker_list_carries_every_field_the_pool_needs() {
        let response = MetadataResponse {
            brokers: vec![MetadataResponseBroker {
                node_id: 3,
                host: "broker-3".to_owned(),
                port: 9092,
                rack: Some("rack-a".to_owned()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let brokers = brokers_of(&response);
        check!(brokers.len() == 1);
        check!(brokers[0].id == 3);
        check!(brokers[0].host == "broker-3");
        check!(brokers[0].port == 9092);
        check!(brokers[0].rack.as_deref() == Some("rack-a"));
    }

    #[test]
    fn the_offset_request_asks_one_partition_at_committed_isolation() {
        check!(
            list_offsets_request(4, LIST_OFFSETS_LATEST)
                == ListOffsetsRequest {
                    replica_id: -1,
                    isolation_level: READ_COMMITTED,
                    topics: vec![ListOffsetsTopic {
                        name: COORDINATION_STATE_TOPIC.to_owned(),
                        partitions: vec![ListOffsetsPartition {
                            partition_index: 4,
                            current_leader_epoch: -1,
                            timestamp: LIST_OFFSETS_LATEST,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }
        );
    }

    #[test]
    fn the_offset_response_gives_the_offset_of_the_asked_partition() {
        check!(list_offsets_offset(&offsets_response(4, 0, 91), 4).unwrap() == 91);
        check!(
            broker_code(&list_offsets_offset(&offsets_response(4, 0, 91), 5).unwrap_err())
                == Some(UNKNOWN_TOPIC_OR_PARTITION)
        );
        check!(
            broker_code(&list_offsets_offset(&offsets_response(4, 6, -1), 4).unwrap_err())
                == Some(6)
        );
    }

    /// The scan reads until the cursor reaches the end that the cluster
    /// reported. It also stops on a fetch that makes no progress, so a broker
    /// that returns nothing cannot spin the loop.
    #[test]
    fn the_scan_advances_until_it_reaches_the_end_it_started_with() {
        let cases = [
            ("a batch inside the range", 0, 10, Some(4), Some(4)),
            ("a batch that reaches the end", 0, 10, Some(10), None),
            ("a batch past the end", 0, 10, Some(11), None),
            ("no batch in the response", 0, 10, None, None),
            ("a cursor that did not move", 4, 10, Some(4), None),
            ("a cursor that went backwards", 4, 10, Some(2), None),
        ];
        for (name, current, end, next_offset, want) in cases {
            check!(
                next_scan_offset(current, end, next_offset) == want,
                "{name}"
            );
        }
    }

    #[test]
    fn the_scan_keeps_the_records_of_the_role_in_offset_order() {
        let (mine_key, mine_value) = registration_bytes("controller", "node-1");
        let (other_key, other_value) = registration_bytes("dispatcher", "node-9");
        let records = [
            fetched(4, Some(&other_key), Some(&other_value)),
            fetched(5, Some(&mine_key), Some(&mine_value)),
            fetched(6, Some(b"not a coordination key"), Some(&mine_value)),
            fetched(7, None, Some(&mine_value)),
            fetched(8, Some(&mine_key), None),
        ];

        let selected = select_role_records(&role("controller"), &records).unwrap();
        check!(
            selected
                == vec![
                    (
                        5,
                        CoordinationKey::registration(role("controller"), member("node-1")),
                        CoordinationRecord::Registration(Registration {
                            member: member("node-1"),
                            registered_at: GRANTED_AT,
                        }),
                    ),
                    (
                        8,
                        CoordinationKey::registration(role("controller"), member("node-1")),
                        CoordinationRecord::Tombstone,
                    ),
                ]
        );
    }

    #[test]
    fn a_malformed_value_under_a_key_of_the_role_is_a_fault() {
        let (key, _) = registration_bytes("controller", "node-1");
        let records = [fetched(1, Some(&key), Some(b"truncated"))];
        assert!(let Err(CoordinationError::Record(_)) =
            select_role_records(&role("controller"), &records));
    }

    #[test]
    fn a_registration_record_carries_the_frozen_key_and_value() {
        let name = role("controller");
        let who = member("node-1");
        let key = CoordinationKey::registration(name.clone(), who.clone());
        let value = CoordinationRecord::Registration(Registration {
            member: who.clone(),
            registered_at: GRANTED_AT,
        });
        check!(
            registration_record(&name, &who, 3, GRANTED_AT)
                == ProducerRecord {
                    topic: COORDINATION_STATE_TOPIC.to_owned(),
                    partition: Some(3),
                    key: Some(encode_key(&key)),
                    value: encode_value(&value),
                    headers: Vec::new(),
                    timestamp_ms: None,
                }
        );
    }

    #[test]
    fn a_lease_record_carries_the_frozen_key_and_value() {
        let name = role("controller");
        let held = lease("node-1");
        let key = CoordinationKey::lease(name.clone());
        let value = CoordinationRecord::Lease(held.clone());
        check!(
            lease_record(&name, &held, 3)
                == ProducerRecord {
                    topic: COORDINATION_STATE_TOPIC.to_owned(),
                    partition: Some(3),
                    key: Some(encode_key(&key)),
                    value: encode_value(&value),
                    headers: Vec::new(),
                    timestamp_ms: None,
                }
        );
        let decoded = decode_key(&encode_key(&key)).unwrap();
        check!(decoded.kind == RecordKind::Lease);
        check!(decoded.member.is_none());
    }

    /// The fence mapping is how a deposed leader learns that it lost the role,
    /// so every shape the producer reports a fence in maps onto
    /// [`CoordinationError::Fenced`].
    #[test]
    fn a_fence_from_the_producer_becomes_the_loss_of_the_role() {
        let fences = [
            ProducerError::FencedProducer,
            ProducerError::Server(INVALID_PRODUCER_EPOCH),
            ProducerError::Server(PRODUCER_FENCED),
            ProducerError::Client(ClientError::Server {
                error_code: INVALID_PRODUCER_EPOCH,
            }),
            ProducerError::Client(ClientError::Server {
                error_code: PRODUCER_FENCED,
            }),
        ];
        for error in fences {
            let reported = error.to_string();
            check!(is_fenced(&error), "producer error {reported}");
            let mapped = fenced_or(&role("controller"), error);
            check!(mapped.is_fenced(), "producer error {reported}");
            assert!(let CoordinationError::Fenced { .. } = &mapped);
            check!(mapped.to_string() == "fenced: another member holds role controller");
        }
    }

    #[test]
    fn a_fault_that_is_not_a_fence_passes_through() {
        let faults = [
            ProducerError::Closed,
            ProducerError::FlushTimeout,
            ProducerError::Server(1),
            ProducerError::Client(ClientError::Disconnected),
            ProducerError::Client(ClientError::Server { error_code: 46 }),
        ];
        for error in faults {
            let reported = error.to_string();
            check!(!is_fenced(&error), "producer error {reported}");
            let mapped = fenced_or(&role("controller"), error);
            check!(!mapped.is_fenced(), "producer error {reported}");
            assert!(let CoordinationError::Producer(_) = &mapped);
        }
    }

    #[test]
    fn a_described_transaction_becomes_a_fencing_token() {
        let describe = |producer_id, producer_epoch| TransactionDescription {
            transactional_id: "controller".to_owned(),
            state: "Empty".to_owned(),
            timeout: secs(30),
            start_time_ms: GRANTED_AT,
            producer_id,
            producer_epoch,
        };
        check!(token_from_description(&describe(4242, 7)) == Some(token(4242, 7)));
        check!(token_from_description(&describe(0, 0)) == Some(token(0, 0)));
        check!(token_from_description(&describe(-1, -1)).is_none());
        check!(token_from_description(&describe(4242, -1)).is_none());
    }

    /// A role that no member has ever held is not a fault. The succession
    /// rules read `None` and let the first candidate take it.
    #[test]
    fn an_unknown_transactional_id_is_not_a_fault() {
        let broker = |code| AdminError::Broker {
            api: "DescribeTransactions",
            code,
            name: "TEST",
            message: None,
        };
        check!(reports_unknown_transactional_id(&broker(
            TRANSACTIONAL_ID_NOT_FOUND
        )));
        check!(!reports_unknown_transactional_id(&broker(16)));
        check!(!reports_unknown_transactional_id(&AdminError::Protocol(
            "no row".to_owned()
        )));
    }

    #[test]
    fn the_clock_reports_an_instant_after_2020() {
        check!(current_millis() > 1_577_836_800_000);
    }

    /// The builder checks its configuration before it opens a connection, so
    /// these cases need no cluster.
    #[tokio::test]
    async fn the_builder_refuses_a_configuration_kafka_would_reject() {
        assert!(let Err(CoordinationError::InvalidConfig(_)) = BrokerTransport::builder()
            .bootstrap("")
            .build()
            .await);
        assert!(let Err(CoordinationError::InvalidConfig(_)) = BrokerTransport::builder()
            .bootstrap("localhost:9092")
            .topic_partitions(0)
            .build()
            .await);
        assert!(let Err(CoordinationError::InvalidConfig(_)) = BrokerTransport::builder()
            .bootstrap("localhost:9092")
            .topic_replication(0)
            .build()
            .await);
    }
}
