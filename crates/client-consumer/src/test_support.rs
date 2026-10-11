use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicI32, AtomicU8},
    },
    time::Duration,
};

use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;

use krabka_client_core::Client;
use krabka_protocol::primitives::uuid::Uuid as WireUuid;
use krabka_units::{ByteSize, Time, millis, minutes, secs};

use crate::{
    AutoOffsetReset, ConsumerRetryPolicy, IsolationLevel,
    assignor::{Assignor, RebalanceProtocol},
    consumer::{CommitIdentity, Consumer},
    coordinator::{CoordinatorRetryPolicy, CoordinatorState, PollErrorSlot, PollSignal, PollTimer},
};

pub(crate) struct ConsumerTestBuilder {
    pub client: Client,
    pub group_id: String,
    pub coordinator_id: Arc<AtomicI32>,
    pub retry_policy: CoordinatorRetryPolicy,
    pub member_id: tokio::sync::watch::Receiver<String>,
    pub commit_identity: Arc<Mutex<CommitIdentity>>,
    pub commit_serialization: Arc<Mutex<()>>,
    pub commit_async_state: Arc<AtomicU8>,
    pub commit_async_callbacks: crate::commit::OffsetCommitCallbacks,
    pub group_instance_id: Option<String>,
    pub current_generation: Arc<AtomicI32>,
    pub subscription: crate::subscription::SharedSubscription,
    pub group_protocol: crate::GroupProtocol,
    pub assigned: Arc<Mutex<Vec<(String, i32)>>>,
    pub assignment_changed: Arc<Notify>,
    pub next_offsets: Arc<Mutex<HashMap<(String, i32), i64>>>,
    pub end_offsets: Arc<Mutex<HashMap<(String, i32), i64>>>,
    pub positions: Arc<Mutex<HashMap<(String, i32), crate::position::PartitionPosition>>>,
    pub topic_ids: Arc<Mutex<HashMap<String, WireUuid>>>,
    pub session_timeout: Time,
    pub heartbeat_interval: Time,
    pub rebalance_protocol: crate::assignor::RebalanceProtocol,
    pub coordinator_shutdown: CancellationToken,
    pub coordinator_handle: Option<tokio::task::JoinHandle<()>>,
    pub isolation_level: IsolationLevel,
    pub fetch_min: ByteSize,
    pub fetch_max: ByteSize,
    pub fetch_partition_max: ByteSize,
    pub fetch_max_wait: Time,
    pub fetches: crate::poll::Fetches,
    pub client_rack: Option<String>,
    pub metadata_max_age: Time,
    pub request_timeout: Time,
    pub wakeup: crate::control::WakeupHandle,
    pub enforced_rebalances: tokio::sync::mpsc::UnboundedSender<(String, u64)>,
    pub default_api_timeout: Time,
    pub paused: crate::partition_state::PausedPartitions,
    pub auto_offset_reset: AutoOffsetReset,
    pub poll_error: crate::coordinator::PollErrorSlot,
    pub auto_commit: Option<crate::commit::AutoCommit>,
    pub poll_signal: crate::coordinator::PollSignal,
    pub rebalance_pending: tokio::sync::watch::Receiver<bool>,
    pub max_poll_records: usize,
    pub fetch_buffer: crate::fetch_buffer::FetchBuffer,
    pub close_operation: tokio::sync::watch::Sender<crate::control::CloseRequest>,
    pub rebalance_listener: Option<crate::rebalance_listener::SharedListener>,
    pub listener_calls:
        tokio::sync::mpsc::UnboundedReceiver<crate::rebalance_listener::ListenerCall>,
    pub assigned_callback_pending: crate::rebalance_listener::AssignedCallbackPending,
}

impl ConsumerTestBuilder {
    pub(crate) fn new(client: Client) -> Self {
        let member_id = "member-a".to_string();
        Self {
            client,
            group_id: "group-a".into(),
            coordinator_id: Arc::new(AtomicI32::new(0)),
            retry_policy: ConsumerRetryPolicy::default().into(),
            member_id: tokio::sync::watch::channel(member_id.clone()).1,
            commit_identity: Arc::new(Mutex::new(CommitIdentity {
                generation: 1,
                member_id,
                ownership_ids: HashMap::new(),
                rejoin_on_poll: false,
            })),
            commit_serialization: Arc::new(Mutex::new(())),
            commit_async_state: Arc::new(AtomicU8::new(0)),
            commit_async_callbacks: Arc::default(),
            group_instance_id: None,
            current_generation: Arc::new(AtomicI32::new(1)),
            subscription: crate::subscription::shared(vec!["topic".into()], None, true),
            group_protocol: crate::GroupProtocol::Classic,
            assigned: Arc::new(Mutex::new(Vec::new())),
            assignment_changed: Arc::new(Notify::new()),
            next_offsets: Arc::new(Mutex::new(HashMap::new())),
            end_offsets: Arc::new(Mutex::new(HashMap::new())),
            positions: Arc::new(Mutex::new(HashMap::new())),
            topic_ids: Arc::new(Mutex::new(HashMap::new())),
            session_timeout: secs(45),
            heartbeat_interval: secs(3),
            rebalance_protocol: crate::assignor::RebalanceProtocol::Eager,
            coordinator_shutdown: CancellationToken::new(),
            coordinator_handle: None,
            isolation_level: IsolationLevel::ReadUncommitted,
            fetch_min: krabka_client_core::DEFAULT_FETCH_MIN,
            fetch_max: crate::poll::DEFAULT_FETCH_MAX,
            fetch_partition_max: crate::poll::DEFAULT_FETCH_PARTITION_MAX,
            fetch_max_wait: crate::consumer::DEFAULT_CONSUMER_FETCH_MAX_WAIT,
            fetches: crate::poll::Fetches::default(),
            client_rack: None,
            metadata_max_age: crate::consumer::DEFAULT_CONSUMER_METADATA_MAX_AGE,
            request_timeout: secs(30),
            wakeup: crate::control::WakeupHandle::default(),
            enforced_rebalances: tokio::sync::mpsc::unbounded_channel().0,
            default_api_timeout: crate::consumer::DEFAULT_CONSUMER_DEFAULT_API_TIMEOUT,
            paused: std::sync::Mutex::default(),
            auto_offset_reset: AutoOffsetReset::Latest,
            poll_error: crate::coordinator::PollErrorSlot::default(),
            auto_commit: None,
            poll_signal: crate::coordinator::PollSignal::default(),
            rebalance_pending: tokio::sync::watch::channel(false).1,
            max_poll_records: crate::consumer::DEFAULT_CONSUMER_MAX_POLL_RECORDS,
            fetch_buffer: crate::fetch_buffer::FetchBuffer::default(),
            close_operation: tokio::sync::watch::Sender::new(
                crate::control::CloseRequest::default(),
            ),
            rebalance_listener: None,
            listener_calls: tokio::sync::mpsc::unbounded_channel().1,
            assigned_callback_pending: Arc::default(),
        }
    }

    pub(crate) fn coordinator_id(mut self, id: Arc<AtomicI32>) -> Self {
        self.coordinator_id = id;
        self
    }

    pub(crate) fn commit_identity(mut self, commit_identity: Arc<Mutex<CommitIdentity>>) -> Self {
        self.commit_identity = commit_identity;
        self
    }

    pub(crate) fn current_generation(mut self, generation: Arc<AtomicI32>) -> Self {
        self.current_generation = generation;
        self
    }

    pub(crate) fn generation_id(mut self, generation: i32) -> Self {
        self.current_generation = Arc::new(AtomicI32::new(generation));
        self
    }

    pub(crate) fn group_instance_id(mut self, id: Option<String>) -> Self {
        self.group_instance_id = id;
        self
    }

    pub(crate) fn subscription(mut self, sub: crate::subscription::SharedSubscription) -> Self {
        self.subscription = sub;
        self
    }

    pub(crate) fn assigned(mut self, assigned: Vec<(String, i32)>) -> Self {
        self.assigned = Arc::new(Mutex::new(assigned));
        self
    }

    pub(crate) fn assignment_changed(mut self, notify: Arc<Notify>) -> Self {
        self.assignment_changed = notify;
        self
    }

    pub(crate) fn next_offsets(mut self, offsets: HashMap<(String, i32), i64>) -> Self {
        self.next_offsets = Arc::new(Mutex::new(offsets));
        self
    }

    pub(crate) fn topic_ids(mut self, topic_ids: HashMap<String, WireUuid>) -> Self {
        self.topic_ids = Arc::new(Mutex::new(topic_ids));
        self
    }

    pub(crate) fn build(self) -> Consumer {
        Consumer {
            client: self.client,
            group_id: self.group_id,
            coordinator_id: self.coordinator_id,
            retry_policy: self.retry_policy,
            member_id: self.member_id,
            commit_identity: self.commit_identity,
            commit_serialization: self.commit_serialization,
            commit_async_state: self.commit_async_state,
            commit_async_callbacks: self.commit_async_callbacks,
            group_instance_id: self.group_instance_id,
            current_generation: self.current_generation,
            subscription: self.subscription,
            group_protocol: self.group_protocol,
            assigned: self.assigned,
            assignment_changed: self.assignment_changed,
            next_offsets: self.next_offsets,
            end_offsets: self.end_offsets,
            positions: self.positions,
            topic_ids: self.topic_ids,
            session_timeout: self.session_timeout,
            heartbeat_interval: self.heartbeat_interval,
            rebalance_protocol: self.rebalance_protocol,
            coordinator_shutdown: self.coordinator_shutdown,
            coordinator_handle: self.coordinator_handle,
            isolation_level: self.isolation_level,
            fetch_min: self.fetch_min,
            fetch_max: self.fetch_max,
            fetch_partition_max: self.fetch_partition_max,
            fetch_max_wait: self.fetch_max_wait,
            fetches: self.fetches,
            client_rack: self.client_rack,
            metadata_max_age: self.metadata_max_age,
            request_timeout: self.request_timeout,
            wakeup: self.wakeup,
            enforced_rebalances: self.enforced_rebalances,
            default_api_timeout: self.default_api_timeout,
            paused: self.paused,
            auto_offset_reset: self.auto_offset_reset,
            poll_error: self.poll_error,
            auto_commit: self.auto_commit,
            poll_signal: self.poll_signal,
            rebalance_pending: self.rebalance_pending,
            max_poll_records: self.max_poll_records,
            fetch_buffer: self.fetch_buffer,
            close_operation: self.close_operation,
            rebalance_listener: self.rebalance_listener,
            listener_calls: self.listener_calls,
            assigned_callback_pending: self.assigned_callback_pending,
        }
    }
}

pub(crate) struct CoordinatorStateTestBuilder {
    pub client: Client,
    pub group_id: String,
    pub coordinator_id: Arc<AtomicI32>,
    pub member_id: String,
    pub published_member_id: tokio::sync::watch::Sender<String>,
    pub commit_identity: Arc<Mutex<CommitIdentity>>,
    pub group_instance_id: Option<String>,
    pub generation_id: i32,
    pub current_generation: Arc<AtomicI32>,
    pub assignors: Vec<Assignor>,
    pub rebalance_protocol: RebalanceProtocol,
    pub subscription: crate::subscription::SharedSubscription,
    pub subscription_changes: tokio::sync::watch::Receiver<crate::subscription::Subscription>,
    pub joined_topics: Vec<String>,
    pub seen_unsubscribes: u64,
    pub group_protocol: crate::GroupProtocol,
    pub server_assignor: Option<String>,
    pub member_epoch: i32,
    pub sent_heartbeat_fields: crate::consumer_group::SentFields,
    pub assigned: Arc<Mutex<Vec<(String, i32)>>>,
    pub assignment_changed: Arc<Notify>,
    pub next_ownership_id: u64,
    pub next_offsets: Arc<Mutex<HashMap<(String, i32), i64>>>,
    pub end_offsets: Arc<Mutex<HashMap<(String, i32), i64>>>,
    pub positions: Arc<Mutex<HashMap<(String, i32), crate::position::PartitionPosition>>>,
    pub topic_ids: Arc<Mutex<HashMap<String, WireUuid>>>,
    pub session_timeout: Time,
    pub max_poll_interval: Time,
    pub heartbeat_interval: Time,
    pub subscription_metadata_refresh_interval: Time,
    pub leave_group_timeout: Time,
    pub auto_offset_reset: AutoOffsetReset,
    pub client_rack: Option<String>,
    pub initial_subscribed_counts: HashMap<String, i32>,
    pub retry_policy: CoordinatorRetryPolicy,
    pub poll_error: PollErrorSlot,
    pub auto_commit: Option<crate::commit::AutoCommit>,
    pub commit_serialization: Arc<Mutex<()>>,
    pub join_prepared: bool,
    pub polls: tokio::sync::watch::Receiver<u64>,
    pub rebalance_pending: tokio::sync::watch::Sender<bool>,
    pub close_operation: tokio::sync::watch::Receiver<crate::control::CloseRequest>,
    pub rejoin_reason: String,
    pub listener_calls: Option<crate::rebalance_listener::ListenerCalls>,
    pub enforced_rebalances: tokio::sync::mpsc::UnboundedReceiver<(String, u64)>,
    pub lost_partitions: Vec<(String, i32)>,
    pub assigned_callback_pending: crate::rebalance_listener::AssignedCallbackPending,
    pub poll_timer: PollTimer,
    pub poll_timeout_in_callback: bool,
}

impl CoordinatorStateTestBuilder {
    pub(crate) fn new(client: Client) -> Self {
        let member_id = "member-a".to_string();
        Self {
            client,
            group_id: "group-a".into(),
            coordinator_id: Arc::new(AtomicI32::new(0)),
            member_id: member_id.clone(),
            published_member_id: tokio::sync::watch::Sender::new(member_id.clone()),
            commit_identity: Arc::new(Mutex::new(CommitIdentity {
                generation: 1,
                member_id,
                ownership_ids: HashMap::new(),
                rejoin_on_poll: false,
            })),
            group_instance_id: None,
            generation_id: 1,
            current_generation: Arc::new(AtomicI32::new(1)),
            assignors: vec![Assignor::Range],
            rebalance_protocol: RebalanceProtocol::Eager,
            subscription: crate::subscription::shared(vec!["topic".into()], None, true),
            subscription_changes: crate::subscription::shared(Vec::new(), None, true).subscribe(),
            joined_topics: Vec::new(),
            seen_unsubscribes: 0,
            group_protocol: crate::GroupProtocol::Classic,
            server_assignor: None,
            member_epoch: 0,
            sent_heartbeat_fields: crate::consumer_group::SentFields::default(),
            assigned: Arc::new(Mutex::new(Vec::new())),
            assignment_changed: Arc::new(Notify::new()),
            next_ownership_id: 1,
            next_offsets: Arc::new(Mutex::new(HashMap::new())),
            end_offsets: Arc::new(Mutex::new(HashMap::new())),
            positions: Arc::new(Mutex::new(HashMap::new())),
            topic_ids: Arc::new(Mutex::new(HashMap::new())),
            session_timeout: secs(45),
            max_poll_interval: minutes(1),
            heartbeat_interval: secs(3),
            subscription_metadata_refresh_interval: millis(37),
            leave_group_timeout: millis(37),
            auto_offset_reset: AutoOffsetReset::Latest,
            client_rack: None,
            initial_subscribed_counts: HashMap::new(),
            retry_policy: CoordinatorRetryPolicy {
                timeout: Duration::from_secs(30),
                initial_backoff: Duration::from_millis(100),
                max_backoff: Duration::from_secs(1),
            },
            poll_error: PollErrorSlot::default(),
            auto_commit: None,
            commit_serialization: Arc::new(Mutex::new(())),
            join_prepared: false,
            polls: PollSignal::default().subscribe(),
            rebalance_pending: tokio::sync::watch::Sender::new(false),
            close_operation: tokio::sync::watch::channel(crate::control::CloseRequest::default()).1,
            rejoin_reason: String::new(),
            listener_calls: None,
            enforced_rebalances: tokio::sync::mpsc::unbounded_channel().1,
            lost_partitions: Vec::new(),
            assigned_callback_pending: Arc::default(),
            poll_timer: PollTimer::new(secs(300)),
            poll_timeout_in_callback: false,
        }
    }

    pub(crate) fn commit_identity(mut self, commit_identity: Arc<Mutex<CommitIdentity>>) -> Self {
        self.commit_identity = commit_identity;
        self
    }

    pub(crate) fn group_instance_id(mut self, id: Option<String>) -> Self {
        self.group_instance_id = id;
        self
    }

    pub(crate) fn subscription(mut self, sub: crate::subscription::SharedSubscription) -> Self {
        self.subscription_changes = sub.subscribe();
        self.subscription = sub;
        self
    }

    pub(crate) fn assigned(mut self, assigned: Vec<(String, i32)>) -> Self {
        self.assigned = Arc::new(Mutex::new(assigned));
        self
    }

    pub(crate) fn next_ownership_id(mut self, id: u64) -> Self {
        self.next_ownership_id = id;
        self
    }

    pub(crate) fn next_offsets(mut self, offsets: HashMap<(String, i32), i64>) -> Self {
        self.next_offsets = Arc::new(Mutex::new(offsets));
        self
    }

    pub(crate) fn retry_policy(mut self, policy: CoordinatorRetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    pub(crate) fn heartbeat_interval(mut self, interval: Time) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    pub(crate) fn leave_group_timeout(mut self, timeout: Time) -> Self {
        self.leave_group_timeout = timeout;
        self
    }

    pub(crate) fn subscription_metadata_refresh_interval(mut self, interval: Time) -> Self {
        self.subscription_metadata_refresh_interval = interval;
        self
    }

    pub(crate) fn polls(mut self, rx: tokio::sync::watch::Receiver<u64>) -> Self {
        self.polls = rx;
        self
    }

    pub(crate) fn build(self) -> CoordinatorState {
        CoordinatorState {
            client: self.client,
            group_id: self.group_id,
            coordinator_id: self.coordinator_id,
            member_id: self.member_id,
            published_member_id: self.published_member_id,
            commit_identity: self.commit_identity,
            group_instance_id: self.group_instance_id,
            generation_id: self.generation_id,
            current_generation: self.current_generation,
            assignors: self.assignors,
            rebalance_protocol: self.rebalance_protocol,
            subscription: self.subscription,
            subscription_changes: self.subscription_changes,
            joined_topics: self.joined_topics,
            seen_unsubscribes: self.seen_unsubscribes,
            group_protocol: self.group_protocol,
            server_assignor: self.server_assignor,
            member_epoch: self.member_epoch,
            sent_heartbeat_fields: self.sent_heartbeat_fields,
            assigned: self.assigned,
            assignment_changed: self.assignment_changed,
            next_ownership_id: self.next_ownership_id,
            next_offsets: self.next_offsets,
            end_offsets: self.end_offsets,
            positions: self.positions,
            topic_ids: self.topic_ids,
            session_timeout: self.session_timeout,
            max_poll_interval: self.max_poll_interval,
            heartbeat_interval: self.heartbeat_interval,
            subscription_metadata_refresh_interval: self.subscription_metadata_refresh_interval,
            leave_group_timeout: self.leave_group_timeout,
            auto_offset_reset: self.auto_offset_reset,
            client_rack: self.client_rack,
            initial_subscribed_counts: self.initial_subscribed_counts,
            retry_policy: self.retry_policy,
            poll_error: self.poll_error,
            auto_commit: self.auto_commit,
            commit_serialization: self.commit_serialization,
            join_prepared: self.join_prepared,
            polls: self.polls,
            rebalance_pending: self.rebalance_pending,
            close_operation: self.close_operation,
            rejoin_reason: self.rejoin_reason,
            listener_calls: self.listener_calls,
            enforced_rebalances: self.enforced_rebalances,
            lost_partitions: self.lost_partitions,
            assigned_callback_pending: self.assigned_callback_pending,
            poll_timer: self.poll_timer,
            poll_timeout_in_callback: self.poll_timeout_in_callback,
        }
    }
}
