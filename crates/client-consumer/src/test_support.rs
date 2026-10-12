use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI32, AtomicU8, Ordering},
    },
    time::Duration,
};

use krabka_client_core::{Client, MockBroker};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        api_versions_request, leave_group_request,
        leave_group_request::{LeaveGroupRequest, MemberIdentity},
        list_offsets_request::{self, ListOffsetsRequest},
        list_offsets_response::{
            ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
        },
        metadata_request::MetadataRequest,
        metadata_response,
    },
    primitives::uuid::Uuid as WireUuid,
};
use krabka_units::{Time, millis, minutes, secs};
use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;

use crate::{
    AutoOffsetReset, ConsumerRetryPolicy, GroupProtocol, IsolationLevel,
    assignor::{Assignor, RebalanceProtocol},
    consumer,
    consumer::{CommitIdentity, Consumer},
    control, coordinator,
    coordinator::{CoordinatorRetryPolicy, CoordinatorState, PollErrorSlot, PollSignal, PollTimer},
    poll, rebalance_listener, subscription,
};

pub(crate) fn wire_uuid(last_byte: u8) -> WireUuid {
    let mut bytes = [0; 16];
    bytes[15] = last_byte;
    WireUuid(bytes)
}

pub(crate) fn encode_protocol_response(response: &impl Encode, version: i16) -> Vec<u8> {
    let mut buffer = bytes::BytesMut::new();
    response
        .encode(&mut buffer, version)
        .expect("encode response");
    buffer.to_vec()
}

pub(crate) fn dynamic_member_leave_request(reason: &str) -> LeaveGroupRequest {
    LeaveGroupRequest {
        group_id: "group-a".into(),
        // Version 5 has no top-level member id.
        member_id: String::new(),
        members: vec![MemberIdentity {
            member_id: "member-a".into(),
            group_instance_id: None,
            reason: Some(reason.into()),
            ..Default::default()
        }],
        ..Default::default()
    }
}

pub(crate) fn coordinator_retry_policy(timeout: Duration) -> CoordinatorRetryPolicy {
    CoordinatorRetryPolicy {
        timeout,
        initial_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(1),
    }
}

#[derive(bon::Builder)]
pub(crate) struct FirstRecorded<T> {
    pub(crate) requests: Arc<std::sync::Mutex<Vec<T>>>,
    pub(crate) timeout: Duration,
    pub(crate) poll_interval: Duration,
}

impl<T: Clone> FirstRecorded<T> {
    pub(crate) async fn wait(self) -> Result<T, tokio::time::error::Elapsed> {
        tokio::time::timeout(self.timeout, async {
            loop {
                if let Some(first) = self
                    .requests
                    .lock()
                    .expect("requests lock")
                    .first()
                    .cloned()
                {
                    break first;
                }
                tokio::time::sleep(self.poll_interval).await;
            }
        })
        .await
    }
}

pub(crate) fn leave_group_api_versions() -> Vec<u8> {
    krabka_client_core::mock_test_support::api_versions_with(
        leave_group_request::API_KEY,
        leave_group_request::MAX_VERSION,
    )
}

async fn stalled_leave_group_broker(saw_leave: Arc<AtomicBool>) -> MockBroker {
    MockBroker::start(move |api_key, _version, _correlation_id, _body| {
        if api_key == api_versions_request::API_KEY {
            Some(leave_group_api_versions())
        } else if api_key == leave_group_request::API_KEY {
            saw_leave.store(true, Ordering::SeqCst);
            None
        } else {
            None
        }
    })
    .await
}

pub(crate) struct StalledLeaveGroup {
    pub(crate) broker: MockBroker,
    pub(crate) client: Client,
    pub(crate) coordinator_id: AtomicI32,
    pub(crate) saw_leave: Arc<AtomicBool>,
}

impl StalledLeaveGroup {
    pub(crate) async fn start() -> Self {
        let saw_leave = Arc::new(AtomicBool::new(false));
        let broker = stalled_leave_group_broker(Arc::clone(&saw_leave)).await;
        let client = Client::builder()
            .bootstrap(broker.addr.to_string())
            .request_timeout(secs(5))
            .build()
            .await
            .expect("client");

        Self {
            broker,
            client,
            coordinator_id: AtomicI32::new(0),
            saw_leave,
        }
    }
}

pub(crate) fn assert_starting_offsets(starting_offset: fn(i64, AutoOffsetReset) -> i64) {
    for (committed, reset, expected) in [
        (12, AutoOffsetReset::Earliest, 12),
        (-1, AutoOffsetReset::Earliest, poll::BEGINNING_SENTINEL),
        (-1, AutoOffsetReset::Latest, poll::LATEST_SENTINEL),
        (-1, AutoOffsetReset::None, poll::NO_OFFSET_SENTINEL),
    ] {
        assert2::assert!(starting_offset(committed, reset) == expected);
    }
}

pub(crate) fn decode_metadata_request(body: &mut impl bytes::Buf, version: i16) -> MetadataRequest {
    let client_id_len = body.get_i16();
    body.advance(usize::try_from(client_id_len.max(0)).expect("client id length"));
    MetadataRequest::decode(body, version).expect("decode Metadata")
}

pub(crate) fn metadata_topic_names(body: &mut impl bytes::Buf, version: i16) -> Vec<String> {
    decode_metadata_request(body, version)
        .topics
        .unwrap_or_default()
        .into_iter()
        .filter_map(|topic| topic.name)
        .collect()
}

pub(crate) fn metadata_partitions(count: i32) -> Vec<metadata_response::MetadataResponsePartition> {
    (0..count)
        .map(
            |partition_index| metadata_response::MetadataResponsePartition {
                partition_index,
                ..Default::default()
            },
        )
        .collect()
}

pub(crate) async fn topic_metadata_client(broker: &MockBroker) -> Client {
    Client::builder()
        .bootstrap(broker.addr.to_string())
        .metadata_scope(krabka_client_core::MetadataScope::Topics {
            allow_auto_topic_creation: false,
        })
        .build()
        .await
        .expect("client")
}

pub(crate) struct OrdersMetadata;

#[bon::bon]
impl OrdersMetadata {
    #[builder(start_fn = builder, finish_fn = build)]
    pub(crate) fn response(
        topic_id: [u8; 16],
        leader_id: i32,
        leader_epoch: i32,
        #[builder(default)] brokers: Vec<metadata_response::MetadataResponseBroker>,
    ) -> metadata_response::MetadataResponse {
        use metadata_response::{
            MetadataResponse, MetadataResponsePartition, MetadataResponseTopic,
        };

        MetadataResponse {
            brokers,
            topics: vec![MetadataResponseTopic {
                name: Some("orders".into()),
                topic_id: WireUuid(topic_id),
                partitions: vec![MetadataResponsePartition {
                    partition_index: 0,
                    leader_id,
                    leader_epoch,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }
}

pub(crate) fn decode_list_offsets_request(
    body: &mut impl bytes::Buf,
    version: i16,
) -> ListOffsetsRequest {
    let client_id_len = body.get_i16();
    body.advance(usize::try_from(client_id_len.max(0)).expect("client id length"));
    if version >= list_offsets_request::FLEXIBLE_MIN {
        body.advance(1);
    }
    ListOffsetsRequest::decode(body, version).expect("decode ListOffsets")
}

pub(crate) fn list_offsets_mock_response(body: &mut impl bytes::Buf, version: i16) -> Vec<u8> {
    let request = decode_list_offsets_request(body, version);
    let flexible = version >= list_offsets_request::FLEXIBLE_MIN;
    let mut response = bytes::BytesMut::new();
    if flexible {
        response.extend_from_slice(&[0]);
    }
    ListOffsetsResponse {
        topics: request
            .topics
            .iter()
            .map(|topic| ListOffsetsTopicResponse {
                name: topic.name.clone(),
                partitions: topic
                    .partitions
                    .iter()
                    .map(|partition| ListOffsetsPartitionResponse {
                        partition_index: partition.partition_index,
                        offset: 0,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
    .encode(&mut response, version)
    .expect("encode list offsets");
    response.to_vec()
}

fn test_commit_identity(member_id: String) -> Arc<Mutex<CommitIdentity>> {
    Arc::new(Mutex::new(CommitIdentity {
        generation: 1,
        member_id,
        ownership_ids: HashMap::new(),
        rejoin_on_poll: false,
    }))
}

pub(crate) fn orders_partition_commit_identity(generation: i32) -> Arc<Mutex<CommitIdentity>> {
    Arc::new(Mutex::new(CommitIdentity {
        generation,
        member_id: "member-a".into(),
        ownership_ids: HashMap::from([(("orders".into(), 0), 1)]),
        rejoin_on_poll: false,
    }))
}

pub(crate) type ListenerCall = (rebalance_listener::ListenerCallKind, Vec<i32>);
pub(crate) type ListenerCalls = Arc<std::sync::Mutex<Vec<ListenerCall>>>;

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ListenerStep {
    Started(&'static str, Vec<(String, i32)>),
    Ended(&'static str, Vec<(String, i32)>),
}

pub(crate) type ListenerSteps = Arc<std::sync::Mutex<Vec<ListenerStep>>>;

#[derive(bon::Builder)]
pub(crate) struct RecordingListener {
    calls: Option<ListenerCalls>,
    steps: Option<ListenerSteps>,
    slow_partition: Option<i32>,
    #[builder(default)]
    slow_for: Duration,
    #[builder(default)]
    slept: bool,
}

#[async_trait::async_trait]
impl crate::ConsumerRebalanceListener for RecordingListener {
    async fn on_partitions_revoked(
        &mut self,
        _consumer: &Consumer,
        partitions: &[(String, i32)],
    ) -> Result<(), crate::RebalanceListenerError> {
        self.record_step(ListenerStep::Started("revoked", partitions.to_vec()));
        self.record(rebalance_listener::ListenerCallKind::Revoked, partitions);
        self.record_step(ListenerStep::Ended("revoked", partitions.to_vec()));
        Ok(())
    }

    async fn on_partitions_assigned(
        &mut self,
        _consumer: &Consumer,
        partitions: &[(String, i32)],
    ) -> Result<(), crate::RebalanceListenerError> {
        self.record_step(ListenerStep::Started("assigned", partitions.to_vec()));
        if !self.slept
            && self
                .slow_partition
                .is_some_and(|index| partitions.iter().any(|(_, partition)| *partition == index))
        {
            self.slept = true;
            tokio::time::sleep(self.slow_for).await;
        }
        self.record(rebalance_listener::ListenerCallKind::Assigned, partitions);
        self.record_step(ListenerStep::Ended("assigned", partitions.to_vec()));
        Ok(())
    }

    async fn on_partitions_lost(
        &mut self,
        _consumer: &Consumer,
        partitions: &[(String, i32)],
    ) -> Result<(), crate::RebalanceListenerError> {
        self.record_step(ListenerStep::Started("lost", partitions.to_vec()));
        self.record(rebalance_listener::ListenerCallKind::Lost, partitions);
        self.record_step(ListenerStep::Ended("lost", partitions.to_vec()));
        Ok(())
    }
}

impl RecordingListener {
    fn record(&self, kind: rebalance_listener::ListenerCallKind, partitions: &[(String, i32)]) {
        if let Some(calls) = &self.calls {
            calls
                .lock()
                .expect("calls lock")
                .push((kind, partitions.iter().map(|(_, index)| *index).collect()));
        }
    }

    fn record_step(&self, step: ListenerStep) {
        if let Some(steps) = &self.steps {
            steps.lock().expect("steps lock").push(step);
        }
    }
}

pub(crate) struct ConsumerTest;

#[bon::bon]
impl ConsumerTest {
    #[builder(start_fn = new, finish_fn = build)]
    pub(crate) fn configure(
        #[builder(start_fn)] client: Client,
        #[builder(into)] coordinator_id: Option<Arc<AtomicI32>>,
        #[builder(into)] commit_identity: Option<Arc<Mutex<CommitIdentity>>>,
        #[builder(into)] current_generation: Option<Arc<AtomicI32>>,
        #[builder(into)] generation_id: Option<i32>,
        #[builder(into)] group_instance_id: Option<String>,
        #[builder(into)] subscription: Option<subscription::SharedSubscription>,
        #[builder(into)] assigned: Option<Vec<(String, i32)>>,
        #[builder(into)] assignment_changed: Option<Arc<Notify>>,
        #[builder(into)] next_offsets: Option<HashMap<(String, i32), i64>>,
        #[builder(into)] topic_ids: Option<HashMap<String, WireUuid>>,
    ) -> Consumer {
        let member_id = "member-a".to_string();
        let mut consumer = Consumer {
            client,
            group_id: "group-a".into(),
            coordinator_id: Arc::new(AtomicI32::new(0)),
            retry_policy: ConsumerRetryPolicy::default().into(),
            member_id: tokio::sync::watch::channel(member_id.clone()).1,
            commit_identity: test_commit_identity(member_id),
            commit_serialization: Arc::new(Mutex::new(())),
            commit_async_state: Arc::new(AtomicU8::new(0)),
            commit_async_callbacks: Arc::default(),
            group_instance_id: None,
            current_generation: Arc::new(AtomicI32::new(1)),
            subscription: subscription::shared(vec!["topic".into()], None, true),
            group_protocol: GroupProtocol::Classic,
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
            fetch_max: poll::DEFAULT_FETCH_MAX,
            fetch_partition_max: poll::DEFAULT_FETCH_PARTITION_MAX,
            fetch_max_wait: consumer::DEFAULT_CONSUMER_FETCH_MAX_WAIT,
            fetches: poll::Fetches::default(),
            client_rack: None,
            metadata_max_age: consumer::DEFAULT_CONSUMER_METADATA_MAX_AGE,
            request_timeout: secs(30),
            wakeup: control::WakeupHandle::default(),
            enforced_rebalances: tokio::sync::mpsc::unbounded_channel().0,
            default_api_timeout: consumer::DEFAULT_CONSUMER_DEFAULT_API_TIMEOUT,
            paused: std::sync::Mutex::default(),
            auto_offset_reset: AutoOffsetReset::Latest,
            poll_error: coordinator::PollErrorSlot::default(),
            auto_commit: None,
            poll_signal: coordinator::PollSignal::default(),
            rebalance_pending: tokio::sync::watch::channel(false).1,
            max_poll_records: consumer::DEFAULT_CONSUMER_MAX_POLL_RECORDS,
            fetch_buffer: crate::fetch_buffer::FetchBuffer::default(),
            close_operation: tokio::sync::watch::Sender::new(control::CloseRequest::default()),
            rebalance_listener: None,
            listener_calls: tokio::sync::mpsc::unbounded_channel().1,
            assigned_callback_pending: Arc::default(),
        };
        if let Some(coordinator_id) = coordinator_id {
            consumer.coordinator_id = coordinator_id;
        }
        if let Some(commit_identity) = commit_identity {
            consumer.commit_identity = commit_identity;
        }
        if let Some(current_generation) = current_generation {
            consumer.current_generation = current_generation;
        }
        if let Some(generation_id) = generation_id {
            consumer.current_generation = Arc::new(AtomicI32::new(generation_id));
        }
        consumer.group_instance_id = group_instance_id;
        if let Some(subscription) = subscription {
            consumer.subscription = subscription;
        }
        if let Some(assigned) = assigned {
            consumer.assigned = Arc::new(Mutex::new(assigned));
        }
        if let Some(assignment_changed) = assignment_changed {
            consumer.assignment_changed = assignment_changed;
        }
        if let Some(next_offsets) = next_offsets {
            consumer.next_offsets = Arc::new(Mutex::new(next_offsets));
        }
        if let Some(topic_ids) = topic_ids {
            consumer.topic_ids = Arc::new(Mutex::new(topic_ids));
        }
        consumer
    }
}

pub(crate) type ConsumerTestBuilder = ConsumerTestConfigureBuilder;

pub(crate) struct CoordinatorStateTest;

#[bon::bon]
impl CoordinatorStateTest {
    #[builder(start_fn = new, finish_fn = build)]
    pub(crate) fn configure(
        #[builder(start_fn)] client: Client,
        #[builder(into)] commit_identity: Option<Arc<Mutex<CommitIdentity>>>,
        #[builder(into)] group_instance_id: Option<String>,
        #[builder(into)] subscription: Option<subscription::SharedSubscription>,
        #[builder(into)] assigned: Option<Vec<(String, i32)>>,
        #[builder(into)] next_ownership_id: Option<u64>,
        #[builder(into)] next_offsets: Option<HashMap<(String, i32), i64>>,
        #[builder(into)] retry_policy: Option<CoordinatorRetryPolicy>,
        #[builder(into)] heartbeat_interval: Option<Time>,
        #[builder(into)] leave_group_timeout: Option<Time>,
        #[builder(into)] subscription_metadata_refresh_interval: Option<Time>,
        #[builder(into)] polls: Option<tokio::sync::watch::Receiver<u64>>,
    ) -> CoordinatorState {
        let member_id = "member-a".to_string();
        let mut state = CoordinatorState {
            client,
            group_id: "group-a".into(),
            coordinator_id: Arc::new(AtomicI32::new(0)),
            member_id: member_id.clone(),
            published_member_id: tokio::sync::watch::Sender::new(member_id.clone()),
            commit_identity: test_commit_identity(member_id),
            group_instance_id: None,
            generation_id: 1,
            current_generation: Arc::new(AtomicI32::new(1)),
            assignors: vec![Assignor::Range],
            rebalance_protocol: RebalanceProtocol::Eager,
            subscription: subscription::shared(vec!["topic".into()], None, true),
            subscription_changes: subscription::shared(Vec::new(), None, true).subscribe(),
            joined_topics: Vec::new(),
            seen_unsubscribes: 0,
            group_protocol: GroupProtocol::Classic,
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
            close_operation: tokio::sync::watch::channel(control::CloseRequest::default()).1,
            rejoin_reason: String::new(),
            listener_calls: None,
            enforced_rebalances: tokio::sync::mpsc::unbounded_channel().1,
            lost_partitions: Vec::new(),
            assigned_callback_pending: Arc::default(),
            poll_timer: PollTimer::new(secs(300)),
            poll_timeout_in_callback: false,
        };
        if let Some(commit_identity) = commit_identity {
            state.commit_identity = commit_identity;
        }
        state.group_instance_id = group_instance_id;
        if let Some(subscription) = subscription {
            state.subscription_changes = subscription.subscribe();
            state.subscription = subscription;
        }
        if let Some(assigned) = assigned {
            state.assigned = Arc::new(Mutex::new(assigned));
        }
        if let Some(next_ownership_id) = next_ownership_id {
            state.next_ownership_id = next_ownership_id;
        }
        if let Some(next_offsets) = next_offsets {
            state.next_offsets = Arc::new(Mutex::new(next_offsets));
        }
        if let Some(retry_policy) = retry_policy {
            state.retry_policy = retry_policy;
        }
        if let Some(heartbeat_interval) = heartbeat_interval {
            state.heartbeat_interval = heartbeat_interval;
        }
        if let Some(leave_group_timeout) = leave_group_timeout {
            state.leave_group_timeout = leave_group_timeout;
        }
        if let Some(subscription_metadata_refresh_interval) = subscription_metadata_refresh_interval
        {
            state.subscription_metadata_refresh_interval = subscription_metadata_refresh_interval;
        }
        if let Some(polls) = polls {
            state.polls = polls;
        }
        state
    }
}

pub(crate) type CoordinatorStateTestBuilder = CoordinatorStateTestConfigureBuilder;
