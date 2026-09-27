use assert2::assert;
use krabka_protocol::owned::{
    get_telemetry_subscriptions_request::GetTelemetrySubscriptionsRequest,
    push_telemetry_request::PushTelemetryRequest,
};

use super::*;
use crate::telemetry::{
    metrics::{ClientMetrics, MetricKey},
    otlp::{
        AggregationTemporality, KeyValue, Metric, MetricData, MetricsData, NumberDataPoint,
        NumberValue, Resource, ResourceMetrics, ScopeMetrics,
    },
};

const INSTANCE: Uuid = Uuid([7; 16]);
const PREFIX: &str = "org.apache.kafka.producer";

/// The observable state of a sender.
#[derive(Debug, PartialEq)]
struct Observed {
    state: TelemetryState,
    enabled: bool,
    interval: Duration,
    last_request: Option<Instant>,
    subscription: Option<Subscription>,
}

fn observe(sender: &TelemetrySender) -> Observed {
    Observed {
        state: sender.state,
        enabled: sender.enabled,
        interval: sender.interval,
        last_request: sender.last_request,
        subscription: sender.subscription.clone(),
    }
}

fn new_sender() -> TelemetrySender {
    TelemetrySender::new(Collector::new(ClientMetrics::new(), Resource::default()))
}

fn subscription_response(
    push_interval_ms: i32,
    requested: &[&str],
) -> GetTelemetrySubscriptionsResponse {
    GetTelemetrySubscriptionsResponse {
        client_instance_id: INSTANCE,
        subscription_id: 11,
        accepted_compression_types: vec![9, 4, 1],
        push_interval_ms,
        telemetry_max_bytes: 1024 * 1024,
        requested_metrics: requested.iter().map(|name| (*name).to_owned()).collect(),
        ..Default::default()
    }
}

fn subscription(push_interval_ms: i32, selector: MetricSelector) -> Subscription {
    Subscription {
        client_instance_id: INSTANCE,
        id: 11,
        push_interval_ms,
        accepted_compression_types: vec![CompressionType::Zstd, CompressionType::Gzip],
        delta_temporality: false,
        selector,
    }
}

fn prefixes() -> MetricSelector {
    MetricSelector::Prefixes(vec![PREFIX.to_owned()])
}

/// A sender that holds the subscription of a 10 s push interval, in the
/// state after its first push went out.
fn pushing(now: Instant) -> TelemetrySender {
    let mut sender = new_sender();
    sender.create_request(0);
    sender.handle_subscriptions_response(&subscription_response(10_000, &[PREFIX]), now, 0.5);
    sender.create_request(0);
    sender
}

#[test]
fn the_first_request_asks_for_an_instance_id_and_later_ones_reuse_it() {
    let now = Instant::now();
    let mut sender = new_sender();
    assert!(sender.time_to_next_update(now, Duration::from_secs(30)) == Some(Duration::ZERO));
    let first = sender.create_request(0);
    assert!(
        sender.time_to_next_update(now, Duration::from_secs(30)) == Some(Duration::from_secs(30))
    );
    // No metrics requested: ask again after the push interval.
    sender.handle_subscriptions_response(&subscription_response(10_000, &[]), now, 0.9);
    assert!(
        observe(&sender)
            == Observed {
                state: TelemetryState::SubscriptionNeeded,
                enabled: true,
                interval: Duration::from_secs(10),
                last_request: Some(now),
                subscription: Some(subscription(10_000, MetricSelector::NoMetrics)),
            }
    );
    let later = now + Duration::from_secs(4);
    assert!(
        sender.time_to_next_update(later, Duration::from_secs(30)) == Some(Duration::from_secs(6))
    );
    let second = sender.create_request(0);
    assert!(
        [first, second]
            == [Uuid::ZERO, INSTANCE].map(|client_instance_id| {
                Some(TelemetryRequest::Subscriptions(
                    GetTelemetrySubscriptionsRequest {
                        client_instance_id,
                        ..Default::default()
                    },
                ))
            })
    );
}

/// The first push of a subscription waits 50% to 150% of the push interval,
/// and later pushes wait the interval.
#[test]
fn the_first_push_of_a_subscription_is_jittered() {
    let cases = [
        (0.0, Duration::from_secs(5)),
        (0.5, Duration::from_secs(10)),
        (0.999_9, Duration::from_millis(14_999)),
    ];
    for (jitter_unit, expected) in cases {
        let now = Instant::now();
        let mut sender = new_sender();
        sender.create_request(0);
        sender.handle_subscriptions_response(
            &subscription_response(10_000, &[PREFIX]),
            now,
            jitter_unit,
        );
        assert!(
            observe(&sender)
                == Observed {
                    state: TelemetryState::PushNeeded,
                    enabled: true,
                    interval: expected,
                    last_request: Some(now),
                    subscription: Some(subscription(10_000, prefixes())),
                },
            "{jitter_unit}"
        );
    }
    let now = Instant::now();
    let mut sender = pushing(now);
    let later = now + Duration::from_secs(3);
    sender.handle_push_response(&PushTelemetryResponse::default(), later);
    assert!(sender.state() == TelemetryState::PushNeeded);
    assert!(
        sender.time_to_next_update(later + Duration::from_secs(1), Duration::from_secs(30))
            == Some(Duration::from_secs(9))
    );
}

#[test]
fn a_push_interval_that_is_not_positive_is_the_default() {
    for push_interval_ms in [0, -1] {
        let now = Instant::now();
        let mut sender = new_sender();
        sender.create_request(0);
        sender.handle_subscriptions_response(
            &subscription_response(push_interval_ms, &["*"]),
            now,
            0.5,
        );
        assert!(
            sender.subscription.as_ref()
                == Some(&subscription(
                    DEFAULT_PUSH_INTERVAL_MS,
                    MetricSelector::AllMetrics
                ))
        );
        assert!(sender.interval == DEFAULT_PUSH_INTERVAL);
    }
}

/// Kafka's `maybeFetchErrorIntervalMs`, for a response of each request.
#[test]
fn error_codes_refetch_back_off_or_stop() {
    let push_interval = Duration::from_secs(10);
    // (error code, wait before the next subscription request, enabled)
    let cases = [
        (UNKNOWN_SUBSCRIPTION_ID, Duration::ZERO, true),
        (UNSUPPORTED_COMPRESSION_TYPE, Duration::ZERO, true),
        (TELEMETRY_TOO_LARGE, push_interval, true),
        (THROTTLING_QUOTA_EXCEEDED, push_interval, true),
        (INVALID_REQUEST, push_interval, false),
        (INVALID_RECORD, push_interval, false),
        (UNSUPPORTED_VERSION, push_interval, false),
        (-1, push_interval, false),
    ];
    for (error_code, interval, enabled) in cases {
        // A push response. The disabling errors keep the interval.
        let now = Instant::now();
        let mut sender = pushing(now);
        let later = now + Duration::from_secs(1);
        sender.handle_push_response(
            &PushTelemetryResponse {
                error_code,
                ..Default::default()
            },
            later,
        );
        assert!(
            observe(&sender)
                == Observed {
                    state: TelemetryState::SubscriptionNeeded,
                    enabled,
                    interval,
                    last_request: Some(later),
                    subscription: Some(subscription(10_000, prefixes())),
                },
            "push {error_code}"
        );
        let expected_wait = enabled.then_some(interval);
        assert!(
            sender.time_to_next_update(later, Duration::from_secs(30)) == expected_wait,
            "push {error_code}"
        );

        // A first subscription response: no push interval yet, so a back
        // off waits the default interval.
        let mut sender = new_sender();
        sender.create_request(0);
        sender.handle_subscriptions_response(
            &GetTelemetrySubscriptionsResponse {
                error_code,
                ..Default::default()
            },
            now,
            0.5,
        );
        let interval = if enabled && !interval.is_zero() {
            DEFAULT_PUSH_INTERVAL
        } else {
            Duration::ZERO
        };
        assert!(
            observe(&sender)
                == Observed {
                    state: TelemetryState::SubscriptionNeeded,
                    enabled,
                    interval,
                    last_request: Some(now),
                    subscription: None,
                },
            "subscriptions {error_code}"
        );
    }
}

#[test]
fn a_subscription_without_an_instance_id_stops_telemetry() {
    let now = Instant::now();
    let mut sender = new_sender();
    sender.create_request(0);
    sender.handle_subscriptions_response(
        &GetTelemetrySubscriptionsResponse {
            client_instance_id: Uuid::ZERO,
            ..subscription_response(10_000, &["*"])
        },
        now,
        0.5,
    );
    assert!(
        observe(&sender)
            == Observed {
                state: TelemetryState::SubscriptionNeeded,
                enabled: false,
                interval: Duration::ZERO,
                last_request: Some(now),
                subscription: None,
            }
    );
    assert!(
        sender
            .time_to_next_update(now, Duration::from_secs(30))
            .is_none()
    );
}

/// A request without a response waits the default push interval after a
/// disconnect or timeout, and stops telemetry otherwise.
#[test]
fn failed_requests_back_off_or_stop() {
    let cases = [(Failure::Retriable, true), (Failure::Fatal, false)];
    for (failure, enabled) in cases {
        for in_flight in ["subscriptions", "push"] {
            let now = Instant::now();
            let mut sender = if in_flight == "push" {
                pushing(now)
            } else {
                let mut sender = new_sender();
                sender.create_request(0);
                sender
            };
            let later = now + Duration::from_secs(1);
            sender.handle_failed_request(failure, later);
            let subscription = (in_flight == "push").then(|| subscription(10_000, prefixes()));
            let interval = match (enabled, &subscription) {
                (true, _) => DEFAULT_PUSH_INTERVAL,
                (false, Some(_)) => Duration::from_secs(10),
                (false, None) => Duration::ZERO,
            };
            assert!(
                observe(&sender)
                    == Observed {
                        state: TelemetryState::SubscriptionNeeded,
                        enabled,
                        interval,
                        last_request: Some(later),
                        subscription,
                    },
                "{failure:?} {in_flight}"
            );
        }
    }
}

/// `initiateClose` asks for a terminating push only once a subscription has
/// loaded, and a response that arrives during the close changes nothing.
#[test]
fn close_sends_a_terminating_push_only_with_a_subscription() {
    let now = Instant::now();
    // (setup, terminating push due, state after initiate_close)
    let never_fetched = new_sender();
    let mut in_flight_first = new_sender();
    in_flight_first.create_request(0);
    let mut no_metrics = new_sender();
    no_metrics.create_request(0);
    no_metrics.handle_subscriptions_response(&subscription_response(10_000, &[]), now, 0.5);
    let mut push_due = new_sender();
    push_due.create_request(0);
    push_due.handle_subscriptions_response(&subscription_response(10_000, &[PREFIX]), now, 0.5);
    let push_in_flight = pushing(now);
    let mut refetching = pushing(now);
    refetching.handle_push_response(&PushTelemetryResponse::default(), now);
    refetching.create_request(0);
    refetching.handle_failed_request(Failure::Retriable, now);
    refetching.create_request(0);
    let cases = [
        (
            "never fetched",
            never_fetched,
            false,
            TelemetryState::SubscriptionNeeded,
        ),
        (
            "first fetch in flight",
            in_flight_first,
            false,
            TelemetryState::SubscriptionInProgress,
        ),
        (
            "no metrics",
            no_metrics,
            false,
            TelemetryState::SubscriptionNeeded,
        ),
        (
            "push due",
            push_due,
            true,
            TelemetryState::TerminatingPushNeeded,
        ),
        (
            "push in flight",
            push_in_flight,
            true,
            TelemetryState::TerminatingPushNeeded,
        ),
        (
            "refetch in flight",
            refetching,
            true,
            TelemetryState::TerminatingPushNeeded,
        ),
    ];
    for (name, mut sender, due, state) in cases {
        assert!(sender.initiate_close() == due, "{name}");
        assert!(sender.state() == state, "{name}");
        assert!(!sender.initiate_close(), "{name}: a second close");
        if due {
            assert!(
                sender.time_to_next_update(now, Duration::from_secs(30)) == Some(Duration::ZERO),
                "{name}"
            );
            // The response of the request in flight is ignored.
            sender.handle_push_response(&PushTelemetryResponse::default(), now);
            sender.handle_failed_request(Failure::Fatal, now);
            let Some(TelemetryRequest::Push(push)) = sender.create_request(0) else {
                panic!("{name}: no terminating push");
            };
            assert!(push.terminating, "{name}");
            assert!(
                sender.state() == TelemetryState::TerminatingPushInProgress,
                "{name}"
            );
            assert!(
                sender
                    .time_to_next_update(now, Duration::from_secs(30))
                    .is_none(),
                "{name}"
            );
        }
        sender.close();
        assert!(sender.state() == TelemetryState::Terminated, "{name}");
        assert!(sender.create_request(0).is_none(), "{name}");
    }
}

fn gauge_data(value: f64, time: u64) -> MetricsData {
    MetricsData {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![KeyValue::new("transactional_id", "tx")],
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: format!("{PREFIX}.connection.count"),
                    data: Some(MetricData::Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: vec![],
                            start_time_unix_nano: 0,
                            time_unix_nano: time,
                            value: Some(NumberValue::Double(value)),
                        }],
                    }),
                }],
            }],
        }],
    }
}

/// The push carries the selected metrics, compressed with the broker's first
/// accepted codec that works. A codec that failed before is skipped.
#[test]
fn a_push_compresses_the_selected_metrics() {
    let cases = [
        (vec![9, 4, 1], vec![], CompressionType::Zstd),
        (
            vec![4, 1],
            vec![CompressionType::Zstd],
            CompressionType::Gzip,
        ),
        (vec![3], vec![], CompressionType::Lz4),
        (vec![2], vec![], CompressionType::Snappy),
        (vec![], vec![], CompressionType::None),
        (vec![4], vec![CompressionType::Zstd], CompressionType::None),
    ];
    for (accepted, unsupported, codec) in cases {
        let metrics = ClientMetrics::new();
        metrics
            .gauge(MetricKey::kafka("producer-metrics", "connection-count", []))
            .set(3);
        let _ = metrics.counter(MetricKey::kafka("consumer-metrics", "request-total", []));
        let mut sender = TelemetrySender::new(Collector::new(
            metrics,
            Resource {
                attributes: vec![KeyValue::new("transactional_id", "tx")],
            },
        ));
        sender.unsupported_compression_types = unsupported;
        sender.create_request(0);
        sender.handle_subscriptions_response(
            &GetTelemetrySubscriptionsResponse {
                accepted_compression_types: accepted.clone(),
                ..subscription_response(10_000, &[PREFIX])
            },
            Instant::now(),
            0.5,
        );
        let Some(TelemetryRequest::Push(push)) = sender.create_request(1_000) else {
            panic!("{accepted:?}: no push");
        };
        let payload =
            krabka_compression::decompress(codec, &push.metrics, krabka_units::mebibytes(1))
                .unwrap();
        assert!(
            (push.clone(), MetricsData::decode(&payload))
                == (
                    PushTelemetryRequest {
                        client_instance_id: INSTANCE,
                        subscription_id: 11,
                        terminating: false,
                        compression_type: i8::try_from(codec.as_attribute_bits()).unwrap(),
                        metrics: push.metrics.clone(),
                        ..Default::default()
                    },
                    Ok(gauge_data(3.0, 1_000)),
                ),
            "{accepted:?}"
        );
    }
}

/// A change of temporality starts the sums again.
#[test]
fn a_temporality_change_resets_the_sums() {
    let metrics = ClientMetrics::new();
    let counter = metrics.counter(MetricKey::kafka("producer-metrics", "request-total", []));
    counter.add(4);
    let mut sender = TelemetrySender::new(Collector::new(metrics, Resource::default()));
    let mut pushes = Vec::new();
    for (delta_temporality, now_unix_nanos) in [(true, 100), (true, 200), (false, 300)] {
        sender.state = TelemetryState::SubscriptionNeeded;
        sender.create_request(0);
        sender.handle_subscriptions_response(
            &GetTelemetrySubscriptionsResponse {
                delta_temporality,
                accepted_compression_types: vec![],
                ..subscription_response(10_000, &["*"])
            },
            Instant::now(),
            0.5,
        );
        let Some(TelemetryRequest::Push(push)) = sender.create_request(now_unix_nanos) else {
            panic!("no push");
        };
        let data = MetricsData::decode(&push.metrics).unwrap();
        let point = data.resource_metrics[0].scope_metrics[0].metrics[0]
            .data
            .clone();
        pushes.push(point);
        sender.handle_push_response(&PushTelemetryResponse::default(), Instant::now());
    }
    let sum = |start, time, value, aggregation_temporality| {
        Some(MetricData::Sum {
            data_points: vec![NumberDataPoint {
                attributes: vec![],
                start_time_unix_nano: start,
                time_unix_nano: time,
                value: Some(NumberValue::Double(value)),
            }],
            aggregation_temporality,
            is_monotonic: true,
        })
    };
    let Some(MetricData::Sum { data_points, .. }) = &pushes[0] else {
        panic!("not a sum");
    };
    let added = data_points[0].start_time_unix_nano;
    assert!(
        pushes
            == [
                sum(added, 100, 4.0, AggregationTemporality::Delta),
                sum(100, 200, 0.0, AggregationTemporality::Delta),
                sum(300, 300, 4.0, AggregationTemporality::Cumulative),
            ]
    );
}

#[test]
fn state_transitions_follow_kafka() {
    use TelemetryState::{
        PushInProgress, PushNeeded, SubscriptionInProgress, SubscriptionNeeded, Terminated,
        TerminatingPushInProgress, TerminatingPushNeeded,
    };
    let all = [
        SubscriptionNeeded,
        SubscriptionInProgress,
        PushNeeded,
        PushInProgress,
        TerminatingPushNeeded,
        TerminatingPushInProgress,
        Terminated,
    ];
    let allowed: [(TelemetryState, &[TelemetryState]); 7] = [
        (SubscriptionNeeded, &[SubscriptionInProgress, Terminated]),
        (
            SubscriptionInProgress,
            &[
                PushNeeded,
                SubscriptionNeeded,
                TerminatingPushNeeded,
                Terminated,
            ],
        ),
        (
            PushNeeded,
            &[
                PushInProgress,
                SubscriptionNeeded,
                TerminatingPushNeeded,
                Terminated,
            ],
        ),
        (
            PushInProgress,
            &[
                PushNeeded,
                SubscriptionNeeded,
                TerminatingPushNeeded,
                Terminated,
            ],
        ),
        (
            TerminatingPushNeeded,
            &[TerminatingPushInProgress, Terminated],
        ),
        (TerminatingPushInProgress, &[Terminated]),
        (Terminated, &[]),
    ];
    for (from, to) in allowed {
        let actual = all
            .iter()
            .copied()
            .filter(|next| from.allows(*next))
            .collect::<Vec<_>>();
        let mut expected = to.to_vec();
        expected.sort_by_key(|state| all.iter().position(|s| s == state));
        assert!(actual == expected, "{from:?}");
    }
}
