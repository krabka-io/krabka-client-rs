//! The metrics that a client pushes, and their conversion to OTLP.
//!
//! [`ClientMetrics`] is the registry of one client, as Kafka's `Metrics` is.
//! [`Collector`] turns a snapshot of it into the `MetricsData` of one push, as
//! Kafka's `KafkaMetricsCollector` and `ClientTelemetryEmitter` do.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
};

use super::otlp::{
    AggregationTemporality, KeyValue, Metric, MetricData, MetricsData, NumberDataPoint,
    NumberValue, Resource, ResourceMetrics, ScopeMetrics,
};

/// The prefix of every client metric name
/// (`ClientTelemetryProvider.DOMAIN`).
pub const TELEMETRY_METRIC_DOMAIN: &str = "org.apache.kafka";

/// The tag that the broker already knows from the request header, so Kafka's
/// `ClientTelemetryReporter` leaves it out of the data point attributes.
const EXCLUDED_ATTRIBUTE: &str = "client_id";

/// The name and attributes of one metric, as KIP-714 names them.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MetricKey {
    /// For example `org.apache.kafka.producer.request.total`.
    pub name: String,
    /// The data point attributes, with snake case keys.
    pub tags: BTreeMap<String, String>,
}

impl MetricKey {
    /// The key of the Kafka metric `name` in `group` with `tags`, as
    /// `TelemetryMetricNamingConvention` maps it: the group and name are lower
    /// case and dot separated, the group loses its `-metrics` part, and each
    /// tag key is snake case.
    ///
    /// ```
    /// use assert2::assert;
    /// use krabka_client_core::telemetry::MetricKey;
    ///
    /// let key = MetricKey::kafka("producer-metrics", "record-send-rate", []);
    /// assert!(key.name == "org.apache.kafka.producer.record.send.rate");
    /// ```
    #[must_use]
    pub fn kafka<'a>(
        group: &str,
        name: &str,
        tags: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Self {
        let group = clean(group, ".").replace(".metrics", "");
        Self {
            name: format!("{TELEMETRY_METRIC_DOMAIN}.{group}.{}", clean(name, ".")),
            tags: tags
                .into_iter()
                .map(|(key, value)| (clean(key, "_"), value.to_owned()))
                .collect(),
        }
    }
}

fn clean(raw: &str, joiner: &str) -> String {
    raw.to_lowercase().replace('-', joiner)
}

/// A monotonic counter, pushed as an OTLP sum (Kafka's `CumulativeSum` and
/// `WindowedCount`).
#[derive(Clone, Debug, Default)]
pub struct Counter(Arc<AtomicU64>);

impl Counter {
    /// Add `amount` to the counter.
    pub fn add(&self, amount: u64) {
        self.0.fetch_add(amount, Ordering::Relaxed);
    }

    /// The current total.
    #[must_use]
    pub fn value(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// A value that goes up and down, pushed as an OTLP gauge.
#[derive(Clone, Debug, Default)]
pub struct Gauge(Arc<AtomicI64>);

impl Gauge {
    /// Add `delta`, which may be negative.
    pub fn add(&self, delta: i64) {
        self.0.fetch_add(delta, Ordering::Relaxed);
    }

    /// Set the value.
    pub fn set(&self, value: i64) {
        self.0.store(value, Ordering::Relaxed);
    }

    /// The current value.
    #[must_use]
    pub fn value(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

#[derive(Clone, Debug)]
enum Cell {
    Counter(Counter),
    Gauge(Gauge),
}

#[derive(Clone, Debug)]
struct Registered {
    cell: Cell,
    /// When the metric was registered, in nanoseconds since the Unix epoch.
    added_unix_nanos: u64,
}

/// The metric registry of one client. A clone shares the registry.
#[derive(Clone, Debug, Default)]
pub struct ClientMetrics {
    registered: Arc<Mutex<BTreeMap<MetricKey, Registered>>>,
}

impl ClientMetrics {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The counter of `key`, registered now if the registry has no counter of
    /// that key. A registration replaces a gauge of the same key, as Kafka's
    /// `metricChange` replaces the metric.
    #[must_use]
    pub fn counter(&self, key: MetricKey) -> Counter {
        let mut registered = self.lock();
        if let Some(Registered {
            cell: Cell::Counter(counter),
            ..
        }) = registered.get(&key)
        {
            return counter.clone();
        }
        let counter = Counter::default();
        registered.insert(
            key,
            Registered {
                cell: Cell::Counter(counter.clone()),
                added_unix_nanos: unix_nanos_now(),
            },
        );
        counter
    }

    /// The gauge of `key`, registered now if the registry has no gauge of
    /// that key.
    #[must_use]
    pub fn gauge(&self, key: MetricKey) -> Gauge {
        let mut registered = self.lock();
        if let Some(Registered {
            cell: Cell::Gauge(gauge),
            ..
        }) = registered.get(&key)
        {
            return gauge.clone();
        }
        let gauge = Gauge::default();
        registered.insert(
            key,
            Registered {
                cell: Cell::Gauge(gauge.clone()),
                added_unix_nanos: unix_nanos_now(),
            },
        );
        gauge
    }

    /// Remove the metric of `key` (Kafka's `metricRemoval`).
    pub fn remove(&self, key: &MetricKey) {
        self.lock().remove(key);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<MetricKey, Registered>> {
        self.registered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn snapshot(&self) -> Vec<(MetricKey, Registered)> {
        self.lock()
            .iter()
            .map(|(key, registered)| (key.clone(), registered.clone()))
            .collect()
    }
}

/// Nanoseconds since the Unix epoch.
pub(crate) fn unix_nanos_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| u64::try_from(elapsed.as_nanos()).ok())
        .unwrap_or(0)
}

/// The network metrics of a client's connections: the `-total` metrics of
/// Kafka's `Selector` in the client's metric group, and its
/// `connection-count`.
#[derive(Clone, Debug)]
pub struct NetworkMetrics {
    connection_creation_total: Counter,
    connection_close_total: Counter,
    connection_count: Gauge,
    request_total: Counter,
    outgoing_byte_total: Counter,
    response_total: Counter,
    incoming_byte_total: Counter,
}

impl NetworkMetrics {
    /// Register the network metrics of `group`, for example
    /// `producer-metrics`, in `metrics`.
    #[must_use]
    pub fn register(metrics: &ClientMetrics, group: &str) -> Self {
        let counter = |name| metrics.counter(MetricKey::kafka(group, name, []));
        Self {
            connection_creation_total: counter("connection-creation-total"),
            connection_close_total: counter("connection-close-total"),
            connection_count: metrics.gauge(MetricKey::kafka(group, "connection-count", [])),
            request_total: counter("request-total"),
            outgoing_byte_total: counter("outgoing-byte-total"),
            response_total: counter("response-total"),
            incoming_byte_total: counter("incoming-byte-total"),
        }
    }

    /// A connection opened. The returned guard counts its close when it
    /// drops.
    pub(crate) fn opened(&self) -> ConnectionMetrics {
        self.connection_creation_total.add(1);
        self.connection_count.add(1);
        ConnectionMetrics(self.clone())
    }
}

/// The network metrics of one open connection.
#[derive(Debug)]
pub(crate) struct ConnectionMetrics(NetworkMetrics);

impl ConnectionMetrics {
    /// A request of `frame_len` bytes went out. Kafka counts the size prefix
    /// too.
    pub(crate) fn request(&self, frame_len: usize) {
        self.0.request_total.add(1);
        self.0.outgoing_byte_total.add(wire_len(frame_len));
    }

    /// A response came in. `body_len` excludes the size prefix and the
    /// correlation id, which Kafka counts.
    pub(crate) fn response(&self, body_len: usize) {
        self.0.response_total.add(1);
        self.0
            .incoming_byte_total
            .add(wire_len(body_len).saturating_add(4));
    }
}

fn wire_len(len: usize) -> u64 {
    u64::try_from(len).unwrap_or(u64::MAX).saturating_add(4)
}

impl Drop for ConnectionMetrics {
    fn drop(&mut self) {
        self.0.connection_close_total.add(1);
        self.0.connection_count.add(-1);
    }
}

/// Which metrics a subscription asks for
/// (`ClientTelemetryUtils.getSelectorFromRequestedMetrics`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum MetricSelector {
    /// An empty `requested_metrics`.
    NoMetrics,
    /// A `requested_metrics` of one `*`.
    AllMetrics,
    /// The metrics whose name starts with one of these prefixes.
    Prefixes(Vec<String>),
}

impl MetricSelector {
    pub(crate) fn from_requested(requested: &[String]) -> Self {
        match requested {
            [] => Self::NoMetrics,
            [all] if all == "*" => Self::AllMetrics,
            prefixes => Self::Prefixes(prefixes.to_vec()),
        }
    }

    pub(crate) fn matches(&self, name: &str) -> bool {
        match self {
            Self::NoMetrics => false,
            Self::AllMetrics => true,
            Self::Prefixes(prefixes) => prefixes.iter().any(|prefix| name.starts_with(prefix)),
        }
    }
}

/// The metrics of one push in OTLP form, with the state that delta
/// temporality needs (Kafka's `KafkaMetricsCollector` and its ledger).
#[derive(Debug)]
pub(crate) struct Collector {
    metrics: ClientMetrics,
    resource: Resource,
    /// The time and value of each sum at the last delta push.
    last_values: HashMap<MetricKey, (u64, f64)>,
    /// The start time of each cumulative sum.
    added: HashMap<MetricKey, u64>,
    /// Set by a temporality change: a sum starts at its next collection.
    reset: bool,
}

impl Collector {
    pub(crate) fn new(metrics: ClientMetrics, resource: Resource) -> Self {
        Self {
            metrics,
            resource,
            last_values: HashMap::new(),
            added: HashMap::new(),
            reset: false,
        }
    }

    /// Forget the pushed values after a temporality change
    /// (`metricsReset`).
    pub(crate) fn reset(&mut self) {
        self.last_values.clear();
        self.added.clear();
        self.reset = true;
    }

    /// The selected metrics at `now_unix_nanos`, one resource metrics entry
    /// per metric as Kafka's `createPayload` builds them.
    pub(crate) fn collect(
        &mut self,
        selector: &MetricSelector,
        delta_temporality: bool,
        now_unix_nanos: u64,
    ) -> MetricsData {
        let mut resource_metrics = Vec::new();
        for (key, registered) in self.metrics.snapshot() {
            if !selector.matches(&key.name) {
                continue;
            }
            let attributes = key
                .tags
                .iter()
                .filter(|(tag, _)| tag.as_str() != EXCLUDED_ATTRIBUTE)
                .map(|(tag, value)| KeyValue::new(tag, value))
                .collect::<Vec<_>>();
            let data = match registered.cell {
                Cell::Gauge(gauge) => MetricData::Gauge {
                    data_points: vec![NumberDataPoint {
                        attributes,
                        start_time_unix_nano: 0,
                        time_unix_nano: now_unix_nanos,
                        value: Some(NumberValue::Double(i64_to_f64(gauge.value()))),
                    }],
                },
                Cell::Counter(counter) => {
                    let value = u64_to_f64(counter.value());
                    let added = *self.added.entry(key.clone()).or_insert(if self.reset {
                        now_unix_nanos
                    } else {
                        registered.added_unix_nanos
                    });
                    let (start, value, aggregation_temporality) = if delta_temporality {
                        match self
                            .last_values
                            .insert(key.clone(), (now_unix_nanos, value))
                        {
                            Some((last_time, last_value)) => {
                                (last_time, value - last_value, AggregationTemporality::Delta)
                            }
                            None => (added, value, AggregationTemporality::Delta),
                        }
                    } else {
                        (added, value, AggregationTemporality::Cumulative)
                    };
                    MetricData::Sum {
                        data_points: vec![NumberDataPoint {
                            attributes,
                            start_time_unix_nano: start,
                            time_unix_nano: now_unix_nanos,
                            value: Some(NumberValue::Double(value)),
                        }],
                        aggregation_temporality,
                        is_monotonic: true,
                    }
                }
            };
            resource_metrics.push(ResourceMetrics {
                resource: Some(self.resource.clone()),
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: key.name,
                        data: Some(data),
                    }],
                }],
            });
        }
        MetricsData { resource_metrics }
    }
}

const TWO_POW_32: f64 = 4_294_967_296.0;

/// The nearest `f64`, as Java's `long` to `double` conversion gives it.
fn u64_to_f64(value: u64) -> f64 {
    let high = u32::try_from(value >> 32).unwrap_or(u32::MAX);
    let low = u32::try_from(value & 0xffff_ffff).unwrap_or(u32::MAX);
    f64::from(high).mul_add(TWO_POW_32, f64::from(low))
}

fn i64_to_f64(value: i64) -> f64 {
    let high = i32::try_from(value >> 32).unwrap_or(0);
    let low = u32::try_from(value & 0xffff_ffff).unwrap_or(0);
    f64::from(high).mul_add(TWO_POW_32, f64::from(low))
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn metric_keys_follow_kafkas_naming_convention() {
        let cases = [
            (
                ("producer-metrics", "record-send-rate", vec![]),
                "org.apache.kafka.producer.record.send.rate",
                vec![],
            ),
            (
                ("admin-client-metrics", "request-total", vec![]),
                "org.apache.kafka.admin.client.request.total",
                vec![],
            ),
            (
                (
                    "consumer-fetch-manager-metrics",
                    "Records-Lag-Max",
                    vec![("client-id", "c"), ("topic", "t")],
                ),
                "org.apache.kafka.consumer.fetch.manager.records.lag.max",
                vec![("client_id", "c"), ("topic", "t")],
            ),
            (
                (
                    "producer-node-metrics",
                    "request-latency-avg",
                    vec![("node-id", "node-1")],
                ),
                "org.apache.kafka.producer.node.request.latency.avg",
                vec![("node_id", "node-1")],
            ),
        ];
        for ((group, name, tags), expected_name, expected_tags) in cases {
            let expected = MetricKey {
                name: expected_name.to_owned(),
                tags: expected_tags
                    .into_iter()
                    .map(|(key, value)| (key.to_owned(), value.to_owned()))
                    .collect(),
            };
            assert!(MetricKey::kafka(group, name, tags) == expected);
        }
    }

    #[test]
    fn selector_follows_the_requested_metrics() {
        let name = "org.apache.kafka.producer.request.total";
        let cases: [(&[&str], MetricSelector, bool); 5] = [
            (&[], MetricSelector::NoMetrics, false),
            (&["*"], MetricSelector::AllMetrics, true),
            (
                &["org.apache.kafka.producer"],
                MetricSelector::Prefixes(vec!["org.apache.kafka.producer".into()]),
                true,
            ),
            (
                &["org.apache.kafka.consumer", "*"],
                MetricSelector::Prefixes(vec!["org.apache.kafka.consumer".into(), "*".into()]),
                false,
            ),
            (&[""], MetricSelector::Prefixes(vec![String::new()]), true),
        ];
        for (requested, expected, matches) in cases {
            let requested = requested
                .iter()
                .map(|s| (*s).to_owned())
                .collect::<Vec<_>>();
            let selector = MetricSelector::from_requested(&requested);
            assert!(selector == expected, "{requested:?}");
            assert!(selector.matches(name) == matches, "{requested:?}");
        }
    }

    #[test]
    fn numbers_convert_to_the_nearest_double() {
        for value in [0u64, 1, 42, u64::from(u32::MAX) + 1, 1 << 53, u64::MAX] {
            assert!(
                u64_to_f64(value).to_bits() == format!("{value}").parse::<f64>().unwrap().to_bits()
            );
        }
        for value in [0i64, 1, -1, i64::from(i32::MIN) - 5, i64::MAX, i64::MIN] {
            assert!(
                i64_to_f64(value).to_bits() == format!("{value}").parse::<f64>().unwrap().to_bits()
            );
        }
    }

    fn sum_point(
        name: &str,
        start: u64,
        time: u64,
        value: f64,
        aggregation_temporality: AggregationTemporality,
    ) -> ResourceMetrics {
        ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![KeyValue::new("transactional_id", "tx")],
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: name.to_owned(),
                    data: Some(MetricData::Sum {
                        data_points: vec![NumberDataPoint {
                            attributes: vec![KeyValue::new("topic", "t")],
                            start_time_unix_nano: start,
                            time_unix_nano: time,
                            value: Some(NumberValue::Double(value)),
                        }],
                        aggregation_temporality,
                        is_monotonic: true,
                    }),
                }],
            }],
        }
    }

    /// A counter at 5 then 12: cumulative pushes carry the total from the
    /// registration, delta pushes the change since the last push, and a
    /// temporality change starts the sums again.
    #[test]
    fn collector_pushes_sums_with_the_subscribed_temporality() {
        let metrics = ClientMetrics::new();
        let key = MetricKey::kafka(
            "producer-topic-metrics",
            "record-send-total",
            [("client-id", "p"), ("topic", "t")],
        );
        let name = key.name.clone();
        let counter = metrics.counter(key.clone());
        let added = metrics.lock()[&key].added_unix_nanos;
        let mut collector = Collector::new(
            metrics,
            Resource {
                attributes: vec![KeyValue::new("transactional_id", "tx")],
            },
        );
        let all = MetricSelector::AllMetrics;
        let mut pushes = Vec::new();
        counter.add(5);
        pushes.push(collector.collect(&all, false, 100));
        counter.add(7);
        pushes.push(collector.collect(&all, false, 200));
        pushes.push(collector.collect(&all, true, 300));
        counter.add(3);
        pushes.push(collector.collect(&all, true, 400));
        collector.reset();
        pushes.push(collector.collect(&all, false, 500));
        pushes.push(collector.collect(&MetricSelector::NoMetrics, false, 600));

        let one = |point| MetricsData {
            resource_metrics: vec![point],
        };
        let cumulative = AggregationTemporality::Cumulative;
        let delta = AggregationTemporality::Delta;
        assert!(
            pushes
                == [
                    one(sum_point(&name, added, 100, 5.0, cumulative)),
                    one(sum_point(&name, added, 200, 12.0, cumulative)),
                    one(sum_point(&name, added, 300, 12.0, delta)),
                    one(sum_point(&name, 300, 400, 3.0, delta)),
                    one(sum_point(&name, 500, 500, 15.0, cumulative)),
                    MetricsData::default(),
                ]
        );
    }

    #[test]
    fn network_metrics_count_connections_requests_and_bytes() {
        let metrics = ClientMetrics::new();
        let network = NetworkMetrics::register(&metrics, "consumer-metrics");
        let first = network.opened();
        let second = network.opened();
        first.request(96);
        first.response(20);
        second.request(10);
        drop(first);

        let mut collector = Collector::new(metrics, Resource::default());
        let values = collector
            .collect(&MetricSelector::AllMetrics, false, 1)
            .resource_metrics
            .into_iter()
            .flat_map(|resource| resource.scope_metrics)
            .flat_map(|scope| scope.metrics)
            .map(|metric| {
                let value = match metric.data {
                    Some(
                        MetricData::Gauge { data_points } | MetricData::Sum { data_points, .. },
                    ) => data_points[0].value,
                    None => None,
                };
                (metric.name, value)
            })
            .collect::<Vec<_>>();
        let expected = [
            ("connection.close.total", 1.0),
            ("connection.count", 1.0),
            ("connection.creation.total", 2.0),
            ("incoming.byte.total", 28.0),
            ("outgoing.byte.total", 114.0),
            ("request.total", 2.0),
            ("response.total", 1.0),
        ]
        .map(|(name, value)| {
            (
                format!("org.apache.kafka.consumer.{name}"),
                Some(NumberValue::Double(value)),
            )
        });
        assert!(values == expected);
    }
}
