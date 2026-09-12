//! Load generator for a running Kafka broker.
//!
//! This example drives sustained produce traffic, and optional fetch traffic,
//! against a broker that the operator starts separately. It prints the
//! throughput it achieved. Use it to profile the client, or to profile a
//! broker under client load.
//!
//!   cargo run --release --example loadgen -p krabka-client-producer
//!
//! Env:
//!   `LOAD_BOOTSTRAP`   broker addr (default 127.0.0.1:9092)
//!   `LOAD_TOPIC`       topic name (default loadgen)
//!   `LOAD_PARTITIONS`  partition count (default 8)
//!   `LOAD_PRODUCERS`   concurrent producer tasks (default 4)
//!   `LOAD_VALUE_BYTES` record value size (default 128)
//!   `LOAD_SECONDS`     run duration seconds (default 20)
//!   `LOAD_INFLIGHT`    max in-flight sends per producer (default 1000)
//!   `LOAD_ACKS`        0 | 1 | all (default 1)
//!   `LOAD_CONSUME`     1 to also run a consumer group (default 0)

use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use krabka_client_consumer::{AutoOffsetReset, Consumer};
use krabka_client_core::Client;
use krabka_client_producer::{Acks, Producer, ProducerRecord};
use krabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};

/// Error code 36 is `TOPIC_ALREADY_EXISTS`. A repeat run must not fail on it.
const TOPIC_ALREADY_EXISTS: i16 = 36;

/// Everything the run reads from the environment.
struct Settings {
    bootstrap: String,
    topic: String,
    partitions: i32,
    producers: usize,
    value_bytes: usize,
    seconds: u64,
    inflight: usize,
    acks_label: String,
    acks: Acks,
    consume: bool,
}

impl Settings {
    fn from_env() -> Self {
        let acks_label: String = std::env::var("LOAD_ACKS").unwrap_or_else(|_| "1".into());
        let acks = match acks_label.as_str() {
            "0" => Acks::Zero,
            "all" => Acks::All,
            _ => Acks::One,
        };
        Self {
            bootstrap: std::env::var("LOAD_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into()),
            topic: std::env::var("LOAD_TOPIC").unwrap_or_else(|_| "loadgen".into()),
            partitions: env("LOAD_PARTITIONS", 8),
            producers: env("LOAD_PRODUCERS", 4),
            value_bytes: env("LOAD_VALUE_BYTES", 128),
            seconds: env("LOAD_SECONDS", 20),
            inflight: env("LOAD_INFLIGHT", 1000),
            acks_label,
            acks,
            consume: env::<u8>("LOAD_CONSUME", 0) == 1,
        }
    }
}

/// Counters the tasks share, plus the flag that stops them.
#[derive(Default)]
struct Counters {
    sent: AtomicU64,
    acked: AtomicU64,
    stop: AtomicBool,
}

fn env<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let settings = Arc::new(Settings::from_env());
    create_topic(&settings).await;

    let value = Bytes::from(vec![0xAB_u8; settings.value_bytes]);
    let counters = Arc::new(Counters::default());
    let start = Instant::now();

    let mut handles = Vec::new();
    for index in 0..settings.producers {
        handles.push(tokio::spawn(run_producer(
            index,
            Arc::clone(&settings),
            Arc::clone(&counters),
            value.clone(),
        )));
    }
    if settings.consume {
        handles.push(tokio::spawn(run_consumer(
            Arc::clone(&settings),
            Arc::clone(&counters),
        )));
    }

    tokio::time::sleep(Duration::from_secs(settings.seconds)).await;
    counters.stop.store(true, Ordering::Relaxed);
    for handle in handles {
        let _ = handle.await;
    }

    report(&settings, &counters, start.elapsed());
}

/// Create the topic. A topic that already exists is not an error, so a repeat
/// run reuses it.
async fn create_topic(settings: &Settings) {
    let client = Client::builder()
        .bootstrap(settings.bootstrap.clone())
        .client_id("loadgen-admin")
        .build()
        .await
        .expect("admin client");
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: settings.topic.clone(),
                num_partitions: settings.partitions,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 10_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    let error_code = response.topics[0].error_code;
    assert2::assert!(error_code == 0 || error_code == TOPIC_ALREADY_EXISTS);
    client.close();
}

/// Send records until the stop flag is set, and keep at most
/// `settings.inflight` sends unacknowledged.
async fn run_producer(
    index: usize,
    settings: Arc<Settings>,
    counters: Arc<Counters>,
    value: Bytes,
) {
    let producer = Producer::builder()
        .bootstrap(settings.bootstrap.clone())
        .client_id(format!("loadgen-{index}"))
        .enable_idempotence(false)
        .acks(settings.acks)
        .linger(Duration::from_millis(5))
        .build()
        .await
        .expect("producer build");

    let mut window = VecDeque::new();
    while !counters.stop.load(Ordering::Relaxed) {
        while window.len() < settings.inflight {
            let acknowledgement = producer
                .send(ProducerRecord {
                    topic: settings.topic.clone(),
                    value: Some(value.clone()),
                    ..Default::default()
                })
                .await;
            counters.sent.fetch_add(1, Ordering::Relaxed);
            window.push_back(acknowledgement);
        }
        if let Some(acknowledgement) = window.pop_front()
            && acknowledgement.await.is_ok()
        {
            counters.acked.fetch_add(1, Ordering::Relaxed);
        }
    }

    let _ = producer.flush().await;
    for acknowledgement in window {
        if acknowledgement.await.is_ok() {
            counters.acked.fetch_add(1, Ordering::Relaxed);
        }
    }
    producer.close().await.ok();
}

/// Poll one consumer group until the stop flag is set. The example discards
/// the records, because the run measures the fetch path only.
async fn run_consumer(settings: Arc<Settings>, counters: Arc<Counters>) {
    let mut consumer = Consumer::builder()
        .bootstrap(settings.bootstrap.clone())
        .client_id("loadgen-consumer")
        .group_id("loadgen-grp")
        .session_timeout(krabka_units::secs(30))
        .rebalance_timeout(krabka_units::secs(5))
        .heartbeat_interval(krabka_units::secs(1))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe([settings.topic.clone()])
        .build()
        .await
        .expect("consumer build");

    while !counters.stop.load(Ordering::Relaxed) {
        let _ = consumer.poll(krabka_units::millis(200)).await;
    }
    consumer.close().await.ok();
}

/// Print the throughput of the run.
///
/// The rates use integer arithmetic. One byte per millisecond is one kilobyte
/// per second, so the byte rate needs no scale factor.
fn report(settings: &Settings, counters: &Counters, elapsed: Duration) {
    let elapsed_ms = elapsed.as_millis().max(1);
    let sent = u128::from(counters.sent.load(Ordering::Relaxed));
    let acked = u128::from(counters.acked.load(Ordering::Relaxed));
    let value_bytes = u128::try_from(settings.value_bytes).expect("value size fits in u128");
    let messages_per_second = acked * 1_000 / elapsed_ms;
    let kilobytes_per_second = acked * value_bytes / elapsed_ms;

    let acks = &settings.acks_label;
    let producers = settings.producers;
    let value_size = settings.value_bytes;
    let partitions = settings.partitions;
    let seconds = elapsed.as_secs_f64();
    println!(
        "loadgen: acks={acks} producers={producers} value={value_size}B parts={partitions} \
         | sent={sent} acked={acked} | {messages_per_second} msg/s \
         | {kilobytes_per_second} kB/s over {seconds:.1}s"
    );
}
