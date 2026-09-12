//! Load generator for a running Kafka broker.
//!
//! This example drives sustained produce traffic, and optional fetch traffic,
//! against a broker that the operator starts separately. It prints the
//! throughput it achieved. Use it to profile the client, or to profile a
//! broker under client load.
//!
//!   cargo run --release --example loadgen -p krabka-client-producer
//!
//! The run reports a figure only when every task finished its work. A
//! configuration error, a build failure, a panic in a task, or a failed
//! consumer poll stops the run and prints the reason on stderr instead. A
//! number that came from a broken run is worse than no number.
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
    process::ExitCode,
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
use krabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    metadata_request::{MetadataRequest, MetadataRequestTopic},
};

/// Error code 36 is `TOPIC_ALREADY_EXISTS`. A repeat run must not fail on it.
const TOPIC_ALREADY_EXISTS: i16 = 36;

/// Why a run stopped early. The text goes to stderr in place of the report.
type Failure = String;

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
    /// Read the settings, and reject any value the run cannot honour.
    ///
    /// A silent fallback would let the report name a mode the producer never
    /// used, so every unreadable value is an error here.
    fn from_env() -> Result<Self, Failure> {
        let acks_label: String = std::env::var("LOAD_ACKS").unwrap_or_else(|_| "1".into());
        let acks = match acks_label.as_str() {
            "0" => Acks::Zero,
            "1" => Acks::One,
            "all" => Acks::All,
            other => return Err(format!("LOAD_ACKS must be 0, 1 or all, not {other:?}")),
        };
        Ok(Self {
            bootstrap: std::env::var("LOAD_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into()),
            topic: std::env::var("LOAD_TOPIC").unwrap_or_else(|_| "loadgen".into()),
            partitions: env("LOAD_PARTITIONS", 8)?,
            producers: env("LOAD_PRODUCERS", 4)?,
            value_bytes: env("LOAD_VALUE_BYTES", 128)?,
            seconds: env("LOAD_SECONDS", 20)?,
            inflight: env("LOAD_INFLIGHT", 1000)?,
            acks_label,
            acks,
            consume: env::<u8>("LOAD_CONSUME", 0)? == 1,
        })
    }
}

/// Counters the tasks share, plus the flag that stops them.
///
/// `acked` counts only the records the broker acknowledged. `rejected` counts
/// the rest, so a run that the broker refused cannot present itself as
/// throughput.
#[derive(Default)]
struct Counters {
    sent: AtomicU64,
    acked: AtomicU64,
    rejected: AtomicU64,
    consumed: AtomicU64,
    stop: AtomicBool,
}

/// Read one counter as the wide integer the rate arithmetic uses.
fn read(counter: &AtomicU64) -> u128 {
    u128::from(counter.load(Ordering::Relaxed))
}

fn env<T: std::str::FromStr>(key: &str, default: T) -> Result<T, Failure> {
    match std::env::var(key) {
        Err(_) => Ok(default),
        Ok(text) => text
            .parse()
            .map_err(|_| format!("{key} is not a valid value: {text:?}")),
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(reason) => {
            eprintln!("loadgen: {reason}");
            ExitCode::FAILURE
        }
    }
}

/// Drive the whole run, and report only when every task succeeded.
async fn run() -> Result<(), Failure> {
    let settings = Arc::new(Settings::from_env()?);
    create_topic(&settings).await?;

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
    let elapsed = start.elapsed();

    let mut failures: Vec<Failure> = Vec::new();
    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(reason)) => failures.push(reason),
            Err(join) => failures.push(format!("a task did not finish: {join}")),
        }
    }
    if !failures.is_empty() {
        return Err(failures.join("; "));
    }

    report(&settings, &counters, elapsed);
    Ok(())
}

/// Create the topic, or check the one that is already there.
///
/// A repeat run reuses an existing topic. It must first prove that the topic
/// has the partition count the report is going to print, because a
/// partition-scaling figure that names the wrong count is misleading.
async fn create_topic(settings: &Settings) -> Result<(), Failure> {
    let client = Client::builder()
        .bootstrap(settings.bootstrap.clone())
        .client_id("loadgen-admin")
        .build()
        .await
        .map_err(|error| format!("admin client: {error}"))?;
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
        .map_err(|error| format!("CreateTopics: {error}"))?;
    let outcome = match response.topics.first().map(|topic| topic.error_code) {
        Some(0) => Ok(()),
        Some(TOPIC_ALREADY_EXISTS) => check_partition_count(&client, settings).await,
        Some(code) => Err(format!("CreateTopics returned error code {code}")),
        None => Err("CreateTopics answered with no topic".to_string()),
    };
    client.close();
    outcome
}

/// Read the partition count of an existing topic and compare it with the one
/// the run asked for.
async fn check_partition_count(client: &Client, settings: &Settings) -> Result<(), Failure> {
    let response = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(settings.topic.clone()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .map_err(|error| format!("Metadata for {}: {error}", settings.topic))?;
    let topic = response
        .topics
        .iter()
        .find(|topic| topic.name.as_deref() == Some(settings.topic.as_str()))
        .ok_or_else(|| format!("Metadata does not list topic {}", settings.topic))?;
    if topic.error_code != 0 {
        return Err(format!(
            "Metadata for {} returned error code {}",
            settings.topic, topic.error_code
        ));
    }
    let held = i32::try_from(topic.partitions.len())
        .map_err(|_| format!("topic {} reports too many partitions", settings.topic))?;
    if held == settings.partitions {
        Ok(())
    } else {
        Err(format!(
            "topic {} has {held} partitions, but LOAD_PARTITIONS is {}. Delete the topic, or \
             set LOAD_PARTITIONS={held}",
            settings.topic, settings.partitions
        ))
    }
}

/// Send records until the stop flag is set, and keep at most
/// `settings.inflight` sends unacknowledged.
async fn run_producer(
    index: usize,
    settings: Arc<Settings>,
    counters: Arc<Counters>,
    value: Bytes,
) -> Result<(), Failure> {
    let producer = Producer::builder()
        .bootstrap(settings.bootstrap.clone())
        .client_id(format!("loadgen-{index}"))
        .enable_idempotence(false)
        .acks(settings.acks)
        .linger(Duration::from_millis(5))
        .build()
        .await
        .map_err(|error| format!("producer {index} build: {error}"))?;

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
        if let Some(acknowledgement) = window.pop_front() {
            count_acknowledgement(&counters, &acknowledgement.await);
        }
    }

    let _ = producer.flush().await;
    for acknowledgement in window {
        count_acknowledgement(&counters, &acknowledgement.await);
    }
    producer.close().await.ok();
    Ok(())
}

/// Count one acknowledgement.
///
/// The outer `Result` reports the oneshot channel, and the inner one reports
/// the broker. Only a record that satisfies both is delivered. A record the
/// broker refused, such as one above its size limit, counts as rejected.
fn count_acknowledgement<T, BrokerError, ChannelError>(
    counters: &Counters,
    acknowledgement: &Result<Result<T, BrokerError>, ChannelError>,
) {
    if matches!(acknowledgement, Ok(Ok(_))) {
        counters.acked.fetch_add(1, Ordering::Relaxed);
    } else {
        counters.rejected.fetch_add(1, Ordering::Relaxed);
    }
}

/// Poll one consumer group until the stop flag is set.
///
/// The example discards the record bodies, because the run measures the fetch
/// path only. It still counts them, and it still fails the run on a poll
/// error: a run that spent its whole time on unsuccessful fetches added no
/// fetch traffic and must not report a figure.
async fn run_consumer(settings: Arc<Settings>, counters: Arc<Counters>) -> Result<(), Failure> {
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
        .map_err(|error| format!("consumer build: {error}"))?;

    while !counters.stop.load(Ordering::Relaxed) {
        match consumer.poll(krabka_units::millis(200)).await {
            Ok(records) => {
                let count = u64::try_from(records.len()).unwrap_or(u64::MAX);
                counters.consumed.fetch_add(count, Ordering::Relaxed);
            }
            Err(error) => {
                // Stop the producers too. The run is void, so there is no
                // reason to keep loading the broker.
                counters.stop.store(true, Ordering::Relaxed);
                consumer.close().await.ok();
                return Err(format!("consumer poll: {error}"));
            }
        }
    }
    consumer.close().await.ok();
    Ok(())
}

/// Print the throughput of the run.
///
/// The rates count acknowledged records only, so a broker that refused the
/// traffic cannot inflate them. The rates use integer arithmetic. One byte per
/// millisecond is one kilobyte per second, so the byte rate needs no scale
/// factor.
fn report(settings: &Settings, counters: &Counters, elapsed: Duration) {
    let elapsed_ms = elapsed.as_millis().max(1);
    let sent = read(&counters.sent);
    let acked = read(&counters.acked);
    let rejected = read(&counters.rejected);
    let consumed = read(&counters.consumed);
    let value_bytes = u128::try_from(settings.value_bytes).expect("value size fits in u128");
    let messages_per_second = acked * 1_000 / elapsed_ms;
    let kilobytes_per_second = acked * value_bytes / elapsed_ms;

    let acks = &settings.acks_label;
    let producers = settings.producers;
    let value_size = settings.value_bytes;
    let partitions = settings.partitions;
    let seconds = elapsed.as_secs_f64();
    let fetched = if settings.consume {
        format!(" consumed={consumed}")
    } else {
        String::new()
    };
    println!(
        "loadgen: acks={acks} producers={producers} value={value_size}B parts={partitions} \
         | sent={sent} acked={acked} rejected={rejected}{fetched} | {messages_per_second} msg/s \
         | {kilobytes_per_second} kB/s over {seconds:.1}s"
    );
}
