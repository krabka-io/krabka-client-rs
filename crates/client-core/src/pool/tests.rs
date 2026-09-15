use std::{
    collections::HashMap,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use assert2::{assert, check};
use krabka_units::{millis, secs};
use tokio::time::Instant;

use super::*;

/// Stand-in connection: the address it was dialed against, and switches
/// that a test flips to close it or load it.
#[derive(Debug)]
struct StubConn {
    addr: SocketAddr,
    server_name: String,
    open: AtomicBool,
    in_flight: AtomicUsize,
}

/// One dial the connector saw.
#[derive(Clone, Debug, PartialEq)]
struct Dial {
    addr: SocketAddr,
    at: Duration,
    setup_timeout: Time,
}

/// A connector that records each dial and each resolve, fails the dials of
/// `refused`, and resolves hosts from `hosts`.
#[derive(Default)]
struct RecordingConnector {
    started: Option<Instant>,
    dials: Mutex<Vec<Dial>>,
    resolves: AtomicUsize,
    refused: Mutex<Vec<SocketAddr>>,
    hosts: Mutex<HashMap<String, Vec<SocketAddr>>>,
}

impl RecordingConnector {
    fn new() -> Self {
        Self {
            started: Some(Instant::now()),
            ..Self::default()
        }
    }

    fn refuse(self, addrs: &[SocketAddr]) -> Self {
        self.refused.lock().unwrap().extend_from_slice(addrs);
        self
    }

    fn host(self, host: &str, addrs: &[SocketAddr]) -> Self {
        self.set_host(host, addrs);
        self
    }

    fn set_host(&self, host: &str, addrs: &[SocketAddr]) {
        self.hosts
            .lock()
            .unwrap()
            .insert(host.to_owned(), addrs.to_vec());
    }

    fn dials(&self) -> Vec<Dial> {
        self.dials.lock().unwrap().clone()
    }

    fn dialed_addrs(&self) -> Vec<SocketAddr> {
        self.dials().into_iter().map(|dial| dial.addr).collect()
    }
}

#[async_trait::async_trait]
impl BrokerConnector for RecordingConnector {
    type Conn = StubConn;

    async fn dial(
        &self,
        addr: SocketAddr,
        server_name: &str,
        setup_timeout: Time,
    ) -> Result<StubConn, ClientError> {
        self.dials.lock().unwrap().push(Dial {
            addr,
            at: self
                .started
                .map(|started| started.elapsed())
                .unwrap_or_default(),
            setup_timeout,
        });
        if self.refused.lock().unwrap().contains(&addr) {
            return Err(ClientError::Connect {
                addr,
                source: std::io::Error::from(std::io::ErrorKind::ConnectionRefused),
            });
        }
        Ok(StubConn {
            addr,
            server_name: server_name.to_owned(),
            open: AtomicBool::new(true),
            in_flight: AtomicUsize::new(0),
        })
    }

    async fn resolve(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>, ClientError> {
        self.resolves.fetch_add(1, Ordering::SeqCst);
        let known = self.hosts.lock().unwrap().get(host).cloned();
        Ok(known.unwrap_or_else(|| {
            host.parse::<std::net::IpAddr>()
                .map(|ip| vec![SocketAddr::new(ip, port)])
                .unwrap_or_default()
        }))
    }

    fn is_open(connection: &StubConn) -> bool {
        connection.open.load(Ordering::SeqCst)
    }

    fn in_flight(connection: &StubConn) -> usize {
        connection.in_flight.load(Ordering::SeqCst)
    }
}

fn addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

fn pool(bootstrap: &[SocketAddr], connector: RecordingConnector) -> BrokerPool<RecordingConnector> {
    BrokerPool::with_connector_and_names(
        bootstrap
            .iter()
            .map(|address| (*address, address.ip().to_string()))
            .collect(),
        connector,
        ConnectPolicy::new(&ConnectionOptions::default()),
    )
}

fn broker(id: i32, host: &str, port: u16) -> BrokerInfo {
    BrokerInfo {
        id,
        host: host.to_owned(),
        port: i32::from(port),
        rack: None,
    }
}

/// Call `get` in a tight loop for `window`, as a retry path with no backoff
/// of its own does.
async fn hammer(pool: &BrokerPool<RecordingConnector>, broker_id: i32, window: Duration) {
    let started = Instant::now();
    while started.elapsed() < window {
        let _ = pool.get(broker_id).await;
    }
}

/// The gaps between dials, in milliseconds.
fn gaps(dials: &[Dial]) -> Vec<u128> {
    dials
        .windows(2)
        .map(|pair| pair[1].at.saturating_sub(pair[0].at).as_millis())
        .collect()
}

/// Kafka's `ClusterConnectionStates.updateReconnectBackoff`: 50 ms times
/// 2^failures, with 20% jitter, up to 1 s. A broker that refuses gets a
/// handful of dials, not one per call.
#[tokio::test(start_paused = true)]
async fn a_refusing_broker_is_dialed_with_exponential_backoff() {
    let connector = RecordingConnector::new().refuse(&[addr(9092)]);
    let pool = pool(&[], connector);
    pool.refresh_brokers(&[broker(1, "127.0.0.1", 9092)]).await;

    hammer(&pool, 1, Duration::from_millis(200)).await;
    let dials = pool
        .connector
        .dials()
        .into_iter()
        .filter(|dial| dial.at < Duration::from_millis(200))
        .collect::<Vec<_>>();
    let short = gaps(&dials);
    check!(
        dials.len() == 3,
        "dials at 0, about 50 and about 150 ms: {short:?}"
    );
    check!((40..=60).contains(&short[0]), "{short:?}");
    check!((80..=120).contains(&short[1]), "{short:?}");

    hammer(&pool, 1, Duration::from_secs(10)).await;
    let long = gaps(&pool.connector.dials());
    check!(
        long.iter().all(|gap| *gap <= 1000),
        "the backoff cap is 1 s: {long:?}"
    );
    check!(long.iter().rev().take(5).all(|gap| *gap >= 800), "{long:?}");
}

/// Kafka's `ClusterConnectionStates.updateConnectionSetupTimeout`: 10 s
/// times 2^failures, with 20% jitter, up to 30 s. A connection resets both
/// backoffs (`ready`).
#[tokio::test(start_paused = true)]
async fn the_connection_setup_timeout_grows_per_failed_attempt_and_resets() {
    let connector = RecordingConnector::new().refuse(&[addr(9092)]);
    let pool = pool(&[], connector);
    pool.refresh_brokers(&[broker(1, "127.0.0.1", 9092)]).await;
    for _ in 0..4 {
        let _ = pool.get(1).await;
    }
    pool.connector.refused.lock().unwrap().clear();
    let connection = pool.get(1).await.unwrap();
    connection.open.store(false, Ordering::SeqCst);
    pool.get(1).await.unwrap();

    let timeouts = pool
        .connector
        .dials()
        .into_iter()
        .map(|dial| dial.setup_timeout.to_std().as_millis())
        .collect::<Vec<_>>();
    let ranges = [
        8_000..=12_000,
        16_000..=24_000,
        24_000..=30_000,
        24_000..=30_000,
        24_000..=30_000,
        8_000..=12_000,
    ];
    check!(timeouts.len() == ranges.len(), "{timeouts:?}");
    for (timeout, range) in timeouts.iter().zip(ranges) {
        check!(range.contains(timeout), "{timeouts:?}");
    }
}

/// Kafka's `client.dns.lookup=use_all_dns_ips`: a failed connection moves to
/// the next resolved address (`moveToNextAddress`), and the list is resolved
/// again after a disconnect.
#[tokio::test(start_paused = true)]
async fn a_host_with_two_addresses_is_dialed_in_turn_and_resolved_again() {
    let (a, b) = (addr(1111), addr(2222));
    let connector = RecordingConnector::new()
        .host("broker-1.example", &[a, b])
        .refuse(&[a]);
    let pool = pool(&[], connector);
    pool.refresh_brokers(&[broker(1, "broker-1.example", 9092)])
        .await;

    check!(pool.get(1).await.is_err());
    let connection = pool.get(1).await.unwrap();
    check!(connection.addr == b);
    check!(connection.server_name == "broker-1.example");
    check!(pool.connector.resolves.load(Ordering::SeqCst) == 1);

    // After a disconnect the pool resolves again, and skips the address that
    // it used last when the new list starts with it.
    pool.connector.set_host("broker-1.example", &[b, a]);
    pool.evict(1);
    let _ = pool.get(1).await;
    check!(pool.connector.resolves.load(Ordering::SeqCst) == 2);
    check!(pool.connector.dialed_addrs() == vec![a, b, a]);
}

#[tokio::test(start_paused = true)]
async fn a_closed_connection_is_replaced_after_the_backoff() {
    let pool = pool(&[], RecordingConnector::new());
    pool.refresh_brokers(&[broker(1, "127.0.0.1", 9092)]).await;
    let first = pool.get(1).await.unwrap();
    check!(Arc::ptr_eq(&first, &pool.get(1).await.unwrap()));

    first.open.store(false, Ordering::SeqCst);
    let started = Instant::now();
    let second = pool.get(1).await.unwrap();
    check!(!Arc::ptr_eq(&first, &second));
    check!((40..=60).contains(&started.elapsed().as_millis()));
    check!(pool.connector.dials().len() == 2);
}

#[tokio::test(start_paused = true)]
async fn concurrent_users_of_one_broker_share_one_dial() {
    let pool = pool(&[], RecordingConnector::new());
    pool.refresh_brokers(&[broker(1, "127.0.0.1", 9092)]).await;
    let (a, b, c) = tokio::join!(pool.get(1), pool.get(1), pool.get(1));
    check!(Arc::ptr_eq(&a.unwrap(), &b.unwrap()));
    check!(c.is_ok());
    check!(pool.connector.dials().len() == 1);
}

/// Kafka's `NetworkClient.leastLoadedNode`. Each row gives the connection of
/// brokers 1, 2 and 3 as `(open, in flight)`, or `None` for no connection.
#[tokio::test(start_paused = true)]
async fn least_loaded_prefers_an_idle_connection_then_the_fewest_in_flight_then_a_new_one() {
    type Connections = [Option<(bool, usize)>; 3];
    let cases: [(&str, bool, Connections, SocketAddr, usize); 4] = [
        (
            "an open connection with nothing in flight",
            true,
            [Some((true, 3)), Some((true, 0)), None],
            addr(9002),
            2,
        ),
        (
            "the fewest in flight",
            true,
            [Some((true, 3)), Some((true, 1)), Some((false, 0))],
            addr(9002),
            3,
        ),
        (
            "a broker with no connection when none is open",
            true,
            [Some((false, 0)), None, Some((false, 0))],
            addr(9002),
            3,
        ),
        (
            "no known broker uses the bootstrap address",
            false,
            [None, None, None],
            addr(1000),
            1,
        ),
    ];
    for (name, known, connections, expected, expected_dials) in cases {
        let pool = pool(&[addr(1000)], RecordingConnector::new());
        if known {
            pool.refresh_brokers(&[
                broker(1, "127.0.0.1", 9001),
                broker(2, "127.0.0.1", 9002),
                broker(3, "127.0.0.1", 9003),
            ])
            .await;
        }
        for (id, connection) in (1..).zip(connections) {
            if let Some((open, in_flight)) = connection {
                let connection = pool.get(id).await.unwrap();
                connection.open.store(open, Ordering::SeqCst);
                connection.in_flight.store(in_flight, Ordering::SeqCst);
            }
        }
        let chosen = pool.least_loaded().await.unwrap();
        check!(chosen.addr == expected, "{name}");
        check!(pool.connector.dials().len() == expected_dials, "{name}");
    }
}

#[tokio::test(start_paused = true)]
async fn bootstrap_connection_caches_and_skips_dead_addresses() {
    let pool = pool(
        &[addr(1111), addr(2222)],
        RecordingConnector::new().refuse(&[addr(1111)]),
    );
    let boot = pool.bootstrap_connection().await.unwrap();
    assert!(boot.addr == addr(2222));
    let again = pool.bootstrap_connection().await.unwrap();
    check!(Arc::ptr_eq(&boot, &again));
    check!(pool.connector.dialed_addrs() == vec![addr(1111), addr(2222)]);
}

#[tokio::test(start_paused = true)]
async fn bootstrap_connection_backs_off_after_every_address_fails() {
    let pool = pool(
        &[addr(1111), addr(2222)],
        RecordingConnector::new().refuse(&[addr(1111), addr(2222)]),
    );
    check!(pool.bootstrap_connection().await.is_err());
    let started = Instant::now();
    check!(pool.bootstrap_connection().await.is_err());
    check!((40..=60).contains(&started.elapsed().as_millis()));
    check!(pool.connector.dialed_addrs() == vec![addr(1111), addr(2222), addr(1111), addr(2222)]);
}

#[tokio::test(start_paused = true)]
async fn evict_forces_reconnect_only_for_that_id() {
    let pool = pool(&[addr(2222)], RecordingConnector::new());
    pool.refresh_brokers(&[broker(1, "127.0.0.1", 9092), broker(2, "127.0.0.1", 9093)])
        .await;
    let one = pool.get(1).await.unwrap();
    let two = pool.get(2).await.unwrap();
    let boot = pool.bootstrap_connection().await.unwrap();

    pool.evict(1);
    check!(!Arc::ptr_eq(&one, &pool.get(1).await.unwrap()));
    check!(Arc::ptr_eq(&two, &pool.get(2).await.unwrap()));
    check!(Arc::ptr_eq(
        &boot,
        &pool.bootstrap_connection().await.unwrap()
    ));

    pool.evict_bootstrap();
    check!(!Arc::ptr_eq(
        &boot,
        &pool.bootstrap_connection().await.unwrap()
    ));
    check!(pool.connector.dials().len() == 5);
}

#[tokio::test(start_paused = true)]
async fn replace_bootstrap_addresses_forces_redial_to_new_address() {
    let pool = pool(&[addr(1111)], RecordingConnector::new());
    check!(pool.bootstrap_connection().await.unwrap().addr == addr(1111));
    pool.replace_bootstrap(vec![addr(2222)]);
    check!(pool.bootstrap_connection().await.unwrap().addr == addr(2222));
}

#[tokio::test(start_paused = true)]
async fn rebootstrap_discards_stale_connections_and_broker_addresses() {
    let pool = pool(&[addr(1111)], RecordingConnector::new());
    pool.refresh_brokers(&[broker(3, "127.0.0.1", 3333)]).await;
    let held_broker = pool.get(3).await.unwrap();
    let _ = pool.bootstrap_connection().await.unwrap();

    pool.rebootstrap(vec![addr(2222)]);

    check!(!pool.knows_broker(3));
    check!(pool.nodes.is_empty());
    check!(Arc::strong_count(&held_broker) == 1);
    check!(pool.bootstrap_connection().await.unwrap().addr == addr(2222));
}

#[tokio::test(start_paused = true)]
async fn refresh_keeps_hosts_skips_undialable_ports_and_drops_a_moved_broker() {
    let pool = pool(&[], RecordingConnector::new());
    pool.refresh_brokers(&[
        broker(1, "127.0.0.1", 9092),
        broker(2, "127.0.0.1", 0),
        BrokerInfo {
            port: -1,
            ..broker(3, "127.0.0.1", 1)
        },
        broker(7, "localhost", 9092),
    ])
    .await;
    check!(pool.broker_ids() == vec![1, 7]);
    check!(pool.by_endpoint.get(&7).unwrap().clone() == ("localhost".to_owned(), 9092));
    check!(pool.connector.resolves.load(Ordering::SeqCst) == 0);

    let before = pool.get(1).await.unwrap();
    pool.refresh_brokers(&[broker(1, "127.0.0.1", 9092)]).await;
    check!(Arc::ptr_eq(&before, &pool.get(1).await.unwrap()));
    pool.refresh_brokers(&[broker(1, "127.0.0.1", 9093)]).await;
    check!(pool.get(1).await.unwrap().addr == addr(9093));
}

#[tokio::test(start_paused = true)]
async fn an_unknown_broker_id_is_disconnected_without_a_dial() {
    let pool = pool(&[], RecordingConnector::new());
    check!(matches!(pool.get(5).await, Err(ClientError::Disconnected)));
    check!(pool.connector.dials().is_empty());
}

#[tokio::test(start_paused = true)]
async fn close_all_releases_every_cached_connection() {
    let pool = pool(&[addr(2222)], RecordingConnector::new());
    pool.refresh_brokers(&[broker(1, "127.0.0.1", 9092)]).await;
    let held = pool.get(1).await.unwrap();
    let _ = pool.bootstrap_connection().await.unwrap();
    check!(Arc::strong_count(&held) == 2);
    pool.close_all();
    check!(Arc::strong_count(&held) == 1);
}

#[test]
fn pool_uses_the_kafka_connection_backoffs_of_the_options() {
    let options = ConnectionOptions {
        reconnect_backoff: millis(10),
        reconnect_backoff_max: millis(80),
        socket_connection_setup_timeout: secs(1),
        socket_connection_setup_timeout_max: secs(4),
        ..ConnectionOptions::default()
    };
    let policy = ConnectPolicy::new(&options);
    check!(policy.reconnect == ExponentialBackoff::kafka(millis(10), millis(80)));
    check!(policy.setup_timeout == ExponentialBackoff::kafka(secs(1), secs(4)));
}
