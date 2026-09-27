//! The connector hook of `krabka_client_core::transport`, end to end.
//!
//! `install_connector` is process-global, so these tests have a test binary
//! of their own. The first test of a process installs a connector that
//! routes each dialed `host:port` to a local listener that a test
//! registered. A dial to a host name that does not resolve, or to an address
//! that no machine answers, can thus connect only through the connector.

use std::{
    collections::HashMap,
    io::{self, Read as _},
    net::{SocketAddr, TcpListener},
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use assert2::{assert, check};
use bytes::BytesMut;
use futures_util::{SinkExt as _, StreamExt as _};
use krabka_client_core::{
    ClientError, Connection, ConnectionOptions,
    transport::{
        ConnectFuture, Connector, ConnectorAlreadyInstalled, SocketOptions, dial, install_connector,
    },
};
use krabka_protocol::{
    Encode as _,
    owned::{
        api_versions_request::{self, ApiVersionsRequest},
        api_versions_response::{ApiVersion, ApiVersionsResponse},
    },
};
use krabka_units::millis;
use tokio::{io::AsyncWriteExt as _, net::TcpStream};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

// Where the router sends a dial.
#[derive(Clone, Copy)]
enum Route {
    // Connect to this local listener.
    Listener(SocketAddr),
    // Never connect: the future of the connection stays pending.
    Stall,
}

// The routes and the dials of this test process.
#[derive(Default)]
struct Router {
    routes: Mutex<HashMap<(String, u16), Route>>,
    dials: Mutex<Vec<(String, u16)>>,
}

// The first use installs `RouterConnector` as the connector of the process.
static ROUTER: LazyLock<Router> = LazyLock::new(|| {
    install_connector(Box::new(RouterConnector)).expect("no other connector in this test process");
    Router::default()
});

// Connects each dial as `ROUTER` routes it.
struct RouterConnector;

impl Connector for RouterConnector {
    fn connect(&self, host: &str, port: u16) -> ConnectFuture {
        let key = (host.to_owned(), port);
        ROUTER.dials.lock().unwrap().push(key.clone());
        let route = ROUTER.routes.lock().unwrap().get(&key).copied();
        Box::pin(async move {
            match route {
                Some(Route::Listener(target)) => TcpStream::connect(target).await,
                Some(Route::Stall) => std::future::pending().await,
                None => Err(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    format!("no route to {}:{}", key.0, key.1),
                )),
            }
        })
    }
}

// A connector that refuses every dial.
struct Refusing;

impl Connector for Refusing {
    fn connect(&self, _host: &str, _port: u16) -> ConnectFuture {
        Box::pin(std::future::ready(Err(io::Error::other(
            "the refused connector is in use",
        ))))
    }
}

// Route `host:port` to a new local listener, and return the listener.
fn listen(host: &str, port: u16) -> TcpListener {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let local = listener.local_addr().unwrap();
    route(host, port, Route::Listener(local));
    listener
}

fn route(host: &str, port: u16, route: Route) {
    ROUTER
        .routes
        .lock()
        .unwrap()
        .insert((host.to_owned(), port), route);
}

// The dials of this process to `host`, in order.
fn dials_to(host: &str) -> Vec<(String, u16)> {
    ROUTER
        .dials
        .lock()
        .unwrap()
        .iter()
        .filter(|(dialed, _)| dialed == host)
        .cloned()
        .collect()
}

fn api_versions() -> ApiVersionsResponse {
    ApiVersionsResponse {
        api_keys: vec![ApiVersion {
            api_key: api_versions_request::API_KEY,
            min_version: 0,
            max_version: api_versions_request::MAX_VERSION,
            ..Default::default()
        }],
        ..Default::default()
    }
}

// Answer each `ApiVersions` request on the first connection to `listener`.
async fn serve_api_versions(listener: TcpListener) {
    listener.set_nonblocking(true).unwrap();
    let listener = tokio::net::TcpListener::from_std(listener).unwrap();
    let (stream, _) = listener.accept().await.unwrap();
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
    while let Some(Ok(request)) = framed.next().await {
        // A request header starts with the API key, the API version and the
        // correlation id.
        let version = i16::from_be_bytes([request[2], request[3]]);
        let mut response = BytesMut::from(&request[4..8]);
        api_versions().encode(&mut response, version).unwrap();
        if framed.send(response.freeze()).await.is_err() {
            break;
        }
    }
}

#[tokio::test]
async fn dial_takes_the_socket_from_the_installed_connector() {
    // A `.test` name does not resolve (RFC 6761).
    let listener = listen("broker.connector.test", 9092);

    let mut stream = dial("broker.connector.test", 9092, SocketOptions::default())
        .await
        .unwrap();
    stream.write_all(b"ping").await.unwrap();
    let (mut accepted, _) = listener.accept().unwrap();
    let mut received = [0; 4];
    accepted.read_exact(&mut received).unwrap();

    check!(&received == b"ping");
    check!(dials_to("broker.connector.test") == vec![("broker.connector.test".to_owned(), 9092)]);
}

#[tokio::test]
async fn a_connection_round_trip_goes_through_the_connector() {
    // No machine answers TEST-NET-1 (RFC 5737).
    let addr: SocketAddr = "192.0.2.10:9092".parse().unwrap();
    let broker = tokio::spawn(serve_api_versions(listen("192.0.2.10", 9092)));

    let connection = Connection::connect(addr, ConnectionOptions::default())
        .await
        .unwrap();
    let response = connection
        .send(ApiVersionsRequest::default())
        .await
        .unwrap();

    check!(response == api_versions());
    check!(dials_to("192.0.2.10") == vec![("192.0.2.10".to_owned(), 9092)]);
    connection.close();
    broker.abort();
}

#[tokio::test]
async fn a_connector_error_fails_the_dial_and_the_connection() {
    LazyLock::force(&ROUTER);
    let addr: SocketAddr = "192.0.2.99:9092".parse().unwrap();

    let dialed = dial("unrouted.connector.test", 9092, SocketOptions::default()).await;
    let connected = Connection::connect(addr, ConnectionOptions::default()).await;

    assert!(let Err(dial_error) = dialed);
    check!(dial_error.kind() == io::ErrorKind::ConnectionRefused);
    assert!(let Err(ClientError::Connect { addr: failed, source }) = connected);
    check!((failed, source.kind()) == (addr, io::ErrorKind::ConnectionRefused));
}

#[tokio::test(start_paused = true)]
async fn a_stalled_connector_blocks_neither_the_runtime_nor_the_deadlines() {
    route("stalled.connector.test", 9092, Route::Stall);
    route("192.0.2.20", 9092, Route::Stall);
    let ticks = Arc::new(AtomicU32::new(0));
    let ticker = tokio::spawn({
        let ticks = Arc::clone(&ticks);
        async move {
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                ticks.fetch_add(1, Ordering::Relaxed);
            }
        }
    });
    let started = tokio::time::Instant::now();

    let dialed = tokio::time::timeout(
        Duration::from_millis(100),
        dial("stalled.connector.test", 9092, SocketOptions::default()),
    )
    .await;
    let connected = Connection::connect(
        "192.0.2.20:9092".parse().unwrap(),
        ConnectionOptions {
            socket_connection_setup_timeout: millis(100),
            ..ConnectionOptions::default()
        },
    )
    .await;
    ticker.abort();

    check!(dialed.is_err());
    assert!(let Err(ClientError::Timeout(timeout)) = connected);
    check!(timeout == millis(100));
    check!(started.elapsed() == Duration::from_millis(200));
    // The ticker ran while both dials waited.
    check!(ticks.load(Ordering::Relaxed) >= 18);
}

#[tokio::test]
async fn a_second_connector_is_refused_and_the_first_stays() {
    let listener = listen("second.connector.test", 9092);

    let installed = install_connector(Box::new(Refusing));
    let dialed = dial("second.connector.test", 9092, SocketOptions::default()).await;

    check!(installed == Err(ConnectorAlreadyInstalled));
    check!(dialed.is_ok());
    check!(dials_to("second.connector.test") == vec![("second.connector.test".to_owned(), 9092)]);
    drop(listener);
}
