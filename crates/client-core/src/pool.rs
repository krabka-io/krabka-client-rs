//! `BrokerPool`: one connection per broker id, opened on first use.
//!
//! The pool follows Kafka's `ClusterConnectionStates`: a reconnect backoff
//! after each failure or disconnect, a connection setup timeout that grows
//! per failed attempt, and a turn through every resolved address of a broker.

use std::{
    net::SocketAddr,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
};

use dashmap::DashMap;
use krabka_units::{Time, convert::TimeExt as _};

use crate::{
    backoff::ExponentialBackoff,
    bootstrap::{bounded_lookup, filter_preferred_addresses},
    connection::{Connection, ConnectionOptions},
    error::ClientError,
};

/// Information about a single Kafka broker, as reported by a `MetadataResponse`.
#[derive(Debug, Clone)]
pub struct BrokerInfo {
    pub id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

/// The live-IO dependency of [`BrokerPool`]: resolve a host and dial an
/// address.
///
/// A trait hides this dependency, so the pool's caching, backoff, address
/// rotation and eviction logic is testable without a socket. The test
/// connector hands back a stand-in connection type and records each dial.
#[async_trait::async_trait]
pub trait BrokerConnector: Send + Sync {
    /// Connection handle this connector produces. It is `Connection` in
    /// production and a cheap stand-in in tests.
    type Conn: Send + Sync;

    /// Dial `addr` with its pre-DNS `server_name`. `setup_timeout` is the TCP
    /// connect deadline of this attempt.
    async fn dial(
        &self,
        addr: SocketAddr,
        server_name: &str,
        setup_timeout: Time,
    ) -> Result<Self::Conn, ClientError>;

    /// Resolve `host` to the addresses to try, in order.
    async fn resolve(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>, ClientError>;

    /// Whether `connection` can still carry requests.
    fn is_open(connection: &Self::Conn) -> bool;

    /// The number of requests that wait for a response on `connection`.
    fn in_flight(connection: &Self::Conn) -> usize;
}

/// Production [`BrokerConnector`]: opens a real [`Connection`] that honours
/// the pool's TLS/SASL policy.
#[derive(Debug)]
pub struct TcpConnector {
    options: ConnectionOptions,
}

#[async_trait::async_trait]
impl BrokerConnector for TcpConnector {
    type Conn = Connection;

    #[tracing::instrument(level = "debug", skip_all, fields(addr = %addr), err)]
    async fn dial(
        &self,
        addr: SocketAddr,
        server_name: &str,
        setup_timeout: Time,
    ) -> Result<Connection, ClientError> {
        let mut options = self.options.clone();
        options.socket_connection_setup_timeout = setup_timeout;
        if let Some(security) = options.security.as_mut() {
            **security = security.for_target_host(server_name);
        }
        Connection::connect_with_options(addr, options).await
    }

    async fn resolve(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>, ClientError> {
        let timeout = self.options.dns_timeout;
        let addresses = bounded_lookup(timeout, tokio::net::lookup_host((host, port)))
            .await
            .map_err(|_| ClientError::Timeout(timeout.time()))??;
        Ok(filter_preferred_addresses(addresses))
    }

    fn is_open(connection: &Connection) -> bool {
        !connection.is_closed()
    }

    fn in_flight(connection: &Connection) -> usize {
        connection.in_flight()
    }
}

/// Synthetic broker id under which the shared bootstrap connection is cached.
/// Never a real Kafka node id (those are `>= 0`).
const BOOTSTRAP_ID: i32 = -1;

/// Kafka's reconnect backoff and connection setup timeout for one pool.
#[derive(Clone, Copy, Debug)]
struct ConnectPolicy {
    reconnect: ExponentialBackoff,
    setup_timeout: ExponentialBackoff,
}

impl ConnectPolicy {
    fn new(options: &ConnectionOptions) -> Self {
        Self {
            reconnect: ExponentialBackoff::kafka(
                options.reconnect_backoff,
                options.reconnect_backoff_max,
            ),
            setup_timeout: ExponentialBackoff::kafka(
                options.socket_connection_setup_timeout,
                options.socket_connection_setup_timeout_max,
            ),
        }
    }
}

/// The connection state of one broker id, as Kafka's `NodeConnectionState`
/// holds it.
struct NodeState<C> {
    connection: Option<Arc<C>>,
    /// The resolved addresses with their TLS server names. The pool resolves
    /// them again when the list is empty.
    addresses: Vec<(SocketAddr, String)>,
    address_index: usize,
    last_attempted: Option<SocketAddr>,
    /// When the last connection attempt started.
    last_attempt_at: Option<tokio::time::Instant>,
    /// Failures in a row, the exponent of the reconnect backoff.
    failed_attempts: u32,
    /// Failed connection attempts in a row, the exponent of the setup timeout.
    failed_connect_attempts: u32,
    /// The pool does not dial before this instant.
    retry_at: Option<tokio::time::Instant>,
}

impl<C> Default for NodeState<C> {
    fn default() -> Self {
        Self {
            connection: None,
            addresses: Vec::new(),
            address_index: 0,
            last_attempted: None,
            last_attempt_at: None,
            failed_attempts: 0,
            failed_connect_attempts: 0,
            retry_at: None,
        }
    }
}

impl<C> NodeState<C> {
    /// A connection became ready (`ClusterConnectionStates.ready`).
    fn ready(&mut self, connection: Arc<C>) {
        self.connection = Some(connection);
        self.failed_attempts = 0;
        self.failed_connect_attempts = 0;
        self.retry_at = None;
    }

    /// An open connection closed (`ClusterConnectionStates.disconnected` for
    /// a connected node). The next connection resolves the host again.
    fn disconnected(&mut self, policy: ConnectPolicy) {
        self.connection = None;
        self.addresses.clear();
        self.back_off(policy);
    }

    /// A connection attempt failed (`ClusterConnectionStates.disconnected`
    /// for a connecting node). The next attempt uses the next address, and
    /// resolves the host again after the last one.
    fn connect_failed(&mut self, policy: ConnectPolicy) {
        self.failed_connect_attempts = self.failed_connect_attempts.saturating_add(1);
        self.address_index += 1;
        if self.address_index >= self.addresses.len() {
            self.addresses.clear();
        }
        self.back_off(policy);
    }

    fn back_off(&mut self, policy: ConnectPolicy) {
        let backoff = policy.reconnect.backoff(self.failed_attempts);
        self.failed_attempts = self.failed_attempts.saturating_add(1);
        self.retry_at = Some(tokio::time::Instant::now() + backoff);
    }

    /// Use `addresses` as the new address list. As Kafka's
    /// `NodeConnectionState.resolveAddresses` does, skip the first address
    /// when it is the one that the last attempt used.
    fn set_addresses(&mut self, addresses: Vec<(SocketAddr, String)>) {
        self.address_index = usize::from(
            addresses.len() > 1 && addresses.first().map(|(addr, _)| *addr) == self.last_attempted,
        );
        self.addresses = addresses;
    }

    /// The cached connection when it is still open. A closed one counts as a
    /// disconnect.
    fn open_connection<K: BrokerConnector<Conn = C>>(
        &mut self,
        policy: ConnectPolicy,
    ) -> Option<Arc<C>> {
        match &self.connection {
            Some(connection) if K::is_open(connection) => Some(Arc::clone(connection)),
            Some(_) => {
                self.disconnected(policy);
                None
            }
            None => None,
        }
    }
}

/// One broker id: its state, and a gate that lets one task at a time make a
/// connection attempt. The state lock is never held across an `.await`.
struct Node<C> {
    state: std::sync::Mutex<NodeState<C>>,
    connect: tokio::sync::Mutex<()>,
}

impl<C> Default for Node<C> {
    fn default() -> Self {
        Self {
            state: std::sync::Mutex::new(NodeState::default()),
            connect: tokio::sync::Mutex::new(()),
        }
    }
}

impl<C> Node<C> {
    fn state(&self) -> std::sync::MutexGuard<'_, NodeState<C>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Pool of `Arc<Connection>` keyed by broker id.
///
/// The pool opens a connection lazily on first use and caches it afterwards.
/// A closed connection is replaced on the next use. After a failure or a
/// disconnect, the pool waits Kafka's reconnect backoff before it dials that
/// broker again. Concurrent users of one broker share one connection attempt.
pub struct BrokerPool<C: BrokerConnector = TcpConnector> {
    nodes: DashMap<i32, Arc<Node<C::Conn>>>,
    by_endpoint: DashMap<i32, (String, u16)>,
    bootstrap: RwLock<Vec<(SocketAddr, String)>>,
    policy: ConnectPolicy,
    connector: C,
    /// Set by a bootstrap reconnect: untargeted requests use the bootstrap
    /// connection until the pool learns brokers again.
    prefer_bootstrap: AtomicBool,
}

impl BrokerPool<TcpConnector> {
    /// Create a new pool with the given bootstrap addresses and connection options.
    #[must_use]
    pub fn new(bootstrap: Vec<SocketAddr>, options: ConnectionOptions) -> Self {
        let bootstrap = bootstrap
            .into_iter()
            .map(|address| (address, address.ip().to_string()))
            .collect();
        Self::new_with_server_names(bootstrap, options)
    }

    /// Create a pool whose bootstrap TLS names retain their pre-DNS hostnames.
    #[must_use]
    pub fn new_with_server_names(
        bootstrap: Vec<(SocketAddr, String)>,
        options: ConnectionOptions,
    ) -> Self {
        let policy = ConnectPolicy::new(&options);
        BrokerPool::with_connector_and_names(bootstrap, TcpConnector { options }, policy)
    }
}

/// A broker to dial for [`BrokerPool::least_loaded`]: (in backoff, backoff
/// end, last attempt, id). The smallest of the first three wins.
type Candidate = (
    bool,
    Option<tokio::time::Instant>,
    Option<tokio::time::Instant>,
    i32,
);

/// What a connection attempt needs, read from the node state.
struct Attempt {
    addr: SocketAddr,
    server_name: String,
    setup_timeout: Time,
}

impl<C: BrokerConnector> BrokerPool<C> {
    fn with_connector_and_names(
        bootstrap: Vec<(SocketAddr, String)>,
        connector: C,
        policy: ConnectPolicy,
    ) -> Self {
        Self {
            nodes: DashMap::new(),
            by_endpoint: DashMap::new(),
            bootstrap: RwLock::new(bootstrap),
            policy,
            connector,
            prefer_bootstrap: AtomicBool::new(false),
        }
    }

    fn node(&self, broker_id: i32) -> Arc<Node<C::Conn>> {
        Arc::clone(self.nodes.entry(broker_id).or_default().value())
    }

    /// The cached open connection of `node`.
    fn cached(&self, node: &Node<C::Conn>) -> Option<Arc<C::Conn>> {
        node.state().open_connection::<C>(self.policy)
    }

    /// Wait for the reconnect backoff of `node` to end.
    async fn wait_for_backoff(node: &Node<C::Conn>) {
        let retry_at = node.state().retry_at;
        if let Some(retry_at) = retry_at {
            tokio::time::sleep_until(retry_at).await;
        }
    }

    /// The address and setup timeout of the next attempt, or `None` when the
    /// node has no address.
    fn next_attempt(&self, node: &Node<C::Conn>) -> Option<Attempt> {
        let mut state = node.state();
        let (addr, server_name) = state.addresses.get(state.address_index).cloned()?;
        state.last_attempted = Some(addr);
        state.last_attempt_at = Some(tokio::time::Instant::now());
        Some(Attempt {
            addr,
            server_name,
            setup_timeout: Time::from_std(
                self.policy
                    .setup_timeout
                    .backoff(state.failed_connect_attempts),
            ),
        })
    }

    /// Dial one attempt and store a new connection.
    async fn dial(
        &self,
        node: &Node<C::Conn>,
        attempt: Attempt,
    ) -> Result<Arc<C::Conn>, ClientError> {
        let connection = self
            .connector
            .dial(attempt.addr, &attempt.server_name, attempt.setup_timeout)
            .await?;
        let connection = Arc::new(connection);
        node.state().ready(Arc::clone(&connection));
        Ok(connection)
    }

    /// Get-or-connect to a specific broker id. The pool must have already
    /// learned the (id, address) mapping with [`refresh_brokers`].
    ///
    /// When the broker is in its reconnect backoff, this method waits for the
    /// backoff to end before it dials. One call makes at most one connection
    /// attempt, to the next address of the broker.
    ///
    /// [`refresh_brokers`]: BrokerPool::refresh_brokers
    ///
    /// # Errors
    /// Returns [`ClientError::Disconnected`] for a broker id that the pool
    /// does not know, the resolve error, or the dial error.
    #[tracing::instrument(level = "debug", skip_all, fields(broker_id), err)]
    pub async fn get(&self, broker_id: i32) -> Result<Arc<C::Conn>, ClientError> {
        let (host, port) = self
            .by_endpoint
            .get(&broker_id)
            .map(|entry| entry.value().clone())
            .ok_or(ClientError::Disconnected)?;
        let node = self.node(broker_id);
        if let Some(connection) = self.cached(&node) {
            return Ok(connection);
        }
        let _gate = node.connect.lock().await;
        if let Some(connection) = self.cached(&node) {
            return Ok(connection);
        }
        Self::wait_for_backoff(&node).await;
        if node.state().addresses.is_empty() {
            match self.connector.resolve(&host, port).await {
                Ok(addresses) => node.state().set_addresses(
                    addresses
                        .into_iter()
                        .map(|addr| (addr, host.clone()))
                        .collect(),
                ),
                Err(error) => {
                    node.state().connect_failed(self.policy);
                    return Err(error);
                }
            }
        }
        let Some(attempt) = self.next_attempt(&node) else {
            node.state().connect_failed(self.policy);
            return Err(ClientError::Disconnected);
        };
        let result = self.dial(&node, attempt).await;
        if result.is_err() {
            node.state().connect_failed(self.policy);
        }
        result
    }

    /// Drop the cached connection to `broker_id`, if there is one, so the next
    /// [`get`](BrokerPool::get) reconnects.
    ///
    /// Call this after a send fails. A bounced or failed-over broker must not
    /// be retried over its dead, cached socket. As Kafka does after a
    /// disconnect, the next connection waits the reconnect backoff and
    /// resolves the host again.
    pub fn evict(&self, broker_id: i32) {
        if let Some(node) = self.nodes.get(&broker_id) {
            let mut state = node.state();
            if state.connection.is_some() {
                state.disconnected(self.policy);
            }
        }
    }

    /// Drop the cached bootstrap connection so the next
    /// [`bootstrap_connection`](BrokerPool::bootstrap_connection) re-iterates the
    /// bootstrap addresses and reconnects to a live broker.
    ///
    /// The pool keys the bootstrap connection by the synthetic id `-1`, which
    /// no real broker id matches, so [`evict`](BrokerPool::evict) can never
    /// reach it.
    pub fn evict_bootstrap(&self) {
        self.evict(BOOTSTRAP_ID);
    }

    /// Replace the bootstrap address list and drop the cached bootstrap
    /// connection so the next bootstrap send dials the fresh addresses.
    pub fn replace_bootstrap(&self, bootstrap: Vec<SocketAddr>) {
        let bootstrap = bootstrap
            .into_iter()
            .map(|address| (address, address.ip().to_string()))
            .collect();
        self.replace_bootstrap_with_server_names(bootstrap);
    }

    /// Replace bootstrap addresses while retaining their TLS server names.
    ///
    /// The bootstrap node keeps its reconnect backoff and failure counts, so
    /// a retry loop that calls this after each failure still backs off. Until
    /// the next [`refresh_brokers`](Self::refresh_brokers) with brokers,
    /// [`least_loaded`](Self::least_loaded) uses the bootstrap connection.
    pub fn replace_bootstrap_with_server_names(&self, bootstrap: Vec<(SocketAddr, String)>) {
        match self.bootstrap.write() {
            Ok(mut guard) => *guard = bootstrap,
            Err(poisoned) => *poisoned.into_inner() = bootstrap,
        }
        if let Some(node) = self.nodes.get(&BOOTSTRAP_ID) {
            let mut state = node.state();
            state.connection = None;
            state.addresses.clear();
            state.address_index = 0;
        }
        self.prefer_bootstrap.store(true, Ordering::Relaxed);
    }

    /// Replace bootstrap addresses and discard every connection and advertised
    /// broker address learned from stale metadata.
    pub fn rebootstrap(&self, bootstrap: Vec<SocketAddr>) {
        let bootstrap = bootstrap
            .into_iter()
            .map(|address| (address, address.ip().to_string()))
            .collect();
        self.rebootstrap_with_server_names(bootstrap);
    }

    /// Replace all broker state while retaining bootstrap TLS server names.
    pub fn rebootstrap_with_server_names(&self, bootstrap: Vec<(SocketAddr, String)>) {
        self.nodes.clear();
        self.by_endpoint.clear();
        match self.bootstrap.write() {
            Ok(mut guard) => *guard = bootstrap,
            Err(poisoned) => *poisoned.into_inner() = bootstrap,
        }
    }

    /// Get-or-connect to the first reachable bootstrap address. The bootstrap
    /// connection is cached under the synthetic broker id `-1`.
    ///
    /// One call dials each bootstrap address at most once, starting after the
    /// address that failed last. When every address fails, the next call
    /// waits the reconnect backoff first.
    ///
    /// # Errors
    /// Returns the error of the last address, an authentication failure at
    /// once, or [`ClientError::Disconnected`] for an empty address list.
    #[tracing::instrument(level = "debug", skip_all, err)]
    pub async fn bootstrap_connection(&self) -> Result<Arc<C::Conn>, ClientError> {
        let node = self.node(BOOTSTRAP_ID);
        if let Some(connection) = self.cached(&node) {
            return Ok(connection);
        }
        let _gate = node.connect.lock().await;
        if let Some(connection) = self.cached(&node) {
            return Ok(connection);
        }
        Self::wait_for_backoff(&node).await;
        let bootstrap = match self.bootstrap.read() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        let rounds = {
            let mut state = node.state();
            state.address_index %= bootstrap.len().max(1);
            state.addresses = bootstrap;
            state.addresses.len()
        };
        let mut last_error = ClientError::Disconnected;
        for _ in 0..rounds {
            let Some(attempt) = self.next_attempt(&node) else {
                break;
            };
            match self.dial(&node, attempt).await {
                Ok(connection) => return Ok(connection),
                // Kafka raises an authentication failure from the next call
                // and does not try another node (`Metadata.fatalError`).
                Err(error) if error.is_authentication_failure() => {
                    node.state().connect_failed(self.policy);
                    return Err(error);
                }
                Err(error) => last_error = error,
            }
            let mut state = node.state();
            state.address_index = (state.address_index + 1) % rounds;
        }
        {
            let mut state = node.state();
            state.failed_connect_attempts = state.failed_connect_attempts.saturating_add(1);
            state.back_off(self.policy);
        }
        Err(last_error)
    }

    /// Get-or-connect to the least loaded known broker, as Kafka's
    /// `NetworkClient.leastLoadedNode` picks the node for a request with no
    /// fixed target. Before the pool knows a broker, this is the bootstrap
    /// connection.
    ///
    /// The order of preference:
    ///
    /// 1. An open connection with no request in flight.
    /// 2. The open connection with the fewest requests in flight.
    /// 3. A broker out of its reconnect backoff, the one with the oldest
    ///    connection attempt first, and a broker never dialed before all.
    /// 4. The broker whose reconnect backoff ends first.
    ///
    /// # Errors
    /// Returns the error of [`get`](Self::get) or
    /// [`bootstrap_connection`](Self::bootstrap_connection).
    pub async fn least_loaded(&self) -> Result<Arc<C::Conn>, ClientError> {
        let ids = self.broker_ids();
        if ids.is_empty() || self.prefer_bootstrap.load(Ordering::Relaxed) {
            return self.bootstrap_connection().await;
        }
        let offset = crate::backoff::random_below(ids.len());
        let now = tokio::time::Instant::now();
        let mut fewest: Option<(usize, Arc<C::Conn>)> = None;
        let mut candidate: Option<Candidate> = None;
        for index in 0..ids.len() {
            let id = ids[(offset + index) % ids.len()];
            let node = self.node(id);
            let state = node.state();
            if let Some(connection) = state.connection.as_ref().filter(|c| C::is_open(c)) {
                let in_flight = C::in_flight(connection);
                if in_flight == 0 {
                    return Ok(Arc::clone(connection));
                }
                if fewest.as_ref().is_none_or(|(least, _)| in_flight < *least) {
                    fewest = Some((in_flight, Arc::clone(connection)));
                }
                continue;
            }
            let backoff_end = state.retry_at.filter(|retry_at| *retry_at > now);
            let key = (
                backoff_end.is_some(),
                backoff_end,
                state.last_attempt_at,
                id,
            );
            if candidate
                .as_ref()
                .is_none_or(|best| (key.0, key.1, key.2) < (best.0, best.1, best.2))
            {
                candidate = Some(key);
            }
        }
        if let Some((_, connection)) = fewest {
            return Ok(connection);
        }
        match candidate {
            // Every known broker is in its reconnect backoff. Kafka's
            // `leastLoadedNode` then has no node, and the metadata recovery
            // goes back to the bootstrap servers.
            Some((true, ..)) | None => self.bootstrap_connection().await,
            Some((false, _, _, id)) => self.get(id).await,
        }
    }

    /// Update the (id, host, port) registry from a list of brokers, which
    /// usually comes from a `MetadataResponse`. This method opens no new
    /// connections, and it does not resolve the hosts: the pool resolves a
    /// host each time it needs new addresses, as Kafka does.
    ///
    /// This method skips brokers that advertise port `0`, because that is not
    /// a dialable address. It shows up for in-process test brokers whose
    /// advertised port never got rewritten to the real bound port. When the
    /// registry leaves such an entry out, [`get`](BrokerPool::get) reports
    /// `Disconnected` for that id. The caller can then fall back to the
    /// bootstrap connection instead of trying a doomed `host:0` connect.
    ///
    /// A broker whose host or port changed loses its cached connection and
    /// addresses.
    #[tracing::instrument(level = "debug", skip_all, fields(brokers = brokers.len()))]
    pub async fn refresh_brokers(&self, brokers: &[BrokerInfo]) {
        if !brokers.is_empty() {
            self.prefer_bootstrap.store(false, Ordering::Relaxed);
        }
        for b in brokers {
            let Ok(port) = u16::try_from(b.port) else {
                continue;
            };
            if port == 0 {
                continue;
            }
            let endpoint = (b.host.clone(), port);
            let previous = self.by_endpoint.insert(b.id, endpoint.clone());
            if previous.is_some_and(|previous| previous != endpoint) {
                tracing::info!(broker_id = b.id, host = %b.host, port, "broker address changed");
                self.nodes.remove(&b.id);
            }
        }
    }

    /// Whether the registry knows a dialable address for this broker id. It
    /// knows one when [`refresh_brokers`](BrokerPool::refresh_brokers)
    /// learned it and the port was not `0`. A caller can use this to choose
    /// between a route to a specific broker and a fallback to the bootstrap
    /// connection, without a speculative connect.
    #[must_use]
    pub fn knows_broker(&self, broker_id: i32) -> bool {
        self.by_endpoint.contains_key(&broker_id)
    }

    /// Return the broker ids from the most recent usable metadata.
    ///
    /// The deterministic order keeps metadata failover predictable. Callers
    /// use this list to exhaust the last-known cluster before deciding that a
    /// fresh bootstrap is required.
    #[must_use]
    pub fn broker_ids(&self) -> Vec<i32> {
        let mut ids = self
            .by_endpoint
            .iter()
            .map(|entry| *entry.key())
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    /// Close every open connection in the pool. Consumes the pool.
    // cargo-mutants: teardown; no observable return to assert against
    #[cfg_attr(test, mutants::skip)]
    pub fn close_all(self) {
        // Dropping the pool drops each node state and its `Arc`. When the
        // last reference goes away the background tasks shut down through
        // the `CancellationToken` in `ConnectionInner`.
        drop(self);
    }
}

#[cfg(test)]
mod tests;
