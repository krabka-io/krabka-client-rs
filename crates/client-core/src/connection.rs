//! Single-broker `Connection`.
//!
//! A `Connection` holds a TCP socket, reader and writer tasks, and
//! correlation-ID multiplexing.

use std::{
    future::Future,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicI32, AtomicU64, Ordering},
    },
};

use bytes::{BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use krabka_ids::{ApiKey, ApiVersion};
use krabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt as _, TimeExt as _},
    kibibytes, mebibytes, millis, minutes, secs,
};
use refined_type::rule::{GreaterI64, GreaterUsize};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpSocket, TcpStream},
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{error::ClientError, request::ProtocolRequest, version::ApiVersionTable};

/// Trait alias for the duplex stream types `Connection::from_stream` accepts,
/// such as `TcpStream` and `tokio_rustls::client::TlsStream`.
///
/// The trait is boxed so callers can hand in different stream types through
/// one path.
pub trait ClientDuplex: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin + ?Sized> ClientDuplex for T {}

type Pending = Arc<DashMap<i32, oneshot::Sender<Result<Bytes, ClientError>>>>;

/// Kafka API key for `ApiVersionsRequest` / `ApiVersionsResponse`.
///
/// `Connection` uses this key to apply the response-header quirk:
/// `ApiVersionsResponse` always uses `ResponseHeader v0` (no tagged-fields
/// byte) even when the request version is flexible (v3+).
const API_VERSIONS_KEY: i16 = 18;

/// Default deadline for one client DNS lookup.
pub const DEFAULT_CLIENT_DNS_TIMEOUT: Time = secs(10);
/// Kafka's `socket.connection.setup.timeout.ms` default: the TCP connect
/// deadline of the first attempt to a broker.
pub const DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT: Time = secs(10);
/// Kafka's `socket.connection.setup.timeout.max.ms` default: the connect
/// deadline grows exponentially per failed attempt up to this value.
pub const DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX: Time = secs(30);
/// Kafka's `reconnect.backoff.ms` default.
pub const DEFAULT_RECONNECT_BACKOFF: Time = millis(50);
/// Kafka's `reconnect.backoff.max.ms` default.
pub const DEFAULT_RECONNECT_BACKOFF_MAX: Time = secs(1);
/// Kafka's producer and consumer `connections.max.idle.ms` default (9
/// minutes, below the broker's 10 minutes).
pub const DEFAULT_CONNECTIONS_MAX_IDLE: Time = minutes(9);
/// Kafka's producer, consumer and admin `send.buffer.bytes` default.
pub const DEFAULT_SEND_BUFFER: ByteSize = kibibytes(128);
/// Kafka's consumer and admin `receive.buffer.bytes` default. The producer
/// default is 32 KiB.
pub const DEFAULT_RECEIVE_BUFFER: ByteSize = kibibytes(64);
/// Default deadline for one client request.
pub const DEFAULT_CLIENT_REQUEST_TIMEOUT: Time = secs(30);
/// Default capacity of one connection's pending request dispatch queue.
pub const DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY: usize = 64;
/// Fixed security ceiling for accepted client frames.
pub const MAX_CLIENT_FRAME_BYTES: ByteSize = mebibytes(100);
/// Default maximum accepted client frame size.
pub const DEFAULT_CLIENT_FRAME_MAX: ByteSize = MAX_CLIENT_FRAME_BYTES;

/// Positive, whole-millisecond DNS lookup deadline.
///
/// Stores the validated millisecond count so policy structs can retain `Eq`
/// while public configuration boundaries use dimensioned [`Time`] values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClientDnsTimeout(i64);

impl ClientDnsTimeout {
    /// Validate a DNS lookup deadline.
    ///
    /// # Errors
    ///
    /// Returns an error when the duration is non-finite, zero, negative,
    /// fractional in milliseconds, or cannot be represented as `i64`
    /// milliseconds.
    pub fn new(value: Time) -> Result<Self, String> {
        let milliseconds = GreaterI64::<0>::new(value.millis_i64())
            .map_err(|error| format!("client DNS timeout: {error}"))?
            .into_value();
        if !value.secs_f64().is_finite() || Time::from_millis(milliseconds) != value {
            return Err("client DNS timeout must be a whole number of milliseconds".to_owned());
        }
        Ok(Self(milliseconds))
    }

    /// Return the validated timeout.
    #[must_use]
    pub fn time(self) -> Time {
        Time::from_millis(self.0)
    }

    /// Return the validated timeout in milliseconds.
    #[must_use]
    pub const fn milliseconds(self) -> i64 {
        self.0
    }
}

impl Default for ClientDnsTimeout {
    fn default() -> Self {
        Self::new(DEFAULT_CLIENT_DNS_TIMEOUT).expect("default client DNS timeout is valid")
    }
}

/// Positive capacity of one connection's pending request dispatch queue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionDispatchQueueCapacity(usize);

impl ConnectionDispatchQueueCapacity {
    /// Validate a dispatch queue capacity.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero.
    pub fn new(value: usize) -> Result<Self, String> {
        GreaterUsize::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| format!("client dispatch queue capacity: {error}"))
    }

    /// Return the validated capacity.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0
    }
}

impl Default for ConnectionDispatchQueueCapacity {
    fn default() -> Self {
        Self::new(DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY)
            .expect("default client dispatch queue capacity is valid")
    }
}

/// Positive whole-byte accepted-frame limit bounded by the fixed security ceiling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClientFrameMax(usize);

impl ClientFrameMax {
    /// Return the validated byte count.
    #[must_use]
    pub const fn bytes(self) -> usize {
        self.0
    }

    /// Return the validated limit as a dimensioned quantity.
    #[must_use]
    pub fn size(self) -> ByteSize {
        ByteSize::from_bytes(u64::try_from(self.0).unwrap_or(u64::MAX))
    }
}

impl TryFrom<ByteSize> for ClientFrameMax {
    type Error = String;

    fn try_from(value: ByteSize) -> Result<Self, Self::Error> {
        let bytes = value.bytes_f64();
        if !bytes.is_finite()
            || bytes.fract() != 0.0
            || !(1.0..=MAX_CLIENT_FRAME_BYTES.bytes_f64()).contains(&bytes)
        {
            return Err(
                "client frame max must be a positive whole-byte value no greater than 100MiB"
                    .to_owned(),
            );
        }
        usize::try_from(value.bytes_u64())
            .map(Self)
            .map_err(|_| "client frame max does not fit usize".to_owned())
    }
}

impl Default for ClientFrameMax {
    fn default() -> Self {
        Self::try_from(DEFAULT_CLIENT_FRAME_MAX).expect("default client frame max is valid")
    }
}

/// How the client resolves broker host names (Kafka `client.dns.lookup`).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ClientDnsLookup {
    /// `use_all_dns_ips`: resolve each host to all its addresses of the family
    /// of the first address, and try them in turn.
    #[default]
    UseAllDnsIps,
    /// `resolve_canonical_bootstrap_servers_only`: replace each bootstrap
    /// address with the canonical host name of its reverse lookup, then use
    /// all addresses as `UseAllDnsIps` does.
    ResolveCanonicalBootstrapServersOnly,
}

/// Connect-time + per-request configuration knobs.
#[derive(Debug, Clone)]
pub struct ConnectionOptions {
    pub client_id: String,
    pub dns_timeout: ClientDnsTimeout,
    /// The TCP connect deadline of one attempt
    /// (`socket.connection.setup.timeout.ms`). The broker pool doubles it,
    /// with jitter, for each failed attempt to the same broker, up to
    /// [`Self::socket_connection_setup_timeout_max`].
    pub socket_connection_setup_timeout: Time,
    /// `socket.connection.setup.timeout.max.ms`.
    pub socket_connection_setup_timeout_max: Time,
    /// The wait before the broker pool connects again to a broker after a
    /// failure or a disconnect (`reconnect.backoff.ms`). It doubles, with
    /// jitter, for each failure in a row.
    pub reconnect_backoff: Time,
    /// `reconnect.backoff.max.ms`.
    pub reconnect_backoff_max: Time,
    /// The connection closes after this time with no request and no response
    /// (`connections.max.idle.ms`).
    pub connections_max_idle: Time,
    /// `client.dns.lookup`.
    pub client_dns_lookup: ClientDnsLookup,
    /// The socket send buffer (`send.buffer.bytes`). `None` keeps the
    /// operating system default, as Kafka's `-1` does.
    pub send_buffer: Option<ByteSize>,
    /// The socket receive buffer (`receive.buffer.bytes`). `None` keeps the
    /// operating system default.
    pub receive_buffer: Option<ByteSize>,
    pub request_timeout: Time,
    pub dispatch_queue_capacity: ConnectionDispatchQueueCapacity,
    pub frame_max: ClientFrameMax,
    /// Client-side TLS/SASL policy. `None` = plaintext (default).
    ///
    /// This field is boxed so `ConnectionOptions` stays small. Many call
    /// sites clone `ConnectionOptions` and embed it in connection-building
    /// futures, and `ClientSecurity` carries several `String`/`PathBuf`
    /// fields that would otherwise make every such future large.
    pub security: Option<Box<crate::security::ClientSecurity>>,
    /// The network metrics of a client that pushes its metrics (KIP-714).
    /// [`Connection::connect_with_options`] counts the new connection in
    /// them. `None` counts nowhere.
    pub network_metrics: Option<crate::telemetry::NetworkMetrics>,
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            client_id: "krabka".into(),
            dns_timeout: ClientDnsTimeout::default(),
            socket_connection_setup_timeout: DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT,
            socket_connection_setup_timeout_max: DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX,
            reconnect_backoff: DEFAULT_RECONNECT_BACKOFF,
            reconnect_backoff_max: DEFAULT_RECONNECT_BACKOFF_MAX,
            connections_max_idle: DEFAULT_CONNECTIONS_MAX_IDLE,
            client_dns_lookup: ClientDnsLookup::default(),
            send_buffer: Some(DEFAULT_SEND_BUFFER),
            receive_buffer: Some(DEFAULT_RECEIVE_BUFFER),
            request_timeout: DEFAULT_CLIENT_REQUEST_TIMEOUT,
            dispatch_queue_capacity: ConnectionDispatchQueueCapacity::default(),
            frame_max: ClientFrameMax::default(),
            security: None,
            network_metrics: None,
        }
    }
}

/// A connection to a single Kafka broker.
#[derive(Clone)]
pub struct Connection {
    inner: Arc<ConnectionInner>,
}

struct ConnectionInner {
    versions: ApiVersionTable,
    options: ConnectionOptions,
    next_corr_id: AtomicI32,
    pending: Pending,
    writer_tx: mpsc::Sender<DispatchItem>,
    shutdown: CancellationToken,
    /// SASL re-authentication state (KIP-368), for a connection whose broker
    /// sent a session lifetime.
    reauth: Option<crate::reauth::Reauth>,
    /// The network metrics of the client, once attached. They count the
    /// requests after the `ApiVersions` exchange.
    metrics: std::sync::OnceLock<crate::telemetry::ConnectionMetrics>,
    _reader: JoinHandle<()>,
    _writer: JoinHandle<()>,
}

struct DispatchItem {
    bytes: Bytes,
}

/// Classify a failed TLS handshake.
///
/// Kafka's `SslTransportLayer.maybeProcessHandshakeFailure` raises an
/// `SslAuthenticationException` for an error from the TLS engine, and keeps a
/// `close_notify` during the handshake and a plain I/O error retriable.
/// `tokio-rustls` wraps an error from the TLS engine as the inner error of the
/// `io::Error`.
fn tls_handshake_error(addr: SocketAddr, source: std::io::Error) -> ClientError {
    let rejected = source
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<rustls::Error>())
        .is_some_and(|error| {
            !matches!(
                error,
                rustls::Error::AlertReceived(rustls::AlertDescription::CloseNotify)
            )
        });
    if rejected {
        ClientError::Authentication {
            addr,
            source: crate::error::AuthenticationError::Tls(source),
        }
    } else {
        ClientError::Tls { addr, source }
    }
}

/// Classify a failed SASL exchange. Only a rejection is an authentication
/// failure.
pub(crate) fn sasl_error(addr: SocketAddr, source: crate::sasl::OutboundSaslError) -> ClientError {
    match source {
        crate::sasl::OutboundSaslError::Authentication(error) => ClientError::Authentication {
            addr,
            source: error.into(),
        },
        source => ClientError::Sasl { addr, source },
    }
}

/// The last time that a connection sent or received a frame, as milliseconds
/// since the connection started.
struct Activity {
    started: tokio::time::Instant,
    last_ms: AtomicU64,
}

impl Activity {
    fn new() -> Self {
        Self {
            started: tokio::time::Instant::now(),
            last_ms: AtomicU64::new(0),
        }
    }

    fn touch(&self) {
        let elapsed = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.last_ms.fetch_max(elapsed, Ordering::Relaxed);
    }

    fn last(&self) -> tokio::time::Instant {
        self.started + std::time::Duration::from_millis(self.last_ms.load(Ordering::Relaxed))
    }
}

/// Open a TCP connection with the socket buffers of `options`.
async fn tcp_connect(
    addr: SocketAddr,
    options: &ConnectionOptions,
) -> Result<TcpStream, ClientError> {
    let stream = async { configured_socket(addr, options)?.connect(addr).await }
        .await
        .map_err(|source| ClientError::Connect { addr, source })?;
    stream.set_nodelay(true).ok();
    Ok(stream)
}

/// A TCP socket for `addr` with the `send.buffer.bytes` and
/// `receive.buffer.bytes` of `options`.
fn configured_socket(addr: SocketAddr, options: &ConnectionOptions) -> std::io::Result<TcpSocket> {
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    if let Some(size) = options.send_buffer {
        socket.set_send_buffer_size(buffer_size(size))?;
    }
    if let Some(size) = options.receive_buffer {
        socket.set_recv_buffer_size(buffer_size(size))?;
    }
    Ok(socket)
}

/// Run `setup` within `options.socket_connection_setup_timeout`.
///
/// Kafka's connection setup timeout covers a node in the `CONNECTING` state:
/// the TCP connection and the TLS and SASL handshakes, until the client sends
/// `ApiVersions` (`ClusterConnectionStates.checkingApiVersions`).
async fn within_setup_timeout<T>(
    options: &ConnectionOptions,
    setup: impl Future<Output = Result<T, ClientError>>,
) -> Result<T, ClientError> {
    tokio::time::timeout(options.socket_connection_setup_timeout.to_std(), setup)
        .await
        .map_err(|_| ClientError::Timeout(options.socket_connection_setup_timeout))?
}

fn buffer_size(size: ByteSize) -> u32 {
    u32::try_from(size.bytes_u64()).unwrap_or(u32::MAX)
}

impl Connection {
    /// Connect to `addr`, negotiate API versions, return a usable `Connection`.
    #[tracing::instrument(level = "debug", skip_all, fields(addr = %addr), err)]
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn connect(
        addr: SocketAddr,
        options: ConnectionOptions,
    ) -> Result<Self, ClientError> {
        let stream = within_setup_timeout(&options, tcp_connect(addr, &options)).await?;
        Self::from_stream(Box::new(stream), options).await
    }

    /// Connect to `addr` and honour `options.security`.
    ///
    /// This method makes a secured (TLS/SASL) dial when a policy is set, and
    /// a plaintext dial otherwise. It is the single connect entry point for
    /// every metadata-client site (pool, admin, RLMM fetch loop), so the
    /// plaintext-versus-secured branch cannot drift between them. The
    /// plaintext (`None`) path is byte-identical to [`Self::connect`].
    ///
    /// # Errors
    /// Propagates [`Self::connect`] / [`Self::connect_secured`] failures.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(addr = %addr, secured = options.security.is_some()),
        err,
    )]
    pub async fn connect_with_options(
        addr: SocketAddr,
        options: ConnectionOptions,
    ) -> Result<Self, ClientError> {
        let metrics = options.network_metrics.clone();
        // The TLS and SASL handshakes make a large future. Boxing it keeps the
        // futures of the callers small, and it keeps the type depth of a
        // caller that nests several connections below the compiler limit.
        let connection = match options.security.clone() {
            Some(sec) => Box::pin(Self::connect_secured(addr, options, sec.as_ref())).await,
            None => Box::pin(Self::connect(addr, options)).await,
        }?;
        if let Some(metrics) = &metrics {
            connection.attach_metrics(metrics);
        }
        Ok(connection)
    }

    /// Connect to `addr` and apply `security` (TLS then SASL) before the
    /// API-versions bootstrap.
    ///
    /// `Plaintext` is identical to [`Self::connect`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Connect`] / [`ClientError::Timeout`] on the
    /// TCP dial, [`ClientError::Tls`] or [`ClientError::Sasl`] if the TLS
    /// handshake or the SASL exchange fails with no verdict from the peer,
    /// [`ClientError::Authentication`] if the peer rejects authentication,
    /// [`ClientError::InvalidConfig`] if the TLS settings do not build, or
    /// [`ClientError::Io`] if the security policy is internally inconsistent
    /// (e.g. a TLS protocol with no TLS config).
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(addr = %addr, protocol = ?security.protocol),
        err,
    )]
    pub async fn connect_secured(
        addr: SocketAddr,
        options: ConnectionOptions,
        security: &crate::security::ClientSecurity,
    ) -> Result<Self, ClientError> {
        let (stream, reauth) = within_setup_timeout(&options, async {
            let tcp = tcp_connect(addr, &options).await?;
            Self::secure_stream(addr, tcp, &options, security).await
        })
        .await?;
        Self::from_stream_with_reauth(stream, options, reauth).await
    }

    /// Run the TLS and SASL handshakes of `security` over `tcp`.
    async fn secure_stream(
        addr: SocketAddr,
        tcp: TcpStream,
        options: &ConnectionOptions,
        security: &crate::security::ClientSecurity,
    ) -> Result<(Box<dyn ClientDuplex>, Option<crate::reauth::Reauth>), ClientError> {
        // 1. TLS (if the protocol demands it).
        let mut stream: Box<dyn ClientDuplex> = if security.protocol.requires_tls() {
            let tls = security.tls.as_ref().ok_or_else(|| {
                ClientError::Io(std::io::Error::other("TLS protocol without tls config"))
            })?;
            let connector = tls
                .connector()
                .map_err(|error| ClientError::InvalidConfig(error.to_string()))?;
            let server_name = if tls.server_name.is_empty() {
                addr.ip().to_string()
            } else {
                tls.server_name.clone()
            };
            let sni = tokio_rustls::rustls::pki_types::ServerName::try_from(server_name)
                .map_err(|e| ClientError::Io(std::io::Error::other(format!("invalid SNI: {e}"))))?;
            let s = connector
                .connect(sni, tcp)
                .await
                .map_err(|source| tls_handshake_error(addr, source))?;
            Box::new(s)
        } else {
            Box::new(tcp)
        };

        // 2. SASL (if the protocol demands it).
        let mut reauth = None;
        if security.protocol.requires_sasl() {
            let creds = security.sasl.as_ref().ok_or_else(|| {
                ClientError::Io(std::io::Error::other("SASL protocol without credentials"))
            })?;
            // GSSAPI SPN host: explicit `sasl_host`, else TLS SNI, else the
            // connection's target IP, else "localhost". The target IP is a
            // last resort — for GSSAPI the caller should set `sasl_host` so
            // the principal matches the broker's advertised hostname.
            let target = addr.ip().to_string();
            let server_name = security.sasl_handshake_host(Some(target.as_str()));
            let session = crate::sasl::outbound_sasl(
                &mut *stream,
                creds,
                server_name,
                &options.client_id,
                options.frame_max,
            )
            .await
            .map_err(|source| sasl_error(addr, source))?;
            reauth = session.needs_reauthentication().then(|| {
                crate::reauth::Reauth::new(addr, creds.clone(), server_name.to_owned(), session)
            });
        }
        Ok((stream, reauth))
    }

    /// Build a `Connection` over a pre-established, optionally
    /// pre-authenticated stream.
    ///
    /// This method negotiates API versions over the stream and returns a
    /// usable `Connection`. The broker's `InterBrokerClient` integration
    /// calls it: the TLS + SASL handshake runs before this call, so the
    /// stream is already authenticated. From here on the connection's normal
    /// request / response framing applies.
    #[tracing::instrument(level = "debug", skip_all, err)]
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub async fn from_stream(
        stream: Box<dyn ClientDuplex>,
        options: ConnectionOptions,
    ) -> Result<Self, ClientError> {
        Self::from_stream_with_reauth(stream, options, None).await
    }

    /// [`Self::from_stream`] for a stream that a SASL exchange authenticated,
    /// with the state to authenticate again before the session ends.
    pub(crate) async fn from_stream_with_reauth(
        stream: Box<dyn ClientDuplex>,
        options: ConnectionOptions,
        reauth: Option<crate::reauth::Reauth>,
    ) -> Result<Self, ClientError> {
        let (writer_tx, writer_rx) =
            mpsc::channel::<DispatchItem>(options.dispatch_queue_capacity.get());
        let shutdown = CancellationToken::new();
        let pending: Pending = Arc::new(DashMap::new());
        let activity = Arc::new(Activity::new());

        let (reader_handle, writer_handle) = spawn_io_tasks(
            stream,
            writer_rx,
            IoContext {
                shutdown: shutdown.clone(),
                pending: Arc::clone(&pending),
                activity,
                frame_max: options.frame_max,
                max_idle: options.connections_max_idle.to_std(),
            },
        );

        let mut conn = Self {
            inner: Arc::new(ConnectionInner {
                versions: ApiVersionTable::default(),
                options: options.clone(),
                next_corr_id: AtomicI32::new(0),
                pending,
                writer_tx,
                shutdown,
                reauth,
                metrics: std::sync::OnceLock::new(),
                _reader: reader_handle,
                _writer: writer_handle,
            }),
        };

        let versions = fetch_api_versions(&conn).await?;
        let inner = Arc::get_mut(&mut conn.inner).expect("unique handle at connect-time");
        inner.versions = versions;

        Ok(conn)
    }

    /// Return the peer-advertised version range for `api_key`.
    ///
    /// Higher-level clients use this to distinguish an API that is absent
    /// from a listener's surface from one whose advertised versions merely do
    /// not overlap their codec range.
    #[must_use]
    pub fn advertised_api_range(&self, api_key: i16) -> Option<(i16, i16)> {
        self.inner.versions.broker_range(api_key)
    }

    /// Send a typed request and await the typed response.
    ///
    /// This method negotiates the version from the broker-advertised table
    /// that `connect` populated. It encodes and decodes the request and
    /// response headers automatically.
    ///
    /// # Errors
    ///
    /// Returns `ClientError::IncompatibleVersion` if there is no mutually
    /// supported version, `ClientError::Disconnected` if the I/O loop has
    /// exited, or `ClientError::Timeout` if no response arrives in time.
    // cargo-mutants: live-broker send path; not unit-testable
    #[cfg_attr(test, mutants::skip)]
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(api_key = R::API_KEY, version = tracing::field::Empty),
        err,
    )]
    pub async fn send<R: ProtocolRequest>(&self, req: R) -> Result<R::Response, ClientError> {
        // 1. Negotiate version.
        let version = self.inner.versions.negotiate::<R>()?;
        tracing::Span::current().record("version", version);

        // 2. Allocate correlation ID.
        let corr_id = self.next_correlation_id();

        // 3. Build request header + encoded body into one frame.
        //
        // The header has a trailing tagged-fields byte (header v2) iff the
        // body is flexible. The `client_id` field is always i16 NULLABLE_STRING
        // per the upstream `RequestHeader.json` schema.
        let body_flexible = version >= R::FLEXIBLE_MIN;
        let mut frame = build_request_header(
            ApiKey(R::API_KEY),
            ApiVersion(version),
            corr_id,
            &self.inner.options.client_id,
            body_flexible,
        );
        req.encode(&mut frame, version)?;

        // 4. Dispatch request and await response.
        let body_bytes = self.dispatch_request(corr_id, frame).await?;

        // 5. Decode the response.
        //
        // The reader has already stripped the 4-byte correlation_id prefix.
        // What remains is: [ResponseHeader fields after corr_id] + [response body].
        //
        // ResponseHeader version rules:
        //   - ApiVersionsResponse (api_key=18): always ResponseHeader v0, which
        //     has NO fields after the correlation_id. This is a long-standing
        //     Kafka asymmetry — even flexible ApiVersions responses use v0 header.
        //   - All other flexible messages (version >= FLEXIBLE_MIN): ResponseHeader
        //     v1 adds 1 byte for the tagged-fields count (0x00 when empty).
        //   - Non-flexible messages: ResponseHeader v0 (no bytes after corr_id).
        let mut cursor: &[u8] = &body_bytes;
        let uses_flexible_resp_header = body_flexible && R::API_KEY != API_VERSIONS_KEY;
        if uses_flexible_resp_header {
            cursor = skip_tagged_fields(cursor)?;
        }

        let resp = <R::Response as krabka_protocol::Decode>::decode(&mut cursor, version)?;
        Ok(resp)
    }

    /// Encode and enqueue a typed request that intentionally has no response.
    /// Kafka Produce with `acks=0` is the standard use case.
    ///
    /// # Errors
    ///
    /// Returns an error if version negotiation or encoding fails, or if the
    /// connection writer has stopped before accepting the frame.
    pub async fn send_no_response<R: ProtocolRequest>(&self, req: R) -> Result<(), ClientError> {
        let version = self.inner.versions.negotiate::<R>()?;
        let corr_id = self.next_correlation_id();
        let body_flexible = version >= R::FLEXIBLE_MIN;
        let mut frame = build_request_header(
            ApiKey(R::API_KEY),
            ApiVersion(version),
            corr_id,
            &self.inner.options.client_id,
            body_flexible,
        );
        req.encode(&mut frame, version)?;
        let guard = self.reauthenticate_if_due().await?;
        if let Some(metrics) = self.inner.metrics.get() {
            metrics.request(frame.len());
        }
        let sent = self
            .inner
            .writer_tx
            .send(DispatchItem {
                bytes: frame.freeze(),
            })
            .await
            .map_err(|_| ClientError::Disconnected);
        drop(guard);
        sent
    }

    /// Send a hand-framed request and await the raw response body.
    ///
    /// This method bypasses the typed [`ProtocolRequest`] codegen path so
    /// callers can speak Krabka-private APIs whose wire types live outside
    /// `krabka-protocol`, for example the controller's Raft RPCs at api keys
    /// 1000+.
    ///
    /// This method always writes the header as `RequestHeader v2` (flexible)
    /// with an empty trailing tagged-fields byte. It assumes the response
    /// uses `ResponseHeader v1` (flexible): the I/O loop strips the 4-byte
    /// correlation id, and this method strips the leading tagged-fields byte
    /// before it returns. Callers receive the raw body bytes only.
    ///
    /// `body` is the encoded request body, which is everything after the
    /// request header, exactly as it should appear on the wire.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Disconnected`] if the I/O loop has exited
    /// or [`ClientError::Timeout`] if no response arrives within the
    /// configured request timeout.
    // cargo-mutants: live-broker I/O path; not unit-testable
    #[cfg_attr(test, mutants::skip)]
    #[tracing::instrument(level = "debug", skip_all, fields(api_key, api_version), err)]
    pub async fn raw_request(
        &self,
        api_key: i16,
        api_version: i16,
        body: Bytes,
    ) -> Result<Bytes, ClientError> {
        let corr_id = self.next_correlation_id();

        // RequestHeader v2 (flexible). Krabka-private api keys are always
        // declared flexible so the header shape is predictable.
        let mut frame = build_request_header(
            ApiKey(api_key),
            ApiVersion(api_version),
            corr_id,
            &self.inner.options.client_id,
            true,
        );
        frame.put_slice(&body);

        let body_bytes = self.dispatch_request(corr_id, frame).await?;

        // ResponseHeader v1: the tagged fields after the already-stripped
        // correlation id.
        let body = skip_tagged_fields(&body_bytes)?;
        Ok(body_bytes.slice(body_bytes.len() - body.len()..))
    }

    /// Negotiated API versions known to this connection.
    // cargo-mutants: one-line accessor returning a borrowed field
    #[must_use]
    #[cfg_attr(test, mutants::skip)]
    pub fn versions(&self) -> &ApiVersionTable {
        &self.inner.versions
    }

    /// The number of requests that wait for a response on this connection.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.inner.pending.len()
    }

    /// The next correlation id of a normal request. As Kafka's
    /// `NetworkClient.nextCorrelationId` does, the ids wrap to 0 before the
    /// range that SASL re-authentication reserves.
    fn next_correlation_id(&self) -> i32 {
        let last_normal = crate::sasl::MIN_RESERVED_CORRELATION_ID - 1;
        self.inner
            .next_corr_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(if current >= last_normal {
                    0
                } else {
                    current + 1
                })
            })
            .unwrap_or(0)
    }

    /// Whether the connection has closed: the peer closed it, an I/O error
    /// ended it, it was idle for `connections_max_idle`, or [`Self::close`]
    /// ran. A closed connection fails every request with `Disconnected`.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.shutdown.is_cancelled()
    }

    /// Close the connection, cancelling all background tasks.
    // cargo-mutants: teardown; no observable return to assert against
    #[cfg_attr(test, mutants::skip)]
    pub fn close(self) {
        self.inner.shutdown.cancel();
        // The Arc gets dropped when `self` does; `JoinHandle`s abort naturally.
    }

    async fn dispatch_request(&self, corr_id: i32, frame: BytesMut) -> Result<Bytes, ClientError> {
        let guard = self.reauthenticate_if_due().await?;
        // A caller that drops this future must not leave its id in `pending`,
        // where it would keep the connection from its idle close.
        let _registration = PendingRegistration {
            pending: &self.inner.pending,
            corr_id,
        };
        let metrics = self.inner.metrics.get();
        if let Some(metrics) = metrics {
            metrics.request(frame.len());
        }
        let rx = self.enqueue(corr_id, frame).await?;
        drop(guard);
        let response = self.await_response(corr_id, rx).await?;
        if let Some(metrics) = metrics {
            metrics.response(response.len());
        }
        Ok(response)
    }

    /// Count this connection in the client's network metrics from now on.
    /// A connection counts in one set of metrics; a second call does
    /// nothing.
    pub fn attach_metrics(&self, metrics: &crate::telemetry::NetworkMetrics) {
        self.inner.metrics.get_or_init(|| metrics.opened());
    }

    /// Send a frame and wait for its response, with no re-authentication
    /// check. A re-authentication sends its own frames this way.
    pub(crate) async fn dispatch_unguarded(
        &self,
        corr_id: i32,
        frame: BytesMut,
    ) -> Result<Bytes, ClientError> {
        let _registration = PendingRegistration {
            pending: &self.inner.pending,
            corr_id,
        };
        let rx = self.enqueue(corr_id, frame).await?;
        self.await_response(corr_id, rx).await
    }

    /// The request header of a frame that this connection sends.
    pub(crate) fn request_header(
        &self,
        api_key: ApiKey,
        version: ApiVersion,
        corr_id: i32,
        flexible: bool,
    ) -> BytesMut {
        build_request_header(
            api_key,
            version,
            corr_id,
            &self.inner.options.client_id,
            flexible,
        )
    }

    /// Wait for a due SASL re-authentication to finish, and return a guard
    /// that keeps a new one from starting until the caller enqueues its frame.
    ///
    /// Kafka's `KafkaChannel.maybeBeginClientReauthentication` starts the
    /// exchange before the first request after the due time. A failed
    /// exchange closes the connection and fails the request.
    async fn reauthenticate_if_due(
        &self,
    ) -> Result<Option<tokio::sync::RwLockReadGuard<'_, Option<tokio::time::Instant>>>, ClientError>
    {
        let Some(reauth) = self.inner.reauth.as_ref() else {
            return Ok(None);
        };
        loop {
            let next = reauth.next.read().await;
            if next.is_none_or(|due| tokio::time::Instant::now() < due) {
                return Ok(Some(next));
            }
            drop(next);
            let mut next = reauth.next.write().await;
            if next.is_some_and(|due| tokio::time::Instant::now() >= due) {
                let mut channel = crate::reauth::ConnectionChannel { connection: self };
                // The exchange nests the whole SASL state machine. Boxing it
                // keeps every send future small.
                match Box::pin(reauth.authenticate(
                    &mut channel,
                    &self.inner.options.client_id,
                    self.inner.options.frame_max,
                ))
                .await
                {
                    Ok(due) => *next = due,
                    Err(error) => {
                        tracing::warn!(error = %error, "SASL re-authentication failed");
                        self.inner.shutdown.cancel();
                        return Err(error);
                    }
                }
            }
        }
    }

    /// Register `corr_id` and queue `frame` for the writer.
    async fn enqueue(
        &self,
        corr_id: i32,
        frame: BytesMut,
    ) -> Result<oneshot::Receiver<Result<Bytes, ClientError>>, ClientError> {
        let (tx, rx) = oneshot::channel::<Result<Bytes, ClientError>>();
        self.inner.pending.insert(corr_id, tx);
        self.inner
            .writer_tx
            .send(DispatchItem {
                bytes: frame.freeze(),
            })
            .await
            .map_err(|_| ClientError::Disconnected)?;
        Ok(rx)
    }

    /// Wait for the response of `corr_id` within the request timeout.
    async fn await_response(
        &self,
        corr_id: i32,
        rx: oneshot::Receiver<Result<Bytes, ClientError>>,
    ) -> Result<Bytes, ClientError> {
        match tokio::time::timeout(self.inner.options.request_timeout.to_std(), rx).await {
            Ok(Ok(Ok(bytes))) => Ok(bytes),
            Ok(Ok(Err(err))) => Err(err),
            Ok(Err(_recv_closed)) => Err(ClientError::Disconnected),
            Err(_timeout) => {
                self.inner.pending.remove(&corr_id);
                // Kafka's `NetworkClient.handleTimedOutRequests` closes the
                // connection of a timed-out request: a late response must
                // not match a later request, and the next use connects again.
                self.inner.shutdown.cancel();
                Err(ClientError::Timeout(self.inner.options.request_timeout))
            }
        }
    }
}

/// Removes a request from `pending` when its caller stops waiting.
struct PendingRegistration<'a> {
    pending: &'a Pending,
    corr_id: i32,
}

impl Drop for PendingRegistration<'_> {
    fn drop(&mut self) {
        self.pending.remove(&self.corr_id);
    }
}

/// Skip the tagged fields of a flexible response header: an unsigned varint
/// count, then for each field a varint tag, a varint size and the data.
///
/// # Errors
/// Returns a codec error for a header that ends early.
pub(crate) fn skip_tagged_fields(mut bytes: &[u8]) -> Result<&[u8], ClientError> {
    fn uvarint(bytes: &mut &[u8]) -> Result<u32, ClientError> {
        let mut value = 0_u32;
        for shift in (0..35).step_by(7) {
            let (&byte, rest) = bytes.split_first().ok_or_else(truncated)?;
            *bytes = rest;
            value |= u32::from(byte & 0x7F) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(truncated())
    }
    fn truncated() -> ClientError {
        ClientError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid tagged fields in a response header",
        ))
    }
    if bytes.is_empty() {
        return Ok(bytes);
    }
    for _ in 0..uvarint(&mut bytes)? {
        uvarint(&mut bytes)?;
        let size = usize::try_from(uvarint(&mut bytes)?).map_err(|_| truncated())?;
        bytes = bytes.get(size..).ok_or_else(truncated)?;
    }
    Ok(bytes)
}

/// Spawn independent reader and writer tasks over the split socket.
///
/// This function splits the socket into a read half and a write half, and one
/// task drives each half. It does not multiplex both directions through one
/// `select!` over a single shared `Framed`. A combined task must `await` the
/// `framed.send(...)` flush *inside* a `select!` arm, and it does not poll the
/// read arm during that time. On a request/response connection the broker
/// stays silent until it receives the next request. A write that does not
/// complete in one poll therefore wedges the whole connection: the frame sits
/// buffered, no inbound traffic ever re-drives the loop, and the caller's
/// request never reaches the wire.
///
/// This is what made `krabka-client-consumer`'s group rejoin hang under the
/// jemalloc heap-profiling allocator. Its per-alloc sampling latency widened
/// that window enough to trip the hang deterministically. Independent halves
/// keep an inbound frame pollable while an outbound write is in flight, and
/// the reverse.
///
/// Liveness on teardown: when either task exits on EOF, an I/O error, a
/// dropped `Connection`, or `close()`, it cancels the shared `shutdown` token
/// so the other task also stops. The reader then fails every outstanding
/// request with `Disconnected`. A write-half failure therefore reaches
/// callers promptly instead of stalling them until the request timeout.
///
/// Idle close: the writer also closes the connection when no frame went out
/// or came in for `max_idle` and no request waits for a response, as Kafka's
/// `Selector` closes a connection idle for `connections.max.idle.ms`.
fn spawn_io_tasks(
    stream: Box<dyn ClientDuplex>,
    mut writer_rx: mpsc::Receiver<DispatchItem>,
    context: IoContext,
) -> (JoinHandle<()>, JoinHandle<()>) {
    use futures_util::StreamExt;
    use tokio_util::codec::{FramedRead, FramedWrite};

    let IoContext {
        shutdown,
        pending,
        activity,
        frame_max,
        max_idle,
    } = context;

    let (read_half, write_half) = tokio::io::split(stream);
    let mut framed_read = FramedRead::new(read_half, crate::transport::codec_with_max(frame_max));
    let mut framed_write =
        FramedWrite::new(write_half, crate::transport::codec_with_max(frame_max));

    // WRITER: drains the dispatch channel, flushing each frame in receive
    // order. Owns only the write half, so a not-yet-writable socket can never
    // block the reader.
    let writer_shutdown = shutdown.clone();
    let writer_activity = Arc::clone(&activity);
    let writer_pending = Arc::clone(&pending);
    let writer = tokio::spawn(async move {
        write_loop(
            &writer_shutdown,
            &writer_activity,
            &writer_pending,
            max_idle,
            &mut writer_rx,
            &mut framed_write,
        )
        .await;
        // A write-side failure (or all senders dropped) must wake the reader
        // so it drains pending callers to `Disconnected` rather than letting
        // them wait out the request timeout.
        writer_shutdown.cancel();
    });

    // READER: pulls frames and fulfils the matching pending oneshot. Owns only
    // the read half.
    let reader = tokio::spawn(async move {
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                maybe_frame = framed_read.next() => {
                    let Some(frame) = maybe_frame else { break; };
                    let Ok(frame) = frame else { break; };
                    activity.touch();
                    if frame.len() < 4 { continue; }
                    let corr_id = i32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]);
                    if let Some((_, tx)) = pending.remove(&corr_id) {
                        let body = Bytes::copy_from_slice(&frame[4..]);
                        let _ = tx.send(Ok(body));
                    }
                }
            }
        }
        // Stop the writer too, then fail every outstanding request.
        shutdown.cancel();
        let keys: Vec<i32> = pending.iter().map(|e| *e.key()).collect();
        for k in keys {
            if let Some((_, tx)) = pending.remove(&k) {
                let _ = tx.send(Err(ClientError::Disconnected));
            }
        }
    });

    (reader, writer)
}

/// The shared state of a connection's I/O tasks.
struct IoContext {
    shutdown: CancellationToken,
    pending: Pending,
    activity: Arc<Activity>,
    frame_max: ClientFrameMax,
    max_idle: std::time::Duration,
}

/// The writer loop: send each dispatched frame in order, and stop when the
/// connection shuts down, a write fails, or the connection is idle for
/// `max_idle` with no pending request.
async fn write_loop<W>(
    shutdown: &CancellationToken,
    activity: &Activity,
    pending: &Pending,
    max_idle: std::time::Duration,
    writer_rx: &mut mpsc::Receiver<DispatchItem>,
    framed_write: &mut W,
) where
    W: futures_util::Sink<Bytes> + Unpin,
{
    use futures_util::SinkExt;

    loop {
        let idle_deadline = activity.last() + max_idle;
        tokio::select! {
            () = shutdown.cancelled() => break,
            item = writer_rx.recv() => {
                let Some(item) = item else { break; };
                activity.touch();
                // A peer that stops reading can hold a write for ever. The
                // write must still end when the connection shuts down, for
                // example after a request timeout.
                let written = tokio::select! {
                    () = shutdown.cancelled() => break,
                    written = framed_write.send(item.bytes) => written,
                };
                if written.is_err() {
                    break;
                }
                activity.touch();
            }
            () = tokio::time::sleep_until(idle_deadline) => {
                let idle = activity.last() + max_idle <= tokio::time::Instant::now();
                if idle && pending.is_empty() && writer_rx.is_empty() {
                    tracing::debug!(
                        max_idle_ms = max_idle.as_millis(),
                        "closing a connection idle for connections.max.idle.ms"
                    );
                    break;
                }
                if idle {
                    // A request waits for its response. Look again after the
                    // idle interval.
                    activity.touch();
                }
            }
        }
    }
}

/// Build an encoded `RequestHeader` into a `BytesMut`.
///
/// Kafka has only two `RequestHeader` formats:
///
/// - **v1** (non-flexible): `api_key` + `version` + `corr_id` + i16
///   `client_id` length + `client_id` bytes.
/// - **v2** (flexible): same fields *plus* a trailing `tagged_fields` byte
///   (`0x00` when empty).
///
/// Note that `client_id` is `NULLABLE_STRING` (i16 length) in **both**
/// versions. The upstream `RequestHeader.json` schema marks the field as
/// `"flexibleVersions": "none"`, so even a v2 header keeps the i16-length
/// encoding. A UVARINT here makes the broker misread the length and throw
/// `InvalidRequestException` during header parsing.
///
/// Pass `with_tagged_fields = true` iff the request body is flexible
/// (`version >= R::FLEXIBLE_MIN`).
fn build_request_header(
    api_key: ApiKey,
    version: ApiVersion,
    corr_id: i32,
    client_id: &str,
    with_tagged_fields: bool,
) -> BytesMut {
    let mut buf = BytesMut::with_capacity(32);
    buf.put_i16(api_key.0);
    buf.put_i16(version.0);
    buf.put_i32(corr_id);
    let n = i16::try_from(client_id.len()).expect("client_id fits in i16");
    buf.put_i16(n);
    buf.put_slice(client_id.as_bytes());
    if with_tagged_fields {
        buf.put_u8(0); // empty tagged fields
    }
    buf
}

/// The client software name that `ApiVersions` v3 and later sends (KIP-511).
pub const CLIENT_SOFTWARE_NAME: &str = "krabka-client-rs";

/// The client software version that `ApiVersions` v3 and later sends.
pub const CLIENT_SOFTWARE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Kafka's `UNSUPPORTED_VERSION` error code.
const UNSUPPORTED_VERSION: i16 = 35;

/// The `ApiVersions` request of a new connection at `version`, as Kafka's
/// `ApiVersionsRequest.Builder` fills it.
pub(crate) fn api_versions_request()
-> krabka_protocol::owned::api_versions_request::ApiVersionsRequest {
    krabka_protocol::owned::api_versions_request::ApiVersionsRequest {
        client_software_name: CLIENT_SOFTWARE_NAME.to_owned(),
        client_software_version: CLIENT_SOFTWARE_VERSION.to_owned(),
        ..Default::default()
    }
}

/// Decode an `ApiVersions` response body sent for a request at `version`.
///
/// Kafka's `ApiVersionsResponse.parse` falls back to version 0: a broker that
/// does not support `version` answers with a version 0 `UNSUPPORTED_VERSION`
/// response. A body that does not decode to its end at `version` is read as
/// version 0.
pub(crate) fn decode_api_versions_response(
    body: &[u8],
    version: i16,
) -> Result<krabka_protocol::owned::api_versions_response::ApiVersionsResponse, ClientError> {
    use krabka_protocol::{Decode as _, owned::api_versions_response::ApiVersionsResponse};

    let decode = |version| {
        let mut cursor = body;
        let response = ApiVersionsResponse::decode(&mut cursor, version)?;
        if cursor.is_empty() {
            Ok(response)
        } else {
            Err(ClientError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "ApiVersions response v{version} has {} bytes after its end",
                    cursor.len()
                ),
            )))
        }
    };
    match decode(version) {
        Err(_) if version != 0 => decode(0),
        result => result,
    }
}

/// The next `ApiVersions` version after a response at `version`, or `None`
/// when the response ends the negotiation.
///
/// Kafka's `NetworkClient.handleApiVersionsResponse` retries an
/// `UNSUPPORTED_VERSION` answer to a version above 0 at the highest
/// `ApiVersions` version that the broker lists, or at version 0.
pub(crate) fn api_versions_retry_version(
    response: &krabka_protocol::owned::api_versions_response::ApiVersionsResponse,
    version: i16,
) -> Option<i16> {
    use krabka_protocol::owned::api_versions_request::API_KEY;

    (response.error_code == UNSUPPORTED_VERSION && version > 0).then(|| {
        response
            .api_keys
            .iter()
            .find(|key| key.api_key == API_KEY)
            .map_or(0, |key| key.max_version)
            .min(version - 1)
            .max(0)
    })
}

/// Send `ApiVersions` and return the broker's table.
///
/// This is the bootstrap step inside `connect`. No version table exists yet,
/// so this function cannot use `Connection::send`. The first request uses the
/// highest version of the client, as Kafka's `NetworkClient` does, and an
/// `UNSUPPORTED_VERSION` answer makes it retry at the version that the broker
/// lists.
#[tracing::instrument(level = "debug", skip_all, err)]
async fn fetch_api_versions(conn: &Connection) -> Result<ApiVersionTable, ClientError> {
    use krabka_protocol::{Encode, owned::api_versions_request::ApiVersionsRequest};

    let mut version = ApiVersionsRequest::MAX_VERSION;
    loop {
        let corr_id = conn.next_correlation_id();
        let mut frame = build_request_header(
            ApiKey(ApiVersionsRequest::API_KEY),
            ApiVersion(version),
            corr_id,
            &conn.inner.options.client_id,
            version >= ApiVersionsRequest::FLEXIBLE_MIN,
        );
        api_versions_request().encode(&mut frame, version)?;

        let (tx, rx) = oneshot::channel::<Result<Bytes, ClientError>>();
        conn.inner.pending.insert(corr_id, tx);
        conn.inner
            .writer_tx
            .send(DispatchItem {
                bytes: frame.freeze(),
            })
            .await
            .map_err(|_| ClientError::Disconnected)?;

        let body_bytes = tokio::time::timeout(conn.inner.options.request_timeout.to_std(), rx)
            .await
            .map_err(|_| ClientError::Timeout(conn.inner.options.request_timeout))?
            .map_err(|_| ClientError::Disconnected)??;

        // ResponseHeader v0: only correlation_id (already stripped by the
        // reader), for every ApiVersions version (the Kafka asymmetry
        // documented in `send`).
        let response = decode_api_versions_response(&body_bytes, version)?;
        if let Some(retry) = api_versions_retry_version(&response, version) {
            version = retry;
            continue;
        }
        if response.error_code != 0 {
            return Err(ClientError::Server {
                error_code: response.error_code,
            });
        }
        return Ok(ApiVersionTable::from_response(&response));
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::{
        ByteSize, bytes, convert::ByteSizeExt as _, kibibytes, mebibytes, micros, millis,
    };

    use super::*;

    #[test]
    fn client_dns_timeout_validates_and_preserves_milliseconds() {
        let timeout = ClientDnsTimeout::new(millis(37)).expect("positive timeout");
        assert!(timeout.time() == millis(37));
        assert!(timeout.milliseconds() == 37);
        assert!(ClientDnsTimeout::new(Time::ZERO).is_err());
        assert!(ClientDnsTimeout::new(micros(1)).is_err());
        assert!(ClientDnsTimeout::new(millis(1) + micros(1)).is_err());
    }

    #[test]
    fn connection_options_default_to_kafka_client_settings() {
        let options = ConnectionOptions::default();
        assert!(options.reconnect_backoff == millis(50));
        assert!(options.reconnect_backoff_max == secs(1));
        assert!(options.socket_connection_setup_timeout_max == secs(30));
        assert!(options.connections_max_idle == minutes(9));
        assert!(options.client_dns_lookup == ClientDnsLookup::UseAllDnsIps);
        assert!(options.send_buffer == Some(kibibytes(128)));
        assert!(options.receive_buffer == Some(kibibytes(64)));
    }

    #[test]
    fn connection_options_own_named_defaults() {
        let options = ConnectionOptions::default();
        assert!(DEFAULT_CLIENT_DNS_TIMEOUT == secs(10));
        assert!(DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT == secs(10));
        assert!(DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX == secs(30));
        assert!(DEFAULT_CLIENT_REQUEST_TIMEOUT == secs(30));
        assert!(options.dns_timeout == ClientDnsTimeout::default());
        assert!(options.dns_timeout.time() == DEFAULT_CLIENT_DNS_TIMEOUT);
        assert!(options.socket_connection_setup_timeout == DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT);
        assert!(options.request_timeout == DEFAULT_CLIENT_REQUEST_TIMEOUT);
    }

    #[test]
    fn connection_resource_defaults_preserve_existing_values() {
        assert!(
            ConnectionDispatchQueueCapacity::default().get()
                == DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY
        );
        assert!(ConnectionDispatchQueueCapacity::default().get() == 64);
        assert!(ClientFrameMax::default().size() == mebibytes(100));
        assert!(MAX_CLIENT_FRAME_BYTES == mebibytes(100));
    }

    #[test]
    fn connection_resource_policy_validates_boundaries() {
        assert!(ConnectionDispatchQueueCapacity::new(0).is_err());
        assert!(ConnectionDispatchQueueCapacity::new(7).unwrap().get() == 7);

        assert!(ClientFrameMax::try_from(bytes(0)).is_err());
        assert!(ClientFrameMax::try_from(ByteSize::from_bytes_f64(1.5)).is_err());
        assert!(ClientFrameMax::try_from(mebibytes(100) + bytes(1)).is_err());
        assert!(ClientFrameMax::try_from(kibibytes(32)).unwrap().size() == kibibytes(32));
    }
}

#[cfg(test)]
mod secured_tests {
    use krabka_security::ListenerProtocol;

    use super::*;
    use crate::security::{ClientSecurity, SaslCredentials};

    // A SASL_PLAINTEXT connect drives the handshake then ApiVersions.
    // The fake broker answers SaslHandshake(0), SaslAuthenticate(0),
    // then a minimal ApiVersionsResponse v0 so from_stream succeeds.
    #[tokio::test]
    async fn connect_secured_runs_sasl_then_api_versions() {
        use krabka_protocol::{
            Encode,
            owned::{
                api_versions_response::ApiVersionsResponse,
                sasl_authenticate_response::SaslAuthenticateResponse,
                sasl_handshake_response::SaslHandshakeResponse,
            },
        };
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            // (body, flexible_response_header)
            let replies: [(BytesMut, bool); 4] = [
                {
                    // The ApiVersions v0 that starts the SASL exchange lists
                    // the SASL APIs.
                    let crate::mock::MockReply::Respond(body) = crate::mock::MockSaslAnswer::Accept
                        .reply(krabka_protocol::owned::api_versions_request::API_KEY, 0)
                        .unwrap()
                    else {
                        unreachable!("ApiVersions v0 has a reply")
                    };
                    (BytesMut::from(&body[..]), false)
                },
                {
                    let mut b = BytesMut::new();
                    SaslHandshakeResponse {
                        error_code: 0,
                        ..Default::default()
                    }
                    .encode(&mut b, 1)
                    .unwrap();
                    (b, false)
                },
                {
                    let mut b = BytesMut::new();
                    SaslAuthenticateResponse {
                        error_code: 0,
                        ..Default::default()
                    }
                    .encode(&mut b, 2)
                    .unwrap();
                    (b, true)
                },
                {
                    let mut b = BytesMut::new();
                    ApiVersionsResponse::default().encode(&mut b, 0).unwrap();
                    // ApiVersions always uses a v0 response header.
                    (b, false)
                },
            ];
            for (body, flex_header) in replies {
                let req_len = s.read_u32().await.unwrap();
                let mut req = vec![0u8; req_len as usize];
                s.read_exact(&mut req).await.unwrap();
                let corr = i32::from_be_bytes([req[4], req[5], req[6], req[7]]);
                let mut frame = BytesMut::new();
                frame.put_i32(corr);
                if flex_header {
                    frame.put_u8(0);
                }
                frame.put_slice(&body);
                s.write_u32(u32::try_from(frame.len()).unwrap())
                    .await
                    .unwrap();
                s.write_all(&frame).await.unwrap();
                s.flush().await.unwrap();
            }
        });
        let security = ClientSecurity {
            protocol: ListenerProtocol::SaslPlaintext,
            tls: None,
            sasl: Some(SaslCredentials::Plain {
                username: "u".into(),
                password: "p".into(),
            }),
            sasl_host: None,
        };
        let conn = Connection::connect_secured(addr, ConnectionOptions::default(), &security)
            .await
            .expect("secured connect completes");
        conn.close();
        server.await.unwrap();
    }
}

#[cfg(test)]
mod tls_handshake_failure_tests {
    use krabka_security::ListenerProtocol;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::*;
    use crate::{
        error::AuthenticationError,
        security::{ClientSecurity, TlsConnectorConfig},
    };

    /// What the TLS listener does after it accepts the connection.
    #[derive(Clone, Copy, Debug)]
    enum Peer {
        /// Answer the `ClientHello` with plaintext, as a plaintext listener
        /// does.
        Plaintext,
        /// Close the connection before the handshake completes.
        Close,
    }

    /// How the handshake failed, as a caller classifies it.
    #[derive(Debug, PartialEq, Eq)]
    enum Failure {
        Rejected(std::io::ErrorKind),
        Transport(std::io::ErrorKind),
        Other(String),
    }

    async fn listener(peer: Peer) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0_u8; 1024];
            let _ = stream.read(&mut buf).await;
            if matches!(peer, Peer::Plaintext) {
                let _ = stream.write_all(b"HTTP/1.0 400 Bad Request\r\n\r\n").await;
                while matches!(stream.read(&mut buf).await, Ok(n) if n > 0) {}
            }
        });
        addr
    }

    /// Kafka's `SslTransportLayer.maybeProcessHandshakeFailure` raises an
    /// `SslAuthenticationException` for "Unrecognized SSL message", and keeps
    /// a disconnect during the handshake retriable.
    #[tokio::test]
    async fn tls_rejection_is_an_authentication_failure_and_eof_is_not() {
        let security = ClientSecurity {
            protocol: ListenerProtocol::Ssl,
            tls: Some(TlsConnectorConfig::default()),
            sasl: None,
            sasl_host: None,
        };
        for (peer, expected) in [
            (
                Peer::Plaintext,
                Failure::Rejected(std::io::ErrorKind::InvalidData),
            ),
            (
                Peer::Close,
                Failure::Transport(std::io::ErrorKind::UnexpectedEof),
            ),
        ] {
            let addr = listener(peer).await;
            let result =
                Connection::connect_secured(addr, ConnectionOptions::default(), &security).await;
            let failure = match result {
                Err(ClientError::Authentication {
                    source: AuthenticationError::Tls(source),
                    ..
                }) => Failure::Rejected(source.kind()),
                Err(ClientError::Tls { source, .. }) => Failure::Transport(source.kind()),
                Err(error) => Failure::Other(error.to_string()),
                Ok(_) => Failure::Other("connected".into()),
            };
            assert2::check!(failure == expected, "{peer:?}");
        }
    }
}

#[cfg(test)]
mod io_task_tests {
    use std::time::{Duration, Instant};

    use krabka_protocol::{
        Encode,
        owned::{
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            metadata_request::MetadataRequest,
            metadata_response::MetadataResponse,
        },
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::*;

    // The split reader/writer tasks must keep their teardown contract: a
    // server that closes the connection mid-request has to surface to the
    // caller promptly as `Disconnected`, NOT stall until the request timeout.
    // The reader's EOF cancels the shared shutdown (stopping the writer) and
    // drains every outstanding request — so a write-half failure can't strand
    // a caller for the full timeout. (Regression guard for the io-task split.)
    #[tokio::test]
    async fn server_close_mid_request_yields_prompt_disconnected() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            // Answer the `from_stream` ApiVersions handshake.
            let len = s.read_u32().await.unwrap();
            let mut req = vec![0u8; len as usize];
            s.read_exact(&mut req).await.unwrap();
            let corr = i32::from_be_bytes([req[4], req[5], req[6], req[7]]);
            let mut body = BytesMut::new();
            ApiVersionsResponse::default().encode(&mut body, 0).unwrap();
            let mut frame = BytesMut::new();
            frame.put_i32(corr);
            frame.put_slice(&body);
            s.write_u32(u32::try_from(frame.len()).unwrap())
                .await
                .unwrap();
            s.write_all(&frame).await.unwrap();
            s.flush().await.unwrap();
            // Read the next request fully (so its pending entry is registered),
            // then drop the socket without replying.
            let len2 = s.read_u32().await.unwrap();
            let mut req2 = vec![0u8; len2 as usize];
            s.read_exact(&mut req2).await.unwrap();
            // `s` drops here -> the connection closes with no response.
        });

        let stream = TcpStream::connect(addr).await.unwrap();
        let opts = ConnectionOptions {
            request_timeout: secs(5),
            socket_connection_setup_timeout: secs(5),
            ..Default::default()
        };
        let conn = Connection::from_stream(Box::new(stream), opts)
            .await
            .expect("plaintext from_stream completes");

        let started = Instant::now();
        // Hand-framed Metadata request; the server reads it then closes.
        let result = conn.raw_request(3, 0, Bytes::new()).await;
        assert!(
            matches!(result, Err(ClientError::Disconnected)),
            "server close mid-request must yield Disconnected, got {result:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "drain must be prompt (reader EOF), not a request-timeout stall"
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn no_response_request_does_not_capture_the_next_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();

            let handshake_len = socket.read_u32().await.unwrap();
            let mut handshake = vec![0u8; handshake_len as usize];
            socket.read_exact(&mut handshake).await.unwrap();
            let handshake_corr =
                i32::from_be_bytes([handshake[4], handshake[5], handshake[6], handshake[7]]);
            let mut handshake_body = BytesMut::new();
            ApiVersionsResponse {
                api_keys: vec![ApiVersion {
                    api_key: MetadataRequest::API_KEY,
                    min_version: 0,
                    max_version: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }
            .encode(&mut handshake_body, 0)
            .unwrap();
            let mut handshake_response = BytesMut::new();
            handshake_response.put_i32(handshake_corr);
            handshake_response.put_slice(&handshake_body);
            socket
                .write_u32(u32::try_from(handshake_response.len()).unwrap())
                .await
                .unwrap();
            socket.write_all(&handshake_response).await.unwrap();
            socket.flush().await.unwrap();

            let one_way_len = socket.read_u32().await.unwrap();
            let mut one_way = vec![0u8; one_way_len as usize];
            socket.read_exact(&mut one_way).await.unwrap();
            let one_way_corr = i32::from_be_bytes([one_way[4], one_way[5], one_way[6], one_way[7]]);

            let request_len = socket.read_u32().await.unwrap();
            let mut request = vec![0u8; request_len as usize];
            socket.read_exact(&mut request).await.unwrap();
            let request_corr = i32::from_be_bytes([request[4], request[5], request[6], request[7]]);
            assert2::assert!(request_corr == one_way_corr.wrapping_add(1));

            let mut body = BytesMut::new();
            MetadataResponse::default().encode(&mut body, 0).unwrap();
            let mut response = BytesMut::new();
            response.put_i32(request_corr);
            response.put_slice(&body);
            socket
                .write_u32(u32::try_from(response.len()).unwrap())
                .await
                .unwrap();
            socket.write_all(&response).await.unwrap();
            socket.flush().await.unwrap();
        });

        let stream = TcpStream::connect(addr).await.unwrap();
        let conn = Connection::from_stream(Box::new(stream), ConnectionOptions::default())
            .await
            .expect("plaintext from_stream completes");
        conn.send_no_response(MetadataRequest::default())
            .await
            .expect("one-way request enqueues");
        let response = conn
            .send(MetadataRequest::default())
            .await
            .expect("subsequent response remains correlated");
        assert2::assert!(response == MetadataResponse::default());

        conn.close();
        server.await.unwrap();
    }
}

#[cfg(test)]
mod connection_policy_tests {
    use std::time::Duration;

    use assert2::check;
    use krabka_protocol::{
        Encode,
        owned::{
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            metadata_request::MetadataRequest,
        },
    };
    use krabka_security::ListenerProtocol;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

    use super::*;
    use crate::security::{ClientSecurity, SaslCredentials};

    /// Answer the `ApiVersions` request of `from_stream` on `server`, with
    /// `Metadata` v0 supported, and return the stream.
    async fn answer_api_versions(mut server: DuplexStream) -> DuplexStream {
        let len = server.read_u32().await.unwrap();
        let mut request = vec![0_u8; len as usize];
        server.read_exact(&mut request).await.unwrap();
        let mut body = BytesMut::new();
        body.put_slice(&request[4..8]);
        ApiVersionsResponse {
            api_keys: vec![ApiVersion {
                api_key: MetadataRequest::API_KEY,
                min_version: 0,
                max_version: 0,
                ..Default::default()
            }],
            ..Default::default()
        }
        .encode(&mut body, 0)
        .unwrap();
        server
            .write_u32(u32::try_from(body.len()).unwrap())
            .await
            .unwrap();
        server.write_all(&body).await.unwrap();
        server
    }

    async fn connection(options: ConnectionOptions) -> (Connection, DuplexStream) {
        let (client, server) = tokio::io::duplex(64 * 1024);
        let (connection, server) = tokio::join!(
            Connection::from_stream(Box::new(client), options),
            answer_api_versions(server)
        );
        (connection.unwrap(), server)
    }

    /// Kafka's `Selector.maybeCloseOldestConnection` closes a connection
    /// with no traffic for `connections.max.idle.ms`. Each row is the idle
    /// time after the last response and whether a request waits.
    #[tokio::test(start_paused = true)]
    async fn an_idle_connection_closes_after_connections_max_idle() {
        let max_idle = Duration::from_mins(9);
        let ms = Duration::from_millis;
        for (name, idle, waiting, closed) in [
            (
                "just below the limit",
                max_idle.saturating_sub(ms(1)),
                false,
                false,
            ),
            ("just past the limit", max_idle + ms(1), false, true),
            (
                "a request waits for its response",
                max_idle * 2,
                true,
                false,
            ),
        ] {
            let (connection, _server) = connection(ConnectionOptions {
                request_timeout: secs(3600),
                ..ConnectionOptions::default()
            })
            .await;
            let waiter = waiting.then(|| {
                let connection = connection.clone();
                tokio::spawn(async move { connection.send(MetadataRequest::default()).await })
            });
            tokio::time::sleep(idle).await;
            check!(connection.is_closed() == closed, "{name}");
            if let Some(waiter) = waiter {
                waiter.abort();
            }
        }
    }

    /// Kafka's `NetworkClient.handleTimedOutRequests` closes the connection
    /// of a request that timed out.
    #[tokio::test(start_paused = true)]
    async fn a_request_timeout_closes_the_connection() {
        let (connection, _server) = connection(ConnectionOptions {
            request_timeout: secs(30),
            ..ConnectionOptions::default()
        })
        .await;
        let result = connection.send(MetadataRequest::default()).await;
        check!(matches!(result, Err(ClientError::Timeout(_))));
        check!(connection.is_closed());
        check!(connection.in_flight() == 0);
    }

    /// The connection setup timeout covers the TLS and SASL handshakes, as a
    /// Kafka node stays `CONNECTING` until it sends `ApiVersions`.
    #[tokio::test]
    async fn the_setup_timeout_covers_a_sasl_handshake_that_never_answers() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = [0_u8; 1024];
            while matches!(stream.read(&mut buffer).await, Ok(read) if read > 0) {}
        });
        let security = ClientSecurity {
            protocol: ListenerProtocol::SaslPlaintext,
            tls: None,
            sasl: Some(SaslCredentials::Plain {
                username: "u".into(),
                password: "p".into(),
            }),
            sasl_host: None,
        };
        let options = ConnectionOptions {
            socket_connection_setup_timeout: millis(100),
            request_timeout: secs(30),
            ..ConnectionOptions::default()
        };
        let started = std::time::Instant::now();
        let result = Connection::connect_secured(addr, options, &security).await;
        check!(matches!(result, Err(ClientError::Timeout(timeout)) if timeout == millis(100)));
        check!(started.elapsed() < Duration::from_secs(5));
        server.abort();
    }

    /// A request timeout closes the connection, and the writer ends even
    /// while a peer that does not read holds its write.
    #[tokio::test(start_paused = true)]
    async fn a_write_blocked_by_a_peer_that_does_not_read_ends_at_shutdown() {
        let (client, server) = tokio::io::duplex(64 * 1024);
        let (connection, _server) = tokio::join!(
            Connection::from_stream(
                Box::new(client),
                ConnectionOptions {
                    request_timeout: secs(30),
                    ..ConnectionOptions::default()
                },
            ),
            answer_api_versions(server)
        );
        let connection = connection.unwrap();
        // The raw request is larger than the duplex buffer, so its write
        // blocks.
        let result = connection
            .raw_request(3, 0, Bytes::from(vec![0_u8; 256 * 1024]))
            .await;
        check!(matches!(result, Err(ClientError::Timeout(_))));
        tokio::time::sleep(Duration::from_millis(1)).await;
        // The writer task drops the receiver when it ends.
        check!(connection.inner.writer_tx.is_closed());
    }

    /// How a scripted broker answers each `ApiVersions` version.
    #[derive(Clone, Copy, Debug)]
    enum Broker {
        /// It supports `ApiVersions` up to this version, and lists it in an
        /// `UNSUPPORTED_VERSION` answer, as Kafka 2.4 and later do.
        ListsUpTo(i16),
        /// It supports only version 0 and lists nothing in its
        /// `UNSUPPORTED_VERSION` answer.
        OnlyVersionZero,
    }

    /// Kafka's `NetworkClient` sends `ApiVersions` at its highest version and
    /// retries an `UNSUPPORTED_VERSION` answer at the version that the broker
    /// lists, or at version 0. A version 3 or later answer holds the finalized
    /// features.
    #[tokio::test]
    async fn api_versions_negotiation_follows_the_broker_and_keeps_the_features() {
        use krabka_protocol::{
            Decode as _,
            owned::{
                api_versions_request::ApiVersionsRequest,
                api_versions_response::{FinalizedFeatureKey, SupportedFeatureKey},
            },
        };

        let features = |version: i16| {
            if version >= 3 {
                crate::FinalizedFeatures {
                    epoch: 7,
                    levels: std::collections::BTreeMap::from([(
                        "transaction.version".to_owned(),
                        2,
                    )]),
                }
            } else {
                crate::FinalizedFeatures::default()
            }
        };
        let software = |version: i16| {
            if version >= 3 {
                (
                    CLIENT_SOFTWARE_NAME.to_owned(),
                    CLIENT_SOFTWARE_VERSION.to_owned(),
                )
            } else {
                (String::new(), String::new())
            }
        };
        for (name, broker, expected_versions, expected_features) in [
            (
                "a broker at version 5",
                Broker::ListsUpTo(5),
                vec![5],
                features(5),
            ),
            (
                "a broker at version 3",
                Broker::ListsUpTo(3),
                vec![5, 3],
                features(3),
            ),
            (
                "a broker at version 2",
                Broker::ListsUpTo(2),
                vec![5, 2],
                features(2),
            ),
            (
                "an old broker",
                Broker::OnlyVersionZero,
                vec![5, 0],
                features(0),
            ),
        ] {
            let (client, mut server) = tokio::io::duplex(64 * 1024);
            let script = tokio::spawn(async move {
                let mut seen = Vec::new();
                loop {
                    let Ok(len) = server.read_u32().await else {
                        return seen;
                    };
                    let mut request = vec![0_u8; len as usize];
                    server.read_exact(&mut request).await.unwrap();
                    let version = i16::from_be_bytes([request[2], request[3]]);
                    let client_id_len = usize::from(u16::from_be_bytes([request[8], request[9]]));
                    let mut body = &request[10 + client_id_len + usize::from(version >= 3)..];
                    let decoded = ApiVersionsRequest::decode(&mut body, version).unwrap();
                    seen.push((
                        version,
                        (
                            decoded.client_software_name,
                            decoded.client_software_version,
                        ),
                    ));
                    let max = match broker {
                        Broker::ListsUpTo(max) => max,
                        Broker::OnlyVersionZero => 0,
                    };
                    let (response, encoded_at) = if version > max {
                        let api_keys = match broker {
                            Broker::ListsUpTo(max) => vec![ApiVersion {
                                api_key: ApiVersionsRequest::API_KEY,
                                min_version: 0,
                                max_version: max,
                                ..Default::default()
                            }],
                            Broker::OnlyVersionZero => Vec::new(),
                        };
                        (
                            ApiVersionsResponse {
                                error_code: UNSUPPORTED_VERSION,
                                api_keys,
                                ..Default::default()
                            },
                            0,
                        )
                    } else {
                        (
                            ApiVersionsResponse {
                                api_keys: vec![ApiVersion {
                                    api_key: MetadataRequest::API_KEY,
                                    min_version: 0,
                                    max_version: 12,
                                    ..Default::default()
                                }],
                                supported_features: vec![SupportedFeatureKey {
                                    name: "transaction.version".into(),
                                    min_version: 0,
                                    max_version: 2,
                                    ..Default::default()
                                }],
                                finalized_features_epoch: 7,
                                finalized_features: vec![FinalizedFeatureKey {
                                    name: "transaction.version".into(),
                                    max_version_level: 2,
                                    min_version_level: 2,
                                    ..Default::default()
                                }],
                                ..Default::default()
                            },
                            version,
                        )
                    };
                    let mut frame = BytesMut::new();
                    frame.put_slice(&request[4..8]);
                    response.encode(&mut frame, encoded_at).unwrap();
                    server
                        .write_u32(u32::try_from(frame.len()).unwrap())
                        .await
                        .unwrap();
                    server.write_all(&frame).await.unwrap();
                }
            });
            let connection =
                Connection::from_stream(Box::new(client), ConnectionOptions::default())
                    .await
                    .unwrap();
            let observed_features = connection.versions().finalized_features().clone();
            let metadata = connection.advertised_api_range(MetadataRequest::API_KEY);
            connection.close();
            let seen = script.await.unwrap();
            check!(
                seen == expected_versions
                    .iter()
                    .map(|version| (*version, software(*version)))
                    .collect::<Vec<_>>(),
                "{name}"
            );
            check!(observed_features == expected_features, "{name}");
            check!(metadata == Some((0, 12)), "{name}");
        }
    }

    /// Kafka's `NetworkClient.nextCorrelationId` wraps before the SASL
    /// reserved range.
    #[tokio::test]
    async fn normal_correlation_ids_wrap_before_the_sasl_range() {
        let (connection, _server) = connection(ConnectionOptions::default()).await;
        let reserved = crate::sasl::MIN_RESERVED_CORRELATION_ID;
        connection
            .inner
            .next_corr_id
            .store(reserved - 2, Ordering::Relaxed);
        let ids = (0..3)
            .map(|_| connection.next_correlation_id())
            .collect::<Vec<_>>();
        check!(ids == vec![reserved - 2, reserved - 1, 0]);
    }

    #[test]
    fn flexible_response_headers_skip_every_tagged_field() {
        for (name, header_and_body, expected) in [
            ("empty body", vec![], Ok(vec![])),
            ("no tagged field", vec![0, 7, 8], Ok(vec![7, 8])),
            (
                "two tagged fields",
                vec![2, 1, 2, 0xAA, 0xBB, 5, 0, 7],
                Ok(vec![7]),
            ),
            ("truncated field", vec![1, 1, 4, 0xAA], Err(())),
        ] {
            check!(
                skip_tagged_fields(&header_and_body)
                    .map(<[u8]>::to_vec)
                    .map_err(drop)
                    == expected,
                "{name}"
            );
        }
    }

    /// A caller that stops waiting leaves nothing in `pending`, so the idle
    /// close still applies.
    #[tokio::test(start_paused = true)]
    async fn a_dropped_request_leaves_no_pending_entry() {
        let (connection, _server) = connection(ConnectionOptions::default()).await;
        let sending = connection.clone();
        let task = tokio::spawn(async move { sending.send(MetadataRequest::default()).await });
        tokio::time::sleep(Duration::from_millis(1)).await;
        check!(connection.in_flight() == 1);
        task.abort();
        let _ = task.await;
        check!(connection.in_flight() == 0);
    }

    #[test]
    fn sockets_get_the_configured_buffer_sizes() {
        let addr: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let requested = kibibytes(96);
        let configured = configured_socket(
            addr,
            &ConnectionOptions {
                send_buffer: Some(requested),
                receive_buffer: Some(requested),
                ..ConnectionOptions::default()
            },
        )
        .unwrap();
        let untouched = configured_socket(
            addr,
            &ConnectionOptions {
                send_buffer: None,
                receive_buffer: None,
                ..ConnectionOptions::default()
            },
        )
        .unwrap();
        // Linux doubles the value that the socket option sets.
        let bytes = buffer_size(requested);
        check!(configured.send_buffer_size().unwrap() >= bytes);
        check!(configured.recv_buffer_size().unwrap() >= bytes);
        check!(untouched.send_buffer_size().unwrap() != configured.send_buffer_size().unwrap());
    }
}
