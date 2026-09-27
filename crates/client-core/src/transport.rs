//! Outbound TCP connections and name resolution.
//!
//! [`dial`] opens every outbound TCP connection of this crate: the broker
//! connections of [`Connection`](crate::Connection) and the connections to an
//! OAuth token endpoint. By default it connects through the operating system.
//!
//! An embedder that supplies its own sockets installs a [`Connector`] once per
//! process with [`install_connector`]. After that, [`dial`] takes every
//! connection from the connector. WASI preview 1 has no `connect` call, so on
//! `target_os = "wasi"` a connector is the only source of connections.
//!
//! [`resolve`] turns a `host:port` address into socket addresses. An IP
//! address literal needs no lookup on any target. WASI has no name
//! resolution, so on WASI a host name is an error.

use std::{
    io,
    net::{IpAddr, SocketAddr},
    sync::OnceLock,
};

use krabka_units::ByteSize;
use thiserror::Error;
use tokio::net::TcpStream;

// The operating system side of this module: connections and name resolution.
#[cfg_attr(not(target_os = "wasi"), path = "transport/native.rs")]
#[cfg_attr(target_os = "wasi", path = "transport/wasi.rs")]
mod platform;

/// A source of outbound TCP connections in place of the operating system.
///
/// An embedder implements this trait when it supplies the sockets itself, for
/// example a WASI host that connects sockets on a virtual network.
/// [`install_connector`] makes a connector the source of every connection
/// that [`dial`] opens, for the life of the process.
///
/// # Examples
///
/// ```
/// use std::{io, net::TcpStream};
///
/// use krabka_client_core::transport::{Connector, install_connector};
///
/// /// Connects every broker address to the same port on this machine.
/// struct Loopback;
///
/// impl Connector for Loopback {
///     fn connect(&self, _host: &str, port: u16) -> io::Result<TcpStream> {
///         TcpStream::connect(("127.0.0.1", port))
///     }
/// }
///
/// install_connector(Box::new(Loopback)).expect("no other connector in this process");
/// ```
pub trait Connector: Send + Sync + 'static {
    /// Open a TCP connection to `host` at `port`.
    ///
    /// `host` is the text that the caller dials: a host name or an IP address
    /// literal, with no resolution. The returned socket can still be
    /// connecting. A refused connection then shows as an error on the first
    /// read or write.
    ///
    /// # Errors
    ///
    /// Returns the error that stops the connection, for example an unknown
    /// host. [`dial`] gives this error to its caller.
    fn connect(&self, host: &str, port: u16) -> io::Result<std::net::TcpStream>;
}

/// The error of [`install_connector`] when the process already has a
/// connector.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("a transport connector is already installed")]
pub struct ConnectorAlreadyInstalled;

/// The connector of the process, once installed.
static CONNECTOR: OnceLock<Box<dyn Connector>> = OnceLock::new();

/// Make `connector` the source of every connection that [`dial`] opens.
///
/// The connector stays installed for the life of the process. Install it
/// before the first connection: a connection that is already open keeps its
/// socket.
///
/// # Errors
///
/// Returns [`ConnectorAlreadyInstalled`] if the process already has a
/// connector. The installed connector stays, and `connector` is dropped.
pub fn install_connector(connector: Box<dyn Connector>) -> Result<(), ConnectorAlreadyInstalled> {
    CONNECTOR
        .set(connector)
        .map_err(|_| ConnectorAlreadyInstalled)
}

/// Socket settings that [`dial`] applies to a connection that it opens
/// through the operating system.
///
/// A connection from a [`Connector`] gets none of these settings.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct SocketOptions {
    /// The socket send buffer (`SO_SNDBUF`), set before the connection
    /// starts. `None` keeps the operating system default.
    pub send_buffer: Option<ByteSize>,
    /// The socket receive buffer (`SO_RCVBUF`), set before the connection
    /// starts. `None` keeps the operating system default.
    pub receive_buffer: Option<ByteSize>,
    /// Turn off Nagle's algorithm (`TCP_NODELAY`) when the connection is
    /// open. If the operating system refuses the option, the connection
    /// stays usable.
    pub nodelay: bool,
}

/// Open a TCP connection to `host` at `port`.
///
/// With an installed [`Connector`], `dial` takes the socket from the
/// connector, makes it non-blocking, and registers it with the tokio runtime.
/// The socket can still be connecting, so a refused connection shows as an
/// error on the first read or write.
///
/// With no connector, `dial` resolves `host` as [`resolve`] does and connects
/// to each address in turn with `options`. It returns the first connection
/// that opens. On WASI, `dial` needs a connector, because WASI preview 1 has
/// no `connect` call.
///
/// # Errors
///
/// Returns the error of the connector, the error of the name lookup, or the
/// error of the last address that did not connect. With no connector on
/// WASI, returns an error of kind [`io::ErrorKind::Unsupported`].
pub async fn dial(host: &str, port: u16, options: SocketOptions) -> io::Result<TcpStream> {
    match CONNECTOR.get() {
        Some(connector) => adopt(connector.connect(host, port)?),
        None => platform::connect(host, port, options).await,
    }
}

/// [`dial`] the IP address and port of `address`.
pub(crate) async fn dial_address(
    address: SocketAddr,
    options: SocketOptions,
) -> io::Result<TcpStream> {
    dial(&host_text(address), address.port(), options).await
}

/// The IP address of `address` as the host text for [`dial`], with the scope
/// id of a scoped IPv6 address.
fn host_text(address: SocketAddr) -> String {
    match address {
        SocketAddr::V6(v6) if v6.scope_id() != 0 => format!("{}%{}", v6.ip(), v6.scope_id()),
        address => address.ip().to_string(),
    }
}

/// Register a socket from a [`Connector`] with the tokio runtime.
fn adopt(stream: std::net::TcpStream) -> io::Result<TcpStream> {
    stream.set_nonblocking(true)?;
    TcpStream::from_std(stream)
}

/// Resolve a `host:port` address to the socket addresses to connect to, in
/// order.
///
/// An address with an IP address literal, such as `10.0.0.1:9092` or
/// `[::1]:9092`, resolves to itself with no lookup. The operating system
/// resolves a host name. WASI has no name resolution, so on WASI a host name
/// fails with an error of kind [`io::ErrorKind::Unsupported`].
///
/// # Errors
///
/// Returns an error of kind [`io::ErrorKind::InvalidInput`] if `address` has
/// no valid port, and the error of the operating system resolver if the
/// lookup fails.
pub async fn resolve(address: &str) -> io::Result<Vec<SocketAddr>> {
    if let Ok(literal) = address.parse::<SocketAddr>() {
        return Ok(vec![literal]);
    }
    // The errors of std's `ToSocketAddrs for str`, which splits the same way.
    let (host, port) = address
        .rsplit_once(':')
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid socket address"))?;
    let port = port
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid port value"))?;
    resolve_host(host, port).await
}

/// Resolve `host` at `port` to the socket addresses to connect to, as
/// [`resolve`] does.
pub(crate) async fn resolve_host(host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
    match literal_address(host, port) {
        Some(literal) => Ok(vec![literal]),
        None => platform::resolve_name(host, port).await,
    }
}

/// The socket address of `host` at `port` when `host` is an IP address
/// literal.
fn literal_address(host: &str, port: u16) -> Option<SocketAddr> {
    host.parse::<IpAddr>()
        .map(|ip| SocketAddr::new(ip, port))
        .ok()
        // Only the socket address syntax carries the scope id of an IPv6
        // address, such as the `3` of `fe80::1%3`.
        .or_else(|| format!("[{host}]:{port}").parse().ok())
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV6};

    use assert2::check;

    use super::*;

    fn scoped(port: u16) -> SocketAddr {
        SocketAddr::V6(SocketAddrV6::new("fe80::1".parse().unwrap(), port, 0, 3))
    }

    #[tokio::test]
    async fn resolve_takes_literals_and_rejects_an_address_without_a_port() {
        let v4 = SocketAddr::from((Ipv4Addr::new(10, 0, 0, 1), 9092));
        let v6 = SocketAddr::from((Ipv6Addr::LOCALHOST, 9093));
        let invalid = |message: &str| Err((io::ErrorKind::InvalidInput, message.to_owned()));
        for (address, expected) in [
            ("10.0.0.1:9092", Ok(vec![v4])),
            ("[::1]:9093", Ok(vec![v6])),
            ("[fe80::1%3]:9092", Ok(vec![scoped(9092)])),
            ("broker", invalid("invalid socket address")),
            (":", invalid("invalid port value")),
            ("broker:port", invalid("invalid port value")),
            ("broker:65536", invalid("invalid port value")),
        ] {
            let resolved = resolve(address)
                .await
                .map_err(|error| (error.kind(), error.to_string()));
            check!(resolved == expected, "{address}");
        }
    }

    #[tokio::test]
    async fn resolve_host_takes_ip_literals_with_and_without_a_scope() {
        for (host, expected) in [
            (
                "10.0.0.1",
                SocketAddr::from((Ipv4Addr::new(10, 0, 0, 1), 9092)),
            ),
            ("::1", SocketAddr::from((Ipv6Addr::LOCALHOST, 9092))),
            ("fe80::1%3", scoped(9092)),
        ] {
            check!(
                resolve_host(host, 9092).await.unwrap() == vec![expected],
                "{host}"
            );
        }
    }

    #[tokio::test]
    async fn resolve_asks_the_operating_system_for_a_host_name() {
        let resolved = resolve("localhost:9092").await.unwrap();
        check!(resolved.contains(&SocketAddr::from((Ipv4Addr::LOCALHOST, 9092))));
    }

    #[test]
    fn host_text_keeps_the_scope_of_an_ipv6_address() {
        for (address, expected) in [
            (
                SocketAddr::from((Ipv4Addr::new(10, 0, 0, 1), 9092)),
                "10.0.0.1",
            ),
            (SocketAddr::from((Ipv6Addr::LOCALHOST, 9092)), "::1"),
            (scoped(9092), "fe80::1%3"),
        ] {
            check!(host_text(address) == expected);
            check!(literal_address(&host_text(address), address.port()) == Some(address));
        }
    }

    #[test]
    fn a_host_name_is_not_a_literal() {
        for host in ["localhost", "broker.example", "[::1]", "fe80::1%eth0", ""] {
            check!(literal_address(host, 9092) == None, "{host}");
        }
    }
}
