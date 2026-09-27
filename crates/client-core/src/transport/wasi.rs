//! The operating system side of [`super`] on WASI.
//!
//! WASI preview 1 has no `connect` call and no name resolution. Connections
//! come from the [`Connector`](super::Connector) of the embedder, and
//! addresses are IP address literals.

use std::{
    future::{Ready, ready},
    io,
    net::SocketAddr,
};

use tokio::net::TcpStream;

use super::SocketOptions;

/// Fail, because WASI preview 1 cannot open a connection.
pub fn connect(host: &str, port: u16, _options: SocketOptions) -> Ready<io::Result<TcpStream>> {
    ready(Err(io::Error::new(
        io::ErrorKind::Unsupported,
        format!(
            "cannot connect to host {host} port {port}: WASI preview 1 has no connect call, so \
             the embedder must install a krabka_client_core::transport::Connector"
        ),
    )))
}

/// Fail, because WASI has no name resolution.
pub fn resolve_name(host: &str, port: u16) -> Ready<io::Result<Vec<SocketAddr>>> {
    ready(Err(io::Error::new(
        io::ErrorKind::Unsupported,
        format!(
            "cannot resolve host {host} port {port}: WASI has no name resolution, so use an IP \
             address literal"
        ),
    )))
}
