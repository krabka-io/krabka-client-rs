//! The operating system side of [`super`] on a native target.

use std::{io, net::SocketAddr};

use krabka_units::{ByteSize, convert::ByteSizeExt as _};
use tokio::net::{TcpSocket, TcpStream};

use super::{SocketOptions, resolve_host};

/// Connect to each address of `host` at `port` in turn, and return the first
/// connection that opens.
pub async fn connect(host: &str, port: u16, options: SocketOptions) -> io::Result<TcpStream> {
    connect_first(resolve_host(host, port).await?, options).await
}

/// Resolve `host` at `port` with the operating system resolver.
pub async fn resolve_name(host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
    Ok(tokio::net::lookup_host((host, port)).await?.collect())
}

/// Connect to each of `addresses` in turn, as tokio's `TcpStream::connect`
/// does, and return the first connection that opens.
async fn connect_first(
    addresses: Vec<SocketAddr>,
    options: SocketOptions,
) -> io::Result<TcpStream> {
    let mut last_error = None;
    for address in addresses {
        match connect_address(address, options).await {
            Ok(stream) => return Ok(stream),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "could not resolve to any address",
        )
    }))
}

/// Connect to `address` with `options`.
async fn connect_address(address: SocketAddr, options: SocketOptions) -> io::Result<TcpStream> {
    let stream = configured_socket(address, options)?
        .connect(address)
        .await?;
    if options.nodelay {
        stream.set_nodelay(true).ok();
    }
    Ok(stream)
}

/// A TCP socket for `address` with the buffer sizes of `options`.
fn configured_socket(address: SocketAddr, options: SocketOptions) -> io::Result<TcpSocket> {
    let socket = if address.is_ipv4() {
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

/// `size` in bytes for a socket option, capped at `u32::MAX`.
fn buffer_size(size: ByteSize) -> u32 {
    u32::try_from(size.bytes_u64()).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_units::kibibytes;
    use tokio::net::TcpListener;

    use super::*;
    use crate::transport::{dial, dial_address};

    // A local address with no listener, which refuses a connection.
    fn refusing_address() -> SocketAddr {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap()
    }

    #[tokio::test]
    async fn dial_connects_through_the_operating_system_with_the_options() {
        for nodelay in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let options = SocketOptions {
                nodelay,
                ..SocketOptions::default()
            };

            let stream = dial_address(address, options).await.unwrap();

            check!(stream.peer_addr().unwrap() == address);
            check!(stream.nodelay().unwrap() == nodelay);
        }
    }

    #[tokio::test]
    async fn dial_resolves_a_host_name() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let stream = dial("localhost", address.port(), SocketOptions::default())
            .await
            .unwrap();

        check!(stream.peer_addr().unwrap() == address);
    }

    #[tokio::test]
    async fn connect_first_moves_past_a_refusing_address() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let stream = connect_first(vec![refusing_address(), address], SocketOptions::default())
            .await
            .unwrap();

        check!(stream.peer_addr().unwrap() == address);
    }

    #[tokio::test]
    async fn connect_first_fails_with_the_last_error_or_with_no_address() {
        let refused = connect_first(vec![refusing_address()], SocketOptions::default())
            .await
            .unwrap_err();
        let unresolved = connect_first(Vec::new(), SocketOptions::default())
            .await
            .unwrap_err();

        check!(refused.kind() == io::ErrorKind::ConnectionRefused);
        check!(
            (unresolved.kind(), unresolved.to_string())
                == (
                    io::ErrorKind::InvalidInput,
                    "could not resolve to any address".to_owned()
                )
        );
    }

    #[test]
    fn sockets_get_the_configured_buffer_sizes() {
        let address: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let requested = kibibytes(96);
        let configured = configured_socket(
            address,
            SocketOptions {
                send_buffer: Some(requested),
                receive_buffer: Some(requested),
                nodelay: true,
            },
        )
        .unwrap();
        let untouched = configured_socket(address, SocketOptions::default()).unwrap();
        // Linux doubles the value that the socket option sets.
        let bytes = buffer_size(requested);
        check!(configured.send_buffer_size().unwrap() >= bytes);
        check!(configured.recv_buffer_size().unwrap() >= bytes);
        check!(untouched.send_buffer_size().unwrap() != configured.send_buffer_size().unwrap());
    }
}
