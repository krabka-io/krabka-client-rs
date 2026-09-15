//! Error type for `krabka-client-core`.

use std::net::SocketAddr;

use krabka_units::{Time, fmt::Human as _};
use thiserror::Error;

/// Errors returned by `Client`, `Connection`, and the broker pool.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ClientError {
    #[error("connect to {addr}: {source}")]
    Connect {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },

    /// The TCP connection opened, but the TLS handshake failed before the
    /// peer gave a verdict, for example on EOF, a reset, or a `close_notify`
    /// alert. The caller can connect again. A rejection by the peer is
    /// [`ClientError::Authentication`].
    #[error("TLS handshake with {addr}: {source}")]
    Tls {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },

    /// The SASL exchange failed after the TCP connection, and TLS if the
    /// protocol uses it, came up, and the broker did not reject the
    /// credentials. The stream failed, a frame was too large, or the broker
    /// sent an error code that is not an authentication error. The caller can
    /// connect again. A rejection by the broker is
    /// [`ClientError::Authentication`].
    #[error("SASL authentication with {addr}: {source}")]
    Sasl {
        addr: SocketAddr,
        #[source]
        source: crate::sasl::OutboundSaslError,
    },

    /// The TLS peer or the SASL broker rejected authentication.
    ///
    /// Kafka's `NetworkClient.processDisconnection` stores this
    /// `AuthenticationException` for the node, and the client raises it from
    /// the next call without a retry. Do not evict and retry on this error.
    #[error("authentication with {addr} failed: {source}")]
    Authentication {
        addr: SocketAddr,
        #[source]
        source: AuthenticationError,
    },

    #[error("connection closed")]
    Disconnected,

    #[error("invalid client configuration: {0}")]
    InvalidConfig(String),

    #[error("request timed out after {}", .0.human())]
    Timeout(Time),

    #[error(
        "incompatible version: broker supports {broker_min}..={broker_max}, \
         client wants {client_min}..={client_max} for api_key {api_key}"
    )]
    IncompatibleVersion {
        api_key: i16,
        broker_min: i16,
        broker_max: i16,
        client_min: i16,
        client_max: i16,
    },

    #[error("protocol error from server: {error_code}")]
    Server { error_code: i16 },

    #[error("FindCoordinator returned no entry for key {key:?}")]
    NoCoordinator { key: String },

    #[error("codec: {0}")]
    Codec(#[from] krabka_protocol::ProtocolError),

    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
}

impl ClientError {
    /// Whether the peer rejected authentication. Kafka raises such an
    /// `AuthenticationException` to the application without a retry.
    #[must_use]
    pub const fn is_authentication_failure(&self) -> bool {
        matches!(self, Self::Authentication { .. })
    }
}

/// The reason that a peer rejected authentication. Each variant is a subclass
/// of Kafka's `AuthenticationException`.
#[derive(Debug, Error)]
pub enum AuthenticationError {
    /// Kafka `SslAuthenticationException`: the TLS handshake failed with an
    /// error from the TLS engine, for example an untrusted certificate, a
    /// fatal alert from the peer, or a peer that does not speak TLS.
    #[error("TLS handshake failed: {0}")]
    Tls(#[source] std::io::Error),

    /// The broker or the SASL mechanism rejected authentication.
    #[error(transparent)]
    Sasl(#[from] crate::sasl::SaslAuthenticationError),
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn display_is_useful() {
        let e = ClientError::Timeout(krabka_units::secs(5));
        assert!(e.to_string() == "request timed out after 5s");
    }

    #[test]
    fn incompatible_version_displays_full_range() {
        let e = ClientError::IncompatibleVersion {
            api_key: 0,
            broker_min: 0,
            broker_max: 5,
            client_min: 7,
            client_max: 10,
        };
        assert!(e.to_string().contains("api_key 0"));
        assert!(e.to_string().contains("broker supports 0..=5"));
    }
}
