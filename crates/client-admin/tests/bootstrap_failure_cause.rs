//! A failed bootstrap keeps its cause.
//!
//! A caller must be able to tell an unreachable broker from a TLS failure and
//! from a SASL rejection without a probe of its own. Each case here makes
//! `AdminClient::connect_secured` fail in one of those ways, and then reads the
//! cause the way a caller does. An unreachable broker gives
//! `AdminError::Connect` with the last cause. A rejected authentication stops
//! the bootstrap at once, as Kafka's `AdminMetadataManager` makes an
//! `AuthenticationException` fatal.

use std::net::SocketAddr;

use assert2::assert;
use bytes::BytesMut;
use krabka_client_admin::{AdminClient, AdminError};
use krabka_client_core::{
    AuthenticationError, ClientError, MockBroker, SaslAuthenticationError,
    security::{ClientSecurity, SaslCredentials, TlsConnectorConfig},
};
use krabka_protocol::{
    Encode,
    owned::{
        sasl_authenticate_request,
        sasl_authenticate_response::{self, SaslAuthenticateResponse},
        sasl_handshake_request,
        sasl_handshake_response::SaslHandshakeResponse,
    },
};
use krabka_security::ListenerProtocol;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

/// Kafka `Errors.SASL_AUTHENTICATION_FAILED`.
const SASL_AUTHENTICATION_FAILED: i16 = 58;
/// The message that Kafka's `PlainServerCallbackHandler` gives for a wrong
/// password.
const REJECTED_MESSAGE: &str = "Authentication failed: Invalid username or password";

/// The cause of a failed bootstrap, as a caller classifies it.
#[derive(Debug, PartialEq, Eq)]
enum Cause {
    Unreachable(SocketAddr, std::io::ErrorKind),
    TlsRejected(SocketAddr, std::io::ErrorKind),
    Rejected(SocketAddr, String),
}

/// The number of addresses tried (`None` for an error that stops the
/// bootstrap at once) and the cause.
fn classify(error: &AdminError) -> Option<(Option<usize>, Cause)> {
    let (tried, source) = match error {
        AdminError::Connect {
            tried,
            source: Some(source),
        } => (Some(*tried), source.as_ref()),
        error => (None, error),
    };
    let cause = match source {
        AdminError::Transport(ClientError::Connect { addr, source }) => {
            Cause::Unreachable(*addr, source.kind())
        }
        AdminError::Transport(ClientError::Authentication {
            addr,
            source: AuthenticationError::Tls(source),
        }) => Cause::TlsRejected(*addr, source.kind()),
        AdminError::Transport(ClientError::Authentication {
            addr,
            source: AuthenticationError::Sasl(SaslAuthenticationError::Failed(message)),
        }) => Cause::Rejected(*addr, message.clone()),
        _ => return None,
    };
    Some((tried, cause))
}

/// An address that refuses TCP connects.
async fn refused_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}

/// A listener that answers a TLS `ClientHello` with plaintext bytes, as a
/// plaintext listener does. It holds the connection open until the client
/// closes it.
async fn plaintext_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                if stream
                    .write_all(b"HTTP/1.0 400 Bad Request\r\n\r\n")
                    .await
                    .is_err()
                {
                    return;
                }
                let mut buf = [0_u8; 1024];
                while matches!(stream.read(&mut buf).await, Ok(n) if n > 0) {}
            });
        }
    });
    addr
}

/// A SASL listener that offers PLAIN and rejects every password.
async fn rejecting_broker() -> MockBroker {
    MockBroker::start(|api_key, version, _correlation_id, _body| {
        let mut bytes = BytesMut::new();
        match api_key {
            sasl_handshake_request::API_KEY => SaslHandshakeResponse {
                mechanisms: vec!["PLAIN".into()],
                ..Default::default()
            }
            .encode(&mut bytes, version)
            .unwrap(),
            sasl_authenticate_request::API_KEY => {
                if version >= sasl_authenticate_response::FLEXIBLE_MIN {
                    bytes.extend_from_slice(&[0]);
                }
                SaslAuthenticateResponse {
                    error_code: SASL_AUTHENTICATION_FAILED,
                    error_message: Some(REJECTED_MESSAGE.into()),
                    ..Default::default()
                }
                .encode(&mut bytes, version)
                .unwrap();
            }
            _ => return None,
        }
        Some(bytes.to_vec())
    })
    .await
}

fn tls() -> ClientSecurity {
    ClientSecurity {
        protocol: ListenerProtocol::Ssl,
        tls: Some(TlsConnectorConfig::default()),
        sasl: None,
        sasl_host: None,
    }
}

fn sasl_plain() -> ClientSecurity {
    ClientSecurity {
        protocol: ListenerProtocol::SaslPlaintext,
        tls: None,
        sasl: Some(SaslCredentials::Plain {
            username: "alice".into(),
            password: "wrong".into(),
        }),
        sasl_host: None,
    }
}

#[derive(Clone, Copy, Debug)]
enum Scenario {
    Unreachable,
    TlsToPlaintextListener,
    RejectedPassword,
    UnreachableThenRejectedPassword,
}

#[tokio::test]
async fn connect_error_keeps_the_last_bootstrap_cause() {
    let rejected = format!(
        "SaslAuthenticate(PLAIN) error_code={SASL_AUTHENTICATION_FAILED} \
         error_message=Some({REJECTED_MESSAGE:?})"
    );
    for scenario in [
        Scenario::Unreachable,
        Scenario::TlsToPlaintextListener,
        Scenario::RejectedPassword,
        Scenario::UnreachableThenRejectedPassword,
    ] {
        let mut brokers = Vec::new();
        let (bootstrap, security, expected) = match scenario {
            Scenario::Unreachable => {
                let addr = refused_addr().await;
                (
                    vec![addr],
                    None,
                    (
                        Some(1),
                        Cause::Unreachable(addr, std::io::ErrorKind::ConnectionRefused),
                    ),
                )
            }
            Scenario::TlsToPlaintextListener => {
                let addr = plaintext_addr().await;
                (
                    vec![addr],
                    Some(tls()),
                    (
                        None,
                        Cause::TlsRejected(addr, std::io::ErrorKind::InvalidData),
                    ),
                )
            }
            Scenario::RejectedPassword => {
                let broker = rejecting_broker().await;
                let addr = broker.addr;
                brokers.push(broker);
                (
                    vec![addr],
                    Some(sasl_plain()),
                    (None, Cause::Rejected(addr, rejected.clone())),
                )
            }
            Scenario::UnreachableThenRejectedPassword => {
                let broker = rejecting_broker().await;
                let addr = broker.addr;
                brokers.push(broker);
                (
                    vec![refused_addr().await, addr],
                    Some(sasl_plain()),
                    (None, Cause::Rejected(addr, rejected.clone())),
                )
            }
        };
        let bootstrap = bootstrap
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();

        let error = AdminClient::connect_secured(&bootstrap, security)
            .await
            .err()
            .unwrap_or_else(|| panic!("{scenario:?}: connect succeeded"));

        assert!(
            classify(&error) == Some(expected),
            "{scenario:?}: {error:?}"
        );
        for broker in brokers {
            broker.stop();
        }
    }
}

#[tokio::test]
async fn connect_error_display_names_the_last_cause() {
    let addr = refused_addr().await;

    let error = AdminClient::connect(&[addr.to_string()])
        .await
        .err()
        .unwrap();

    let prefix = format!(
        "no bootstrap address connected: tried 1; last error: client-core: connect to {addr}: "
    );
    assert!(error.to_string().starts_with(&prefix), "{error}");
}

#[tokio::test]
async fn connect_error_has_no_cause_for_an_empty_bootstrap_list() {
    let error = AdminClient::connect(&[]).await.err().unwrap();

    assert!(matches!(
        error,
        AdminError::Connect {
            tried: 0,
            source: None
        }
    ));
}
