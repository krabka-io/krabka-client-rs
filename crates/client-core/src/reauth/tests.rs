use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use assert2::check;
use bytes::BytesMut;
use krabka_protocol::{
    Encode,
    owned::{
        api_versions_request,
        api_versions_response::{ApiVersion as Advertised, ApiVersionsResponse},
        metadata_request::{self, MetadataRequest},
        metadata_response::MetadataResponse,
        sasl_authenticate_request,
        sasl_authenticate_response::SaslAuthenticateResponse,
        sasl_handshake_request,
        sasl_handshake_response::SaslHandshakeResponse,
    },
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

use super::*;
use crate::{Connection, ConnectionOptions};

/// What the scripted broker saw.
#[derive(Debug, Default)]
struct Seen {
    handshakes: usize,
    /// The `auth_bytes` of each `SaslAuthenticate`.
    tokens: Vec<Vec<u8>>,
}

/// A SASL broker over `server`. It gives each session `lifetime_ms`, answers
/// the `SaslAuthenticate` with index `reject_authenticate` with error 58, and
/// closes the connection at a Metadata request after the session ended.
async fn broker(
    mut server: DuplexStream,
    lifetime_ms: i64,
    reject_authenticate: Option<usize>,
    seen: Arc<Mutex<Seen>>,
) {
    let mut session_end: Option<Instant> = None;
    loop {
        let Ok(len) = server.read_u32().await else {
            return;
        };
        let mut request = vec![0_u8; len as usize];
        server.read_exact(&mut request).await.unwrap();
        let api_key = i16::from_be_bytes([request[0], request[1]]);
        let version = i16::from_be_bytes([request[2], request[3]]);
        let client_id_len = usize::from(u16::from_be_bytes([request[8], request[9]]));
        let flexible = api_key == sasl_authenticate_request::API_KEY && version >= 2;
        let mut body = BytesMut::new();
        body.put_slice(&request[4..8]);
        match api_key {
            api_versions_request::API_KEY => ApiVersionsResponse {
                api_keys: [
                    (metadata_request::API_KEY, 0),
                    (sasl_handshake_request::API_KEY, 1),
                    (sasl_authenticate_request::API_KEY, 2),
                ]
                .into_iter()
                .map(|(api_key, max_version)| Advertised {
                    api_key,
                    min_version: 0,
                    max_version,
                    ..Default::default()
                })
                .collect(),
                ..Default::default()
            }
            .encode(&mut body, 0)
            .unwrap(),
            sasl_handshake_request::API_KEY => {
                seen.lock().unwrap().handshakes += 1;
                SaslHandshakeResponse::default()
                    .encode(&mut body, version)
                    .unwrap();
            }
            sasl_authenticate_request::API_KEY => {
                let start = 10 + client_id_len + usize::from(flexible);
                let mut cursor = &request[start..];
                let decoded = <krabka_protocol::owned::sasl_authenticate_request::SaslAuthenticateRequest as krabka_protocol::Decode>::decode(&mut cursor, version).unwrap();
                let index = {
                    let mut seen = seen.lock().unwrap();
                    seen.tokens.push(decoded.auth_bytes.to_vec());
                    seen.tokens.len() - 1
                };
                let rejected = reject_authenticate == Some(index);
                if flexible {
                    body.put_u8(0);
                }
                SaslAuthenticateResponse {
                    error_code: if rejected { 58 } else { 0 },
                    error_message: rejected.then(|| "rejected".to_owned()),
                    session_lifetime_ms: if rejected { 0 } else { lifetime_ms },
                    ..Default::default()
                }
                .encode(&mut body, version)
                .unwrap();
                if !rejected && lifetime_ms > 0 {
                    session_end = Some(
                        Instant::now() + Duration::from_millis(u64::try_from(lifetime_ms).unwrap()),
                    );
                }
            }
            metadata_request::API_KEY => {
                if session_end.is_some_and(|end| Instant::now() > end) {
                    // Kafka's broker closes a connection whose session ended.
                    return;
                }
                MetadataResponse::default().encode(&mut body, 0).unwrap();
            }
            _ => continue,
        }
        server
            .write_u32(u32::try_from(body.len()).unwrap())
            .await
            .unwrap();
        server.write_all(&body).await.unwrap();
    }
}

/// What the client saw.
#[derive(Debug, PartialEq, Eq)]
struct Observed {
    requests: Vec<Result<(), String>>,
    handshakes: usize,
    tokens: Vec<Vec<u8>>,
    closed: bool,
}

/// Authenticate a connection with `creds`, then send a Metadata request
/// after each wait in `waits`, counted from the start.
async fn run(
    creds: SaslCredentials,
    lifetime_ms: i64,
    reject_authenticate: Option<usize>,
    waits: &[Duration],
    between: impl Fn(usize),
) -> Observed {
    let (mut client, server) = tokio::io::duplex(64 * 1024);
    let seen = Arc::new(Mutex::new(Seen::default()));
    let script = tokio::spawn(broker(
        server,
        lifetime_ms,
        reject_authenticate,
        Arc::clone(&seen),
    ));
    let options = ConnectionOptions {
        request_timeout: krabka_units::secs(3600),
        connections_max_idle: krabka_units::hours(24),
        ..ConnectionOptions::default()
    };
    let session = crate::sasl::outbound_sasl(
        &mut client,
        &creds,
        "localhost",
        &options.client_id,
        options.frame_max,
    )
    .await
    .unwrap();
    let reauth = session.needs_reauthentication().then(|| {
        Reauth::new(
            "127.0.0.1:9092".parse().unwrap(),
            creds,
            "localhost".to_owned(),
            session,
        )
    });
    let connection = Connection::from_stream_with_reauth(Box::new(client), options, reauth)
        .await
        .unwrap();
    let started = Instant::now();
    let mut requests = Vec::new();
    for (index, wait) in waits.iter().enumerate() {
        tokio::time::sleep_until(started + *wait).await;
        between(index);
        requests.push(
            connection
                .send(MetadataRequest::default())
                .await
                .map(drop)
                .map_err(|error| error.to_string()),
        );
    }
    let closed = connection.is_closed();
    connection.close();
    script.abort();
    let seen = seen.lock().unwrap();
    Observed {
        requests,
        handshakes: seen.handshakes,
        tokens: seen.tokens.clone(),
        closed,
    }
}

fn plain() -> SaslCredentials {
    SaslCredentials::Plain {
        username: "u".into(),
        password: "p".into(),
    }
}

const PLAIN_TOKEN: &[u8] = b"\0u\0p";

/// The table of KIP-368 cases. The due time is 85 to 95 percent of the
/// lifetime, so with 10 s a request at 9.6 s always comes after it.
#[tokio::test(start_paused = true)]
async fn connections_authenticate_again_before_the_session_ends() {
    let secs = Duration::from_secs_f64;
    for (name, lifetime_ms, reject, waits, expected) in [
        (
            "no session lifetime",
            0,
            None,
            vec![secs(3600.0)],
            Observed {
                requests: vec![Ok(())],
                handshakes: 1,
                tokens: vec![PLAIN_TOKEN.to_vec()],
                closed: false,
            },
        ),
        (
            "a request before the due time",
            10_000,
            None,
            vec![secs(8.0)],
            Observed {
                requests: vec![Ok(())],
                handshakes: 1,
                tokens: vec![PLAIN_TOKEN.to_vec()],
                closed: false,
            },
        ),
        (
            "a request after the due time authenticates first",
            10_000,
            None,
            vec![secs(9.6)],
            Observed {
                requests: vec![Ok(())],
                handshakes: 2,
                tokens: vec![PLAIN_TOKEN.to_vec(), PLAIN_TOKEN.to_vec()],
                closed: false,
            },
        ),
        (
            "the connection outlives the first session",
            10_000,
            None,
            vec![secs(9.6), secs(11.0)],
            Observed {
                requests: vec![Ok(()), Ok(())],
                handshakes: 2,
                tokens: vec![PLAIN_TOKEN.to_vec(), PLAIN_TOKEN.to_vec()],
                closed: false,
            },
        ),
        (
            "a rejected re-authentication fails the request and closes",
            10_000,
            Some(1),
            vec![secs(9.6)],
            Observed {
                requests: vec![Err("authentication with 127.0.0.1:9092 failed: SASL \
                                    authentication failed: SaslAuthenticate(PLAIN) \
                                    error_code=58 error_message=Some(\"rejected\")"
                    .to_owned())],
                handshakes: 2,
                tokens: vec![PLAIN_TOKEN.to_vec(), PLAIN_TOKEN.to_vec()],
                closed: true,
            },
        ),
    ] {
        let observed = run(plain(), lifetime_ms, reject, &waits, |_| {}).await;
        check!(observed == expected, "{name}");
    }
}

/// Each OAUTHBEARER exchange reads the token file again, so a
/// re-authentication sends a refreshed token.
#[tokio::test(start_paused = true)]
async fn oauthbearer_reauthentication_sends_the_refreshed_token() {
    let dir = tempfile::tempdir().unwrap();
    let token_path = dir.path().join("token");
    std::fs::write(&token_path, "first").unwrap();
    let refreshed = token_path.clone();
    let observed = run(
        SaslCredentials::OAuthBearer {
            token: crate::sasl::OAuthBearerTokenSource::File(token_path),
            extensions: std::collections::BTreeMap::new(),
        },
        10_000,
        None,
        &[Duration::from_secs_f64(9.6)],
        move |_| std::fs::write(&refreshed, "second").unwrap(),
    )
    .await;
    check!(
        observed
            == Observed {
                requests: vec![Ok(())],
                handshakes: 2,
                tokens: vec![
                    b"n,,\x01auth=Bearer first\x01\x01".to_vec(),
                    b"n,,\x01auth=Bearer second\x01\x01".to_vec(),
                ],
                closed: false,
            }
    );
}

#[test]
fn the_due_time_is_85_to_95_percent_of_the_lifetime() {
    let session = |authenticate_version, lifetime| SaslSession {
        handshake_version: 1,
        authenticate_version,
        session_lifetime_ms: lifetime,
    };
    let now = Instant::now();
    let after = |due: Option<Instant>| due.map(|due| due.duration_since(now).as_millis());
    for (name, session, unit, expected) in [
        (
            "low jitter",
            session(Some(2), Some(10_000)),
            0.0,
            Some(8500),
        ),
        (
            "high jitter",
            session(Some(2), Some(10_000)),
            0.999_9,
            Some(9499),
        ),
        ("no lifetime", session(Some(2), None), 0.5, None),
        (
            "SaslAuthenticate v0",
            session(Some(0), Some(10_000)),
            0.5,
            None,
        ),
        (
            "no SaslAuthenticate",
            session(None, Some(10_000)),
            0.5,
            None,
        ),
    ] {
        let due = after(due_time(&session, unit));
        check!(
            due.is_none_or(|ms| expected.is_some_and(|e| ms >= e && ms <= e + 1))
                && due.is_some() == expected.is_some(),
            "{name}: {due:?}"
        );
    }
}
