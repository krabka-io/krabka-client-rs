//! Transport-agnostic outbound SASL handshake for the client.
//!
//! [`outbound_sasl`] drives the client side of Kafka's
//! `SaslHandshake` + `SaslAuthenticate` exchange over any
//! `AsyncRead + AsyncWrite` stream. That stream can be a plaintext
//! `TcpStream`, a `tokio_rustls` TLS stream, or an in-process duplex for
//! tests. It supports PLAIN with one round-trip, SCRAM-SHA-256/512 with two
//! round-trips and server-final verification, OAUTHBEARER with the RFC 7628
//! success/failure exchange, and GSSAPI with multi-round AP-REQ / AP-REP plus
//! RFC 4752 security-layer negotiation.
//!
//! This is the shared implementation the broker's inter-broker dialer
//! and the public clients both call. The only difference is the
//! credentials value and the reporter `client_id`.

use std::path::PathBuf;

use bytes::{Buf, BufMut, BytesMut};
use krabka_ids::{ApiKey, ApiVersion};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse,
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
    },
};
use krabka_security::{SaslMechanism, ScramClientExchange};
use krabka_units::{ByteSize, kibibytes};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::ClientFrameMax;

const API_KEY_SASL_HANDSHAKE: i16 = 17;
const API_KEY_SASL_AUTHENTICATE: i16 = 36;
const API_KEY_API_VERSIONS: i16 = 18;

/// Kafka's `SaslClientAuthenticator.MAX_RESERVED_CORRELATION_ID`.
const MAX_RESERVED_CORRELATION_ID: i32 = i32::MAX;
/// Kafka's `SaslClientAuthenticator.MIN_RESERVED_CORRELATION_ID`. The SASL
/// requests of a connection use the reserved ids, so they never share an id
/// with a normal request.
pub(crate) const MIN_RESERVED_CORRELATION_ID: i32 = MAX_RESERVED_CORRELATION_ID - 7;

/// The next reserved SASL correlation id, as Kafka's
/// `SaslClientAuthenticator.nextCorrelationId` gives it.
fn next_correlation_id(corr_id: &mut i32) -> i32 {
    if *corr_id < MIN_RESERVED_CORRELATION_ID {
        *corr_id = MIN_RESERVED_CORRELATION_ID;
    }
    let current = *corr_id;
    *corr_id = corr_id.wrapping_add(1);
    current
}

#[derive(Clone, Copy)]
struct SaslPolicy<'a> {
    client_id: &'a str,
    frame_max: ClientFrameMax,
    /// The `SaslAuthenticate` version, or `None` when the broker does not list
    /// `SaslAuthenticate` and the tokens go without a Kafka header
    /// (`DISABLE_KAFKA_SASL_AUTHENTICATE_HEADER`).
    authenticate_version: Option<i16>,
}

/// The `SaslHandshake` and `SaslAuthenticate` versions for a broker's
/// `ApiVersions` answer, as Kafka's
/// `SaslClientAuthenticator.setSaslAuthenticateAndHandshakeVersions` picks
/// them: the highest version of each that both sides support. With no
/// `SaslHandshake` entry the handshake uses version 0, and with no
/// `SaslAuthenticate` entry the tokens go without a Kafka header.
fn sasl_versions(response: &ApiVersionsResponse) -> (i16, Option<i16>) {
    let max = |api_key: i16| {
        response
            .api_keys
            .iter()
            .find(|key| key.api_key == api_key)
            .map(|key| key.max_version)
    };
    let handshake = max(API_KEY_SASL_HANDSHAKE).map_or(0, |version| {
        version.min(krabka_protocol::owned::sasl_handshake_request::MAX_VERSION)
    });
    let authenticate = max(API_KEY_SASL_AUTHENTICATE)
        .map(|version| version.min(krabka_protocol::owned::sasl_authenticate_request::MAX_VERSION));
    (handshake, authenticate)
}

/// Maximum receive size advertised in the client's RFC 4752 security-layer
/// choice. Auth-only QOP wraps no data after the handshake, so the value only
/// needs to be a reasonable non-zero buffer. It mirrors the server's offer
/// size.
const GSSAPI_MAX_RECV: ByteSize = kibibytes(64);

/// Outbound SASL credentials.
///
/// This enum mirrors the broker's `InterBrokerCredentials`. It has one
/// variant per supported mechanism.
#[derive(Debug, Clone)]
pub enum SaslCredentials {
    /// SASL/PLAIN: `\0username\0password`.
    Plain { username: String, password: String },
    /// SASL/SCRAM (SHA-256 or SHA-512).
    Scram {
        mechanism: SaslMechanism,
        username: String,
        password: String,
    },
    /// SASL/GSSAPI: authenticate as `client_principal` with the long-term
    /// key in `keytab_path`. This mechanism needs no password.
    Gssapi {
        keytab_path: PathBuf,
        client_principal: String,
        service_name: String,
        kdc_url: String,
    },
    /// SASL/OAUTHBEARER: a file containing an RFC 6750 bearer token. The file
    /// is read for every new connection so token rotation needs no restart.
    OAuthBearer { token_path: PathBuf },
}

impl SaslCredentials {
    /// The SASL mechanism this credential set authenticates with.
    #[must_use]
    pub fn mechanism(&self) -> SaslMechanism {
        match self {
            Self::Plain { .. } => SaslMechanism::Plain,
            Self::Scram { mechanism, .. } => *mechanism,
            Self::Gssapi { .. } => SaslMechanism::Gssapi,
            Self::OAuthBearer { .. } => SaslMechanism::OAuthBearer,
        }
    }
}

/// Kafka `Errors.UNSUPPORTED_SASL_MECHANISM`.
const UNSUPPORTED_SASL_MECHANISM: i16 = 33;
/// Kafka `Errors.ILLEGAL_SASL_STATE`.
const ILLEGAL_SASL_STATE: i16 = 34;
/// Kafka `Errors.SASL_AUTHENTICATION_FAILED`.
const SASL_AUTHENTICATION_FAILED: i16 = 58;

/// Errors raised during the outbound SASL handshake.
///
/// Only [`OutboundSaslError::Authentication`] is a rejection. The other
/// variants give no verdict from the broker, and the caller can connect again.
#[derive(Debug, Error)]
pub enum OutboundSaslError {
    /// The stream failed before the broker answered, for example on EOF or a
    /// reset. Kafka's `NetworkClient` keeps a disconnect in the `AUTHENTICATE`
    /// state retriable.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// The client could not encode a request, or a response frame was larger
    /// than the frame limit.
    #[error("codec: {0}")]
    Codec(String),
    /// The broker answered `SaslAuthenticate` with an error code that is not
    /// an `AuthenticationException` in Kafka. Kafka's `Selector` closes the
    /// connection in the `AUTHENTICATE` state, and the client retries.
    #[error("SaslAuthenticate error_code={error_code} error_message={error_message:?}")]
    Server {
        error_code: i16,
        error_message: Option<String>,
    },
    /// The broker or the SASL mechanism rejected authentication.
    #[error(transparent)]
    Authentication(#[from] SaslAuthenticationError),
}

/// A SASL authentication rejection. Each variant is a subclass of Kafka's
/// `AuthenticationException`, which the client raises to the application
/// without a retry.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum SaslAuthenticationError {
    /// Kafka `UnsupportedSaslMechanismException`: the broker does not enable
    /// the client mechanism (`UNSUPPORTED_SASL_MECHANISM`, 33).
    #[error("unsupported SASL mechanism: {0}")]
    UnsupportedMechanism(String),
    /// Kafka `IllegalSaslStateException`: `ILLEGAL_SASL_STATE` (34), an
    /// unknown `SaslHandshake` error code, or a response that the client
    /// cannot parse.
    #[error("illegal SASL state: {0}")]
    IllegalState(String),
    /// Kafka `SaslAuthenticationException`: `SASL_AUTHENTICATION_FAILED` (58),
    /// or a failure of the mechanism on the client.
    #[error("SASL authentication failed: {0}")]
    Failed(String),
}

/// Map a `SaslAuthenticate` response with a non-zero error code to the error
/// that Kafka's `SaslClientAuthenticator.receiveToken` raises
/// (`Errors.exception`).
fn authenticate_error(mechanism: &str, response: &SaslAuthenticateResponse) -> OutboundSaslError {
    let message = format!(
        "SaslAuthenticate({mechanism}) error_code={} error_message={:?}",
        response.error_code, response.error_message
    );
    match response.error_code {
        UNSUPPORTED_SASL_MECHANISM => SaslAuthenticationError::UnsupportedMechanism(message).into(),
        ILLEGAL_SASL_STATE => SaslAuthenticationError::IllegalState(message).into(),
        SASL_AUTHENTICATION_FAILED => SaslAuthenticationError::Failed(message).into(),
        error_code => OutboundSaslError::Server {
            error_code,
            error_message: response.error_message.clone(),
        },
    }
}

/// A failure of the mechanism on the client. Kafka's
/// `SaslClientAuthenticator.createSaslToken` raises it as a
/// `SaslAuthenticationException`.
fn mechanism_failure(message: String) -> OutboundSaslError {
    SaslAuthenticationError::Failed(message).into()
}

/// A response that the client cannot parse. Kafka's
/// `SaslClientAuthenticator.receiveKafkaResponse` raises it as an
/// `IllegalSaslStateException`.
fn unparsable_response(message: String) -> OutboundSaslError {
    SaslAuthenticationError::IllegalState(message).into()
}

/// Run the outbound SASL handshake to completion over `stream`.
///
/// This function sends `ApiVersions` v0, then `SaslHandshake` with the
/// mechanism in `creds` at the version that the broker lists, then drives the
/// mechanism-specific `SaslAuthenticate` round-trips, as Kafka's
/// `SaslClientAuthenticator` does. The requests use Kafka's reserved SASL
/// correlation ids. `server_name`
/// is the broker's canonical hostname. Only the GSSAPI path uses it, to build
/// the target SPN (`service_name/server_name`). The function returns once the
/// broker has accepted the credentials. The same `stream` is then usable for
/// normal Kafka RPCs.
///
/// # Errors
///
/// Returns [`OutboundSaslError`] on I/O failure, codec failure, or a
/// non-zero broker SASL error code.
pub async fn outbound_sasl<S>(
    stream: &mut S,
    creds: &SaslCredentials,
    server_name: &str,
    client_id: &str,
    frame_max: ClientFrameMax,
) -> Result<(), OutboundSaslError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + ?Sized,
{
    let mut corr_id = MIN_RESERVED_CORRELATION_ID;
    // Step 1: ApiVersions v0, as Kafka's `SaslClientAuthenticator` sends it
    //         first. A broker reads a request that it cannot parse as a
    //         GSSAPI token, so the client uses version 0.
    let api_versions = send_api_versions(stream, &mut corr_id, client_id, frame_max).await?;
    let (handshake_version, authenticate_version) = sasl_versions(&api_versions);
    let policy = SaslPolicy {
        client_id,
        frame_max,
        authenticate_version,
    };
    // Step 2: SaslHandshake with the chosen mechanism, at the version that
    //         ApiVersions allows.
    send_sasl_handshake(
        stream,
        creds.mechanism(),
        handshake_version,
        &mut corr_id,
        policy,
    )
    .await?;
    // Step 3: SaslAuthenticate (one round for PLAIN, two for SCRAM, three
    //         for GSSAPI).
    match creds {
        SaslCredentials::Plain { username, password } => {
            send_plain_authenticate(stream, username, password, &mut corr_id, policy).await
        }
        SaslCredentials::Scram {
            mechanism,
            username,
            password,
        } => run_scram_client(stream, username, password, *mechanism, &mut corr_id, policy).await,
        SaslCredentials::Gssapi {
            keytab_path,
            client_principal,
            service_name,
            kdc_url,
        } => {
            run_gssapi_client(
                stream,
                keytab_path,
                client_principal,
                (service_name, server_name),
                kdc_url,
                &mut corr_id,
                policy,
            )
            .await
        }
        SaslCredentials::OAuthBearer { token_path } => {
            let token = tokio::fs::read(token_path).await.map_err(|error| {
                mechanism_failure(format!(
                    "cannot read OAUTHBEARER token {}: {error}",
                    token_path.display()
                ))
            })?;
            run_oauthbearer_client(stream, token.trim_ascii(), &mut corr_id, policy).await
        }
    }
}

/// Run the RFC 7628 OAUTHBEARER client exchange.
///
/// Success is one `SaslAuthenticate` round with empty server `auth_bytes`.
/// On rejection the server returns RFC 7628 error JSON with `error_code = 0`;
/// the client must send a single `\x01` final message before surfacing the
/// authentication failure.
async fn run_oauthbearer_client<S>(
    stream: &mut S,
    token: &[u8],
    corr_id: &mut i32,
    policy: SaslPolicy<'_>,
) -> Result<(), OutboundSaslError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + ?Sized,
{
    if token.is_empty() || token.contains(&b'\x01') {
        return Err(mechanism_failure(
            "OAUTHBEARER token must be non-empty and contain no RFC 7628 separator".into(),
        ));
    }

    let mut initial = Vec::with_capacity(token.len() + 20);
    initial.extend_from_slice(b"n,,\x01auth=Bearer ");
    initial.extend_from_slice(token);
    initial.extend_from_slice(b"\x01\x01");

    let response = send_sasl_authenticate(stream, initial, corr_id, policy).await?;
    if response.error_code != 0 {
        return Err(authenticate_error("OAUTHBEARER", &response));
    }
    if response.auth_bytes.is_empty() {
        return Ok(());
    }

    let final_response = send_sasl_authenticate(stream, vec![b'\x01'], corr_id, policy).await?;
    if final_response.error_code != 0 {
        return Err(authenticate_error("OAUTHBEARER", &final_response));
    }
    Err(mechanism_failure(format!(
        "SaslAuthenticate(OAUTHBEARER) rejected bearer token: {}",
        String::from_utf8_lossy(&response.auth_bytes)
    )))
}

/// Send `ApiVersions` v0 and read the response.
async fn send_api_versions<S>(
    stream: &mut S,
    corr_id: &mut i32,
    client_id: &str,
    frame_max: ClientFrameMax,
) -> Result<ApiVersionsResponse, OutboundSaslError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + ?Sized,
{
    let mut body = BytesMut::new();
    ApiVersionsRequest::default()
        .encode(&mut body, 0)
        .map_err(|e| OutboundSaslError::Codec(format!("ApiVersions encode: {e}")))?;
    let resp_bytes = round_trip(
        stream,
        ApiKey(API_KEY_API_VERSIONS),
        ApiVersion(0),
        next_correlation_id(corr_id),
        false,
        &body,
        SaslPolicy {
            client_id,
            frame_max,
            authenticate_version: None,
        },
    )
    .await?;
    let mut cur: &[u8] = &resp_bytes;
    ApiVersionsResponse::decode(&mut cur, 0)
        .map_err(|e| unparsable_response(format!("ApiVersions decode: {e}")))
}

/// Send `SaslHandshake` at `version` with the wire name for `mechanism`, read
/// the response, and fail if `error_code != 0`.
///
/// Wire framing: `SaslHandshake` v0 and v1 use the non-flexible request
/// header (v1, no trailing tagged-fields byte) and a non-flexible response
/// header (v0, bare `correlation_id`).
async fn send_sasl_handshake<S>(
    stream: &mut S,
    mechanism: SaslMechanism,
    version: i16,
    corr_id: &mut i32,
    policy: SaslPolicy<'_>,
) -> Result<(), OutboundSaslError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + ?Sized,
{
    let req = SaslHandshakeRequest {
        mechanism: mechanism.wire_name().to_string(),
        ..Default::default()
    };
    let mut body = BytesMut::new();
    req.encode(&mut body, version)
        .map_err(|e| OutboundSaslError::Codec(format!("SaslHandshake encode: {e}")))?;
    let resp_bytes = round_trip(
        stream,
        ApiKey(API_KEY_SASL_HANDSHAKE),
        ApiVersion(version),
        next_correlation_id(corr_id),
        false,
        &body,
        policy,
    )
    .await?;
    let mut cur: &[u8] = &resp_bytes;
    let resp = SaslHandshakeResponse::decode(&mut cur, version)
        .map_err(|e| unparsable_response(format!("SaslHandshake decode: {e}")))?;
    handshake_result(mechanism, &resp)
}

/// Map a `SaslHandshake` response to the result that Kafka's
/// `SaslClientAuthenticator.handleSaslHandshakeResponse` gives.
fn handshake_result(
    mechanism: SaslMechanism,
    response: &SaslHandshakeResponse,
) -> Result<(), OutboundSaslError> {
    let mechanism = mechanism.wire_name();
    let enabled = &response.mechanisms;
    let error = match response.error_code {
        0 => return Ok(()),
        UNSUPPORTED_SASL_MECHANISM => SaslAuthenticationError::UnsupportedMechanism(format!(
            "client SASL mechanism '{mechanism}' not enabled in the server, enabled mechanisms \
             are {enabled:?}"
        )),
        ILLEGAL_SASL_STATE => SaslAuthenticationError::IllegalState(format!(
            "unexpected handshake request with client mechanism {mechanism}, enabled mechanisms \
             are {enabled:?}"
        )),
        code => SaslAuthenticationError::IllegalState(format!(
            "unknown error code {code}, client mechanism is {mechanism}, enabled mechanisms are \
             {enabled:?}"
        )),
    };
    Err(error.into())
}

/// Send `SaslAuthenticate v2` with PLAIN payload `\0user\0password`, read
/// the response, fail if `error_code != 0`. PLAIN is one round-trip.
async fn send_plain_authenticate<S>(
    stream: &mut S,
    user: &str,
    pass: &str,
    corr_id: &mut i32,
    policy: SaslPolicy<'_>,
) -> Result<(), OutboundSaslError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + ?Sized,
{
    let mut payload = Vec::with_capacity(2 + user.len() + pass.len());
    payload.push(0); // authzid (empty)
    payload.extend_from_slice(user.as_bytes());
    payload.push(0);
    payload.extend_from_slice(pass.as_bytes());

    let resp = send_sasl_authenticate(stream, payload, corr_id, policy).await?;
    if resp.error_code != 0 {
        return Err(authenticate_error("PLAIN", &resp));
    }
    Ok(())
}

/// Run the RFC 5802 SCRAM (SHA-256 or SHA-512) client state machine
/// over two `SaslAuthenticate v2` round-trips.
///
/// This function verifies the server-final signature before it declares the
/// connection authenticated.
async fn run_scram_client<S>(
    stream: &mut S,
    user: &str,
    pass: &str,
    mechanism: SaslMechanism,
    corr_id: &mut i32,
    policy: SaslPolicy<'_>,
) -> Result<(), OutboundSaslError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + ?Sized,
{
    let exch = ScramClientExchange::new(user.to_string(), pass.as_bytes().to_vec(), mechanism);

    // Round 1: client-first → server-first.
    let (client_first, exch) = exch
        .client_first()
        .map_err(|e| mechanism_failure(format!("scram client_first: {e:?}")))?;
    let resp1 = send_sasl_authenticate(stream, client_first, corr_id, policy).await?;
    if resp1.error_code != 0 {
        return Err(authenticate_error("SCRAM round 1", &resp1));
    }
    let server_first = resp1.auth_bytes.to_vec();

    // Round 2: client-final → server-final.
    let (client_final, exch) = exch
        .step(&server_first)
        .map_err(|e| mechanism_failure(format!("scram client step: {e:?}")))?;
    let resp2 = send_sasl_authenticate(stream, client_final, corr_id, policy).await?;
    if resp2.error_code != 0 {
        return Err(authenticate_error("SCRAM round 2", &resp2));
    }
    // Server-final verification proves the broker holds the matching
    // `server_key` — not just any compatible `stored_key`.
    exch.verify_server_final(&resp2.auth_bytes)
        .map_err(|e| mechanism_failure(format!("server-final verify: {e:?}")))?;
    Ok(())
}

/// Run the SASL/GSSAPI (Kerberos) client state machine over
/// `SaslAuthenticate v2` round-trips.
///
/// This function builds an `sspi`-backed initiator that authenticates as
/// `client_principal` with the long-term key in `keytab_path` and no password.
/// The initiator targets the SPN `service_name/server_name` of the broker it
/// dials. `server_name` is the broker's canonical hostname, the same value
/// used for TLS SNI, and not the dialed IP. The SPN must match the service key
/// in the broker's keytab.
///
/// The function then drives [`GssapiClientExchange`]: the GSS context
/// establishment (AP-REQ → AP-REP) and then the RFC 4752 auth-only
/// security-layer negotiation. It sends each non-terminal client token as
/// request `auth_bytes`. The server's reply token feeds the next step until
/// the exchange reports `Done`.
///
/// The first initiator step does the synchronous AS/TGS exchange with the
/// KDC. Later steps only process tokens locally.
async fn run_gssapi_client<S>(
    stream: &mut S,
    keytab_path: &std::path::Path,
    client_principal: &str,
    service_and_server_name: (&str, &str),
    kdc_url: &str,
    corr_id: &mut i32,
    policy: SaslPolicy<'_>,
) -> Result<(), OutboundSaslError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + ?Sized,
{
    use krabka_security::gssapi::{
        client::{ClientStep, GssapiClientExchange},
        provider::SspiInitiator,
    };

    let (service_name, server_name) = service_and_server_name;
    let target_spn = format!("{service_name}/{server_name}");
    let keytab = keytab_path.to_string_lossy().into_owned();
    let (client_principal, kdc_url) = (client_principal.to_owned(), kdc_url.to_owned());
    // The first step runs the synchronous AS and TGS exchanges with the KDC.
    // A blocking worker runs it, so a slow KDC does not hold the runtime and
    // the connection setup timeout still applies.
    let mut step = tokio::task::spawn_blocking(move || {
        let initiator = SspiInitiator::new(&keytab, &client_principal, &target_spn, &kdc_url)
            .map_err(|e| mechanism_failure(format!("GSSAPI initiator init failed: {e}")))?;
        let exchange = GssapiClientExchange::new(Box::new(initiator), GSSAPI_MAX_RECV, None);
        // Seed the exchange with no server token; this produces the AP-REQ.
        exchange
            .step(None)
            .map_err(|e| mechanism_failure(format!("GSSAPI initiate failed: {e}")))
    })
    .await
    .map_err(|e| mechanism_failure(format!("GSSAPI initiate task failed: {e}")))??;
    loop {
        match step {
            ClientStep::Token(token, next) => {
                let resp = send_sasl_authenticate(stream, token, corr_id, policy).await?;
                if resp.error_code != 0 {
                    return Err(authenticate_error("GSSAPI", &resp));
                }
                step = next
                    .step(Some(&resp.auth_bytes))
                    .map_err(|e| mechanism_failure(format!("GSSAPI step failed: {e}")))?;
            }
            ClientStep::Final(token) => {
                let resp = send_sasl_authenticate(stream, token, corr_id, policy).await?;
                if resp.error_code != 0 {
                    return Err(authenticate_error("GSSAPI", &resp));
                }
                return Ok(());
            }
        }
    }
}

/// Send one SASL token and read the answer of the broker.
///
/// With a `SaslAuthenticate` version, the token goes in a `SaslAuthenticate`
/// request at that version. Without one, the token goes as a size-prefixed
/// frame with no Kafka header, and the answer is a size-prefixed token, as
/// Kafka's `SaslClientAuthenticator.sendSaslClientToken` and `receiveToken`
/// do. Such an answer counts as a response with no error.
async fn send_sasl_authenticate<S>(
    stream: &mut S,
    auth_bytes: Vec<u8>,
    corr_id: &mut i32,
    policy: SaslPolicy<'_>,
) -> Result<SaslAuthenticateResponse, OutboundSaslError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + ?Sized,
{
    let Some(version) = policy.authenticate_version else {
        return send_raw_token(stream, &auth_bytes, policy.frame_max).await;
    };
    let req = SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(auth_bytes),
        ..Default::default()
    };
    let mut body = BytesMut::new();
    req.encode(&mut body, version)
        .map_err(|e| OutboundSaslError::Codec(format!("SaslAuthenticate encode: {e}")))?;
    let resp_bytes = round_trip(
        stream,
        ApiKey(API_KEY_SASL_AUTHENTICATE),
        ApiVersion(version),
        next_correlation_id(corr_id),
        version >= krabka_protocol::owned::sasl_authenticate_request::FLEXIBLE_MIN,
        &body,
        policy,
    )
    .await?;
    let mut cur: &[u8] = &resp_bytes;
    let resp = SaslAuthenticateResponse::decode(&mut cur, version)
        .map_err(|e| unparsable_response(format!("SaslAuthenticate decode: {e}")))?;
    Ok(resp)
}

/// Send `token` as a size-prefixed frame and read the size-prefixed answer.
async fn send_raw_token<S>(
    stream: &mut S,
    token: &[u8],
    frame_max: ClientFrameMax,
) -> Result<SaslAuthenticateResponse, OutboundSaslError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + ?Sized,
{
    if token.len() > frame_max.bytes() {
        return Err(OutboundSaslError::Codec(format!(
            "SASL token encoded {} bytes, maximum {}",
            token.len(),
            frame_max.bytes()
        )));
    }
    stream
        .write_u32(
            u32::try_from(token.len())
                .map_err(|_| OutboundSaslError::Codec("token size exceeds u32".into()))?,
        )
        .await?;
    stream.write_all(token).await?;
    stream.flush().await?;
    let len = usize::try_from(stream.read_u32().await?)
        .map_err(|_| OutboundSaslError::Codec("SASL token length does not fit usize".into()))?;
    if len > frame_max.bytes() {
        return Err(OutboundSaslError::Codec(format!(
            "SASL token announced {len} bytes, maximum {}",
            frame_max.bytes()
        )));
    }
    let mut answer = vec![0_u8; len];
    stream.read_exact(&mut answer).await?;
    Ok(SaslAuthenticateResponse {
        auth_bytes: bytes::Bytes::from(answer),
        ..Default::default()
    })
}

/// Send one framed request and return the response body bytes.
///
/// This function builds a `RequestHeader v1` (or v2 when `flexible`), appends
/// `body`, writes the length-prefixed frame, reads one response frame, and
/// strips the `ResponseHeader`.
///
/// Header rules that match Kafka:
/// - Request header: v1 for non-flexible, v2 for flexible (trailing 0x00
///   tagged-fields byte).
/// - Response header: v0 for non-flexible *and* for `ApiVersions(18)`
///   regardless of body flexibility; v1 (`corr_id` + 0x00 tagged byte)
///   for every other flexible response.
async fn round_trip<S>(
    stream: &mut S,
    api_key: ApiKey,
    api_version: ApiVersion,
    corr_id: i32,
    flexible: bool,
    body: &[u8],
    policy: SaslPolicy<'_>,
) -> Result<Vec<u8>, OutboundSaslError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + ?Sized,
{
    let SaslPolicy {
        client_id,
        frame_max,
        ..
    } = policy;
    let mut frame = BytesMut::with_capacity(16 + body.len());
    // RequestHeader: api_key + version + corr_id + client_id (i16 NULLABLE_STRING).
    frame.put_i16(api_key.0);
    frame.put_i16(api_version.0);
    frame.put_i32(corr_id);
    frame.put_i16(
        i16::try_from(client_id.len())
            .map_err(|_| OutboundSaslError::Codec("client_id too long".into()))?,
    );
    frame.put_slice(client_id.as_bytes());
    if flexible {
        frame.put_u8(0); // empty header tagged-fields
    }
    frame.put_slice(body);

    if frame.len() > frame_max.bytes() {
        return Err(OutboundSaslError::Codec(format!(
            "SASL request encoded {} bytes, maximum {}",
            frame.len(),
            frame_max.bytes()
        )));
    }

    stream
        .write_u32(
            u32::try_from(frame.len())
                .map_err(|_| OutboundSaslError::Codec("frame size exceeds u32".into()))?,
        )
        .await?;
    stream.write_all(&frame).await?;
    stream.flush().await?;

    // Read length prefix then exactly that many bytes.
    let resp_len = usize::try_from(stream.read_u32().await?)
        .map_err(|_| OutboundSaslError::Codec("SASL response length does not fit usize".into()))?;
    if resp_len > frame_max.bytes() {
        return Err(OutboundSaslError::Codec(format!(
            "SASL response announced {resp_len} bytes, maximum {}",
            frame_max.bytes()
        )));
    }
    let mut resp = vec![0u8; resp_len];
    stream.read_exact(&mut resp).await?;

    // Strip ResponseHeader: 4-byte corr_id, plus 1-byte tagged-fields for
    // v1 (flexible body AND api_key != 18). ApiVersions is special-cased
    // by the Kafka spec — its response header is always v0.
    let mut cur = &resp[..];
    if cur.len() < 4 {
        return Err(unparsable_response("response missing corr_id".into()));
    }
    let _resp_corr_id = cur.get_i32();
    let uses_v1_header = flexible && api_key != 18;
    if uses_v1_header {
        if cur.is_empty() {
            return Err(unparsable_response(
                "flexible response missing tagged-fields byte".into(),
            ));
        }
        cur = crate::connection::skip_tagged_fields(cur)
            .map_err(|error| unparsable_response(error.to_string()))?;
    }
    Ok(cur.to_vec())
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_protocol::owned::{
        sasl_authenticate_response::SaslAuthenticateResponse,
        sasl_handshake_response::SaslHandshakeResponse,
    };
    use krabka_security::{ScramServerExchange, StepResult, hash_scram_password};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        time::{Duration, timeout},
    };

    use super::*;

    const TEST_CLIENT_ID: &str = "configured-sasl-client";

    // Minimal server: read one request frame, reply with a response
    // header (corr_id, plus a 0x00 tagged-fields byte when `flex_header`)
    // carrying `body`. SaslHandshake uses a v0 response header; the
    // flexible SaslAuthenticate v2 uses a v1 response header.
    async fn reply_frame<S>(stream: &mut S, body: &[u8], flex_header: bool) -> Vec<u8>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let req_len = stream.read_u32().await.unwrap();
        let mut req = vec![0u8; req_len as usize];
        stream.read_exact(&mut req).await.unwrap();
        // corr_id is at request header bytes [4..8] (api_key,version,corr_id).
        let corr_id = i32::from_be_bytes([req[4], req[5], req[6], req[7]]);
        let mut frame = BytesMut::new();
        frame.put_i32(corr_id);
        if flex_header {
            frame.put_u8(0); // empty response-header tagged-fields
        }
        frame.put_slice(body);
        stream
            .write_u32(u32::try_from(frame.len()).unwrap())
            .await
            .unwrap();
        stream.write_all(&frame).await.unwrap();
        stream.flush().await.unwrap();
        req
    }

    /// Answer the `ApiVersions` v0 request that starts the SASL exchange, and
    /// list `SaslHandshake` v0-v1 and `SaslAuthenticate` v0-v2.
    async fn answer_api_versions<S>(stream: &mut S)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        use krabka_protocol::owned::api_versions_response::ApiVersion as Advertised;

        let mut body = BytesMut::new();
        ApiVersionsResponse {
            api_keys: vec![
                Advertised {
                    api_key: API_KEY_SASL_HANDSHAKE,
                    min_version: 0,
                    max_version: 1,
                    ..Default::default()
                },
                Advertised {
                    api_key: API_KEY_SASL_AUTHENTICATE,
                    min_version: 0,
                    max_version: 2,
                    ..Default::default()
                },
            ],
            ..Default::default()
        }
        .encode(&mut body, 0)
        .unwrap();
        let request = reply_frame(stream, &body, false).await;
        assert_request_header(
            &request,
            ApiKey(API_KEY_API_VERSIONS),
            ApiVersion(0),
            MIN_RESERVED_CORRELATION_ID,
            false,
        );
    }

    macro_rules! decode_sasl_authenticate_frame {
        ($req:expr, $corr_id:expr) => {{
            assert_request_header(
                &$req,
                ApiKey(API_KEY_SASL_AUTHENTICATE),
                ApiVersion(2),
                $corr_id,
                true,
            );
            let mut body = request_body(&$req, true);
            let decoded = SaslAuthenticateRequest::decode(&mut body, 2).unwrap();
            assert!(body.is_empty());
            decoded
        }};
    }

    /// One frame that the client sent in a SASL exchange.
    #[derive(Debug, PartialEq, Eq)]
    enum Frame {
        /// A Kafka request: API key, version and correlation id.
        Request(i16, i16, i32),
        /// A size-prefixed token with no Kafka header.
        Token(Vec<u8>),
    }

    /// Kafka's `SaslClientAuthenticator` sends `ApiVersions` v0 first, then
    /// `SaslHandshake` and `SaslAuthenticate` at the highest versions that the
    /// broker lists. With no `SaslAuthenticate` entry the token goes with no
    /// Kafka header. Each row gives the `(SaslHandshake, SaslAuthenticate)`
    /// maxima that the broker lists.
    #[tokio::test]
    async fn plain_exchange_uses_the_sasl_versions_that_api_versions_lists() {
        use krabka_protocol::owned::api_versions_response::ApiVersion as Advertised;

        const C: i32 = MIN_RESERVED_CORRELATION_ID;
        for (name, listed, expected) in [
            (
                "current broker",
                (Some(1), Some(2)),
                vec![
                    Frame::Request(API_KEY_API_VERSIONS, 0, C),
                    Frame::Request(API_KEY_SASL_HANDSHAKE, 1, C + 1),
                    Frame::Request(API_KEY_SASL_AUTHENTICATE, 2, C + 2),
                ],
            ),
            (
                "SaslAuthenticate v1",
                (Some(1), Some(1)),
                vec![
                    Frame::Request(API_KEY_API_VERSIONS, 0, C),
                    Frame::Request(API_KEY_SASL_HANDSHAKE, 1, C + 1),
                    Frame::Request(API_KEY_SASL_AUTHENTICATE, 1, C + 2),
                ],
            ),
            (
                "no SaslAuthenticate",
                (Some(0), None),
                vec![
                    Frame::Request(API_KEY_API_VERSIONS, 0, C),
                    Frame::Request(API_KEY_SASL_HANDSHAKE, 0, C + 1),
                    Frame::Token(b"\0u\0p".to_vec()),
                ],
            ),
        ] {
            let (mut client, mut server) = tokio::io::duplex(8192);
            let server_task = tokio::spawn(async move {
                let mut frames = Vec::new();
                for _ in 0..3 {
                    let len = server.read_u32().await.unwrap();
                    let mut request = vec![0_u8; len as usize];
                    server.read_exact(&mut request).await.unwrap();
                    if frames.len() == 2 && listed.1.is_none() {
                        frames.push(Frame::Token(request));
                        server.write_u32(0).await.unwrap();
                        continue;
                    }
                    let api_key = i16::from_be_bytes([request[0], request[1]]);
                    let version = i16::from_be_bytes([request[2], request[3]]);
                    let corr_id =
                        i32::from_be_bytes([request[4], request[5], request[6], request[7]]);
                    frames.push(Frame::Request(api_key, version, corr_id));
                    let mut body = BytesMut::new();
                    body.put_i32(corr_id);
                    match api_key {
                        API_KEY_API_VERSIONS => ApiVersionsResponse {
                            api_keys: [
                                (API_KEY_SASL_HANDSHAKE, listed.0),
                                (API_KEY_SASL_AUTHENTICATE, listed.1),
                            ]
                            .into_iter()
                            .filter_map(|(api_key, max)| {
                                max.map(|max_version| Advertised {
                                    api_key,
                                    min_version: 0,
                                    max_version,
                                    ..Default::default()
                                })
                            })
                            .collect(),
                            ..Default::default()
                        }
                        .encode(&mut body, 0)
                        .unwrap(),
                        API_KEY_SASL_HANDSHAKE => SaslHandshakeResponse::default()
                            .encode(&mut body, version)
                            .unwrap(),
                        _ => {
                            if version >= 2 {
                                body.put_u8(0);
                            }
                            SaslAuthenticateResponse::default()
                                .encode(&mut body, version)
                                .unwrap();
                        }
                    }
                    server
                        .write_u32(u32::try_from(body.len()).unwrap())
                        .await
                        .unwrap();
                    server.write_all(&body).await.unwrap();
                }
                frames
            });
            let result = outbound_sasl(
                &mut client,
                &SaslCredentials::Plain {
                    username: "u".into(),
                    password: "p".into(),
                },
                "localhost",
                TEST_CLIENT_ID,
                ClientFrameMax::default(),
            )
            .await;
            let frames = timeout(Duration::from_secs(1), server_task)
                .await
                .expect("server saw three frames")
                .unwrap();
            check!(result.is_ok(), "{name}: {result:?}");
            check!(frames == expected, "{name}");
        }
    }

    #[test]
    fn sasl_correlation_ids_stay_in_the_reserved_range() {
        let mut corr_id = i32::MAX - 1;
        let ids = (0..4)
            .map(|_| next_correlation_id(&mut corr_id))
            .collect::<Vec<_>>();
        check!(
            ids == vec![
                i32::MAX - 1,
                i32::MAX,
                MIN_RESERVED_CORRELATION_ID,
                MIN_RESERVED_CORRELATION_ID + 1
            ]
        );
    }

    #[tokio::test]
    async fn outbound_plain_completes() {
        let (mut client, mut server) = tokio::io::duplex(8192);
        let server_task = tokio::spawn(async move {
            answer_api_versions(&mut server).await;
            // 1. SaslHandshake v1 → error_code 0 + empty mechanisms.
            let mut hs = BytesMut::new();
            SaslHandshakeResponse {
                error_code: 0,
                ..Default::default()
            }
            .encode(&mut hs, 1)
            .unwrap();
            let hs_req = reply_frame(&mut server, &hs, false).await;
            assert_request_header(
                &hs_req,
                ApiKey(API_KEY_SASL_HANDSHAKE),
                ApiVersion(1),
                MIN_RESERVED_CORRELATION_ID + 1,
                false,
            );
            let mut hs_body = request_body(&hs_req, false);
            let hs_decoded = SaslHandshakeRequest::decode(&mut hs_body, 1).unwrap();
            assert_eq!(hs_decoded.mechanism, "PLAIN");
            assert!(hs_body.is_empty());

            // 2. SaslAuthenticate v2 → error_code 0 (flexible response header).
            let mut au = BytesMut::new();
            SaslAuthenticateResponse {
                error_code: 0,
                ..Default::default()
            }
            .encode(&mut au, 2)
            .unwrap();
            let au_req = reply_frame(&mut server, &au, true).await;
            let au_decoded =
                decode_sasl_authenticate_frame!(au_req, MIN_RESERVED_CORRELATION_ID + 2);
            assert_eq!(au_decoded.auth_bytes.as_ref(), b"\0u\0p");
        });
        let creds = SaslCredentials::Plain {
            username: "u".into(),
            password: "p".into(),
        };
        outbound_sasl(
            &mut client,
            &creds,
            "localhost",
            TEST_CLIENT_ID,
            ClientFrameMax::default(),
        )
        .await
        .expect("PLAIN outbound handshake completes");
        timeout(Duration::from_secs(1), server_task)
            .await
            .expect("server observed both SASL client frames")
            .unwrap();
    }

    #[tokio::test]
    async fn outbound_oauthbearer_sends_rfc7628_initial_response_and_rereads_token_file() {
        let dir = tempfile::tempdir().unwrap();
        let token_path = dir.path().join("token");
        for token in ["header.payload.", "rotated.token."] {
            std::fs::write(&token_path, format!("{token}\n")).unwrap();
            let expected = format!("n,,\x01auth=Bearer {token}\x01\x01").into_bytes();
            let (mut client, mut server) = tokio::io::duplex(8192);
            let server_task = tokio::spawn(async move {
                answer_api_versions(&mut server).await;
                let mut handshake = BytesMut::new();
                SaslHandshakeResponse {
                    error_code: 0,
                    ..Default::default()
                }
                .encode(&mut handshake, 1)
                .unwrap();
                let request = reply_frame(&mut server, &handshake, false).await;
                let mut body = request_body(&request, false);
                let decoded = SaslHandshakeRequest::decode(&mut body, 1).unwrap();
                check!(decoded.mechanism == "OAUTHBEARER");

                let mut authenticate = BytesMut::new();
                SaslAuthenticateResponse {
                    error_code: 0,
                    auth_bytes: bytes::Bytes::new(),
                    session_lifetime_ms: 60_000,
                    ..Default::default()
                }
                .encode(&mut authenticate, 2)
                .unwrap();
                let request = reply_frame(&mut server, &authenticate, true).await;
                let decoded =
                    decode_sasl_authenticate_frame!(request, MIN_RESERVED_CORRELATION_ID + 2);
                check!(decoded.auth_bytes.as_ref() == expected);
            });

            let credentials = SaslCredentials::OAuthBearer {
                token_path: token_path.clone(),
            };
            outbound_sasl(
                &mut client,
                &credentials,
                "localhost",
                TEST_CLIENT_ID,
                ClientFrameMax::default(),
            )
            .await
            .expect("OAUTHBEARER outbound handshake completes");
            timeout(Duration::from_secs(1), server_task)
                .await
                .expect("server observed OAUTHBEARER frames")
                .unwrap();
        }
    }

    #[tokio::test]
    async fn outbound_oauthbearer_completes_rfc7628_rejection_exchange() {
        let dir = tempfile::tempdir().unwrap();
        let token_path = dir.path().join("token");
        std::fs::write(&token_path, "invalid").unwrap();
        let (mut client, mut server) = tokio::io::duplex(8192);
        let server_task = tokio::spawn(async move {
            answer_api_versions(&mut server).await;
            let mut handshake = BytesMut::new();
            SaslHandshakeResponse {
                error_code: 0,
                ..Default::default()
            }
            .encode(&mut handshake, 1)
            .unwrap();
            let _ = reply_frame(&mut server, &handshake, false).await;

            let mut challenge = BytesMut::new();
            SaslAuthenticateResponse {
                error_code: 0,
                auth_bytes: bytes::Bytes::from_static(br#"{"status":"invalid_token"}"#),
                ..Default::default()
            }
            .encode(&mut challenge, 2)
            .unwrap();
            let first = reply_frame(&mut server, &challenge, true).await;
            let _ = decode_sasl_authenticate_frame!(first, MIN_RESERVED_CORRELATION_ID + 2);

            let mut rejected = BytesMut::new();
            SaslAuthenticateResponse {
                error_code: 58,
                error_message: Some("oauthbearer token rejected".into()),
                ..Default::default()
            }
            .encode(&mut rejected, 2)
            .unwrap();
            let final_request = reply_frame(&mut server, &rejected, true).await;
            let decoded =
                decode_sasl_authenticate_frame!(final_request, MIN_RESERVED_CORRELATION_ID + 3);
            check!(decoded.auth_bytes.as_ref() == b"\x01");
        });

        let credentials = SaslCredentials::OAuthBearer { token_path };
        let error = outbound_sasl(
            &mut client,
            &credentials,
            "localhost",
            TEST_CLIENT_ID,
            ClientFrameMax::default(),
        )
        .await
        .expect_err("invalid bearer token is rejected");
        check!(matches!(
            error,
            OutboundSaslError::Authentication(SaslAuthenticationError::Failed(message))
                if message
                    == "SaslAuthenticate(OAUTHBEARER) error_code=58 \
                        error_message=Some(\"oauthbearer token rejected\")"
        ));
        timeout(Duration::from_secs(1), server_task)
            .await
            .expect("server observed RFC 7628 final message")
            .unwrap();
    }

    #[tokio::test]
    async fn send_sasl_authenticate_increments_correlation_id_and_sends_auth_bytes() {
        let (mut client, mut server) = tokio::io::duplex(8192);
        let server_task = tokio::spawn(async move {
            for (expected_corr, expected_payload) in [
                (MIN_RESERVED_CORRELATION_ID, b"first".as_ref()),
                (MIN_RESERVED_CORRELATION_ID + 1, b"second".as_ref()),
            ] {
                let mut au = BytesMut::new();
                SaslAuthenticateResponse {
                    error_code: 0,
                    ..Default::default()
                }
                .encode(&mut au, 2)
                .unwrap();
                let req = reply_frame(&mut server, &au, true).await;
                let decoded = decode_sasl_authenticate_frame!(req, expected_corr);
                assert_eq!(decoded.auth_bytes.as_ref(), expected_payload);
            }
        });

        // An id below the reserved range starts the range, as Kafka's
        // `nextCorrelationId` does.
        let mut corr_id = 7;
        send_sasl_authenticate(
            &mut client,
            b"first".to_vec(),
            &mut corr_id,
            SaslPolicy {
                client_id: TEST_CLIENT_ID,
                frame_max: ClientFrameMax::default(),
                authenticate_version: Some(2),
            },
        )
        .await
        .unwrap();
        assert_eq!(corr_id, MIN_RESERVED_CORRELATION_ID + 1);
        send_sasl_authenticate(
            &mut client,
            b"second".to_vec(),
            &mut corr_id,
            SaslPolicy {
                client_id: TEST_CLIENT_ID,
                frame_max: ClientFrameMax::default(),
                authenticate_version: Some(2),
            },
        )
        .await
        .unwrap();
        assert_eq!(corr_id, MIN_RESERVED_CORRELATION_ID + 2);
        timeout(Duration::from_secs(1), server_task)
            .await
            .expect("server observed both authenticate frames")
            .unwrap();
    }

    #[tokio::test]
    async fn outbound_scram_rejects_broker_error_on_first_round() {
        let (mut client, mut server) = tokio::io::duplex(8192);
        let server_task = tokio::spawn(async move {
            answer_api_versions(&mut server).await;
            let mut hs = BytesMut::new();
            SaslHandshakeResponse {
                error_code: 0,
                ..Default::default()
            }
            .encode(&mut hs, 1)
            .unwrap();
            let hs_req = reply_frame(&mut server, &hs, false).await;
            assert_request_header(
                &hs_req,
                ApiKey(API_KEY_SASL_HANDSHAKE),
                ApiVersion(1),
                MIN_RESERVED_CORRELATION_ID + 1,
                false,
            );
            let mut hs_body = request_body(&hs_req, false);
            let hs_decoded = SaslHandshakeRequest::decode(&mut hs_body, 1).unwrap();
            assert_eq!(hs_decoded.mechanism, "SCRAM-SHA-256");

            let mut au = BytesMut::new();
            SaslAuthenticateResponse {
                error_code: SASL_AUTHENTICATION_FAILED,
                error_message: Some("nope".into()),
                ..Default::default()
            }
            .encode(&mut au, 2)
            .unwrap();
            let au_req = reply_frame(&mut server, &au, true).await;
            let _ = decode_sasl_authenticate_frame!(au_req, MIN_RESERVED_CORRELATION_ID + 2);
        });

        let creds = SaslCredentials::Scram {
            mechanism: SaslMechanism::ScramSha256,
            username: "u".into(),
            password: "p".into(),
        };
        let err = outbound_sasl(
            &mut client,
            &creds,
            "localhost",
            TEST_CLIENT_ID,
            ClientFrameMax::default(),
        )
        .await
        .unwrap_err();
        assert2::assert!(
            let OutboundSaslError::Authentication(SaslAuthenticationError::Failed(msg)) = err
        );
        check!(msg.contains("round 1"));
        timeout(Duration::from_secs(1), server_task)
            .await
            .expect("server observed SCRAM first round")
            .unwrap();
    }

    #[tokio::test]
    async fn outbound_scram_rejects_broker_error_on_second_round() {
        let (mut client, mut server) = tokio::io::duplex(8192);
        let server_task = tokio::spawn(async move {
            answer_api_versions(&mut server).await;
            let mut hs = BytesMut::new();
            SaslHandshakeResponse {
                error_code: 0,
                ..Default::default()
            }
            .encode(&mut hs, 1)
            .unwrap();
            let hs_req = reply_frame(&mut server, &hs, false).await;
            assert_request_header(
                &hs_req,
                ApiKey(API_KEY_SASL_HANDSHAKE),
                ApiVersion(1),
                MIN_RESERVED_CORRELATION_ID + 1,
                false,
            );

            let cred = hash_scram_password(b"p", SaslMechanism::ScramSha256, 4096);
            let scram_server = ScramServerExchange::new("u".to_string(), cred);

            let first_req_len = server.read_u32().await.unwrap();
            let mut first_req = vec![0u8; first_req_len as usize];
            server.read_exact(&mut first_req).await.unwrap();
            let first_auth =
                decode_sasl_authenticate_frame!(first_req, MIN_RESERVED_CORRELATION_ID + 2);
            let server_first = match scram_server.step(&first_auth.auth_bytes) {
                StepResult::Continue(bytes, _next) => bytes,
                other => panic!("server first SCRAM step must continue, got {other:?}"),
            };

            let mut first_resp = BytesMut::new();
            SaslAuthenticateResponse {
                error_code: 0,
                auth_bytes: bytes::Bytes::from(server_first),
                ..Default::default()
            }
            .encode(&mut first_resp, 2)
            .unwrap();
            write_response_frame(
                &mut server,
                MIN_RESERVED_CORRELATION_ID + 2,
                &first_resp,
                true,
            )
            .await;

            let mut second_resp = BytesMut::new();
            SaslAuthenticateResponse {
                error_code: SASL_AUTHENTICATION_FAILED,
                error_message: Some("second nope".into()),
                ..Default::default()
            }
            .encode(&mut second_resp, 2)
            .unwrap();
            let second_req = reply_frame(&mut server, &second_resp, true).await;
            let _ = decode_sasl_authenticate_frame!(second_req, MIN_RESERVED_CORRELATION_ID + 3);
        });

        let creds = SaslCredentials::Scram {
            mechanism: SaslMechanism::ScramSha256,
            username: "u".into(),
            password: "p".into(),
        };
        let err = outbound_sasl(
            &mut client,
            &creds,
            "localhost",
            TEST_CLIENT_ID,
            ClientFrameMax::default(),
        )
        .await
        .unwrap_err();
        assert2::assert!(
            let OutboundSaslError::Authentication(SaslAuthenticationError::Failed(msg)) = err
        );
        check!(msg.contains("round 2"));
        timeout(Duration::from_secs(1), server_task)
            .await
            .expect("server observed SCRAM second round")
            .unwrap();
    }

    #[tokio::test]
    async fn round_trip_rejects_response_without_correlation_id() {
        let (mut client, mut server) = tokio::io::duplex(8192);
        let server_task = tokio::spawn(async move {
            let req_len = server.read_u32().await.unwrap();
            let mut req = vec![0u8; req_len as usize];
            server.read_exact(&mut req).await.unwrap();
            server.write_u32(3).await.unwrap();
            server.write_all(&[1, 2, 3]).await.unwrap();
            server.flush().await.unwrap();
        });

        let err = round_trip(
            &mut client,
            ApiKey(API_KEY_SASL_HANDSHAKE),
            ApiVersion(1),
            99,
            false,
            &[],
            SaslPolicy {
                client_id: TEST_CLIENT_ID,
                frame_max: ClientFrameMax::default(),
                authenticate_version: Some(2),
            },
        )
        .await
        .unwrap_err();
        check!(matches!(
            err,
            OutboundSaslError::Authentication(SaslAuthenticationError::IllegalState(msg))
                if msg == "response missing corr_id"
        ));
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn round_trip_rejects_flexible_response_without_tagged_fields_byte() {
        let (mut client, mut server) = tokio::io::duplex(8192);
        let server_task = tokio::spawn(async move {
            let req_len = server.read_u32().await.unwrap();
            let mut req = vec![0u8; req_len as usize];
            server.read_exact(&mut req).await.unwrap();
            server.write_u32(4).await.unwrap();
            server.write_i32(99).await.unwrap();
            server.flush().await.unwrap();
        });

        let err = round_trip(
            &mut client,
            ApiKey(API_KEY_SASL_AUTHENTICATE),
            ApiVersion(2),
            99,
            true,
            &[],
            SaslPolicy {
                client_id: TEST_CLIENT_ID,
                frame_max: ClientFrameMax::default(),
                authenticate_version: Some(2),
            },
        )
        .await
        .unwrap_err();
        check!(matches!(
            err,
            OutboundSaslError::Authentication(SaslAuthenticationError::IllegalState(msg))
                if msg == "flexible response missing tagged-fields byte"
        ));
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn round_trip_uses_the_configured_client_id() {
        let (mut client, mut server) = tokio::io::duplex(8192);
        let server_task = tokio::spawn(async move {
            let req = reply_frame(&mut server, &[], false).await;
            let client_len = usize::try_from(i16::from_be_bytes([req[8], req[9]])).unwrap();
            assert_eq!(&req[10..10 + client_len], b"configured-sasl-client");
        });

        round_trip(
            &mut client,
            ApiKey(API_KEY_SASL_HANDSHAKE),
            ApiVersion(1),
            99,
            false,
            &[],
            SaslPolicy {
                client_id: TEST_CLIENT_ID,
                frame_max: crate::ClientFrameMax::default(),
                authenticate_version: Some(2),
            },
        )
        .await
        .unwrap();
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn round_trip_rejects_an_oversized_request_before_writing() {
        let (mut client, _server) = tokio::io::duplex(8192);
        let error = round_trip(
            &mut client,
            ApiKey(API_KEY_SASL_HANDSHAKE),
            ApiVersion(1),
            99,
            false,
            &[0],
            SaslPolicy {
                client_id: "",
                frame_max: crate::ClientFrameMax::try_from(krabka_units::bytes(10)).unwrap(),
                authenticate_version: Some(2),
            },
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("encoded 11"));
        assert!(error.to_string().contains("maximum 10"));
    }

    #[tokio::test]
    async fn round_trip_rejects_an_oversized_response_before_allocating() {
        let (mut client, mut server) = tokio::io::duplex(8192);
        let server_task = tokio::spawn(async move {
            let request_len = server.read_u32().await.unwrap();
            let mut request = vec![0; request_len as usize];
            server.read_exact(&mut request).await.unwrap();
            server.write_u32(17).await.unwrap();
            server.flush().await.unwrap();
        });

        let error = round_trip(
            &mut client,
            ApiKey(API_KEY_SASL_HANDSHAKE),
            ApiVersion(1),
            99,
            false,
            &[],
            SaslPolicy {
                client_id: "",
                frame_max: crate::ClientFrameMax::try_from(krabka_units::bytes(16)).unwrap(),
                authenticate_version: Some(2),
            },
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("announced 17"));
        assert!(error.to_string().contains("maximum 16"));
        server_task.await.unwrap();
    }

    /// How the client classifies one SASL response.
    #[derive(Debug, PartialEq, Eq)]
    enum Verdict {
        Ok,
        Rejected(SaslAuthenticationError),
        Server(i16),
        Other(String),
    }

    fn verdict(result: Result<(), OutboundSaslError>) -> Verdict {
        match result {
            Ok(()) => Verdict::Ok,
            Err(OutboundSaslError::Authentication(error)) => Verdict::Rejected(error),
            Err(OutboundSaslError::Server { error_code, .. }) => Verdict::Server(error_code),
            Err(error) => Verdict::Other(error.to_string()),
        }
    }

    /// `SaslClientAuthenticator.handleSaslHandshakeResponse` raises
    /// `UnsupportedSaslMechanismException` for 33 and
    /// `IllegalSaslStateException` for 34 and any other code. Both extend
    /// `AuthenticationException`.
    #[test]
    fn handshake_error_codes_map_to_kafka_authentication_exceptions() {
        let enabled = "enabled mechanisms are [\"GSSAPI\"]";
        for (error_code, expected) in [
            (0, Verdict::Ok),
            (
                UNSUPPORTED_SASL_MECHANISM,
                Verdict::Rejected(SaslAuthenticationError::UnsupportedMechanism(format!(
                    "client SASL mechanism 'PLAIN' not enabled in the server, {enabled}"
                ))),
            ),
            (
                ILLEGAL_SASL_STATE,
                Verdict::Rejected(SaslAuthenticationError::IllegalState(format!(
                    "unexpected handshake request with client mechanism PLAIN, {enabled}"
                ))),
            ),
            (
                87,
                Verdict::Rejected(SaslAuthenticationError::IllegalState(format!(
                    "unknown error code 87, client mechanism is PLAIN, {enabled}"
                ))),
            ),
        ] {
            let response = SaslHandshakeResponse {
                error_code,
                mechanisms: vec!["GSSAPI".into()],
                ..Default::default()
            };
            check!(
                verdict(handshake_result(SaslMechanism::Plain, &response)) == expected,
                "error_code {error_code}"
            );
        }
    }

    /// `SaslClientAuthenticator.receiveToken` raises `Errors.exception()`.
    /// Only 33, 34 and 58 give an `AuthenticationException`. Another code
    /// closes the connection in the `AUTHENTICATE` state, which Kafka retries.
    #[test]
    fn authenticate_error_codes_map_to_kafka_exceptions() {
        let message = |code: i16| {
            format!("SaslAuthenticate(PLAIN) error_code={code} error_message=Some(\"no\")")
        };
        for (error_code, expected) in [
            (
                UNSUPPORTED_SASL_MECHANISM,
                Verdict::Rejected(SaslAuthenticationError::UnsupportedMechanism(message(33))),
            ),
            (
                ILLEGAL_SASL_STATE,
                Verdict::Rejected(SaslAuthenticationError::IllegalState(message(34))),
            ),
            (
                SASL_AUTHENTICATION_FAILED,
                Verdict::Rejected(SaslAuthenticationError::Failed(message(58))),
            ),
            (35, Verdict::Server(35)),
            (-1, Verdict::Server(-1)),
        ] {
            let response = SaslAuthenticateResponse {
                error_code,
                error_message: Some("no".into()),
                ..Default::default()
            };
            check!(
                verdict(Err(authenticate_error("PLAIN", &response))) == expected,
                "error_code {error_code}"
            );
        }
    }

    fn assert_request_header(
        req: &[u8],
        api_key: ApiKey,
        api_version: ApiVersion,
        corr_id: i32,
        flexible: bool,
    ) {
        check!(ApiKey(i16::from_be_bytes([req[0], req[1]])) == api_key);
        check!(ApiVersion(i16::from_be_bytes([req[2], req[3]])) == api_version);
        check!(i32::from_be_bytes([req[4], req[5], req[6], req[7]]) == corr_id);
        let client_len = i16::from_be_bytes([req[8], req[9]]);
        assert_eq!(client_len, i16::try_from(TEST_CLIENT_ID.len()).unwrap());
        assert_eq!(
            &req[10..10 + TEST_CLIENT_ID.len()],
            TEST_CLIENT_ID.as_bytes()
        );
        if flexible {
            assert_eq!(req[10 + TEST_CLIENT_ID.len()], 0);
        }
    }

    fn request_body(req: &[u8], flexible: bool) -> &[u8] {
        let header_len = 10 + TEST_CLIENT_ID.len() + usize::from(flexible);
        &req[header_len..]
    }

    async fn write_response_frame<S>(stream: &mut S, corr_id: i32, body: &[u8], flex_header: bool)
    where
        S: tokio::io::AsyncWrite + Unpin,
    {
        let mut frame = BytesMut::new();
        frame.put_i32(corr_id);
        if flex_header {
            frame.put_u8(0);
        }
        frame.put_slice(body);
        stream
            .write_u32(u32::try_from(frame.len()).unwrap())
            .await
            .unwrap();
        stream.write_all(&frame).await.unwrap();
        stream.flush().await.unwrap();
    }
}
