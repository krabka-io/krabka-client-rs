//! SASL re-authentication of a live connection (KIP-368).
//!
//! When the final `SaslAuthenticate` response of an exchange carries a
//! positive `session_lifetime_ms`, the broker closes the connection at the end
//! of the session unless the client authenticates again on it. Kafka's
//! `SaslClientAuthenticator` schedules the new exchange at 85 to 95 percent of
//! the lifetime and starts it before the next request that it sends after
//! that time (`KafkaChannel.maybeBeginClientReauthentication`).

use std::net::SocketAddr;

use bytes::BufMut;
use krabka_ids::{ApiKey, ApiVersion};
use tokio::{sync::RwLock, time::Instant};

use crate::{
    ClientFrameMax,
    error::ClientError,
    sasl::{OutboundSaslError, SaslChannel, SaslCredentials, SaslPolicy, SaslSession},
};

/// Kafka's `ReauthInfo` factor that leaves time for network latency and clock
/// drift.
const LIFETIME_FACTOR: f64 = 0.85;
/// Kafka's `ReauthInfo` jitter that spreads the re-authentication of many
/// connections.
const LIFETIME_JITTER: f64 = 0.10;

/// The SASL state that a connection needs to authenticate again.
pub(crate) struct Reauth {
    addr: SocketAddr,
    creds: SaslCredentials,
    server_name: String,
    versions: SaslSession,
    /// When the next exchange is due, or `None` when the session has no
    /// lifetime. A send holds a read lock while it enqueues its frame, and an
    /// exchange holds the write lock, so no request goes out between the
    /// frames of an exchange.
    pub(crate) next: RwLock<Option<Instant>>,
}

impl Reauth {
    /// The state after the first exchange, which ended now.
    pub(crate) fn new(
        addr: SocketAddr,
        creds: SaslCredentials,
        server_name: String,
        session: SaslSession,
    ) -> Self {
        Self {
            addr,
            creds,
            server_name,
            versions: SaslSession {
                session_lifetime_ms: None,
                ..session
            },
            next: RwLock::new(due_time(&session, crate::backoff::random_unit())),
        }
    }

    /// Run the exchange again over `channel` and return the next due time.
    pub(crate) async fn authenticate<C>(
        &self,
        channel: &mut C,
        client_id: &str,
        frame_max: ClientFrameMax,
    ) -> Result<Option<Instant>, ClientError>
    where
        C: SaslChannel,
    {
        let mut corr_id = 0;
        let session = crate::sasl::authenticate(
            channel,
            &self.creds,
            &self.server_name,
            self.versions,
            (&mut corr_id, client_id, frame_max),
        )
        .await
        .map_err(|source| crate::connection::sasl_error(self.addr, source))?;
        Ok(due_time(&session, crate::backoff::random_unit()))
    }
}

/// When a session that ends now must authenticate again: the lifetime times a
/// factor from 0.85 to 0.95, as Kafka's
/// `ReauthInfo.setAuthenticationEndAndSessionReauthenticationTimes` picks it.
/// `unit` is a random value in `[0, 1)`.
pub(crate) fn due_time(session: &SaslSession, unit: f64) -> Option<Instant> {
    if !session.needs_reauthentication() {
        return None;
    }
    let lifetime =
        std::time::Duration::from_millis(u64::try_from(session.session_lifetime_ms?).ok()?);
    Some(Instant::now() + lifetime.mul_f64(LIFETIME_FACTOR + unit * LIFETIME_JITTER))
}

/// The request path of a re-authentication: the connection's own dispatch.
pub(crate) struct ConnectionChannel<'a> {
    pub(crate) connection: &'a crate::Connection,
}

impl SaslChannel for ConnectionChannel<'_> {
    async fn request(
        &mut self,
        (api_key, api_version, corr_id): (ApiKey, ApiVersion, i32),
        flexible: bool,
        body: &[u8],
        _policy: SaslPolicy<'_>,
    ) -> Result<Vec<u8>, OutboundSaslError> {
        let mut frame = self
            .connection
            .request_header(api_key, api_version, corr_id, flexible);
        frame.put_slice(body);
        let response = self
            .connection
            .dispatch_unguarded(corr_id, frame)
            .await
            .map_err(channel_error)?;
        // A flexible response has tagged fields after the correlation id,
        // which the reader already removed.
        let body = if flexible {
            crate::connection::skip_tagged_fields(&response).map_err(channel_error)?
        } else {
            &response[..]
        };
        Ok(body.to_vec())
    }

    async fn token(
        &mut self,
        _token: &[u8],
        _frame_max: ClientFrameMax,
    ) -> Result<
        krabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse,
        OutboundSaslError,
    > {
        // A session lifetime needs `SaslAuthenticate` v1 or later, so a
        // re-authentication never sends a token without a Kafka header.
        Err(OutboundSaslError::Codec(
            "re-authentication needs SaslAuthenticate".to_owned(),
        ))
    }
}

fn channel_error(error: ClientError) -> OutboundSaslError {
    match error {
        ClientError::Io(error) => OutboundSaslError::Io(error),
        error => OutboundSaslError::Io(std::io::Error::other(error.to_string())),
    }
}

#[cfg(test)]
mod tests;
