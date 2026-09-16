//! OAUTHBEARER tokens: the token provider seam and an OAuth 2 client
//! credentials provider (KIP-768).
//!
//! Kafka's `OAuthBearerLoginCallbackHandler` gets a token from a
//! `JwtRetriever`. `ClientCredentialsJwtRetriever` posts a
//! `client_credentials` grant to `sasl.oauthbearer.token.endpoint.url`, and
//! `ExpiringCredentialRefreshingLogin` fetches a new token before the old one
//! expires.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD as B64, URL_SAFE_NO_PAD},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _},
    net::TcpStream,
};

use crate::security::{Password, TlsConnectorConfig};

/// The future of [`OAuthBearerTokenProvider::token`].
pub type TokenFuture<'a> = Pin<Box<dyn Future<Output = Result<String, String>> + Send + 'a>>;

/// A source of OAUTHBEARER tokens, as Kafka's `sasl.login.callback.handler.class`
/// with an `OAuthBearerTokenCallback` is.
///
/// Each SASL exchange, including a re-authentication, asks for a token.
pub trait OAuthBearerTokenProvider: fmt::Debug + Send + Sync {
    /// The compact serialized token to send.
    ///
    /// # Errors
    /// Returns a description when no token is available.
    fn token(&self) -> TokenFuture<'_>;
}

/// Kafka's `sasl.login.retry.backoff.ms` default.
pub const DEFAULT_LOGIN_RETRY_BACKOFF: Duration = Duration::from_millis(100);
/// Kafka's `sasl.login.retry.backoff.max.ms` default.
pub const DEFAULT_LOGIN_RETRY_BACKOFF_MAX: Duration = Duration::from_secs(10);
/// Kafka's `sasl.login.refresh.window.factor` default.
pub const DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR: f64 = 0.80;
/// Kafka's `sasl.login.refresh.window.jitter` default.
pub const DEFAULT_LOGIN_REFRESH_WINDOW_JITTER: f64 = 0.05;
/// Kafka's `sasl.login.refresh.min.period.seconds` default.
pub const DEFAULT_LOGIN_REFRESH_MIN_PERIOD: Duration = Duration::from_mins(1);
/// Kafka's `sasl.login.refresh.buffer.seconds` default.
pub const DEFAULT_LOGIN_REFRESH_BUFFER: Duration = Duration::from_mins(5);
/// Kafka's `JwtResponseParser.MAX_RESPONSE_BODY_LENGTH`.
const MAX_RESPONSE_SNIPPET: usize = 1000;
/// The largest token endpoint response that the client reads.
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;

/// The settings of [`ClientCredentialsTokenProvider`], as Kafka's
/// `SaslConfigs` names them.
#[derive(Clone, Debug)]
pub struct ClientCredentialsConfig {
    /// `sasl.oauthbearer.token.endpoint.url`, an `http` or `https` URL.
    pub token_endpoint_url: String,
    /// `sasl.oauthbearer.client.credentials.client.id`.
    pub client_id: String,
    /// `sasl.oauthbearer.client.credentials.client.secret`.
    pub client_secret: Password,
    /// `sasl.oauthbearer.scope`. `None` requests the default scope.
    pub scope: Option<String>,
    /// `sasl.oauthbearer.header.urlencode`: URL-encode the client id, secret
    /// and scope (RFC 6749 section 2.3.1). Kafka's default is `false`.
    pub url_encode: bool,
    /// The TLS settings of an `https` endpoint.
    pub tls: TlsConnectorConfig,
    /// `sasl.login.connect.timeout.ms`. `None` has no limit.
    pub connect_timeout: Option<Duration>,
    /// `sasl.login.read.timeout.ms`. `None` has no limit.
    pub read_timeout: Option<Duration>,
    /// `sasl.login.retry.backoff.ms`.
    pub retry_backoff: Duration,
    /// `sasl.login.retry.backoff.max.ms`.
    pub retry_backoff_max: Duration,
    /// `sasl.login.refresh.window.factor`.
    pub refresh_window_factor: f64,
    /// `sasl.login.refresh.window.jitter`.
    pub refresh_window_jitter: f64,
    /// `sasl.login.refresh.min.period.seconds`.
    pub refresh_min_period: Duration,
    /// `sasl.login.refresh.buffer.seconds`.
    pub refresh_buffer: Duration,
}

impl ClientCredentialsConfig {
    /// Settings with Kafka's defaults for `endpoint`, `client_id` and
    /// `client_secret`.
    #[must_use]
    pub fn new(
        endpoint: impl Into<String>,
        client_id: impl Into<String>,
        client_secret: impl Into<Password>,
    ) -> Self {
        Self {
            token_endpoint_url: endpoint.into(),
            client_id: client_id.into(),
            client_secret: client_secret.into(),
            scope: None,
            url_encode: false,
            tls: TlsConnectorConfig::default(),
            connect_timeout: None,
            read_timeout: None,
            retry_backoff: DEFAULT_LOGIN_RETRY_BACKOFF,
            retry_backoff_max: DEFAULT_LOGIN_RETRY_BACKOFF_MAX,
            refresh_window_factor: DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR,
            refresh_window_jitter: DEFAULT_LOGIN_REFRESH_WINDOW_JITTER,
            refresh_min_period: DEFAULT_LOGIN_REFRESH_MIN_PERIOD,
            refresh_buffer: DEFAULT_LOGIN_REFRESH_BUFFER,
        }
    }
}

/// A token and the time to fetch the next one.
#[derive(Debug)]
struct Cached {
    token: String,
    refresh_at: SystemTime,
}

/// An OAUTHBEARER token provider that uses the OAuth 2 client credentials
/// grant, as Kafka's `ClientCredentialsJwtRetriever` with
/// `ClientSecretRequestFormatter` does. It keeps the token until the refresh
/// time of Kafka's `ExpiringCredentialRefreshingLogin`, then fetches a new one.
#[derive(Debug)]
pub struct ClientCredentialsTokenProvider {
    config: ClientCredentialsConfig,
    cached: tokio::sync::Mutex<Option<Cached>>,
}

impl ClientCredentialsTokenProvider {
    /// A provider for `config`.
    ///
    /// # Errors
    /// Returns a description when the client id or secret is blank, or the
    /// endpoint is not an `http` or `https` URL, as Kafka's `ConfigException`.
    pub fn new(config: ClientCredentialsConfig) -> Result<Arc<Self>, String> {
        if config.client_id.trim().is_empty() {
            return Err("sasl.oauthbearer.client.credentials.client.id is blank".into());
        }
        if config.client_secret.value().trim().is_empty() {
            return Err("sasl.oauthbearer.client.credentials.client.secret is blank".into());
        }
        Endpoint::parse(&config.token_endpoint_url)?;
        // Kafka's `ExpiringCredentialRefreshConfig` refuses a window factor or
        // jitter outside its range. A bad one would reach
        // `Duration::mul_f64` and panic at the first token.
        for (name, value, range) in [
            (
                "sasl.login.refresh.window.factor",
                config.refresh_window_factor,
                (0.5, 1.0),
            ),
            (
                "sasl.login.refresh.window.jitter",
                config.refresh_window_jitter,
                (0.0, 0.25),
            ),
        ] {
            if !value.is_finite() || value < range.0 || value > range.1 {
                return Err(format!(
                    "{name} must be between {} and {}, and it is {value}",
                    range.0, range.1
                ));
            }
        }
        Ok(Arc::new(Self {
            config,
            cached: tokio::sync::Mutex::new(None),
        }))
    }

    async fn fetch(&self) -> Result<String, String> {
        let request = self.request()?;
        // Kafka's `Retry.execute`: retry until `retry.backoff.max.ms` passed,
        // doubling the wait.
        let started = tokio::time::Instant::now();
        let end = started + self.config.retry_backoff_max;
        let mut attempt = 0_u32;
        loop {
            attempt += 1;
            match self.post(&request).await {
                Ok(body) => return parse_token(&body),
                Err(Failure::Final(error)) => return Err(error),
                Err(Failure::Retriable(error)) => {
                    let wait = self
                        .config
                        .retry_backoff
                        .saturating_mul(2_u32.saturating_pow(attempt - 1))
                        .min(end.saturating_duration_since(tokio::time::Instant::now()));
                    if wait.is_zero() {
                        return Err(error);
                    }
                    tracing::warn!(attempt, error = %error, "token endpoint request failed");
                    tokio::time::sleep(wait).await;
                }
            }
        }
    }

    /// The headers and body of `ClientSecretRequestFormatter`.
    fn request(&self) -> Result<TokenRequest, String> {
        let encode = |value: &str| {
            if self.config.url_encode {
                form_urlencode(value)
            } else {
                value.to_owned()
            }
        };
        let client_id = encode(self.config.client_id.trim());
        let client_secret = encode(self.config.client_secret.value().trim());
        let scope = self
            .config
            .scope
            .as_deref()
            .map(str::trim)
            .filter(|scope| !scope.is_empty())
            .map(encode);
        let mut body = "grant_type=client_credentials".to_owned();
        if let Some(scope) = scope {
            body.push_str("&scope=");
            body.push_str(&scope);
        }
        Ok(TokenRequest {
            endpoint: Endpoint::parse(&self.config.token_endpoint_url)?,
            authorization: format!(
                "Basic {}",
                B64.encode(format!("{client_id}:{client_secret}"))
            ),
            body,
        })
    }

    async fn post(&self, request: &TokenRequest) -> Result<String, Failure> {
        let endpoint = &request.endpoint;
        let connect = TcpStream::connect((endpoint.host.as_str(), endpoint.port));
        let tcp = within(self.config.connect_timeout, connect)
            .await
            .map_err(|()| Failure::Retriable("token endpoint connect timed out".into()))?
            .map_err(|error| Failure::Retriable(format!("token endpoint connect: {error}")))?;
        let exchange = async {
            if endpoint.https {
                let connector = self
                    .config
                    .tls
                    .connector()
                    .map_err(|error| Failure::Final(error.to_string()))?;
                // `tls.server_name` overrides the SNI name, as it does on a
                // broker connection.
                let server_name = if self.config.tls.server_name.is_empty() {
                    endpoint.host.clone()
                } else {
                    self.config.tls.server_name.clone()
                };
                let name = rustls::pki_types::ServerName::try_from(server_name)
                    .map_err(|error| Failure::Final(format!("invalid endpoint host: {error}")))?;
                let stream = connector
                    .connect(name, tcp)
                    .await
                    .map_err(|error| Failure::Retriable(format!("token endpoint TLS: {error}")))?;
                http_post(stream, request).await
            } else {
                http_post(tcp, request).await
            }
        };
        within(self.config.read_timeout, exchange)
            .await
            .map_err(|()| Failure::Retriable("token endpoint read timed out".into()))?
    }

    /// The refresh time of Kafka's
    /// `ExpiringCredentialRefreshingLogin.refreshMs` for a token issued at
    /// `start` and expiring at `expire`, now being `now`.
    fn refresh_at(
        &self,
        now: SystemTime,
        start: SystemTime,
        expire: SystemTime,
        unit: f64,
    ) -> SystemTime {
        let config = &self.config;
        let factor = config.refresh_window_factor + config.refresh_window_jitter * unit;
        if now + config.refresh_min_period + config.refresh_buffer > expire {
            return now
                + expire
                    .duration_since(now)
                    .unwrap_or_default()
                    .mul_f64(factor);
        }
        let proposed = start
            + expire
                .duration_since(start)
                .unwrap_or_default()
                .mul_f64(factor);
        let buffer_start = expire - config.refresh_buffer;
        if proposed > buffer_start {
            return buffer_start;
        }
        proposed.max(now + config.refresh_min_period)
    }
}

impl OAuthBearerTokenProvider for ClientCredentialsTokenProvider {
    fn token(&self) -> TokenFuture<'_> {
        Box::pin(async move {
            let mut cached = self.cached.lock().await;
            let now = SystemTime::now();
            if let Some(current) = cached.as_ref()
                && now < current.refresh_at
            {
                return Ok(current.token.clone());
            }
            let token = self.fetch().await?;
            let claims = token_times(&token)?;
            let refresh_at = self.refresh_at(
                now,
                claims.0.unwrap_or(now),
                claims.1,
                crate::backoff::random_unit(),
            );
            *cached = Some(Cached {
                token: token.clone(),
                refresh_at,
            });
            Ok(token)
        })
    }
}

/// How a token endpoint request failed.
enum Failure {
    /// Kafka's `HttpJwtRetriever` retries it.
    Retriable(String),
    /// An `UnretryableException`: fail at once.
    Final(String),
}

struct TokenRequest {
    endpoint: Endpoint,
    authorization: String,
    body: String,
}

/// A parsed token endpoint URL.
#[derive(Debug, PartialEq, Eq)]
struct Endpoint {
    https: bool,
    host: String,
    port: u16,
    /// The path and query, starting with `/`.
    target: String,
}

impl Endpoint {
    fn parse(url: &str) -> Result<Self, String> {
        let invalid =
            || format!("sasl.oauthbearer.token.endpoint.url {url:?} is not an http or https URL");
        let (https, rest) = if let Some(rest) = url.strip_prefix("https://") {
            (true, rest)
        } else if let Some(rest) = url.strip_prefix("http://") {
            (false, rest)
        } else {
            return Err(invalid());
        };
        // The authority ends at the first `/`, `?` or `#`, as RFC 3986 says.
        // A query with no path still posts to `/?...`, and a fragment never
        // goes on the wire.
        let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
        let (authority, after) = rest.split_at(authority_end);
        let after = after.split('#').next().unwrap_or("");
        let target = match after.as_bytes().first() {
            None => "/".to_owned(),
            Some(b'?') => format!("/{after}"),
            Some(_) => after.to_owned(),
        };
        let default_port = if https { 443 } else { 80 };
        let (host, port) = if let Some(bracketed) = authority.strip_prefix('[') {
            let (host, after) = bracketed.split_once(']').ok_or_else(invalid)?;
            let port = match after.strip_prefix(':') {
                Some(port) => port.parse().map_err(|_| invalid())?,
                None => default_port,
            };
            (host.to_owned(), port)
        } else {
            match authority.rsplit_once(':') {
                Some((host, port)) => (host.to_owned(), port.parse().map_err(|_| invalid())?),
                None => (authority.to_owned(), default_port),
            }
        };
        if host.is_empty() {
            return Err(invalid());
        }
        Ok(Self {
            https,
            host,
            port,
            target,
        })
    }
}

async fn within<F: Future>(limit: Option<Duration>, future: F) -> Result<F::Output, ()> {
    match limit {
        Some(limit) => tokio::time::timeout(limit, future).await.map_err(|_| ()),
        None => Ok(future.await),
    }
}

/// Send the POST of `HttpJwtRetriever.handleInput` and read the response.
async fn http_post<S>(mut stream: S, request: &TokenRequest) -> Result<String, Failure>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let endpoint = &request.endpoint;
    let host = if endpoint.host.contains(':') {
        format!("[{}]", endpoint.host)
    } else {
        endpoint.host.clone()
    };
    let head = format!(
        "POST {} HTTP/1.1\r\nHost: {host}:{}\r\nAccept: application/json\r\nAuthorization: {}\r\n\
         Cache-Control: no-cache\r\nContent-Type: application/x-www-form-urlencoded\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        endpoint.target,
        endpoint.port,
        request.authorization,
        request.body.len()
    );
    let io = |error: std::io::Error| Failure::Retriable(format!("token endpoint I/O: {error}"));
    stream.write_all(head.as_bytes()).await.map_err(io)?;
    stream
        .write_all(request.body.as_bytes())
        .await
        .map_err(io)?;
    stream.flush().await.map_err(io)?;
    let mut response = Vec::new();
    (&mut stream)
        .take(u64::try_from(MAX_RESPONSE_BYTES).unwrap_or(u64::MAX))
        .read_to_end(&mut response)
        .await
        .map_err(io)?;
    let (status, body) = parse_http_response(&response).map_err(Failure::Retriable)?;
    match status {
        200 | 201 => Ok(body),
        // Kafka's `HttpJwtRetriever.UNRETRYABLE_HTTP_CODES`.
        400 | 401 | 402 | 403 | 404 | 405 | 406 | 407 | 409 | 410 | 411 | 412 | 413 | 414 | 415
        | 501 | 505 => Err(Failure::Final(format!(
            "the token endpoint answered {status}: {}",
            snippet(&body)
        ))),
        status => Err(Failure::Retriable(format!(
            "the token endpoint answered {status}: {}",
            snippet(&body)
        ))),
    }
}

/// The status code and body of an HTTP/1.1 response that ends at EOF.
///
/// The body stays bytes until the chunk framing is off it. A chunk boundary
/// may cut a multibyte character in two, so a decode before the dechunk would
/// move every later byte offset.
fn parse_http_response(response: &[u8]) -> Result<(u16, String), String> {
    let separator = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| "incomplete token endpoint response".to_owned())?;
    let head = String::from_utf8_lossy(&response[..separator]).into_owned();
    let body = &response[separator + 4..];
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|line| line.split(' ').nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| "invalid token endpoint status line".to_owned())?;
    let chunked = lines.any(|line| {
        line.split_once(':').is_some_and(|(name, value)| {
            name.trim().eq_ignore_ascii_case("transfer-encoding")
                && value.trim().eq_ignore_ascii_case("chunked")
        })
    });
    let body = if chunked {
        dechunk(body)?
    } else {
        body.to_vec()
    };
    let body = String::from_utf8(body)
        .map_err(|_| "the token endpoint response is not UTF-8".to_owned())?;
    Ok((status, body))
}

/// Strip the chunk framing of a chunked body, in bytes.
fn dechunk(mut body: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    loop {
        let line_end = body
            .windows(2)
            .position(|window| window == b"\r\n")
            .ok_or_else(|| "invalid chunked token endpoint response".to_owned())?;
        let size = String::from_utf8_lossy(&body[..line_end]);
        let size = usize::from_str_radix(size.split(';').next().unwrap_or("").trim(), 16)
            .map_err(|_| "invalid chunk size in token endpoint response".to_owned())?;
        let rest = &body[line_end + 2..];
        if size == 0 {
            return Ok(out);
        }
        let chunk = rest
            .get(..size)
            .ok_or_else(|| "truncated chunk in token endpoint response".to_owned())?;
        out.extend_from_slice(chunk);
        body = rest.get(size + 2..).unwrap_or(&[]);
    }
}

fn snippet(body: &str) -> String {
    if body.len() <= MAX_RESPONSE_SNIPPET {
        return body.to_owned();
    }
    let end = (0..=MAX_RESPONSE_SNIPPET)
        .rev()
        .find(|index| body.is_char_boundary(*index))
        .unwrap_or(0);
    format!(
        "{} (trimmed to first {MAX_RESPONSE_SNIPPET} characters out of {} total)",
        &body[..end],
        body.len()
    )
}

/// Kafka's `JwtResponseParser.parseJwt`: `access_token`, else `id_token`.
fn parse_token(body: &str) -> Result<String, String> {
    let json: serde_json::Value = serde_json::from_str(body)
        .map_err(|error| format!("invalid token endpoint response: {error}"))?;
    ["access_token", "id_token"]
        .into_iter()
        .filter_map(|name| json.get(name).and_then(serde_json::Value::as_str))
        .map(str::trim)
        .find(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            format!(
                "The token endpoint response did not contain a valid JWT. Response: ({})",
                snippet(body)
            )
        })
}

/// The `iat` and `exp` times of a compact JWT. The client does not verify the
/// signature: the broker validates the token.
fn token_times(token: &str) -> Result<(Option<SystemTime>, SystemTime), String> {
    let payload = token
        .split('.')
        .nth(1)
        .ok_or_else(|| "the token is not a JWT".to_owned())?;
    let json: serde_json::Value = URL_SAFE_NO_PAD
        .decode(payload.trim_end_matches('='))
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .ok_or_else(|| "the JWT payload is not base64url JSON".to_owned())?;
    // RFC 7519 NumericDate is a JSON number, so a provider may send a
    // fractional second. A negative or unreal value has no time.
    let time = |name: &str| {
        json.get(name)
            .and_then(serde_json::Value::as_f64)
            .filter(|seconds| seconds.is_finite() && *seconds >= 0.0)
            .map(|seconds| UNIX_EPOCH + Duration::from_secs_f64(seconds))
    };
    let expire = time("exp").ok_or_else(|| "the JWT has no exp claim".to_owned())?;
    Ok((time("iat"), expire))
}

/// `application/x-www-form-urlencoded`, as Java's `URLEncoder.encode` writes it.
fn form_urlencode(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'.' | b'-' | b'*' | b'_' => {
                char::from(byte).to_string()
            }
            b' ' => "+".to_owned(),
            other => format!("%{other:02X}"),
        })
        .collect()
}

#[cfg(test)]
mod tests;
