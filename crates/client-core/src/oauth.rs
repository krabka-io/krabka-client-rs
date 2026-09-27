//! OAUTHBEARER tokens: the token provider seam, an OAuth 2 client credentials
//! provider and a jwt-bearer client assertion provider (KIP-768), and a
//! background refresh task for either.
//!
//! Kafka's `OAuthBearerLoginCallbackHandler` gets a token from a
//! `JwtRetriever`. `ClientCredentialsJwtRetriever` posts a
//! `client_credentials` grant to `sasl.oauthbearer.token.endpoint.url`,
//! `JwtBearerJwtRetriever` instead posts the
//! `urn:ietf:params:oauth:grant-type:jwt-bearer` grant with a signed client
//! assertion, and `ExpiringCredentialRefreshingLogin` fetches a new token
//! before the old one expires. [`spawn_background_refresh`] is this crate's
//! equivalent of that last part.

use std::{
    fmt, fs,
    future::Future,
    path::{Path, PathBuf},
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

/// The future of [`OAuthBearerTokenProvider::refresh_at`].
pub type RefreshFuture<'a> = Pin<Box<dyn Future<Output = Option<SystemTime>> + Send + 'a>>;

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

    /// Fetch a new token even when a cached one has not reached its refresh
    /// time, as Kafka's `ExpiringCredentialRefreshingLogin` re-logs in once
    /// its refresh timer fires. [`spawn_background_refresh`] calls this at
    /// each [`Self::refresh_at`]; a provider that caches nothing can leave
    /// the default, which is [`Self::token`].
    ///
    /// # Errors
    /// Returns a description when no token is available.
    fn refresh(&self) -> TokenFuture<'_> {
        self.token()
    }

    /// The time a cached token should next be refreshed, once
    /// [`Self::token`] has fetched one. [`spawn_background_refresh`] polls
    /// this to know when to wake up and fetch proactively; a provider that
    /// caches nothing can leave the default, which never schedules a
    /// background fetch.
    fn refresh_at(&self) -> RefreshFuture<'_> {
        Box::pin(async { None })
    }

    /// Whether [`spawn_background_refresh`] should run a background task for
    /// this provider.
    ///
    /// Kafka's `ExpiringCredentialRefreshingLogin` background thread always
    /// runs once a login completes. krabka instead makes starting the
    /// equivalent task an explicit call, so building a provider outside a
    /// Tokio runtime never panics; this flag is each provider's own opt-in
    /// for that call, and it defaults to `true` so a caller that does invoke
    /// [`spawn_background_refresh`] gets Kafka's normal, always-on behavior
    /// unless it explicitly turns the flag off.
    fn background_refresh_enabled(&self) -> bool {
        true
    }
}

/// Starts a background task that keeps `provider`'s token fresh by calling
/// [`OAuthBearerTokenProvider::refresh`] at the time
/// [`OAuthBearerTokenProvider::refresh_at`] reports, rather than waiting for
/// the next SASL exchange to notice the cached token needs a refresh. This is
/// krabka's equivalent of Kafka's `ExpiringCredentialRefreshingLogin`
/// background thread.
///
/// Returns `None` when
/// [`background_refresh_enabled`](OAuthBearerTokenProvider::background_refresh_enabled)
/// says not to. Otherwise the task runs until its `JoinHandle` is dropped or
/// aborted, so the caller should abort it (dropping the last reference to
/// `provider`) once the client using it shuts down.
#[must_use]
pub fn spawn_background_refresh(
    provider: Arc<dyn OAuthBearerTokenProvider>,
) -> Option<tokio::task::JoinHandle<()>> {
    if !provider.background_refresh_enabled() {
        return None;
    }
    Some(tokio::spawn(async move {
        let mut result = provider.token().await;
        loop {
            if let Err(error) = result {
                tracing::warn!(error = %error, "background OAUTHBEARER token refresh failed");
            }
            let wait = provider
                .refresh_at()
                .await
                .and_then(|refresh_at| refresh_at.duration_since(SystemTime::now()).ok())
                .unwrap_or(DEFAULT_LOGIN_RETRY_BACKOFF_MAX);
            tokio::time::sleep(wait).await;
            result = provider.refresh().await;
        }
    }))
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
    /// [`OAuthBearerTokenProvider::background_refresh_enabled`].
    pub background_refresh: bool,
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
            background_refresh: true,
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

    /// Fetch a new token as of `now` and store it with its refresh time in
    /// `cached`.
    async fn fetch_into(
        &self,
        cached: &mut Option<Cached>,
        now: SystemTime,
    ) -> Result<String, String> {
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
    }

    async fn fetch(&self) -> Result<String, String> {
        let request = self.request()?;
        retry_token_request(
            self.config.retry_backoff,
            self.config.retry_backoff_max,
            || Box::pin(self.post(&request)),
        )
        .await
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
            authorization: Some(format!(
                "Basic {}",
                B64.encode(format!("{client_id}:{client_secret}"))
            )),
            body,
        })
    }

    async fn post(&self, request: &TokenRequest) -> Result<String, Failure> {
        post_to_endpoint(
            &self.config.tls,
            self.config.connect_timeout,
            self.config.read_timeout,
            request,
        )
        .await
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
        expiring_credential_refresh_at(
            now,
            start,
            expire,
            unit,
            &RefreshWindow {
                factor: self.config.refresh_window_factor,
                jitter: self.config.refresh_window_jitter,
                min_period: self.config.refresh_min_period,
                buffer: self.config.refresh_buffer,
            },
        )
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
            self.fetch_into(&mut cached, now).await
        })
    }

    fn refresh(&self) -> TokenFuture<'_> {
        Box::pin(async move {
            let mut cached = self.cached.lock().await;
            self.fetch_into(&mut cached, SystemTime::now()).await
        })
    }

    fn refresh_at(&self) -> RefreshFuture<'_> {
        Box::pin(async move {
            self.cached
                .lock()
                .await
                .as_ref()
                .map(|cached| cached.refresh_at)
        })
    }

    fn background_refresh_enabled(&self) -> bool {
        self.config.background_refresh
    }
}

/// The window settings of Kafka's `ExpiringCredentialRefreshConfig`:
/// `sasl.login.refresh.window.factor`, `.jitter`, `sasl.login.refresh.min.period.seconds`
/// and `sasl.login.refresh.buffer.seconds`.
#[derive(Clone, Copy, Debug)]
struct RefreshWindow {
    factor: f64,
    jitter: f64,
    min_period: Duration,
    buffer: Duration,
}

/// Kafka's `ExpiringCredentialRefreshingLogin.refreshMs` for a token issued at
/// `start` and expiring at `expire`, now being `now`, with `window`'s factor
/// plus `window.jitter * unit`, no sooner than `window.min_period` from `now`
/// and no later than `window.buffer` before `expire`.
fn expiring_credential_refresh_at(
    now: SystemTime,
    start: SystemTime,
    expire: SystemTime,
    unit: f64,
    window: &RefreshWindow,
) -> SystemTime {
    let factor = window.factor + window.jitter * unit;
    if now + window.min_period + window.buffer > expire {
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
    let buffer_start = expire - window.buffer;
    if proposed > buffer_start {
        return buffer_start;
    }
    proposed.max(now + window.min_period)
}

/// Kafka's `urn:ietf:params:oauth:grant-type:jwt-bearer` grant type.
const JWT_BEARER_GRANT_TYPE: &str = "urn:ietf:params:oauth:grant-type:jwt-bearer";

/// The settings of [`JwtBearerTokenProvider`], as Kafka's `SaslConfigs` names
/// them.
#[derive(Clone, Debug)]
pub struct JwtBearerConfig {
    /// `sasl.oauthbearer.token.endpoint.url`, an `http` or `https` URL.
    pub token_endpoint_url: String,
    /// `sasl.oauthbearer.assertion.algorithm`. Only `RS256` is implemented.
    pub algorithm: String,
    /// `sasl.oauthbearer.assertion.claim.exp.seconds`: how far past `iat` the
    /// assertion's `exp` claim is set.
    pub exp_seconds: u64,
    /// `sasl.oauthbearer.assertion.claim.nbf.seconds`: how far before `iat`
    /// the assertion's `nbf` claim is set.
    pub nbf_seconds: u64,
    /// `sasl.oauthbearer.assertion.claim.jti.include`: add a random `jti`
    /// claim to the assertion.
    pub include_jti: bool,
    /// `sasl.oauthbearer.assertion.claim.iss`.
    pub iss: Option<String>,
    /// `sasl.oauthbearer.assertion.claim.sub`.
    pub sub: Option<String>,
    /// `sasl.oauthbearer.assertion.claim.aud`.
    pub aud: Option<String>,
    /// `sasl.oauthbearer.scope`. `None` requests the default scope.
    pub scope: Option<String>,
    /// `sasl.oauthbearer.assertion.private.key.file`: a PEM file with one RSA
    /// private key, PKCS#8 (`PRIVATE KEY`), PKCS#8 encrypted with PBES2
    /// (`ENCRYPTED PRIVATE KEY`, decrypted with
    /// [`Self::private_key_passphrase`]), or PKCS#1 (`RSA PRIVATE KEY`).
    pub private_key_file: PathBuf,
    /// `sasl.oauthbearer.assertion.private.key.passphrase`.
    pub private_key_passphrase: Option<Password>,
    /// `sasl.oauthbearer.assertion.template.file`: a JSON file of the shape
    /// `{"header": {...}, "payload": {...}}`, either key optional. Its
    /// entries seed the assertion's header and payload, and the generated
    /// `alg`, `typ`, `iat`, `exp`, `nbf` and (if configured) `jti` then
    /// overwrite same-named entries, as `LayeredAssertionJwtTemplate` layers
    /// the static claims, the template file and the dynamic claims, each on
    /// top of the last.
    pub template_file: Option<PathBuf>,
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
    /// [`OAuthBearerTokenProvider::background_refresh_enabled`].
    pub background_refresh: bool,
}

impl JwtBearerConfig {
    /// Settings with Kafka's defaults for `endpoint` and `private_key_file`:
    /// `RS256`, a 300 s `exp` claim, a 60 s `nbf` claim, and no `jti`, `iss`,
    /// `sub`, `aud`, scope or template file.
    #[must_use]
    pub fn new(endpoint: impl Into<String>, private_key_file: impl Into<PathBuf>) -> Self {
        Self {
            token_endpoint_url: endpoint.into(),
            algorithm: "RS256".to_owned(),
            exp_seconds: 300,
            nbf_seconds: 60,
            include_jti: false,
            iss: None,
            sub: None,
            aud: None,
            scope: None,
            private_key_file: private_key_file.into(),
            private_key_passphrase: None,
            template_file: None,
            tls: TlsConnectorConfig::default(),
            connect_timeout: None,
            read_timeout: None,
            retry_backoff: DEFAULT_LOGIN_RETRY_BACKOFF,
            retry_backoff_max: DEFAULT_LOGIN_RETRY_BACKOFF_MAX,
            refresh_window_factor: DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR,
            refresh_window_jitter: DEFAULT_LOGIN_REFRESH_WINDOW_JITTER,
            refresh_min_period: DEFAULT_LOGIN_REFRESH_MIN_PERIOD,
            refresh_buffer: DEFAULT_LOGIN_REFRESH_BUFFER,
            background_refresh: true,
        }
    }
}

/// A signed JSON claim map, kept in the order the file or config gave it so a
/// deterministic assertion is easy to test.
type ClaimMap = serde_json::Map<String, serde_json::Value>;

/// An OAUTHBEARER token provider that uses the OAuth 2 jwt-bearer grant with a
/// signed client assertion (RFC 7523), as Kafka's `JwtBearerJwtRetriever` with
/// `JwtBearerRequestFormatter` does. It keeps the token until the refresh time
/// of Kafka's `ExpiringCredentialRefreshingLogin`, then fetches a new one.
pub struct JwtBearerTokenProvider {
    config: JwtBearerConfig,
    key_pair: ring::signature::RsaKeyPair,
    template_header: ClaimMap,
    template_payload: ClaimMap,
    cached: tokio::sync::Mutex<Option<Cached>>,
}

impl fmt::Debug for JwtBearerTokenProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JwtBearerTokenProvider")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl JwtBearerTokenProvider {
    /// A provider for `config`.
    ///
    /// # Errors
    /// Returns a description when the algorithm is not `RS256`, the endpoint
    /// is not an `http` or `https` URL, the private key file or template file
    /// cannot be read or parsed, or a refresh window setting is out of range,
    /// as Kafka's `ConfigException`.
    pub fn new(config: JwtBearerConfig) -> Result<Arc<Self>, String> {
        if config.algorithm != "RS256" {
            return Err(format!(
                "sasl.oauthbearer.assertion.algorithm {:?} is not supported; only RS256 is \
                 implemented",
                config.algorithm
            ));
        }
        Endpoint::parse(&config.token_endpoint_url)?;
        // Kafka's `ExpiringCredentialRefreshConfig` refuses a window factor or
        // jitter outside its range. A bad one would reach `Duration::mul_f64`
        // and panic at the first token.
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
        let key_pair = load_rsa_private_key(
            &config.private_key_file,
            config.private_key_passphrase.as_ref(),
        )
        .map_err(|error| {
            format!(
                "sasl.oauthbearer.assertion.private.key.file {}: {error}",
                config.private_key_file.display()
            )
        })?;
        let (template_header, template_payload) = match &config.template_file {
            Some(path) => load_assertion_template(path)?,
            None => (ClaimMap::new(), ClaimMap::new()),
        };
        Ok(Arc::new(Self {
            config,
            key_pair,
            template_header,
            template_payload,
            cached: tokio::sync::Mutex::new(None),
        }))
    }

    /// Fetch a new token as of `now` and store it with its refresh time in
    /// `cached`.
    async fn fetch_into(
        &self,
        cached: &mut Option<Cached>,
        now: SystemTime,
    ) -> Result<String, String> {
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
    }

    async fn fetch(&self) -> Result<String, String> {
        let request = self.request()?;
        retry_token_request(
            self.config.retry_backoff,
            self.config.retry_backoff_max,
            || Box::pin(self.post(&request)),
        )
        .await
    }

    /// The header and payload of `LayeredAssertionJwtTemplate`: the template
    /// file's entries (if any) sit under the static `iss`/`sub`/`aud` claims,
    /// and the dynamic `alg`, `typ`, `iat`, `exp`, `nbf` and `jti` claims are
    /// generated fresh and win any conflict, as
    /// `AssertionSupplierFactory.layeredAssertionJwtTemplate` orders its
    /// layers.
    fn claims(&self, now: SystemTime) -> (ClaimMap, ClaimMap) {
        let mut header = self.template_header.clone();
        header.insert("typ".to_owned(), "JWT".into());
        header.insert("alg".to_owned(), self.config.algorithm.clone().into());

        let mut payload = ClaimMap::new();
        if let Some(iss) = &self.config.iss {
            payload.insert("iss".to_owned(), iss.clone().into());
        }
        if let Some(sub) = &self.config.sub {
            payload.insert("sub".to_owned(), sub.clone().into());
        }
        if let Some(aud) = &self.config.aud {
            payload.insert("aud".to_owned(), aud.clone().into());
        }
        for (claim, value) in &self.template_payload {
            payload.insert(claim.clone(), value.clone());
        }
        let now_secs = now.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        payload.insert("iat".to_owned(), now_secs.into());
        payload.insert(
            "exp".to_owned(),
            (now_secs + self.config.exp_seconds).into(),
        );
        payload.insert(
            "nbf".to_owned(),
            now_secs.saturating_sub(self.config.nbf_seconds).into(),
        );
        if self.config.include_jti {
            payload.insert("jti".to_owned(), uuid::Uuid::new_v4().to_string().into());
        }
        (header, payload)
    }

    /// The signed compact JWT assertion of `DefaultAssertionCreator`.
    fn assertion(&self) -> Result<String, String> {
        let (header, payload) = self.claims(SystemTime::now());
        let signing_input = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(serde_json::Value::Object(header).to_string()),
            URL_SAFE_NO_PAD.encode(serde_json::Value::Object(payload).to_string()),
        );
        let signature = sign_rs256(&self.key_pair, signing_input.as_bytes())?;
        Ok(format!(
            "{signing_input}.{}",
            URL_SAFE_NO_PAD.encode(signature)
        ))
    }

    /// The headers and body of `JwtBearerRequestFormatter.formatBody`: the
    /// jwt-bearer grant with the signed assertion, and the optional scope. No
    /// `Authorization` header is sent.
    fn request(&self) -> Result<TokenRequest, String> {
        let assertion = self.assertion()?;
        let mut body = format!(
            "grant_type={}&assertion={}",
            form_urlencode(JWT_BEARER_GRANT_TYPE),
            form_urlencode(&assertion)
        );
        if let Some(scope) = self
            .config
            .scope
            .as_deref()
            .map(str::trim)
            .filter(|scope| !scope.is_empty())
        {
            body.push_str("&scope=");
            body.push_str(&form_urlencode(scope));
        }
        Ok(TokenRequest {
            endpoint: Endpoint::parse(&self.config.token_endpoint_url)?,
            authorization: None,
            body,
        })
    }

    async fn post(&self, request: &TokenRequest) -> Result<String, Failure> {
        post_to_endpoint(
            &self.config.tls,
            self.config.connect_timeout,
            self.config.read_timeout,
            request,
        )
        .await
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
        expiring_credential_refresh_at(
            now,
            start,
            expire,
            unit,
            &RefreshWindow {
                factor: self.config.refresh_window_factor,
                jitter: self.config.refresh_window_jitter,
                min_period: self.config.refresh_min_period,
                buffer: self.config.refresh_buffer,
            },
        )
    }
}

impl OAuthBearerTokenProvider for JwtBearerTokenProvider {
    fn token(&self) -> TokenFuture<'_> {
        Box::pin(async move {
            let mut cached = self.cached.lock().await;
            let now = SystemTime::now();
            if let Some(current) = cached.as_ref()
                && now < current.refresh_at
            {
                return Ok(current.token.clone());
            }
            self.fetch_into(&mut cached, now).await
        })
    }

    fn refresh(&self) -> TokenFuture<'_> {
        Box::pin(async move {
            let mut cached = self.cached.lock().await;
            self.fetch_into(&mut cached, SystemTime::now()).await
        })
    }

    fn refresh_at(&self) -> RefreshFuture<'_> {
        Box::pin(async move {
            self.cached
                .lock()
                .await
                .as_ref()
                .map(|cached| cached.refresh_at)
        })
    }

    fn background_refresh_enabled(&self) -> bool {
        self.config.background_refresh
    }
}

/// Sign `message` with `key_pair`, as `DefaultAssertionCreator` does with
/// Kafka's `sasl.oauthbearer.assertion.algorithm` `RS256`.
fn sign_rs256(key_pair: &ring::signature::RsaKeyPair, message: &[u8]) -> Result<Vec<u8>, String> {
    let rng = ring::rand::SystemRandom::new();
    let mut signature = vec![0_u8; key_pair.public().modulus_len()];
    key_pair
        .sign(
            &ring::signature::RSA_PKCS1_SHA256,
            &rng,
            message,
            &mut signature,
        )
        .map_err(|_| "RSA signing failed".to_owned())?;
    Ok(signature)
}

/// The one PEM block of `text`, as `(label, DER)`.
fn pem_block(text: &str) -> Result<(String, Vec<u8>), String> {
    const BEGIN: &str = "-----BEGIN ";
    let start = text
        .find(BEGIN)
        .ok_or_else(|| "no PEM block found".to_owned())?;
    let block = &text[start..];
    let label_end = block[BEGIN.len()..]
        .find("-----")
        .ok_or_else(|| "unterminated BEGIN line".to_owned())?;
    let label = block[BEGIN.len()..BEGIN.len() + label_end].to_owned();
    let end_line = format!("-----END {label}-----");
    let end = block
        .find(&end_line)
        .ok_or_else(|| format!("no END line for {label}"))?
        + end_line.len();
    let (_, der) = pkcs8::der::pem::decode_vec(&block.as_bytes()[..end])
        .map_err(|error| format!("{label}: {error}"))?;
    Ok((label, der))
}

/// Decrypt a PKCS#8 `EncryptedPrivateKeyInfo` with PBES2, as Kafka's
/// `PemStore.privateKey` does with `sasl.oauthbearer.assertion.private.key.passphrase`.
fn decrypt_pkcs8(der: &[u8], passphrase: &Password) -> Result<Vec<u8>, String> {
    let encrypted = pkcs8::EncryptedPrivateKeyInfoRef::try_from(der)
        .map_err(|error| format!("invalid encrypted private key: {error}"))?;
    let document = encrypted
        .decrypt(passphrase.value())
        .map_err(|error| format!("private key decryption failed: {error}"))?;
    Ok(document.as_bytes().to_vec())
}

/// Load the one RSA private key of a PEM file, as `DefaultAssertionCreator`
/// does for `sasl.oauthbearer.assertion.private.key.file`. Accepts a PKCS#8
/// key (`PRIVATE KEY`), a PKCS#8 key encrypted with PBES2
/// (`ENCRYPTED PRIVATE KEY`, decrypted with `passphrase`), or a PKCS#1 RSA
/// key (`RSA PRIVATE KEY`).
fn load_rsa_private_key(
    path: &Path,
    passphrase: Option<&Password>,
) -> Result<ring::signature::RsaKeyPair, String> {
    let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
    let (label, der) = pem_block(&text)?;
    let der = match (label.as_str(), passphrase) {
        ("ENCRYPTED PRIVATE KEY", Some(passphrase)) => decrypt_pkcs8(&der, passphrase)?,
        ("ENCRYPTED PRIVATE KEY", None) => {
            return Err(
                "the private key is encrypted, but sasl.oauthbearer.assertion.private.key.\
                 passphrase is not set"
                    .to_owned(),
            );
        }
        (_, Some(_)) => {
            return Err(
                "sasl.oauthbearer.assertion.private.key.passphrase is set, but the private key \
                 is not encrypted"
                    .to_owned(),
            );
        }
        ("PRIVATE KEY" | "RSA PRIVATE KEY", None) => der,
        (other, None) => return Err(format!("unsupported private key format {other:?}")),
    };
    match label.as_str() {
        "RSA PRIVATE KEY" => ring::signature::RsaKeyPair::from_der(&der),
        _ => ring::signature::RsaKeyPair::from_pkcs8(&der),
    }
    .map_err(|error| format!("invalid RSA private key: {error}"))
}

/// Load an assertion template file's `header` and `payload` maps, as
/// `FileAssertionJwtTemplate` does for
/// `sasl.oauthbearer.assertion.template.file`. Either key may be absent,
/// which is the same as an empty map.
fn load_assertion_template(path: &Path) -> Result<(ClaimMap, ClaimMap), String> {
    let shown = path.display();
    let text = fs::read_to_string(path)
        .map_err(|error| format!("sasl.oauthbearer.assertion.template.file {shown}: {error}"))?;
    let json: serde_json::Value = serde_json::from_str(&text)
        .map_err(|error| format!("sasl.oauthbearer.assertion.template.file {shown}: {error}"))?;
    let map = |key: &str| match json.get(key) {
        None => Ok(ClaimMap::new()),
        Some(serde_json::Value::Object(map)) => Ok(map.clone()),
        Some(_) => Err(format!(
            "sasl.oauthbearer.assertion.template.file {shown}: {key:?} is not a JSON object"
        )),
    };
    Ok((map("header")?, map("payload")?))
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
    /// `None` for the jwt-bearer grant: `JwtBearerRequestFormatter` sends no
    /// `Authorization` header, since the assertion itself authenticates the
    /// client.
    authorization: Option<String>,
    body: String,
}

/// Kafka's `Retry.execute`: post `request` with `attempt`, retrying a
/// [`Failure::Retriable`] until `retry_backoff_max` has passed since the
/// first attempt, doubling the wait each time, and returning the parsed
/// access or id token of a success.
async fn retry_token_request<'a>(
    retry_backoff: Duration,
    retry_backoff_max: Duration,
    mut attempt: impl FnMut() -> Pin<Box<dyn Future<Output = Result<String, Failure>> + Send + 'a>>,
) -> Result<String, String> {
    let started = tokio::time::Instant::now();
    let end = started + retry_backoff_max;
    let mut attempt_number = 0_u32;
    loop {
        attempt_number += 1;
        match attempt().await {
            Ok(body) => return parse_token(&body),
            Err(Failure::Final(error)) => return Err(error),
            Err(Failure::Retriable(error)) => {
                let wait = retry_backoff
                    .saturating_mul(2_u32.saturating_pow(attempt_number - 1))
                    .min(end.saturating_duration_since(tokio::time::Instant::now()));
                if wait.is_zero() {
                    return Err(error);
                }
                tracing::warn!(
                    attempt = attempt_number,
                    error = %error,
                    "token endpoint request failed"
                );
                tokio::time::sleep(wait).await;
            }
        }
    }
}

/// Connect to `request`'s endpoint, over TLS with `tls` when it is `https`,
/// and post it, as `HttpJwtRetriever.doPost` does.
async fn post_to_endpoint(
    tls: &TlsConnectorConfig,
    connect_timeout: Option<Duration>,
    read_timeout: Option<Duration>,
    request: &TokenRequest,
) -> Result<String, Failure> {
    let endpoint = &request.endpoint;
    let connect = TcpStream::connect((endpoint.host.as_str(), endpoint.port));
    let tcp = within(connect_timeout, connect)
        .await
        .map_err(|()| Failure::Retriable("token endpoint connect timed out".into()))?
        .map_err(|error| Failure::Retriable(format!("token endpoint connect: {error}")))?;
    let exchange = async {
        if endpoint.https {
            let connector = tls
                .connector()
                .map_err(|error| Failure::Final(error.to_string()))?;
            // `tls.server_name` overrides the SNI name, as it does on a
            // broker connection.
            let server_name = if tls.server_name.is_empty() {
                endpoint.host.clone()
            } else {
                tls.server_name.clone()
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
    within(read_timeout, exchange)
        .await
        .map_err(|()| Failure::Retriable("token endpoint read timed out".into()))?
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
    let authorization = request
        .authorization
        .as_deref()
        .map_or_else(String::new, |value| format!("Authorization: {value}\r\n"));
    let head = format!(
        "POST {} HTTP/1.1\r\nHost: {host}:{}\r\nAccept: application/json\r\n{authorization}\
         Cache-Control: no-cache\r\nContent-Type: application/x-www-form-urlencoded\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        endpoint.target,
        endpoint.port,
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
