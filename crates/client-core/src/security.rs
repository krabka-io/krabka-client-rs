//! Client-side TLS/SASL security surface for [`crate::Client`].
//!
//! This module mirrors the TLS settings of Kafka's `SslConfigs` and the
//! trust and key store handling of `DefaultSslEngineFactory`. The public
//! clients and the inter-broker dialer negotiate the same way.

mod stores;

use std::{
    fmt,
    path::PathBuf,
    sync::{Arc, OnceLock},
};

use krabka_security::ListenerProtocol;
use rustls::{
    CertificateError, DigitallySignedStruct, SignatureScheme, SupportedProtocolVersion,
    client::{
        WebPkiServerVerifier,
        danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    },
    crypto::CryptoProvider,
    pki_types::{CertificateDer, ServerName, UnixTime},
};
use thiserror::Error;
use tokio_rustls::TlsConnector;

pub use crate::sasl::SaslCredentials;

/// Return the hostname from one Kafka `host:port` address.
#[must_use]
pub fn connection_target_host(address: &str) -> &str {
    match address.strip_prefix('[') {
        Some(bracketed) => bracketed
            .split_once(']')
            .map_or(bracketed, |(host, _)| host),
        None => address.rsplit_once(':').map_or(address, |(host, _)| host),
    }
}

/// A secret configuration value, such as Kafka's `ssl.key.password`.
///
/// `Debug` prints `[hidden]`, as Kafka's `Password.toString` does, so a
/// logged configuration does not show the secret.
#[derive(Clone, PartialEq, Eq)]
pub struct Password(String);

impl Password {
    /// Wrap a secret value.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Return the secret value.
    #[must_use]
    pub fn value(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Password {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[hidden]")
    }
}

impl From<&str> for Password {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for Password {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// The certificates that verify the broker certificate (Kafka
/// `ssl.truststore.*`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum TrustStore {
    /// The platform trust store. Kafka uses the JVM default trust store when
    /// `ssl.truststore.location` and `ssl.truststore.certificates` are not
    /// set (`DefaultSslEngineFactory.getTrustManagers` with a null
    /// `KeyStore`).
    #[default]
    Platform,
    /// A PEM file of CA certificates (`ssl.truststore.type=PEM` with
    /// `ssl.truststore.location`).
    PemFile(PathBuf),
    /// PEM CA certificates in the configuration
    /// (`ssl.truststore.certificates`).
    Pem(String),
    /// A PKCS#12 trust store file (`ssl.truststore.type=PKCS12`). The client
    /// trusts each certificate entry that has no private key.
    Pkcs12 { path: PathBuf, password: Password },
    /// A JKS trust store file (`ssl.truststore.type=JKS`). Without a password
    /// the client does not check the store digest, as Java's
    /// `KeyStore.load(stream, null)` does.
    Jks {
        path: PathBuf,
        password: Option<Password>,
    },
}

/// The client certificate chain and private key for mutual TLS (Kafka
/// `ssl.keystore.*`).
///
/// A private key can be PKCS#8 (`PRIVATE KEY`) or PKCS#8 encrypted with
/// PBES2 (`ENCRYPTED PRIVATE KEY`). An encrypted key needs `key_password`
/// (`ssl.key.password`), as Kafka's `PemStore.privateKey` does.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeyStore {
    /// One PEM file with the private key and the certificate chain
    /// (`ssl.keystore.type=PEM` with `ssl.keystore.location`).
    PemFile {
        path: PathBuf,
        key_password: Option<Password>,
    },
    /// A PEM certificate chain file and a separate PEM private key file.
    PemFiles {
        certificate_chain: PathBuf,
        private_key: PathBuf,
        key_password: Option<Password>,
    },
    /// A PEM certificate chain and private key in the configuration
    /// (`ssl.keystore.certificate.chain` and `ssl.keystore.key`).
    Pem {
        certificate_chain: String,
        private_key: String,
        key_password: Option<Password>,
    },
    /// A PKCS#12 key store file (`ssl.keystore.type=PKCS12`). The store
    /// password also decrypts the key.
    Pkcs12 { path: PathBuf, password: Password },
    /// A JKS key store file (`ssl.keystore.type=JKS`). `key_password`
    /// decrypts the key. Without it, the store password decrypts the key, as
    /// Kafka's `FileBasedStore` does when `ssl.key.password` is not set.
    Jks {
        path: PathBuf,
        password: Password,
        key_password: Option<Password>,
    },
}

/// A TLS protocol version name of `ssl.protocol` and `ssl.enabled.protocols`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TlsVersion {
    /// `TLSv1.2`.
    Tls12,
    /// `TLSv1.3`.
    Tls13,
}

impl TlsVersion {
    /// Parse a Kafka protocol name, `TLSv1.2` or `TLSv1.3`.
    ///
    /// # Errors
    /// Returns [`TlsConfigError::UnsupportedProtocol`] for any other name,
    /// such as `TLSv1.1`, which this client does not implement.
    pub fn parse(name: &str) -> Result<Self, TlsConfigError> {
        match name {
            "TLSv1.2" => Ok(Self::Tls12),
            "TLSv1.3" => Ok(Self::Tls13),
            other => Err(TlsConfigError::UnsupportedProtocol(other.to_owned())),
        }
    }

    const fn rustls(self) -> &'static SupportedProtocolVersion {
        match self {
            Self::Tls12 => &rustls::version::TLS12,
            Self::Tls13 => &rustls::version::TLS13,
        }
    }
}

/// An invalid TLS configuration. Kafka raises these as an
/// `InvalidConfigurationException` or a `KafkaException` when it builds the
/// SSL engine factory.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TlsConfigError {
    /// A store file could not be read.
    #[error("cannot read {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// A PEM value holds no certificate or no usable private key.
    #[error("invalid PEM: {0}")]
    Pem(String),
    /// A private key could not be decrypted or decoded.
    #[error("invalid private key: {0}")]
    PrivateKey(String),
    /// A PKCS#12 store could not be read.
    #[error("invalid PKCS#12 store: {0}")]
    Pkcs12(String),
    /// A JKS store could not be read.
    #[error("invalid JKS store: {0}")]
    Jks(String),
    /// The platform trust store holds no certificate.
    #[error("the platform trust store holds no certificate: {0}")]
    PlatformTrustStore(String),
    /// A trust store holds no certificate that can be a trust anchor.
    #[error("the trust store holds no usable CA certificate")]
    EmptyTrustStore,
    /// A protocol name is not `TLSv1.2` or `TLSv1.3`.
    #[error("unsupported TLS protocol {0:?}")]
    UnsupportedProtocol(String),
    /// No enabled protocol is at or below `ssl.protocol`.
    #[error("no enabled TLS protocol is at or below {0:?}")]
    NoProtocol(TlsVersion),
    /// A cipher suite name is not a TLS cipher suite name.
    #[error("unknown cipher suite {0:?}")]
    UnknownCipherSuite(String),
    /// None of the configured cipher suites is available.
    #[error("no configured cipher suite is supported: {0:?}")]
    NoCipherSuite(Vec<String>),
    /// rustls rejected the configuration.
    #[error("TLS configuration: {0}")]
    Rustls(String),
}

/// Client-side TLS settings, as Kafka's `SslConfigs` defines them.
///
/// `Default` gives Kafka's defaults: the platform trust store, no client
/// certificate, hostname verification on, `ssl.protocol=TLSv1.3`,
/// `ssl.enabled.protocols=TLSv1.2,TLSv1.3`, and the default cipher suites.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TlsConnectorConfig {
    /// The CA certificates that verify the broker certificate.
    pub trust_store: TrustStore,
    /// The client certificate and key for mutual TLS. `None` presents no
    /// client certificate.
    pub key_store: Option<KeyStore>,
    /// Explicit SNI / server-name override. An empty string uses each
    /// connection's target hostname, including brokers learned from metadata.
    pub server_name: String,
    /// Whether the client checks that the broker certificate names the host
    /// it connects to. Kafka's `ssl.endpoint.identification.algorithm=https`
    /// (the default) turns it on, and an empty value turns it off. With the
    /// check off, the client still verifies the certificate chain.
    pub hostname_verification: bool,
    /// The highest protocol version that the client uses (`ssl.protocol`).
    pub protocol: TlsVersion,
    /// The protocol versions that the client offers
    /// (`ssl.enabled.protocols`).
    pub enabled_protocols: Vec<TlsVersion>,
    /// The cipher suites that the client offers, by IANA name
    /// (`ssl.cipher.suites`). `None` offers the default suites. The client
    /// skips a known suite name that it does not implement, such as a CBC
    /// suite.
    pub cipher_suites: Option<Vec<String>>,
    /// The configuration that [`Self::connector`] built last, shared by the
    /// clones of this value.
    built: BuiltConfig,
}

/// The last built client configuration and the settings that built it.
///
/// Kafka builds the SSL context once per client. The pool clones the
/// settings for each broker, so a clone reuses the configuration while its
/// settings, other than the server name, stay the same.
#[derive(Clone, Default)]
struct BuiltConfig(Arc<std::sync::Mutex<Option<Built>>>);

/// The settings of a build and the configuration that they gave.
type Built = (TlsConnectorConfig, Arc<rustls::ClientConfig>);

impl PartialEq for BuiltConfig {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for BuiltConfig {}

impl fmt::Debug for BuiltConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BuiltConfig")
    }
}

impl Default for TlsConnectorConfig {
    fn default() -> Self {
        Self {
            trust_store: TrustStore::Platform,
            key_store: None,
            server_name: String::new(),
            hostname_verification: true,
            protocol: TlsVersion::Tls13,
            enabled_protocols: vec![TlsVersion::Tls12, TlsVersion::Tls13],
            cipher_suites: None,
            built: BuiltConfig::default(),
        }
    }
}

/// The platform trust store, loaded once for the process as the JVM loads its
/// default trust store once.
fn platform_roots() -> Result<Arc<rustls::RootCertStore>, TlsConfigError> {
    static ROOTS: OnceLock<Result<Arc<rustls::RootCertStore>, String>> = OnceLock::new();
    ROOTS
        .get_or_init(|| {
            let loaded = rustls_native_certs::load_native_certs();
            let mut roots = rustls::RootCertStore::empty();
            let (_, ignored) = roots.add_parsable_certificates(loaded.certs);
            if roots.is_empty() {
                let errors = loaded
                    .errors
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>();
                return Err(format!(
                    "{ignored} unparsable certificates, errors {errors:?}"
                ));
            }
            Ok(Arc::new(roots))
        })
        .clone()
        .map_err(TlsConfigError::PlatformTrustStore)
}

impl TlsConnectorConfig {
    /// Build a `rustls::ClientConfig`.
    ///
    /// # Errors
    /// Returns [`TlsConfigError`] when a store does not load, when a protocol
    /// or cipher suite setting leaves nothing to offer, or when the platform
    /// trust store is empty.
    pub fn build(&self) -> Result<Arc<rustls::ClientConfig>, TlsConfigError> {
        self.build_with_platform_roots(platform_roots)
    }

    /// Build the client configuration. `platform` supplies the trust store
    /// for [`TrustStore::Platform`], which lets a test use its own CA as the
    /// platform store.
    fn build_with_platform_roots(
        &self,
        platform: impl FnOnce() -> Result<Arc<rustls::RootCertStore>, TlsConfigError>,
    ) -> Result<Arc<rustls::ClientConfig>, TlsConfigError> {
        let roots = match &self.trust_store {
            TrustStore::Platform => platform()?,
            store => Arc::new(stores::trust_anchors(store)?),
        };
        let provider = Arc::new(self.crypto_provider()?);
        let versions = self.protocol_versions()?;
        let webpki = WebPkiServerVerifier::builder_with_provider(roots, Arc::clone(&provider))
            .build()
            .map_err(|error| TlsConfigError::Rustls(error.to_string()))?;
        let builder = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&versions)
            .map_err(|error| TlsConfigError::Rustls(error.to_string()))?;
        let builder = if self.hostname_verification {
            builder.with_webpki_verifier(webpki)
        } else {
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(ChainOnlyVerifier { inner: webpki }))
        };
        let config = match &self.key_store {
            Some(store) => {
                let key_provider = builder.crypto_provider().key_provider;
                let identities = stores::key_pairs(store)?
                    .into_iter()
                    .map(|(chain, key)| ClientIdentity::new(chain, key, key_provider))
                    .collect::<Result<Vec<_>, _>>()?;
                builder.with_client_cert_resolver(Arc::new(IssuerResolver { identities }))
            }
            None => builder.with_no_client_auth(),
        };
        Ok(Arc::new(config))
    }

    /// Build a ready `TlsConnector`.
    ///
    /// The first call builds the client configuration, and later calls on
    /// this value or its clones reuse it while the settings stay the same. A
    /// store file that changes after the first build therefore does not
    /// change the connections, as a Kafka client reads its stores once.
    ///
    /// # Errors
    /// Propagates [`Self::build`] failures.
    pub fn connector(&self) -> Result<TlsConnector, TlsConfigError> {
        let settings = Self {
            server_name: String::new(),
            built: BuiltConfig::default(),
            ..self.clone()
        };
        let mut built = self
            .built
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((built_settings, config)) = built.as_ref()
            && *built_settings == settings
        {
            return Ok(TlsConnector::from(Arc::clone(config)));
        }
        let config = self.build()?;
        *built = Some((settings, Arc::clone(&config)));
        Ok(TlsConnector::from(config))
    }

    /// The enabled protocol versions at or below [`Self::protocol`].
    fn protocol_versions(&self) -> Result<Vec<&'static SupportedProtocolVersion>, TlsConfigError> {
        let mut enabled = self
            .enabled_protocols
            .iter()
            .copied()
            .filter(|version| *version <= self.protocol)
            .collect::<Vec<_>>();
        enabled.sort_unstable();
        enabled.dedup();
        if enabled.is_empty() {
            return Err(TlsConfigError::NoProtocol(self.protocol));
        }
        Ok(enabled.into_iter().map(TlsVersion::rustls).collect())
    }

    /// The process crypto provider, limited to [`Self::cipher_suites`].
    fn crypto_provider(&self) -> Result<CryptoProvider, TlsConfigError> {
        let mut provider = CryptoProvider::get_default()
            .map_or_else(rustls::crypto::ring::default_provider, |provider| {
                provider.as_ref().clone()
            });
        let Some(names) = &self.cipher_suites else {
            return Ok(provider);
        };
        let mut wanted = Vec::with_capacity(names.len());
        for name in names {
            wanted.push(cipher_suite(name)?);
        }
        provider
            .cipher_suites
            .retain(|suite| wanted.contains(&suite.suite()));
        if provider.cipher_suites.is_empty() {
            return Err(TlsConfigError::NoCipherSuite(names.clone()));
        }
        Ok(provider)
    }
}

/// Map an IANA cipher suite name, as Kafka's `ssl.cipher.suites` holds it, to
/// the rustls suite. rustls names the TLS 1.3 suites `TLS13_*` where IANA
/// names them `TLS_*`.
fn cipher_suite(name: &str) -> Result<rustls::CipherSuite, TlsConfigError> {
    let tls13 = name
        .strip_prefix("TLS_")
        .map(|rest| format!("TLS13_{rest}"));
    (0..=u16::MAX)
        .map(rustls::CipherSuite::from)
        .find(|suite| {
            suite
                .as_str()
                .is_some_and(|known| known == name || tls13.as_deref() == Some(known))
        })
        .ok_or_else(|| TlsConfigError::UnknownCipherSuite(name.to_owned()))
}

/// One client certificate chain and its signing key.
#[derive(Debug)]
struct ClientIdentity {
    key: Arc<rustls::sign::CertifiedKey>,
    /// The DER issuer names of the certificates of the chain.
    issuers: Vec<Vec<u8>>,
}

impl ClientIdentity {
    fn new(
        chain: Vec<CertificateDer<'static>>,
        key: rustls::pki_types::PrivateKeyDer<'static>,
        key_provider: &dyn rustls::crypto::KeyProvider,
    ) -> Result<Self, TlsConfigError> {
        use x509_cert::der::{Decode as _, Encode as _};

        let issuers = chain
            .iter()
            .map(|certificate| {
                x509_cert::Certificate::from_der(certificate)
                    .and_then(|certificate| certificate.tbs_certificate().issuer().to_der())
                    .map_err(|error| TlsConfigError::Pem(format!("invalid certificate: {error}")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let signing_key = key_provider
            .load_private_key(key)
            .map_err(|error| TlsConfigError::PrivateKey(error.to_string()))?;
        Ok(Self {
            key: Arc::new(rustls::sign::CertifiedKey::new(chain, signing_key)),
            issuers,
        })
    }
}

/// Pick the client identity that the broker can accept, as the `SunX509`
/// key manager of Kafka's default `ssl.keymanager.algorithm` does in
/// `chooseClientAlias`: the first key entry whose key type the broker allows
/// and, when the broker names certificate authorities, whose chain has a
/// certificate issued by one of them. With no match the client sends no
/// certificate.
#[derive(Debug)]
struct IssuerResolver {
    identities: Vec<ClientIdentity>,
}

impl rustls::client::ResolvesClientCert for IssuerResolver {
    fn resolve(
        &self,
        root_hint_subjects: &[&[u8]],
        sigschemes: &[SignatureScheme],
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        self.identities
            .iter()
            .find(|identity| {
                identity.key.key.choose_scheme(sigschemes).is_some()
                    && (root_hint_subjects.is_empty()
                        || identity
                            .issuers
                            .iter()
                            .any(|issuer| root_hint_subjects.contains(&issuer.as_slice())))
            })
            .map(|identity| Arc::clone(&identity.key))
    }

    fn has_certs(&self) -> bool {
        !self.identities.is_empty()
    }
}

/// A server certificate verifier that checks the chain and skips the host
/// name, as Kafka's empty `ssl.endpoint.identification.algorithm` does.
#[derive(Debug)]
struct ChainOnlyVerifier {
    inner: Arc<WebPkiServerVerifier>,
}

impl ServerCertVerifier for ChainOnlyVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        // rustls-webpki checks the chain before the name, so a name error
        // means that the chain is valid.
        match self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ) {
            Err(rustls::Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            result => result,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

/// Full client security policy: the listener protocol to speak, plus the TLS
/// and SASL material it implies.
///
/// The `None` fields must match `protocol`. A `SaslSsl` policy needs both
/// `tls` and `sasl`.
#[derive(Debug, Clone)]
pub struct ClientSecurity {
    pub protocol: ListenerProtocol,
    pub tls: Option<TlsConnectorConfig>,
    pub sasl: Option<SaslCredentials>,
    /// Canonical hostname for the SASL handshake. This is the GSSAPI service
    /// principal host (`service_name/<sasl_host>`). It is meaningful whenever
    /// the protocol [`requires_sasl`], independent of TLS: a
    /// `SASL_PLAINTEXT` listener has no `tls` to source the host from, so
    /// without this GSSAPI would fall back to `localhost` and Kerberos
    /// would reject the principal. `None` falls back to a non-empty
    /// `tls.server_name`, then the connection's target host. PLAIN/SCRAM
    /// ignore it.
    ///
    /// [`requires_sasl`]: ListenerProtocol::requires_sasl
    pub sasl_host: Option<String>,
}

impl ClientSecurity {
    /// Clone this policy and fill its TLS server name from a connection target.
    #[must_use]
    pub fn for_target_host(&self, target_host: &str) -> Self {
        let mut security = self.clone();
        if let Some(tls) = &mut security.tls
            && tls.server_name.is_empty()
        {
            target_host.clone_into(&mut tls.server_name);
        }
        security
    }

    /// Resolve the hostname handed to the SASL handshake, the GSSAPI SPN host.
    ///
    /// This method prefers the explicit [`Self::sasl_host`], then the TLS SNI
    /// ([`TlsConnectorConfig::server_name`]), then the connection's target
    /// `host` if known. If it knows none of them, it returns `"localhost"`.
    #[must_use]
    pub fn sasl_handshake_host<'a>(&'a self, target_host: Option<&'a str>) -> &'a str {
        if let Some(h) = self.sasl_host.as_deref() {
            h
        } else if let Some(server_name) = self
            .tls
            .as_ref()
            .map(|tls| tls.server_name.as_str())
            .filter(|name| !name.is_empty())
        {
            server_name
        } else if let Some(h) = target_host {
            h
        } else {
            "localhost"
        }
    }
}

#[cfg(test)]
mod tests;
