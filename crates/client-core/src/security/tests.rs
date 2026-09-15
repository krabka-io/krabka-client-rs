use std::{path::Path, sync::Arc};

use assert2::{assert, check};
use krabka_security::ListenerProtocol;
use rustls::{
    ServerConfig,
    crypto::ring,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject},
    server::WebPkiClientVerifier,
};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use super::*;

const CA: &str = include_str!("../../tests/fixtures/tls/ca.pem");
const OTHER_CA: &str = include_str!("../../tests/fixtures/tls/other-ca.pem");
const SERVER: &str = include_str!("../../tests/fixtures/tls/server.pem");
const SERVER_KEY: &str = include_str!("../../tests/fixtures/tls/server.key");
const SERVER_WRONG_NAME: &str = include_str!("../../tests/fixtures/tls/server-wrong-name.pem");
const SERVER_WRONG_NAME_KEY: &str = include_str!("../../tests/fixtures/tls/server-wrong-name.key");
const CLIENT: &str = include_str!("../../tests/fixtures/tls/client.pem");
const CLIENT_KEY: &str = include_str!("../../tests/fixtures/tls/client.key");
const CLIENT_ENCRYPTED_KEY: &str = include_str!("../../tests/fixtures/tls/client-encrypted.key");
const CLIENT_KEYSTORE_PEM: &str = include_str!("../../tests/fixtures/tls/client-keystore.pem");
const CLIENT_P12: &[u8] = include_bytes!("../../tests/fixtures/tls/client.p12");
const CLIENT_JKS: &[u8] = include_bytes!("../../tests/fixtures/tls/client.jks");
const CLIENT_LEGACY_P12: &[u8] = include_bytes!("../../tests/fixtures/tls/client-legacy.p12");
const CLIENT_TWO_P12: &[u8] = include_bytes!("../../tests/fixtures/tls/client-two.p12");
const CLIENT_TWO_JKS: &[u8] = include_bytes!("../../tests/fixtures/tls/client-two.jks");
const OTHER_CLIENT: &str = include_str!("../../tests/fixtures/tls/other-client.pem");
const TRUSTSTORE_P12: &[u8] = include_bytes!("../../tests/fixtures/tls/truststore.p12");
const TRUSTSTORE_JKS: &[u8] = include_bytes!("../../tests/fixtures/tls/truststore.jks");

/// The fixture files, written to a temporary directory for the file-based
/// stores.
struct Fixtures {
    dir: tempfile::TempDir,
}

impl Fixtures {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        for (name, bytes) in [
            ("ca.pem", CA.as_bytes()),
            ("client.pem", CLIENT.as_bytes()),
            ("client.key", CLIENT_KEY.as_bytes()),
            ("client-keystore.pem", CLIENT_KEYSTORE_PEM.as_bytes()),
            ("client.p12", CLIENT_P12),
            ("client.jks", CLIENT_JKS),
            ("client-legacy.p12", CLIENT_LEGACY_P12),
            ("client-two.p12", CLIENT_TWO_P12),
            ("client-two.jks", CLIENT_TWO_JKS),
            ("truststore.p12", TRUSTSTORE_P12),
            ("truststore.jks", TRUSTSTORE_JKS),
        ] {
            std::fs::write(dir.path().join(name), bytes).unwrap();
        }
        Self { dir }
    }

    fn path(&self, name: &str) -> std::path::PathBuf {
        self.dir.path().join(name)
    }
}

fn roots(pem: &str) -> Arc<rustls::RootCertStore> {
    let mut roots = rustls::RootCertStore::empty();
    for certificate in CertificateDer::pem_slice_iter(pem.as_bytes()) {
        roots.add(certificate.unwrap()).unwrap();
    }
    Arc::new(roots)
}

/// How the test broker answers the handshake.
#[derive(Clone, Copy, Debug)]
struct Broker {
    certificate: &'static str,
    key: &'static str,
    versions: &'static [&'static SupportedProtocolVersion],
    require_client_certificate: bool,
}

const TLS12_ONLY: &[&SupportedProtocolVersion] = &[&rustls::version::TLS12];
const TLS13_ONLY: &[&SupportedProtocolVersion] = &[&rustls::version::TLS13];

const BROKER: Broker = Broker {
    certificate: SERVER,
    key: SERVER_KEY,
    versions: &[&rustls::version::TLS12, &rustls::version::TLS13],
    require_client_certificate: false,
};

/// The result of a handshake, as the test sees it.
#[derive(Debug, PartialEq, Eq)]
enum Handshake {
    Done {
        version: TlsVersion,
        cipher_suite: String,
        client_certificate: bool,
    },
    UnknownIssuer,
    NameMismatch,
    NoCommonProtocol,
    Failed(String),
}

fn done(version: TlsVersion) -> Handshake {
    Handshake::Done {
        version,
        cipher_suite: match version {
            TlsVersion::Tls12 => "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384".to_owned(),
            TlsVersion::Tls13 => "TLS13_AES_256_GCM_SHA384".to_owned(),
        },
        client_certificate: false,
    }
}

fn mutual() -> Handshake {
    Handshake::Done {
        version: TlsVersion::Tls13,
        cipher_suite: "TLS13_AES_256_GCM_SHA384".to_owned(),
        client_certificate: true,
    }
}

fn classify(error: &std::io::Error) -> Handshake {
    match error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<rustls::Error>())
    {
        Some(rustls::Error::InvalidCertificate(CertificateError::UnknownIssuer)) => {
            Handshake::UnknownIssuer
        }
        Some(rustls::Error::InvalidCertificate(
            CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
        )) => Handshake::NameMismatch,
        Some(
            rustls::Error::AlertReceived(rustls::AlertDescription::ProtocolVersion)
            | rustls::Error::PeerIncompatible(_),
        ) => Handshake::NoCommonProtocol,
        _ => Handshake::Failed(error.to_string()),
    }
}

/// Run one handshake over an in-process duplex stream. The client names
/// `localhost`.
async fn handshake(client: Arc<rustls::ClientConfig>, broker: Broker) -> Handshake {
    let provider = Arc::new(ring::default_provider());
    let builder = ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(broker.versions)
        .unwrap();
    let builder = if broker.require_client_certificate {
        builder.with_client_cert_verifier(
            WebPkiClientVerifier::builder_with_provider(roots(CA), provider)
                .build()
                .unwrap(),
        )
    } else {
        builder.with_no_client_auth()
    };
    let server = builder
        .with_single_cert(
            CertificateDer::pem_slice_iter(broker.certificate.as_bytes())
                .map(Result::unwrap)
                .collect(),
            PrivateKeyDer::from_pem_slice(broker.key.as_bytes()).unwrap(),
        )
        .unwrap();

    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let accept = tokio::spawn(async move {
        let stream = TlsAcceptor::from(Arc::new(server))
            .accept(server_io)
            .await?;
        Ok::<_, std::io::Error>(stream.get_ref().1.peer_certificates().is_some())
    });
    let connected = TlsConnector::from(client)
        .connect(ServerName::try_from("localhost").unwrap(), client_io)
        .await;
    let accepted = accept.await.unwrap();
    match connected {
        Ok(stream) => {
            let (_, connection) = stream.get_ref();
            let version = match connection.protocol_version() {
                Some(rustls::ProtocolVersion::TLSv1_2) => TlsVersion::Tls12,
                _ => TlsVersion::Tls13,
            };
            let cipher_suite = connection
                .negotiated_cipher_suite()
                .and_then(|suite| suite.suite().as_str())
                .unwrap_or_default()
                .to_owned();
            match accepted {
                Ok(client_certificate) => Handshake::Done {
                    version,
                    cipher_suite,
                    client_certificate,
                },
                Err(error) => Handshake::Failed(format!("server: {error}")),
            }
        }
        Err(error) => classify(&error),
    }
}

fn with_platform(
    config: &TlsConnectorConfig,
    platform: &str,
) -> Result<Arc<rustls::ClientConfig>, TlsConfigError> {
    let platform = roots(platform);
    config.build_with_platform_roots(|| Ok(platform))
}

type HandshakeCase = (
    &'static str,
    TlsConnectorConfig,
    &'static str,
    Broker,
    Handshake,
);

async fn run_handshakes(cases: Vec<HandshakeCase>) {
    for (name, config, platform, broker, expected) in cases {
        let client = with_platform(&config, platform).unwrap();
        check!(handshake(client, broker).await == expected, "{name}");
    }
}

#[tokio::test]
async fn trust_stores_verify_the_broker_as_kafka_does() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let fixtures = Fixtures::new();
    let pem_trust = || TrustStore::PemFile(fixtures.path("ca.pem"));
    let config = |trust_store, key_store| TlsConnectorConfig {
        trust_store,
        key_store,
        ..TlsConnectorConfig::default()
    };
    run_handshakes(vec![
        (
            "no trust store uses the platform store",
            TlsConnectorConfig::default(),
            CA,
            BROKER,
            done(TlsVersion::Tls13),
        ),
        (
            "platform store without the broker CA",
            TlsConnectorConfig::default(),
            OTHER_CA,
            BROKER,
            Handshake::UnknownIssuer,
        ),
        (
            "PEM trust store file",
            config(pem_trust(), None),
            OTHER_CA,
            BROKER,
            done(TlsVersion::Tls13),
        ),
        (
            "inline PEM trust store",
            config(TrustStore::Pem(CA.to_owned()), None),
            OTHER_CA,
            BROKER,
            done(TlsVersion::Tls13),
        ),
        (
            "PKCS12 trust store",
            config(
                TrustStore::Pkcs12 {
                    path: fixtures.path("truststore.p12"),
                    password: "trust-secret".into(),
                },
                None,
            ),
            OTHER_CA,
            BROKER,
            done(TlsVersion::Tls13),
        ),
        (
            "JKS trust store",
            config(
                TrustStore::Jks {
                    path: fixtures.path("truststore.jks"),
                    password: Some("trust-secret".into()),
                },
                None,
            ),
            OTHER_CA,
            BROKER,
            done(TlsVersion::Tls13),
        ),
        (
            "JKS trust store without a password",
            config(
                TrustStore::Jks {
                    path: fixtures.path("truststore.jks"),
                    password: None,
                },
                None,
            ),
            OTHER_CA,
            BROKER,
            done(TlsVersion::Tls13),
        ),
        (
            "certificate name does not match, verification on",
            config(pem_trust(), None),
            OTHER_CA,
            Broker {
                certificate: SERVER_WRONG_NAME,
                key: SERVER_WRONG_NAME_KEY,
                ..BROKER
            },
            Handshake::NameMismatch,
        ),
        (
            "certificate name does not match, verification off",
            TlsConnectorConfig {
                hostname_verification: false,
                ..config(pem_trust(), None)
            },
            OTHER_CA,
            Broker {
                certificate: SERVER_WRONG_NAME,
                key: SERVER_WRONG_NAME_KEY,
                ..BROKER
            },
            done(TlsVersion::Tls13),
        ),
        (
            "verification off still checks the chain",
            TlsConnectorConfig {
                hostname_verification: false,
                ..config(TrustStore::Pem(OTHER_CA.to_owned()), None)
            },
            OTHER_CA,
            BROKER,
            Handshake::UnknownIssuer,
        ),
    ])
    .await;
}

#[tokio::test]
async fn key_stores_give_mutual_tls_as_kafka_does() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let fixtures = Fixtures::new();
    let pem_trust = || TrustStore::PemFile(fixtures.path("ca.pem"));
    let config = |trust_store, key_store| TlsConnectorConfig {
        trust_store,
        key_store,
        ..TlsConnectorConfig::default()
    };
    let mutual_broker = Broker {
        require_client_certificate: true,
        ..BROKER
    };
    run_handshakes(vec![
        (
            "mutual TLS with separate PEM files",
            config(
                pem_trust(),
                Some(KeyStore::PemFiles {
                    certificate_chain: fixtures.path("client.pem"),
                    private_key: fixtures.path("client.key"),
                    key_password: None,
                }),
            ),
            OTHER_CA,
            mutual_broker,
            mutual(),
        ),
        (
            "mutual TLS with one PEM key store file and an encrypted key",
            config(
                pem_trust(),
                Some(KeyStore::PemFile {
                    path: fixtures.path("client-keystore.pem"),
                    key_password: Some("key-secret".into()),
                }),
            ),
            OTHER_CA,
            mutual_broker,
            mutual(),
        ),
        (
            "mutual TLS with inline PEM and an encrypted key",
            config(
                pem_trust(),
                Some(KeyStore::Pem {
                    certificate_chain: CLIENT.to_owned(),
                    private_key: CLIENT_ENCRYPTED_KEY.to_owned(),
                    key_password: Some("key-secret".into()),
                }),
            ),
            OTHER_CA,
            mutual_broker,
            mutual(),
        ),
        (
            "mutual TLS with a PKCS12 key store",
            config(
                pem_trust(),
                Some(KeyStore::Pkcs12 {
                    path: fixtures.path("client.p12"),
                    password: "store-secret".into(),
                }),
            ),
            OTHER_CA,
            mutual_broker,
            mutual(),
        ),
        (
            "mutual TLS with a JKS key store and a key password",
            config(
                pem_trust(),
                Some(KeyStore::Jks {
                    path: fixtures.path("client.jks"),
                    password: "store-secret".into(),
                    key_password: Some("key-secret".into()),
                }),
            ),
            OTHER_CA,
            mutual_broker,
            mutual(),
        ),
        (
            "mutual TLS with a legacy PBES1 PKCS12 key store",
            config(
                pem_trust(),
                Some(KeyStore::Pkcs12 {
                    path: fixtures.path("client-legacy.p12"),
                    password: "store-secret".into(),
                }),
            ),
            OTHER_CA,
            mutual_broker,
            mutual(),
        ),
        (
            "PKCS12 key store picks the identity that the broker CA issued",
            config(
                pem_trust(),
                Some(KeyStore::Pkcs12 {
                    path: fixtures.path("client-two.p12"),
                    password: "store-secret".into(),
                }),
            ),
            OTHER_CA,
            mutual_broker,
            mutual(),
        ),
        (
            "JKS key store picks the identity that the broker CA issued",
            config(
                pem_trust(),
                Some(KeyStore::Jks {
                    path: fixtures.path("client-two.jks"),
                    password: "store-secret".into(),
                    key_password: None,
                }),
            ),
            OTHER_CA,
            mutual_broker,
            mutual(),
        ),
    ])
    .await;
}

#[tokio::test]
async fn protocols_and_cipher_suites_follow_kafka_settings() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let fixtures = Fixtures::new();
    let pem_trust = || TrustStore::PemFile(fixtures.path("ca.pem"));
    let config = |trust_store, key_store| TlsConnectorConfig {
        trust_store,
        key_store,
        ..TlsConnectorConfig::default()
    };
    run_handshakes(vec![
        (
            "enabled protocols TLSv1.3, broker offers only TLSv1.2",
            TlsConnectorConfig {
                enabled_protocols: vec![TlsVersion::Tls13],
                ..config(pem_trust(), None)
            },
            OTHER_CA,
            Broker {
                versions: TLS12_ONLY,
                ..BROKER
            },
            Handshake::NoCommonProtocol,
        ),
        (
            "default protocols fall back to TLSv1.2",
            config(pem_trust(), None),
            OTHER_CA,
            Broker {
                versions: TLS12_ONLY,
                ..BROKER
            },
            done(TlsVersion::Tls12),
        ),
        (
            "protocol TLSv1.2 does not use TLSv1.3",
            TlsConnectorConfig {
                protocol: TlsVersion::Tls12,
                ..config(pem_trust(), None)
            },
            OTHER_CA,
            Broker {
                versions: TLS13_ONLY,
                ..BROKER
            },
            Handshake::NoCommonProtocol,
        ),
        (
            "cipher suites by IANA name",
            TlsConnectorConfig {
                cipher_suites: Some(vec![
                    "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA".to_owned(),
                    "TLS_AES_128_GCM_SHA256".to_owned(),
                ]),
                ..config(pem_trust(), None)
            },
            OTHER_CA,
            BROKER,
            Handshake::Done {
                version: TlsVersion::Tls13,
                cipher_suite: "TLS13_AES_128_GCM_SHA256".to_owned(),
                client_certificate: false,
            },
        ),
    ])
    .await;
}

#[test]
fn invalid_store_and_protocol_settings_fail_to_build() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let fixtures = Fixtures::new();
    let trust = TrustStore::Pem(CA.to_owned());
    let key_store = |key_store| TlsConnectorConfig {
        trust_store: trust.clone(),
        key_store: Some(key_store),
        ..TlsConnectorConfig::default()
    };
    let cases = [
        (
            "PKCS12 with a wrong password",
            key_store(KeyStore::Pkcs12 {
                path: fixtures.path("client.p12"),
                password: "wrong".into(),
            }),
            "invalid PKCS#12 store: ",
        ),
        (
            "JKS with a wrong store password",
            key_store(KeyStore::Jks {
                path: fixtures.path("client.jks"),
                password: "wrong".into(),
                key_password: Some("key-secret".into()),
            }),
            "invalid JKS store: keystore password was incorrect (the store digest does not match)",
        ),
        (
            "JKS with a wrong key password",
            key_store(KeyStore::Jks {
                path: fixtures.path("client.jks"),
                password: "store-secret".into(),
                key_password: None,
            }),
            "invalid JKS store: cannot recover key (the key password is incorrect)",
        ),
        (
            "encrypted PEM key without a key password",
            key_store(KeyStore::Pem {
                certificate_chain: CLIENT.to_owned(),
                private_key: CLIENT_ENCRYPTED_KEY.to_owned(),
                key_password: None,
            }),
            "invalid private key: the private key is encrypted, but no key password is set",
        ),
        (
            "encrypted PEM key with a wrong key password",
            key_store(KeyStore::Pem {
                certificate_chain: CLIENT.to_owned(),
                private_key: CLIENT_ENCRYPTED_KEY.to_owned(),
                key_password: Some("wrong".into()),
            }),
            "invalid private key: decryption failed: ",
        ),
        (
            "PEM key store without a certificate",
            key_store(KeyStore::Pem {
                certificate_chain: String::new(),
                private_key: CLIENT_KEY.to_owned(),
                key_password: None,
            }),
            "invalid PEM: at least one certificate expected, but none found",
        ),
        (
            "PKCS12 trust store read as a JKS store",
            TlsConnectorConfig {
                trust_store: TrustStore::Jks {
                    path: fixtures.path("truststore.p12"),
                    password: None,
                },
                ..TlsConnectorConfig::default()
            },
            "invalid JKS store: not a JKS store (a JCEKS or PKCS#12 store needs its own type)",
        ),
        (
            "missing trust store file",
            TlsConnectorConfig {
                trust_store: TrustStore::PemFile(fixtures.path("missing.pem")),
                ..TlsConnectorConfig::default()
            },
            "cannot read ",
        ),
        (
            "no enabled protocol at or below the protocol",
            TlsConnectorConfig {
                trust_store: trust.clone(),
                protocol: TlsVersion::Tls12,
                enabled_protocols: vec![TlsVersion::Tls13],
                ..TlsConnectorConfig::default()
            },
            "no enabled TLS protocol is at or below Tls12",
        ),
        (
            "unknown cipher suite name",
            TlsConnectorConfig {
                trust_store: trust.clone(),
                cipher_suites: Some(vec!["TLS_NOT_A_SUITE".to_owned()]),
                ..TlsConnectorConfig::default()
            },
            "unknown cipher suite \"TLS_NOT_A_SUITE\"",
        ),
        (
            "only cipher suites that the client does not implement",
            TlsConnectorConfig {
                trust_store: trust.clone(),
                cipher_suites: Some(vec!["TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA".to_owned()]),
                ..TlsConnectorConfig::default()
            },
            "no configured cipher suite is supported: [\"TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA\"]",
        ),
    ];
    for (name, config, expected_prefix) in cases {
        let error = config.build().expect_err(name).to_string();
        check!(error.starts_with(expected_prefix), "{name}: {error}");
    }
}

#[test]
fn key_stores_of_every_format_load_every_key_entry_in_order() {
    let fixtures = Fixtures::new();
    let certificate = |pem: &str| CertificateDer::from_pem_slice(pem.as_bytes()).unwrap();
    let key = || PrivateKeyDer::from_pem_slice(CLIENT_KEY.as_bytes()).unwrap();
    let chain = || vec![certificate(CLIENT), certificate(CA)];
    let leaves = |pairs: Vec<stores::KeyPair>| {
        pairs
            .into_iter()
            .map(|(chain, _)| chain[0].clone())
            .collect::<Vec<_>>()
    };
    let cases = [
        (
            "PEM files",
            KeyStore::PemFiles {
                certificate_chain: fixtures.path("client.pem"),
                private_key: fixtures.path("client.key"),
                key_password: None,
            },
            vec![(vec![certificate(CLIENT)], key())],
        ),
        (
            "PEM key store file",
            KeyStore::PemFile {
                path: fixtures.path("client-keystore.pem"),
                key_password: Some("key-secret".into()),
            },
            vec![(chain(), key())],
        ),
        (
            "PKCS12",
            KeyStore::Pkcs12 {
                path: fixtures.path("client.p12"),
                password: "store-secret".into(),
            },
            vec![(chain(), key())],
        ),
        (
            "legacy PKCS12",
            KeyStore::Pkcs12 {
                path: fixtures.path("client-legacy.p12"),
                password: "store-secret".into(),
            },
            vec![(chain(), key())],
        ),
        (
            "JKS",
            KeyStore::Jks {
                path: fixtures.path("client.jks"),
                password: "store-secret".into(),
                key_password: Some("key-secret".into()),
            },
            vec![(chain(), key())],
        ),
    ];
    for (name, store, expected) in cases {
        check!(stores::key_pairs(&store).unwrap() == expected, "{name}");
    }

    // The two-entry stores hold the untrusted identity first, so a client
    // that takes the first entry sends the wrong certificate.
    for store in [
        KeyStore::Pkcs12 {
            path: fixtures.path("client-two.p12"),
            password: "store-secret".into(),
        },
        KeyStore::Jks {
            path: fixtures.path("client-two.jks"),
            password: "store-secret".into(),
            key_password: None,
        },
    ] {
        check!(
            leaves(stores::key_pairs(&store).unwrap())
                == vec![certificate(OTHER_CLIENT), certificate(CLIENT)],
            "{store:?}"
        );
    }
}

#[tokio::test]
async fn connector_reuses_the_configuration_until_a_setting_changes() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let fixtures = Fixtures::new();
    let config = TlsConnectorConfig {
        trust_store: TrustStore::PemFile(fixtures.path("ca.pem")),
        key_store: Some(KeyStore::Pkcs12 {
            path: fixtures.path("client.p12"),
            password: "store-secret".into(),
        }),
        ..TlsConnectorConfig::default()
    };
    config.connector().expect("the stores load");
    std::fs::remove_file(fixtures.path("ca.pem")).unwrap();
    std::fs::remove_file(fixtures.path("client.p12")).unwrap();

    let broker_clone = ClientSecurity {
        protocol: ListenerProtocol::Ssl,
        tls: Some(config.clone()),
        sasl: None,
        sasl_host: None,
    }
    .for_target_host("localhost");
    let changed = TlsConnectorConfig {
        protocol: TlsVersion::Tls12,
        ..config.clone()
    };
    let outcomes = (
        config.connector().is_ok(),
        broker_clone.tls.unwrap().connector().is_ok(),
        changed
            .connector()
            .map(|_| ())
            .map_err(|error| error.to_string()),
    );
    check!(outcomes.0);
    check!(outcomes.1);
    check!(
        outcomes
            .2
            .is_err_and(|error| error.starts_with("cannot read "))
    );
}

#[test]
fn protocol_names_parse_as_kafka_names_them() {
    for (name, expected) in [
        ("TLSv1.2", Ok(TlsVersion::Tls12)),
        ("TLSv1.3", Ok(TlsVersion::Tls13)),
        (
            "TLSv1.1",
            Err("unsupported TLS protocol \"TLSv1.1\"".to_owned()),
        ),
        ("TLS", Err("unsupported TLS protocol \"TLS\"".to_owned())),
    ] {
        check!(TlsVersion::parse(name).map_err(|error| error.to_string()) == expected);
    }
}

#[test]
fn kafka_default_tls_settings() {
    assert!(
        TlsConnectorConfig::default()
            == TlsConnectorConfig {
                trust_store: TrustStore::Platform,
                key_store: None,
                server_name: String::new(),
                hostname_verification: true,
                protocol: TlsVersion::Tls13,
                enabled_protocols: vec![TlsVersion::Tls12, TlsVersion::Tls13],
                cipher_suites: None,
                built: BuiltConfig::default(),
            }
    );
}

#[test]
fn passwords_do_not_appear_in_debug_output() {
    let store = KeyStore::Pkcs12 {
        path: Path::new("client.p12").to_owned(),
        password: "store-secret".into(),
    };
    assert!(format!("{store:?}") == "Pkcs12 { path: \"client.p12\", password: [hidden] }");
}

#[test]
fn plaintext_security_has_no_tls_or_sasl() {
    let s = ClientSecurity {
        protocol: ListenerProtocol::Plaintext,
        tls: None,
        sasl: None,
        sasl_host: None,
    };
    assert!(!s.protocol.requires_tls());
    assert!(!s.protocol.requires_sasl());
}

#[test]
fn sasl_plaintext_carries_creds() {
    let s = ClientSecurity {
        protocol: ListenerProtocol::SaslPlaintext,
        tls: None,
        sasl: Some(SaslCredentials::Plain {
            username: "u".into(),
            password: "p".into(),
        }),
        sasl_host: None,
    };
    assert!(s.protocol.requires_sasl());
    assert!(matches!(s.sasl, Some(SaslCredentials::Plain { .. })));
}

#[test]
fn sasl_handshake_host_prefers_explicit_field() {
    // SASL_PLAINTEXT (no TLS) with an explicit host: GSSAPI must get
    // the real SPN host, not "localhost" or the target host.
    let s = ClientSecurity {
        protocol: ListenerProtocol::SaslPlaintext,
        tls: None,
        sasl: None,
        sasl_host: Some("kdc-broker.example.com".into()),
    };
    assert!(s.sasl_handshake_host(Some("10.0.0.5")) == "kdc-broker.example.com");
}

#[test]
fn sasl_handshake_host_falls_back_to_tls_then_target_then_localhost() {
    // No explicit sasl_host → TLS SNI wins.
    let with_tls = ClientSecurity {
        protocol: ListenerProtocol::SaslSsl,
        tls: Some(TlsConnectorConfig {
            server_name: "tls-host".into(),
            ..TlsConnectorConfig::default()
        }),
        sasl: None,
        sasl_host: None,
    };
    assert!(with_tls.sasl_handshake_host(Some("10.0.0.5")) == "tls-host");

    // No sasl_host, no TLS → target host wins.
    let no_tls = ClientSecurity {
        protocol: ListenerProtocol::SaslPlaintext,
        tls: None,
        sasl: None,
        sasl_host: None,
    };
    assert!(no_tls.sasl_handshake_host(Some("10.0.0.5")) == "10.0.0.5");

    // Nothing set at all → localhost.
    assert!(no_tls.sasl_handshake_host(None) == "localhost");
}

#[test]
fn target_host_fills_dynamic_tls_name_but_preserves_an_override() {
    let policy = |server_name| ClientSecurity {
        protocol: ListenerProtocol::Ssl,
        tls: Some(TlsConnectorConfig {
            server_name,
            ..TlsConnectorConfig::default()
        }),
        sasl: None,
        sasl_host: None,
    };

    assert!(
        policy(String::new())
            .for_target_host("broker-2.example")
            .tls
            .unwrap()
            .server_name
            == "broker-2.example"
    );
    assert!(
        policy("shared.example".into())
            .for_target_host("broker-2.example")
            .tls
            .unwrap()
            .server_name
            == "shared.example"
    );
}
