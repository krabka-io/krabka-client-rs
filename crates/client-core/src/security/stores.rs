//! Trust store and key store loading, as Kafka's `DefaultSslEngineFactory`
//! loads `PemStore`, `FileBasedPemStore` and `FileBasedStore`.

mod jks;

use std::path::Path;

use p12_keystore::{KeyStoreEntry, Pkcs12ImportPolicy};
use pkcs8::{EncryptedPrivateKeyInfoRef, der::pem};
use rustls::pki_types::{
    CertificateDer, PrivateKeyDer, PrivatePkcs1KeyDer, PrivatePkcs8KeyDer, PrivateSec1KeyDer,
};

use super::{KeyStore, Password, TlsConfigError, TrustStore};

/// A certificate chain and its private key.
pub(super) type KeyPair = (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>);

/// Load the trust anchors of a file or inline trust store.
///
/// # Errors
/// Returns [`TlsConfigError`] when the store does not load or holds no usable
/// CA certificate.
pub(super) fn trust_anchors(store: &TrustStore) -> Result<rustls::RootCertStore, TlsConfigError> {
    let certificates = match store {
        // The caller loads the platform trust store. It is not a file.
        TrustStore::Platform => Vec::new(),
        TrustStore::PemFile(path) => pem_certificates(&read_text(path)?)?,
        TrustStore::Pem(text) => pem_certificates(text)?,
        TrustStore::Pkcs12 { path, password } => {
            let store = pkcs12(&read(path)?, password)?;
            store
                .entries()
                .filter_map(|(_, entry)| match entry {
                    KeyStoreEntry::Certificate(certificate) => {
                        Some(CertificateDer::from(certificate.as_der().to_vec()))
                    }
                    _ => None,
                })
                .collect()
        }
        TrustStore::Jks { path, password } => {
            jks::JksStore::parse(&read(path)?, password.as_ref())?.trusted_certificates
        }
    };
    let mut roots = rustls::RootCertStore::empty();
    roots.add_parsable_certificates(certificates);
    if roots.is_empty() {
        return Err(TlsConfigError::EmptyTrustStore);
    }
    Ok(roots)
}

/// Load the certificate chain and private key of a key store.
///
/// # Errors
/// Returns [`TlsConfigError`] when the store does not load, holds no key, or
/// the key does not decrypt.
pub(super) fn key_pair(store: &KeyStore) -> Result<KeyPair, TlsConfigError> {
    match store {
        KeyStore::PemFile { path, key_password } => {
            let text = read_text(path)?;
            pem_key_pair(&text, &text, key_password.as_ref())
        }
        KeyStore::PemFiles {
            certificate_chain,
            private_key,
            key_password,
        } => pem_key_pair(
            &read_text(certificate_chain)?,
            &read_text(private_key)?,
            key_password.as_ref(),
        ),
        KeyStore::Pem {
            certificate_chain,
            private_key,
            key_password,
        } => pem_key_pair(certificate_chain, private_key, key_password.as_ref()),
        KeyStore::Pkcs12 { path, password } => {
            let store = pkcs12(&read(path)?, password)?;
            let (_, chain) = store.private_key_chain().ok_or_else(|| {
                TlsConfigError::Pkcs12("the store holds no private key entry".to_owned())
            })?;
            Ok((
                chain
                    .certs()
                    .iter()
                    .map(|certificate| CertificateDer::from(certificate.as_der().to_vec()))
                    .collect(),
                PrivateKeyDer::from(PrivatePkcs8KeyDer::from(chain.key().as_der().to_vec())),
            ))
        }
        KeyStore::Jks {
            path,
            password,
            key_password,
        } => {
            let store = jks::JksStore::parse(&read(path)?, Some(password))?;
            store.key_pair(key_password.as_ref().unwrap_or(password))
        }
    }
}

fn read(path: &Path) -> Result<Vec<u8>, TlsConfigError> {
    std::fs::read(path).map_err(|source| TlsConfigError::Read {
        path: path.to_owned(),
        source,
    })
}

fn read_text(path: &Path) -> Result<String, TlsConfigError> {
    std::fs::read_to_string(path).map_err(|source| TlsConfigError::Read {
        path: path.to_owned(),
        source,
    })
}

fn pkcs12(data: &[u8], password: &Password) -> Result<p12_keystore::KeyStore, TlsConfigError> {
    p12_keystore::KeyStore::from_pkcs12(data, password.value(), Pkcs12ImportPolicy::Strict)
        .map_err(|error| TlsConfigError::Pkcs12(error.to_string()))
}

/// The PEM blocks of `text`, as `(label, DER)` pairs, in file order.
fn pem_blocks(text: &str) -> Result<Vec<(String, Vec<u8>)>, TlsConfigError> {
    const BEGIN: &str = "-----BEGIN ";
    let mut blocks = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find(BEGIN) {
        let block = &rest[start..];
        let label_end = block[BEGIN.len()..]
            .find("-----")
            .ok_or_else(|| TlsConfigError::Pem("unterminated BEGIN line".to_owned()))?;
        let label = &block[BEGIN.len()..BEGIN.len() + label_end];
        let end_line = format!("-----END {label}-----");
        let end = block
            .find(&end_line)
            .ok_or_else(|| TlsConfigError::Pem(format!("no END line for {label}")))?
            + end_line.len();
        let (decoded_label, der) = pem::decode_vec(&block.as_bytes()[..end])
            .map_err(|error| TlsConfigError::Pem(format!("{label}: {error}")))?;
        blocks.push((decoded_label.to_owned(), der));
        rest = &block[end..];
    }
    Ok(blocks)
}

/// The certificates of a PEM value. Kafka's `PemStore.certs` requires at
/// least one.
fn pem_certificates(text: &str) -> Result<Vec<CertificateDer<'static>>, TlsConfigError> {
    let certificates = pem_blocks(text)?
        .into_iter()
        .filter(|(label, _)| label == "CERTIFICATE")
        .map(|(_, der)| CertificateDer::from(der))
        .collect::<Vec<_>>();
    if certificates.is_empty() {
        return Err(TlsConfigError::Pem(
            "at least one certificate expected, but none found".to_owned(),
        ));
    }
    Ok(certificates)
}

/// The certificate chain of `chain` and the one private key of `key`, as
/// Kafka's `PemStore.createKeyStoreFromPem` reads them.
fn pem_key_pair(
    chain: &str,
    key: &str,
    key_password: Option<&Password>,
) -> Result<KeyPair, TlsConfigError> {
    let certificates = pem_certificates(chain)?;
    let keys = pem_blocks(key)?
        .into_iter()
        .filter(|(label, _)| label.ends_with("PRIVATE KEY"))
        .collect::<Vec<_>>();
    let [(label, der)] = keys.as_slice() else {
        return Err(TlsConfigError::PrivateKey(format!(
            "expected one private key, but found {}",
            keys.len()
        )));
    };
    let key = match (label.as_str(), key_password) {
        ("ENCRYPTED PRIVATE KEY", Some(password)) => {
            PrivateKeyDer::from(PrivatePkcs8KeyDer::from(decrypt_pkcs8(der, password)?))
        }
        ("ENCRYPTED PRIVATE KEY", None) => {
            return Err(TlsConfigError::PrivateKey(
                "the private key is encrypted, but no key password is set".to_owned(),
            ));
        }
        (_, Some(_)) => {
            return Err(TlsConfigError::PrivateKey(
                "a key password is set, but the private key is not encrypted".to_owned(),
            ));
        }
        ("PRIVATE KEY", None) => PrivateKeyDer::from(PrivatePkcs8KeyDer::from(der.clone())),
        ("RSA PRIVATE KEY", None) => PrivateKeyDer::from(PrivatePkcs1KeyDer::from(der.clone())),
        ("EC PRIVATE KEY", None) => PrivateKeyDer::from(PrivateSec1KeyDer::from(der.clone())),
        (other, None) => {
            return Err(TlsConfigError::PrivateKey(format!(
                "unsupported private key format {other:?}"
            )));
        }
    };
    Ok((certificates, key))
}

/// Decrypt a PKCS#8 `EncryptedPrivateKeyInfo` with PBES2.
fn decrypt_pkcs8(der: &[u8], password: &Password) -> Result<Vec<u8>, TlsConfigError> {
    let encrypted = EncryptedPrivateKeyInfoRef::try_from(der)
        .map_err(|error| TlsConfigError::PrivateKey(error.to_string()))?;
    let document = encrypted
        .decrypt(password.value())
        .map_err(|error| TlsConfigError::PrivateKey(format!("decryption failed: {error}")))?;
    Ok(document.as_bytes().to_vec())
}
