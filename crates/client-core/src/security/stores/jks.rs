//! Java `JKS` key store reading, as `sun.security.provider.JavaKeyStore`
//! and `sun.security.provider.KeyProtector` define the format.
//!
//! Kafka's default `ssl.keystore.type` and `ssl.truststore.type` is `JKS`.

use pkcs8::{
    AlgorithmIdentifierRef, ObjectIdentifier,
    der::{Decode, Reader, SliceReader, asn1::OctetStringRef},
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use sha1::{Digest, Sha1};

use super::{KeyPair, Password, TlsConfigError};

/// `JavaKeyStore.MAGIC`.
const MAGIC: u32 = 0xFEED_FEED;
/// `JavaKeyStore` entry tag of a private key with its certificate chain.
const PRIVATE_KEY_ENTRY: u32 = 1;
/// `JavaKeyStore` entry tag of a trusted certificate.
const TRUSTED_CERTIFICATE_ENTRY: u32 = 2;
/// The text that `JavaKeyStore.getPreKeyedHash` adds to the digest.
const DIGEST_WHITENER: &[u8] = b"Mighty Aphrodite";
/// `KeyProtector.KEY_PROTECTOR_OID`.
const KEY_PROTECTOR_OID: ObjectIdentifier = ObjectIdentifier::new_unwrap("1.3.6.1.4.1.42.2.17.1.1");
/// The length of a SHA-1 digest.
const DIGEST_LEN: usize = 20;

/// The entries of a JKS store.
#[derive(Debug, Default)]
pub(super) struct JksStore {
    /// The certificates of the trusted certificate entries.
    pub(super) trusted_certificates: Vec<CertificateDer<'static>>,
    /// The protected keys and certificate chains of the private key entries.
    private_keys: Vec<(Vec<u8>, Vec<CertificateDer<'static>>)>,
}

impl JksStore {
    /// Parse a JKS store. With a password, check the store digest.
    ///
    /// # Errors
    /// Returns [`TlsConfigError::Jks`] for data that is not a JKS store, and
    /// for a digest that does not match the password.
    pub(super) fn parse(data: &[u8], password: Option<&Password>) -> Result<Self, TlsConfigError> {
        let mut input = Input { data, offset: 0 };
        if input.u32()? != MAGIC {
            return Err(jks_error(
                "not a JKS store (a JCEKS or PKCS#12 store needs its own type)",
            ));
        }
        let version = input.u32()?;
        if !matches!(version, 1 | 2) {
            return Err(jks_error(&format!("unsupported JKS version {version}")));
        }
        let count = input.u32()?;
        let mut store = Self::default();
        for _ in 0..count {
            let tag = input.u32()?;
            input.utf()?; // alias
            input.bytes(8)?; // creation date
            match tag {
                PRIVATE_KEY_ENTRY => {
                    let protected_key = input.sized()?.to_vec();
                    let chain_len = input.u32()?;
                    let mut chain = Vec::new();
                    for _ in 0..chain_len {
                        chain.push(input.certificate(version)?);
                    }
                    store.private_keys.push((protected_key, chain));
                }
                TRUSTED_CERTIFICATE_ENTRY => {
                    let certificate = input.certificate(version)?;
                    store.trusted_certificates.push(certificate);
                }
                other => return Err(jks_error(&format!("unknown JKS entry tag {other}"))),
            }
        }
        let signed_len = input.offset;
        let digest = input.bytes(DIGEST_LEN)?;
        if let Some(password) = password {
            let mut hash = Sha1::new();
            hash.update(password_bytes(password));
            hash.update(DIGEST_WHITENER);
            hash.update(&data[..signed_len]);
            if hash.finalize().as_slice() != digest {
                return Err(jks_error(
                    "keystore password was incorrect (the store digest does not match)",
                ));
            }
        }
        Ok(store)
    }

    /// Recover every private key entry with `key_password`, in store order.
    ///
    /// # Errors
    /// Returns [`TlsConfigError::Jks`] when the store holds no private key
    /// entry, or when the key password does not recover a key.
    pub(super) fn key_pairs(
        &self,
        key_password: &Password,
    ) -> Result<Vec<KeyPair>, TlsConfigError> {
        if self.private_keys.is_empty() {
            return Err(jks_error("the store holds no private key entry"));
        }
        self.private_keys
            .iter()
            .map(|(protected_key, chain)| {
                let key = recover_key(protected_key, key_password)?;
                Ok((
                    chain.clone(),
                    PrivateKeyDer::from(PrivatePkcs8KeyDer::from(key)),
                ))
            })
            .collect()
    }
}

fn jks_error(message: &str) -> TlsConfigError {
    TlsConfigError::Jks(message.to_owned())
}

/// A password as Java's `char[]`, in UTF-16 big-endian bytes.
fn password_bytes(password: &Password) -> Vec<u8> {
    password
        .value()
        .encode_utf16()
        .flat_map(u16::to_be_bytes)
        .collect()
}

/// Recover a PKCS#8 key from its `EncryptedPrivateKeyInfo`, as
/// `KeyProtector.recover` does.
///
/// The encrypted data is a 20-byte salt, the key XOR a SHA-1 key stream, and
/// a 20-byte SHA-1 check of the password and the key.
fn recover_key(protected_key: &[u8], password: &Password) -> Result<Vec<u8>, TlsConfigError> {
    let invalid = |error: pkcs8::der::Error| jks_error(&format!("invalid protected key: {error}"));
    let mut reader = SliceReader::new(protected_key).map_err(invalid)?;
    let (oid, encrypted) = reader
        .sequence(|body| {
            let algorithm = AlgorithmIdentifierRef::decode(body)?;
            let data = <&OctetStringRef>::decode(body)?;
            Ok::<_, pkcs8::der::Error>((algorithm.oid, data.as_bytes().to_vec()))
        })
        .map_err(invalid)?;
    if oid != KEY_PROTECTOR_OID {
        return Err(jks_error(&format!(
            "unsupported key protection algorithm {oid}"
        )));
    }
    if encrypted.len() < 2 * DIGEST_LEN {
        return Err(jks_error("protected key is too short"));
    }
    let (salt, rest) = encrypted.split_at(DIGEST_LEN);
    let (cipher_text, check) = rest.split_at(rest.len() - DIGEST_LEN);
    let password = password_bytes(password);

    let mut key = Vec::with_capacity(cipher_text.len());
    let mut stream = salt.to_vec();
    for block in cipher_text.chunks(DIGEST_LEN) {
        let mut hash = Sha1::new();
        hash.update(&password);
        hash.update(&stream);
        stream = hash.finalize().to_vec();
        key.extend(block.iter().zip(&stream).map(|(byte, mask)| byte ^ mask));
    }

    let mut hash = Sha1::new();
    hash.update(&password);
    hash.update(&key);
    if hash.finalize().as_slice() != check {
        return Err(jks_error(
            "cannot recover key (the key password is incorrect)",
        ));
    }
    Ok(key)
}

/// A reader of `java.io.DataInputStream` values.
struct Input<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> Input<'a> {
    fn bytes(&mut self, len: usize) -> Result<&'a [u8], TlsConfigError> {
        let end = self
            .offset
            .checked_add(len)
            .filter(|end| *end <= self.data.len())
            .ok_or_else(|| jks_error("truncated JKS store"))?;
        let bytes = &self.data[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }

    fn u32(&mut self) -> Result<u32, TlsConfigError> {
        let bytes = self.bytes(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    /// A `DataInputStream.readUTF` value. The reader skips the text.
    fn utf(&mut self) -> Result<(), TlsConfigError> {
        let bytes = self.bytes(2)?;
        self.bytes(usize::from(u16::from_be_bytes([bytes[0], bytes[1]])))?;
        Ok(())
    }

    /// An `int` length and that many bytes.
    fn sized(&mut self) -> Result<&'a [u8], TlsConfigError> {
        let len = usize::try_from(self.u32()?).map_err(|_| jks_error("JKS length overflow"))?;
        self.bytes(len)
    }

    /// A certificate: the type name (version 2 only) and the encoded bytes.
    fn certificate(&mut self, version: u32) -> Result<CertificateDer<'static>, TlsConfigError> {
        if version == 2 {
            self.utf()?;
        }
        Ok(CertificateDer::from(self.sized()?.to_vec()))
    }
}
