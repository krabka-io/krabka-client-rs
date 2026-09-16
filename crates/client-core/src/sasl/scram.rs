//! The SCRAM client of Kafka's `ScramSaslClient` (RFC 5802), with the
//! `tokenauth` extension of delegation token login (KIP-48).

use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use hmac::{Hmac, KeyInit as _, Mac as _};
use krabka_security::SaslMechanism;
use ring::rand::{SecureRandom as _, SystemRandom};
use sha2::{Digest as _, Sha256, Sha512};

/// Kafka's `ScramMechanism.minIterations` for SCRAM-SHA-256 and SCRAM-SHA-512.
const MIN_ITERATIONS: u32 = 4096;

/// Kafka's `ScramLoginModule.TOKEN_AUTH_CONFIG`.
pub(crate) const TOKEN_AUTH: &str = "tokenauth";

/// The GS2 header of a client with no channel binding and no authorization
/// id. `c=biws` in the final message is its base64 form.
const GS2_HEADER: &str = "n,,";

/// A SCRAM exchange before the server-first message.
pub(crate) struct ClientFirst {
    mechanism: SaslMechanism,
    password: Vec<u8>,
    nonce: String,
    bare: String,
}

/// A SCRAM exchange before the server-final message.
pub(crate) struct ClientFinal {
    mechanism: SaslMechanism,
    server_key: Vec<u8>,
    auth_message: String,
}

/// Kafka's `ScramFormatter.saslName`: `=` becomes `=3D` and `,` becomes `=2C`.
fn sasl_name(username: &str) -> String {
    username.replace('=', "=3D").replace(',', "=2C")
}

/// A random nonce of printable characters without a comma.
fn nonce() -> Result<String, String> {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut bytes = [0_u8; 26];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| "no random bytes for the SCRAM nonce".to_owned())?;
    Ok(bytes
        .iter()
        .map(|byte| char::from(ALPHABET[usize::from(*byte) % ALPHABET.len()]))
        .collect())
}

/// Start an exchange: the client-first message, as Kafka's
/// `ScramMessages.ClientFirstMessage` writes it. A delegation token login adds
/// the `tokenauth=true` extension.
pub(crate) fn client_first(
    mechanism: SaslMechanism,
    username: &str,
    password: &str,
    delegation_token: bool,
) -> Result<(Vec<u8>, ClientFirst), String> {
    let nonce = nonce()?;
    let extensions = if delegation_token {
        format!(",{TOKEN_AUTH}=true")
    } else {
        String::new()
    };
    let bare = format!("n={},r={nonce}{extensions}", sasl_name(username));
    let message = format!("{GS2_HEADER}{bare}").into_bytes();
    Ok((
        message,
        ClientFirst {
            mechanism,
            password: password.as_bytes().to_vec(),
            nonce,
            bare,
        },
    ))
}

impl ClientFirst {
    /// Read the server-first message and return the client-final message.
    pub(crate) fn step(self, server_first: &[u8]) -> Result<(Vec<u8>, ClientFinal), String> {
        let server_first = std::str::from_utf8(server_first)
            .map_err(|_| "the SCRAM server-first message is not UTF-8".to_owned())?;
        let mut nonce = None;
        let mut salt = None;
        let mut iterations = None;
        for attribute in server_first.split(',') {
            if let Some(value) = attribute.strip_prefix("r=") {
                nonce = Some(value);
            } else if let Some(value) = attribute.strip_prefix("s=") {
                salt = Some(
                    B64.decode(value)
                        .map_err(|_| "invalid SCRAM salt".to_owned())?,
                );
            } else if let Some(value) = attribute.strip_prefix("i=") {
                iterations = Some(
                    value
                        .parse::<u32>()
                        .map_err(|_| "invalid SCRAM iteration count".to_owned())?,
                );
            }
        }
        let (Some(nonce), Some(salt), Some(iterations)) = (nonce, salt, iterations) else {
            return Err(format!(
                "invalid SCRAM server-first message format: {server_first}"
            ));
        };
        // Kafka's `ScramSaslClient.evaluateChallenge` checks the nonce and the
        // iteration count.
        if !nonce.starts_with(&self.nonce) {
            return Err("invalid SCRAM server nonce: does not start with the client nonce".into());
        }
        if iterations < MIN_ITERATIONS {
            return Err(format!(
                "requested iterations {iterations} is less than the minimum {MIN_ITERATIONS} \
                 for {}",
                self.mechanism.wire_name()
            ));
        }
        let without_proof = format!("c={},r={nonce}", B64.encode(GS2_HEADER));
        let auth_message = format!("{},{server_first},{without_proof}", self.bare);
        let keys = Keys::derive(self.mechanism, &self.password, &salt, iterations)?;
        let client_signature = hmac(self.mechanism, &keys.stored, auth_message.as_bytes())?;
        let proof = keys
            .client
            .iter()
            .zip(&client_signature)
            .map(|(key, signature)| key ^ signature)
            .collect::<Vec<_>>();
        let message = format!("{without_proof},p={}", B64.encode(proof)).into_bytes();
        Ok((
            message,
            ClientFinal {
                mechanism: self.mechanism,
                server_key: keys.server,
                auth_message,
            },
        ))
    }
}

impl ClientFinal {
    /// Check the server-final message: an `e=` error, or the server signature.
    pub(crate) fn verify(self, server_final: &[u8]) -> Result<(), String> {
        let server_final = std::str::from_utf8(server_final)
            .map_err(|_| "the SCRAM server-final message is not UTF-8".to_owned())?;
        if let Some(error) = server_final.strip_prefix("e=") {
            return Err(format!(
                "Sasl authentication using {} failed with error: {error}",
                self.mechanism.wire_name()
            ));
        }
        let signature = server_final
            .strip_prefix("v=")
            .and_then(|value| B64.decode(value).ok())
            .ok_or_else(|| format!("invalid SCRAM server-final message format: {server_final}"))?;
        let expected = hmac(
            self.mechanism,
            &self.server_key,
            self.auth_message.as_bytes(),
        )?;
        if signature != expected {
            return Err("invalid SCRAM server signature in server final message".into());
        }
        Ok(())
    }
}

/// The client key, stored key and server key of RFC 5802.
struct Keys {
    client: Vec<u8>,
    stored: Vec<u8>,
    server: Vec<u8>,
}

impl Keys {
    fn derive(
        mechanism: SaslMechanism,
        password: &[u8],
        salt: &[u8],
        iterations: u32,
    ) -> Result<Self, String> {
        let salted = match mechanism {
            SaslMechanism::ScramSha256 => {
                pbkdf2::pbkdf2_hmac_array::<Sha256, 32>(password, salt, iterations).to_vec()
            }
            SaslMechanism::ScramSha512 => {
                pbkdf2::pbkdf2_hmac_array::<Sha512, 64>(password, salt, iterations).to_vec()
            }
            other => return Err(format!("{} is not a SCRAM mechanism", other.wire_name())),
        };
        let client_key = hmac(mechanism, &salted, b"Client Key")?;
        let stored_key = match mechanism {
            SaslMechanism::ScramSha512 => Sha512::digest(&client_key).to_vec(),
            _ => Sha256::digest(&client_key).to_vec(),
        };
        Ok(Self {
            client: client_key,
            stored: stored_key,
            server: hmac(mechanism, &salted, b"Server Key")?,
        })
    }
}

fn hmac(mechanism: SaslMechanism, key: &[u8], data: &[u8]) -> Result<Vec<u8>, String> {
    let invalid = |_| "invalid SCRAM HMAC key".to_owned();
    Ok(if mechanism == SaslMechanism::ScramSha512 {
        let mut mac = Hmac::<Sha512>::new_from_slice(key).map_err(invalid)?;
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    } else {
        let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(invalid)?;
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    })
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_security::{ScramServerExchange, StepResult, hash_scram_password};

    use super::*;

    /// The client completes an exchange with krabka's SCRAM server for both
    /// mechanisms, with and without the delegation token extension.
    #[test]
    fn exchange_completes_against_a_scram_server() {
        for mechanism in [SaslMechanism::ScramSha256, SaslMechanism::ScramSha512] {
            for delegation_token in [false, true] {
                let credential = hash_scram_password(b"p", mechanism, 4096);
                let server = ScramServerExchange::new("u".into(), credential);
                let (first, client) = client_first(mechanism, "u", "p", delegation_token).unwrap();
                let text = String::from_utf8(first.clone()).unwrap();
                check!(text.starts_with("n,,n=u,r="));
                check!(text.ends_with(",tokenauth=true") == delegation_token);
                let StepResult::Continue(server_first, server) = server.step(&first) else {
                    panic!("server-first");
                };
                let (final_message, client) = client.step(&server_first).unwrap();
                let StepResult::Done(_, server_final) = server.step(&final_message) else {
                    panic!("server-final for {mechanism:?}");
                };
                check!(client.verify(&server_final).is_ok());
            }
        }
    }

    #[test]
    fn sasl_names_escape_equals_and_comma() {
        check!(sasl_name("a=b,c") == "a=3Db=2Cc");
    }

    #[test]
    fn server_messages_that_kafka_rejects_fail() {
        let start = || {
            client_first(SaslMechanism::ScramSha256, "u", "p", false)
                .unwrap()
                .1
        };
        let nonce = start().nonce;
        for (name, server_first, expected) in [
            (
                "foreign nonce",
                "r=other,s=c2FsdA==,i=4096".to_owned(),
                "invalid SCRAM server nonce: does not start with the client nonce".to_owned(),
            ),
            (
                "too few iterations",
                format!("r={nonce}x,s=c2FsdA==,i=1024"),
                "requested iterations 1024 is less than the minimum 4096 for SCRAM-SHA-256"
                    .to_owned(),
            ),
            (
                "no salt and no iteration count",
                format!("r={nonce}x"),
                format!("invalid SCRAM server-first message format: r={nonce}x"),
            ),
            (
                "an iteration count that is not a number",
                format!("r={nonce}x,s=c2FsdA==,i=many"),
                "invalid SCRAM iteration count".to_owned(),
            ),
        ] {
            let client = ClientFirst {
                nonce: nonce.clone(),
                ..start()
            };
            check!(
                client.step(server_first.as_bytes()).err() == Some(expected),
                "{name}"
            );
        }
        let (_, client) = ClientFirst {
            nonce: nonce.clone(),
            ..start()
        }
        .step(format!("r={nonce}x,s=c2FsdA==,i=4096").as_bytes())
        .unwrap();
        check!(
            client.verify(b"e=invalid-proof").err()
                == Some(
                    "Sasl authentication using SCRAM-SHA-256 failed with error: invalid-proof"
                        .to_owned()
                )
        );
        let (_, client) = ClientFirst {
            nonce: nonce.clone(),
            ..start()
        }
        .step(format!("r={nonce}x,s=c2FsdA==,i=4096").as_bytes())
        .unwrap();
        check!(
            client.verify(b"v=c2lnbmF0dXJl").err()
                == Some("invalid SCRAM server signature in server final message".to_owned())
        );
    }
}
