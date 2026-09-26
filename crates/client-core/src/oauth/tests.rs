use std::{
    io::Write as _,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use assert2::check;
use tokio::{io::AsyncBufReadExt as _, net::TcpListener};

use super::*;

/// An unencrypted PKCS#8 2048-bit RSA test key, for signing test assertions
/// only.
const TEST_RSA_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC0nTwkhQGaBKIU\n\
S+hM7NtxjxCPDPC8hKJ9HeDAPOKJxlkoYazgN6cbbpHjo8pmFfbnrdQZM6gW7VWP\n\
lwHa7ZUZID9R8nIpwRWn3dKPk/+1jkg22R9zE0Nft8skjOUfTAeM+lqehYgWqXHS\n\
NsB3HupRfi+FMpqZ7bJUuRr1tC00PzoIEzA00TIHZsxyDngNp2c7ZAapGPpQ62hY\n\
tKes9KPSX28+uiYyJiq7RN6XbMJ4EYiCWpI05f/zhG69O+0LsQRY1tjA57FrHBoE\n\
etv5Dx3c6v7fxzUweyPHJnkc7NW1ljwIoG3vOxBq+xPEO65Rj7xG1thiCnpYfnqa\n\
7RuoWiN1AgMBAAECggEAGJFqz5eSvYieeMmlMmblqBogfn+cH4ighwm8EL+MxoDx\n\
5R75jREE7M5Vj0mFtpfg8L13JGvKaZP5wivLAiS0gKkfnoiuzs8ySutO67OODPxA\n\
W8ryZDeH6p/tmhiFnNmSuAfgsRTTi4Gbt081+ajS5tLrU+BWTKnwoBBYgnjyd9tc\n\
P1WNFlAaXjLteUPtoV2cab9OyalfjVeLqBt2cqig0RJE/IP3TkURimlVk5lRytqe\n\
bGrN+6XjrPcqSYQQt8UvuI0TTV6V3qA2LkRGasnYVSqXd2a2G6+i7q67D14gaBYb\n\
iJtwBNMv3mAMq3l/MmriVsEjdVXMNOVb+ZeH8D844QKBgQDz+sWtF1ZBl75FZxXL\n\
eCAEasOhsU8tcPzBnukKAs/OjQIQ6+VzSlfdAOkjqjJqSycTxjKhB0KdYS8SBR1E\n\
PE10MD6X2aX+YcQsZaWq7ag4R814cC/00f0lyibYW9NVLryTmAZ8YKC4NW8kxVlS\n\
9mxmZbYM3JXObbNahvevsTxhHwKBgQC9g0H5n5cZrxDf1FTApg2dQygm1dwRS7QK\n\
XRW1Tfh4o7JZYYGnvMvvZh3bBjlY5eDFPv4Pxv06+bVpkJOHnkk8JBeRi9hPy4YL\n\
Ee95+T7VYFiCoFjCQMSi/EL3ayUl/IQ/B2mud50yQhuVOMDQymwtec0bvgLBguio\n\
zLeUVAeE6wKBgFWKvTg9EG8bBwlKZWfbjE5AKKtOgZZLITO5xbdO2RFweyL3spFD\n\
pZ7FLPjmOZrvEppqSWIQK5kGc/x7cpF0Gyv7plaTZxHTsXZnhThy7yIcerwZiZbq\n\
8TkIsan2OBiLtG6DRPLi5jbv9TINR45A/CzCyJul05h2+gVpgPpGyAa9AoGBAK6Y\n\
ML4jU3fsG6W63uIlmcFaz7EHshmVHye1HnzMeq/aUEOcW3EHtPK3p6XTlB3cmznd\n\
kP9EGqSszX+WHPUC1QG9VqFWr1DEdpfYTEKZaFP40VJ3G47LUN2/foqnga//dm8D\n\
C1AbDw3wba0KfkldVFCJOHfolG1nh6WMBU14JP1FAoGADUPDLTYteZE/oPsZRAqX\n\
NVCi6+iC8/5xLEA6rH6LEiZhmKDZRjkhyBhGxv30TXCedZqA54CdFQVWdPIXzoRP\n\
wwSbrMIMNHq/QnlehwK9HT4a3CymV0eXiStEdzvrxYuQNQnWEiobnYOzH5C1dQHo\n\
1Zrgkvmo+7IJs1Xl6h6XTQY=\n\
-----END PRIVATE KEY-----\n";

/// A PEM file holding `pem`.
fn temp_pem_file(pem: &str) -> tempfile::NamedTempFile {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(pem.as_bytes()).unwrap();
    file
}

/// The DER `RSAPublicKey` of the key in `TEST_RSA_KEY_PEM`, to verify a
/// signature made with it.
fn test_rsa_public_key() -> Vec<u8> {
    load_rsa_private_key(temp_pem_file(TEST_RSA_KEY_PEM).path(), None)
        .unwrap()
        .public()
        .as_ref()
        .to_vec()
}

/// A compact JWT with the given `iat` and `exp` claims and no signature.
fn jwt(iat: Option<u64>, exp: u64) -> String {
    let mut claims = serde_json::json!({ "sub": "client", "exp": exp });
    if let Some(iat) = iat {
        claims["iat"] = iat.into();
    }
    format!(
        "{}.{}.sig",
        URL_SAFE_NO_PAD.encode(br#"{"alg":"none"}"#),
        URL_SAFE_NO_PAD.encode(claims.to_string())
    )
}

#[test]
fn endpoints_parse_as_urls() {
    let endpoint = |https, host: &str, port, target: &str| {
        Ok(Endpoint {
            https,
            host: host.to_owned(),
            port,
            target: target.to_owned(),
        })
    };
    for (url, expected) in [
        (
            "https://idp.example/oauth2/token",
            endpoint(true, "idp.example", 443, "/oauth2/token"),
        ),
        (
            "http://127.0.0.1:8080/token?x=1",
            endpoint(false, "127.0.0.1", 8080, "/token?x=1"),
        ),
        ("https://[::1]:8443", endpoint(true, "::1", 8443, "/")),
        ("http://[::1]/token", endpoint(false, "::1", 80, "/token")),
        (
            "https://idp.example?realm=x",
            endpoint(true, "idp.example", 443, "/?realm=x"),
        ),
        (
            "https://idp.example/token?realm=x#frag",
            endpoint(true, "idp.example", 443, "/token?realm=x"),
        ),
        (
            "https://[::1:8443",
            Err(
                "sasl.oauthbearer.token.endpoint.url \"https://[::1:8443\" is not an http or \
                 https URL"
                    .to_owned(),
            ),
        ),
        (
            "https://idp.example:http/token",
            Err(
                "sasl.oauthbearer.token.endpoint.url \"https://idp.example:http/token\" is not \
                 an http or https URL"
                    .to_owned(),
            ),
        ),
        (
            "https:///token",
            Err(
                "sasl.oauthbearer.token.endpoint.url \"https:///token\" is not an http or \
                 https URL"
                    .to_owned(),
            ),
        ),
        (
            "file:///tmp/token",
            Err(
                "sasl.oauthbearer.token.endpoint.url \"file:///tmp/token\" is not an http or \
                 https URL"
                    .to_owned(),
            ),
        ),
    ] {
        check!(Endpoint::parse(url) == expected, "{url}");
    }
}

/// Kafka's `ClientSecretRequestFormatter`: Basic authorization of
/// `client_id:client_secret`, and `grant_type` with the optional scope,
/// URL-encoded only with `sasl.oauthbearer.header.urlencode`.
#[test]
fn token_requests_follow_the_client_secret_formatter() {
    let config = |scope: Option<&str>, url_encode| ClientCredentialsConfig {
        scope: scope.map(ToOwned::to_owned),
        url_encode,
        ..ClientCredentialsConfig::new("http://127.0.0.1:1/token", " my id ", "s&cret")
    };
    for (name, config, authorization, body) in [
        (
            "no scope",
            config(None, false),
            format!("Basic {}", B64.encode("my id:s&cret")),
            "grant_type=client_credentials",
        ),
        (
            "scope",
            config(Some("kafka produce"), false),
            format!("Basic {}", B64.encode("my id:s&cret")),
            "grant_type=client_credentials&scope=kafka produce",
        ),
        (
            "URL-encoded",
            config(Some("kafka produce"), true),
            format!("Basic {}", B64.encode("my+id:s%26cret")),
            "grant_type=client_credentials&scope=kafka+produce",
        ),
    ] {
        let provider = ClientCredentialsTokenProvider::new(config).unwrap();
        let request = provider.request().unwrap();
        check!(
            (request.authorization, request.body) == (Some(authorization), body.to_owned()),
            "{name}"
        );
    }
}

#[test]
fn blank_credentials_are_rejected() {
    for (name, config, expected) in [
        (
            "blank id",
            ClientCredentialsConfig::new("https://idp/token", " ", "secret"),
            "sasl.oauthbearer.client.credentials.client.id is blank",
        ),
        (
            "blank secret",
            ClientCredentialsConfig::new("https://idp/token", "id", ""),
            "sasl.oauthbearer.client.credentials.client.secret is blank",
        ),
        (
            "window factor above the range",
            ClientCredentialsConfig {
                refresh_window_factor: 1.5,
                ..ClientCredentialsConfig::new("https://idp/token", "id", "secret")
            },
            "sasl.login.refresh.window.factor must be between 0.5 and 1, and it is 1.5",
        ),
        (
            "window jitter that is not a number",
            ClientCredentialsConfig {
                refresh_window_jitter: f64::NAN,
                ..ClientCredentialsConfig::new("https://idp/token", "id", "secret")
            },
            "sasl.login.refresh.window.jitter must be between 0 and 0.25, and it is NaN",
        ),
    ] {
        check!(
            ClientCredentialsTokenProvider::new(config).err() == Some(expected.to_owned()),
            "{name}"
        );
    }
}

/// Kafka's `JwtResponseParser` and the expiry of the token.
#[test]
fn responses_give_the_access_token_or_the_id_token() {
    let token = jwt(Some(100), 200);
    for (name, body, expected) in [
        (
            "access token",
            format!(r#"{{"access_token":" {token} ","token_type":"bearer"}}"#),
            Ok(token.clone()),
        ),
        (
            "id token",
            format!(r#"{{"id_token":"{token}"}}"#),
            Ok(token.clone()),
        ),
        (
            "no token",
            r#"{"error":"invalid_client"}"#.to_owned(),
            Err(
                "The token endpoint response did not contain a valid JWT. Response: \
                 ({\"error\":\"invalid_client\"})"
                    .to_owned(),
            ),
        ),
    ] {
        check!(parse_token(&body) == expected, "{name}");
    }
    check!(
        token_times(&token)
            == Ok((
                Some(UNIX_EPOCH + Duration::from_secs(100)),
                UNIX_EPOCH + Duration::from_secs(200)
            ))
    );
    check!(token_times("opaque").err() == Some("the token is not a JWT".to_owned()));
    // RFC 7519 NumericDate may carry a fraction of a second.
    let fractional = format!(
        "{}.{}.sig",
        URL_SAFE_NO_PAD.encode(br#"{"alg":"none"}"#),
        URL_SAFE_NO_PAD.encode(r#"{"exp":200.5}"#)
    );
    check!(token_times(&fractional) == Ok((None, UNIX_EPOCH + Duration::from_secs_f64(200.5))));
}

/// A chunk boundary may cut a multibyte character in two. The dechunk runs on
/// bytes, so the body still decodes.
#[test]
fn a_chunked_body_that_splits_a_character_still_decodes() {
    let body = r#"{"access_token":"t","scope":"café"}"#.as_bytes();
    // The second chunk starts inside the two bytes of "é".
    let split = body.len() - 3;
    let (first, second) = body.split_at(split);
    let mut response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
    for chunk in [first, second] {
        response.extend_from_slice(format!("{:x}\r\n", chunk.len()).as_bytes());
        response.extend_from_slice(chunk);
        response.extend_from_slice(b"\r\n");
    }
    response.extend_from_slice(b"0\r\n\r\n");

    check!(
        parse_http_response(&response)
            == Ok((200, r#"{"access_token":"t","scope":"café"}"#.to_owned()))
    );
}

/// Kafka's `ExpiringCredentialRefreshingLogin.refreshMs` with the default
/// window 0.8 plus 0.05 jitter, 60 s minimum period and 300 s buffer.
#[test]
fn refresh_times_follow_the_expiring_credential_rules() {
    let provider = ClientCredentialsTokenProvider::new(ClientCredentialsConfig::new(
        "https://idp/token",
        "id",
        "s",
    ))
    .unwrap();
    let at = |seconds| UNIX_EPOCH + Duration::from_secs(seconds);
    for (name, now, start, expire, unit, expected) in [
        ("one hour token, low jitter", 0, 0, 3600, 0.0, at(2880)),
        ("one hour token, high jitter", 0, 0, 3600, 1.0, at(3060)),
        ("end buffer", 0, 0, 1000, 1.0, at(700)),
        ("minimum period", 3000, 0, 3600, 0.0, at(3060)),
        ("too short for the buffers", 0, 0, 300, 0.0, at(240)),
    ] {
        check!(
            provider.refresh_at(at(now), at(start), at(expire), unit) == expected,
            "{name}"
        );
    }
}

/// A token endpoint that answers each request with the next `(status,
/// body)` of `answers` and records each request.
async fn endpoint(answers: Vec<(u16, String)>) -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/oauth2/token", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&requests);
    let next = AtomicUsize::new(0);
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = tokio::io::BufReader::new(stream);
            let mut head = String::new();
            let mut length = 0;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                if let Some(value) = line.strip_prefix("Content-Length: ") {
                    length = value.trim().parse().unwrap();
                }
                head.push_str(&line);
                if line == "\r\n" {
                    break;
                }
            }
            let mut body = vec![0_u8; length];
            reader.read_exact(&mut body).await.unwrap();
            seen.lock()
                .unwrap()
                .push(format!("{head}{}", String::from_utf8(body).unwrap()));
            let index = next.fetch_add(1, Ordering::SeqCst);
            let (status, body) = answers[index.min(answers.len() - 1)].clone();
            let response = format!(
                "HTTP/1.1 {status} X\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n{body}\r\n0\r\n\r\n",
                body.len()
            );
            let mut stream = reader.into_inner();
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();
        }
    });
    (url, requests)
}

fn far_future_token() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    jwt(Some(now), now + 3600)
}

/// The provider posts Kafka's request, keeps the token until its refresh
/// time, retries a 503 and does not retry a 401.
#[tokio::test]
async fn the_client_credentials_provider_fetches_caches_and_retries_as_kafka_does() {
    let token = far_future_token();
    let ok = (200, format!(r#"{{"access_token":"{token}"}}"#));
    for (name, answers, calls, expected, expected_requests) in [
        (
            "one fetch for two exchanges",
            vec![ok.clone()],
            2,
            Ok(token.clone()),
            1,
        ),
        (
            "503 then 200",
            vec![(503, "busy".to_owned()), ok.clone()],
            1,
            Ok(token.clone()),
            2,
        ),
        (
            "401 is final",
            vec![
                (401, r#"{"error":"invalid_client"}"#.to_owned()),
                ok.clone(),
            ],
            1,
            Err("the token endpoint answered 401: {\"error\":\"invalid_client\"}".to_owned()),
            1,
        ),
    ] {
        let (url, requests) = endpoint(answers).await;
        let provider = ClientCredentialsTokenProvider::new(ClientCredentialsConfig {
            scope: Some("kafka".into()),
            ..ClientCredentialsConfig::new(url, "id", "secret")
        })
        .unwrap();
        let mut result = Err(String::new());
        for _ in 0..calls {
            result = provider.token().await;
        }
        let requests = requests.lock().unwrap().clone();
        check!(result == expected, "{name}");
        check!(requests.len() == expected_requests, "{name}");
        let authorization = format!("Authorization: Basic {}\r\n", B64.encode("id:secret"));
        check!(
            requests[0].starts_with("POST /oauth2/token HTTP/1.1\r\n"),
            "{name}"
        );
        check!(requests[0].contains(&authorization), "{name}");
        check!(
            requests[0].contains("Content-Type: application/x-www-form-urlencoded\r\n"),
            "{name}"
        );
        check!(
            requests[0].ends_with("\r\n\r\ngrant_type=client_credentials&scope=kafka"),
            "{name}"
        );
    }
}

/// Kafka's `ConfigException`s for a bad `JwtBearerConfig`, and a private key
/// file that cannot be found.
#[test]
fn jwt_bearer_configs_are_rejected_as_kafka_rejects_them() {
    let key = temp_pem_file(TEST_RSA_KEY_PEM);
    for (name, config, expected) in [
        (
            "unsupported algorithm",
            JwtBearerConfig {
                algorithm: "ES256".to_owned(),
                ..JwtBearerConfig::new("https://idp/token", key.path())
            },
            "sasl.oauthbearer.assertion.algorithm \"ES256\" is not supported; only RS256 is \
             implemented"
                .to_owned(),
        ),
        (
            "window factor above the range",
            JwtBearerConfig {
                refresh_window_factor: 1.5,
                ..JwtBearerConfig::new("https://idp/token", key.path())
            },
            "sasl.login.refresh.window.factor must be between 0.5 and 1, and it is 1.5".to_owned(),
        ),
        (
            "window jitter that is not a number",
            JwtBearerConfig {
                refresh_window_jitter: f64::NAN,
                ..JwtBearerConfig::new("https://idp/token", key.path())
            },
            "sasl.login.refresh.window.jitter must be between 0 and 0.25, and it is NaN".to_owned(),
        ),
    ] {
        check!(
            JwtBearerTokenProvider::new(config).err() == Some(expected),
            "{name}"
        );
    }
    let missing = JwtBearerConfig::new("https://idp/token", "/does/not/exist.pem");
    check!(JwtBearerTokenProvider::new(missing).is_err());

    let encrypted_key = temp_pem_file(
        "-----BEGIN ENCRYPTED PRIVATE KEY-----\nAA==\n-----END ENCRYPTED PRIVATE KEY-----\n",
    );
    let encrypted_without_passphrase =
        JwtBearerConfig::new("https://idp/token", encrypted_key.path());
    check!(
        JwtBearerTokenProvider::new(encrypted_without_passphrase).err()
            == Some(format!(
                "sasl.oauthbearer.assertion.private.key.file {}: the private key is \
                 encrypted, but sasl.oauthbearer.assertion.private.key.passphrase is not set",
                encrypted_key.path().display()
            ))
    );
}

/// `LayeredAssertionJwtTemplate`: the static `iss`/`sub`/`aud` claims sit
/// under the template file's entries, and the generated `alg`, `typ`, `iat`,
/// `exp`, `nbf` and `jti` claims win over both.
#[test]
fn jwt_bearer_claims_layer_static_then_template_then_dynamic() {
    let key = temp_pem_file(TEST_RSA_KEY_PEM);
    let mut template = tempfile::NamedTempFile::new().unwrap();
    write!(
        template,
        r#"{{"header":{{"kid":"k1"}},"payload":{{"sub":"from-template","aud":"from-template","custom":"x"}}}}"#
    )
    .unwrap();

    let config = JwtBearerConfig {
        iss: Some("static-iss".to_owned()),
        sub: Some("static-sub".to_owned()),
        aud: Some("static-aud".to_owned()),
        template_file: Some(template.path().to_path_buf()),
        include_jti: true,
        ..JwtBearerConfig::new("https://idp/token", key.path())
    };
    let provider = JwtBearerTokenProvider::new(config).unwrap();
    let now = UNIX_EPOCH + Duration::from_secs(1_000_000);
    let (header, payload) = provider.claims(now);

    check!(header.get("alg") == Some(&serde_json::Value::from("RS256")));
    check!(header.get("typ") == Some(&serde_json::Value::from("JWT")));
    check!(header.get("kid") == Some(&serde_json::Value::from("k1")));

    check!(payload.get("iss") == Some(&serde_json::Value::from("static-iss")));
    check!(payload.get("sub") == Some(&serde_json::Value::from("from-template")));
    check!(payload.get("aud") == Some(&serde_json::Value::from("from-template")));
    check!(payload.get("custom") == Some(&serde_json::Value::from("x")));
    check!(payload.get("iat") == Some(&serde_json::Value::from(1_000_000)));
    check!(payload.get("exp") == Some(&serde_json::Value::from(1_000_300)));
    check!(payload.get("nbf") == Some(&serde_json::Value::from(999_940)));
    check!(payload.get("jti").is_some_and(serde_json::Value::is_string));
}

/// `JwtBearerRequestFormatter.formatBody`: the jwt-bearer grant, the signed
/// assertion, an optional scope, and no `Authorization` header.
#[test]
fn jwt_bearer_requests_follow_the_request_formatter() {
    let key = temp_pem_file(TEST_RSA_KEY_PEM);
    for (name, scope, expect_scope) in [
        ("no scope", None, false),
        ("scope", Some("kafka produce"), true),
    ] {
        let config = JwtBearerConfig {
            scope: scope.map(ToOwned::to_owned),
            iss: Some("client-1".to_owned()),
            ..JwtBearerConfig::new("http://127.0.0.1:1/token", key.path())
        };
        let provider = JwtBearerTokenProvider::new(config).unwrap();
        let request = provider.request().unwrap();
        check!(request.authorization.is_none(), "{name}");
        check!(
            request.body.starts_with(
                "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion="
            ),
            "{name}"
        );
        check!(
            request.body.ends_with("&scope=kafka+produce") == expect_scope,
            "{name}"
        );
    }
}

/// The provider signs the assertion with the configured RSA key and posts the
/// jwt-bearer grant, as a token endpoint would receive it: this test verifies
/// the signature with the matching public key and checks the claims, the way
/// the "Test idea" of the issue describes.
#[tokio::test]
async fn the_jwt_bearer_provider_posts_a_signed_assertion_the_endpoint_can_verify() {
    let key = temp_pem_file(TEST_RSA_KEY_PEM);
    let token = far_future_token();
    let (url, requests) = endpoint(vec![(200, format!(r#"{{"access_token":"{token}"}}"#))]).await;
    let config = JwtBearerConfig {
        iss: Some("client-1".to_owned()),
        sub: Some("client-1".to_owned()),
        aud: Some("https://idp.example".to_owned()),
        include_jti: true,
        ..JwtBearerConfig::new(url, key.path())
    };
    let provider = JwtBearerTokenProvider::new(config).unwrap();
    let result = provider.token().await;
    check!(result == Ok(token));

    let requests = requests.lock().unwrap().clone();
    check!(requests.len() == 1);
    check!(!requests[0].contains("Authorization:"));
    check!(
        requests[0].contains(
            "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion="
        )
    );

    let body_start = requests[0].find("\r\n\r\n").unwrap() + 4;
    let body = &requests[0][body_start..];
    let assertion = body
        .strip_prefix("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=")
        .unwrap();
    let mut parts = assertion.split('.');
    let header = parts.next().unwrap();
    let payload = parts.next().unwrap();
    let signature = URL_SAFE_NO_PAD.decode(parts.next().unwrap()).unwrap();
    check!(parts.next().is_none());

    let signing_input = format!("{header}.{payload}");
    let public_key = test_rsa_public_key();
    ring::signature::UnparsedPublicKey::new(
        &ring::signature::RSA_PKCS1_2048_8192_SHA256,
        &public_key,
    )
    .verify(signing_input.as_bytes(), &signature)
    .unwrap();

    let claims: serde_json::Value =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).unwrap()).unwrap();
    check!(claims["iss"] == "client-1");
    check!(claims["sub"] == "client-1");
    check!(claims["aud"] == "https://idp.example");
    check!(claims["jti"].is_string());
}

/// Polls until `requests` has at least `count` entries, or panics after many
/// attempts. Real loopback I/O completes without the paused clock advancing,
/// so this only needs to give the executor a chance to poll the background
/// task, not to wait on a timer.
async fn wait_for_requests(requests: &Mutex<Vec<String>>, count: usize) {
    for _ in 0..10_000 {
        if requests.lock().unwrap().len() >= count {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("timed out waiting for {count} request(s)");
}

/// [`spawn_background_refresh`] fetches a new token at the computed refresh
/// time on its own, with no `token()` call from application code, as Kafka's
/// `ExpiringCredentialRefreshingLogin` background thread does.
#[tokio::test(start_paused = true)]
async fn background_refresh_fetches_at_the_refresh_time_with_no_application_exchange() {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    // Expires soon, so its refresh time (Kafka's default 0.8-0.85 window of
    // the remaining lifetime) is under a couple of minutes away.
    let short_lived = jwt(Some(now), now + 100);
    let far = far_future_token();
    let (url, requests) = endpoint(vec![
        (200, format!(r#"{{"access_token":"{short_lived}"}}"#)),
        (200, format!(r#"{{"access_token":"{far}"}}"#)),
    ])
    .await;
    let provider =
        ClientCredentialsTokenProvider::new(ClientCredentialsConfig::new(url, "id", "secret"))
            .unwrap();

    let handle = spawn_background_refresh(provider.clone()).unwrap();
    wait_for_requests(&requests, 1).await;
    check!(
        requests.lock().unwrap().len() == 1,
        "no exchange happened yet"
    );

    // Advance a second at a time, well past the worst-case refresh time
    // (100 s * 0.85), without any application code calling `token()`.
    // Stepping keeps the test independent of whether the task has reached
    // its sleep before the clock first moves.
    for _ in 0..95 {
        tokio::time::advance(Duration::from_secs(1)).await;
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
    }
    wait_for_requests(&requests, 2).await;
    check!(requests.lock().unwrap().len() == 2);

    handle.abort();
}

/// [`background_refresh_enabled`](OAuthBearerTokenProvider::background_refresh_enabled)
/// gates [`spawn_background_refresh`], and it defaults to Kafka's always-on
/// behavior for both grant types.
#[test]
fn background_refresh_is_on_by_default_and_can_be_turned_off() {
    let key = temp_pem_file(TEST_RSA_KEY_PEM);
    let client_credentials = ClientCredentialsTokenProvider::new(ClientCredentialsConfig::new(
        "https://idp/token",
        "id",
        "secret",
    ))
    .unwrap();
    check!(client_credentials.background_refresh_enabled());
    let client_credentials_off = ClientCredentialsTokenProvider::new(ClientCredentialsConfig {
        background_refresh: false,
        ..ClientCredentialsConfig::new("https://idp/token", "id", "secret")
    })
    .unwrap();
    check!(!client_credentials_off.background_refresh_enabled());

    let jwt_bearer =
        JwtBearerTokenProvider::new(JwtBearerConfig::new("https://idp/token", key.path())).unwrap();
    check!(jwt_bearer.background_refresh_enabled());
    let jwt_bearer_off = JwtBearerTokenProvider::new(JwtBearerConfig {
        background_refresh: false,
        ..JwtBearerConfig::new("https://idp/token", key.path())
    })
    .unwrap();
    check!(!jwt_bearer_off.background_refresh_enabled());
}
