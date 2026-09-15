use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

use assert2::check;
use tokio::{io::AsyncBufReadExt as _, net::TcpListener};

use super::*;

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
            (request.authorization, request.body) == (authorization, body.to_owned()),
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
