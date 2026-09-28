//! KIP-48 delegation tokens, as Kafka's `Admin.createDelegationToken`,
//! `renewDelegationToken`, `expireDelegationToken`, and
//! `describeDelegationToken`.
//!
//! Each call takes an options struct that mirrors Kafka's
//! `CreateDelegationTokenOptions`, `RenewDelegationTokenOptions`,
//! `ExpireDelegationTokenOptions`, or `DescribeDelegationTokenOptions`, and
//! sends one request on the admin connection, as `KafkaAdminClient` sends it
//! to the least-loaded node. A non-zero top-level error code becomes
//! [`AdminError::Broker`], as Kafka completes the call with
//! `response.error().exception()`.

use bytes::Bytes;
use krabka_protocol::owned::{
    create_delegation_token_request::{CreatableRenewers, CreateDelegationTokenRequest},
    create_delegation_token_response::CreateDelegationTokenResponse,
    describe_delegation_token_request::{
        DescribeDelegationTokenOwner, DescribeDelegationTokenRequest,
    },
    describe_delegation_token_response::{
        DescribeDelegationTokenResponse, DescribedDelegationToken,
    },
    expire_delegation_token_request::ExpireDelegationTokenRequest,
    renew_delegation_token_request::RenewDelegationTokenRequest,
};
use krabka_security::KafkaPrincipal;
use krabka_units::{Time, convert::wire::opt_time_to_millis_i64};

use crate::{AdminClient, AdminError, kafka_error_name};

/// The first `CreateDelegationToken` version with the owner fields
/// (KIP-373).
const CREATE_WITH_OWNER_MIN_VERSION: i16 = 3;

/// What [`AdminClient::create_delegation_token`] asks for, as Kafka's
/// `CreateDelegationTokenOptions`. The default mints a token that the caller
/// owns, with no renewers and the broker's maximum lifetime.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CreateDelegationTokenOptions {
    /// The owner of the token, of any principal type. `None` makes the
    /// caller the owner. Another owner needs the `CreateTokens` operation on
    /// the owner's `User` resource, and `CreateDelegationToken` v3 (KIP-373).
    pub owner: Option<KafkaPrincipal>,
    /// The principals that may renew the token.
    pub renewers: Vec<KafkaPrincipal>,
    /// The maximum lifetime of the token. `None` sends Kafka's `-1`, and the
    /// broker then uses `delegation.token.max.lifetime.ms`. The broker also
    /// uses that ceiling for a lifetime of zero or less, and caps a longer
    /// one at it.
    pub max_lifetime: Option<Time>,
}

/// What [`AdminClient::renew_delegation_token`] asks for, as Kafka's
/// `RenewDelegationTokenOptions`.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct RenewDelegationTokenOptions {
    /// How far past now to move the expiry. `None` sends Kafka's `-1`, and
    /// the broker then uses `delegation.token.expiry.time.ms`. The broker
    /// also uses that default for a period of zero or less, caps a longer
    /// one at it, and never moves the expiry past the token's maximum
    /// timestamp.
    pub renew_time_period: Option<Time>,
}

/// What [`AdminClient::expire_delegation_token`] asks for, as Kafka's
/// `ExpireDelegationTokenOptions`.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct ExpireDelegationTokenOptions {
    /// How far past now the token expires. `None` sends Kafka's `-1`, which
    /// expires the token now, as does any negative period. A period of zero
    /// or more sets the expiry to that time, capped at the token's maximum
    /// timestamp.
    pub expiry_time_period: Option<Time>,
}

/// What [`AdminClient::describe_delegation_token`] asks for, as Kafka's
/// `DescribeDelegationTokenOptions`. The default describes every token that
/// the caller may describe.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DescribeDelegationTokenOptions {
    /// The owners whose tokens to describe. `None` sends a null owner list,
    /// and the broker returns every token that the caller owns or may
    /// describe. An empty list sends an empty owner list, and the broker
    /// returns no tokens.
    pub owners: Option<Vec<KafkaPrincipal>>,
}

/// One delegation token, as Kafka's `DelegationToken` with its
/// `TokenInformation`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelegationToken {
    /// The token ID, which a client presents as its SCRAM user name.
    pub token_id: String,
    /// The HMAC of the token, which a client presents as its SCRAM password.
    pub hmac: Vec<u8>,
    /// The principal that owns the token.
    pub owner: KafkaPrincipal,
    /// The principal that asked for the token. It differs from
    /// [`owner`](Self::owner) when a caller created a token for another
    /// owner.
    pub token_requester: KafkaPrincipal,
    /// The principals that may renew the token.
    pub renewers: Vec<KafkaPrincipal>,
    /// When the broker issued the token, in epoch milliseconds.
    pub issue_timestamp_ms: i64,
    /// When the token expires unless someone renews it, in epoch
    /// milliseconds.
    pub expiry_timestamp_ms: i64,
    /// The latest expiry that a renewal can set, in epoch milliseconds.
    pub max_timestamp_ms: i64,
}

impl AdminClient {
    /// Creates a delegation token, as Kafka's `createDelegationToken`
    /// operation does.
    ///
    /// The broker does not return the renewers, so the result has the
    /// renewers of `options`, as Kafka's result does. With an
    /// [`owner`](CreateDelegationTokenOptions::owner) the client negotiates
    /// `CreateDelegationToken` v3 or higher, as Kafka's request refuses to
    /// write the owner fields below v3.
    ///
    /// # Errors
    ///
    /// Returns [`AdminError::Broker`] for a non-zero error code, such as
    /// `DELEGATION_TOKEN_AUTH_DISABLED` (61),
    /// `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED` (64),
    /// `DELEGATION_TOKEN_AUTHORIZATION_FAILED` (65), or
    /// `INVALID_PRINCIPAL_TYPE` (67). Returns [`AdminError::Transport`] when
    /// the connection fails, or when an owner is set and the broker does not
    /// support v3.
    pub async fn create_delegation_token(
        &self,
        options: &CreateDelegationTokenOptions,
    ) -> Result<DelegationToken, AdminError> {
        let request = create_request(options);
        let response = if options.owner.is_some() {
            self.conn
                .send_at_least(request, CREATE_WITH_OWNER_MIN_VERSION)
                .await?
        } else {
            self.conn.send(request).await?
        };
        created_token(response, &options.renewers)
    }

    /// Renews the delegation token with `hmac`, as Kafka's
    /// `renewDelegationToken` operation does, and returns its new expiry in
    /// epoch milliseconds.
    ///
    /// # Errors
    ///
    /// Returns [`AdminError::Broker`] for a non-zero error code, such as
    /// `DELEGATION_TOKEN_NOT_FOUND` (62), `DELEGATION_TOKEN_OWNER_MISMATCH`
    /// (63), or `DELEGATION_TOKEN_EXPIRED` (66). Returns
    /// [`AdminError::Transport`] when the connection fails.
    pub async fn renew_delegation_token(
        &self,
        hmac: &[u8],
        options: RenewDelegationTokenOptions,
    ) -> Result<i64, AdminError> {
        let response = self.conn.send(renew_request(hmac, options)).await?;
        check("RenewDelegationToken", response.error_code)?;
        Ok(response.expiry_timestamp_ms)
    }

    /// Expires the delegation token with `hmac`, as Kafka's
    /// `expireDelegationToken` operation does, and returns the expiry that
    /// the broker set, in epoch milliseconds. A token that expires now gets
    /// the broker's clock at the time of the request.
    ///
    /// # Errors
    ///
    /// Returns [`AdminError::Broker`] for a non-zero error code, such as
    /// `DELEGATION_TOKEN_NOT_FOUND` (62), `DELEGATION_TOKEN_OWNER_MISMATCH`
    /// (63), or `DELEGATION_TOKEN_EXPIRED` (66). Returns
    /// [`AdminError::Transport`] when the connection fails.
    pub async fn expire_delegation_token(
        &self,
        hmac: &[u8],
        options: ExpireDelegationTokenOptions,
    ) -> Result<i64, AdminError> {
        let response = self.conn.send(expire_request(hmac, options)).await?;
        check("ExpireDelegationToken", response.error_code)?;
        Ok(response.expiry_timestamp_ms)
    }

    /// Describes the delegation tokens that `options` selects, as Kafka's
    /// `describeDelegationToken` operation does.
    ///
    /// # Errors
    ///
    /// Returns [`AdminError::Broker`] for a non-zero error code, such as
    /// `DELEGATION_TOKEN_AUTH_DISABLED` (61) or
    /// `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED` (64). Returns
    /// [`AdminError::Transport`] when the connection fails.
    pub async fn describe_delegation_token(
        &self,
        options: &DescribeDelegationTokenOptions,
    ) -> Result<Vec<DelegationToken>, AdminError> {
        let response = self.conn.send(describe_request(options)).await?;
        described_tokens(response)
    }
}

/// The `CreateDelegationToken` request of `options`, as
/// `KafkaAdminClient.createDelegationToken` builds it. With no owner the
/// owner fields keep their empty-string defaults, as Kafka's
/// `CreateDelegationTokenRequestData` does, and the broker makes the caller
/// the owner.
fn create_request(options: &CreateDelegationTokenOptions) -> CreateDelegationTokenRequest {
    let mut request = CreateDelegationTokenRequest {
        renewers: options
            .renewers
            .iter()
            .map(|renewer| CreatableRenewers {
                principal_type: renewer.principal_type.clone(),
                principal_name: renewer.name.clone(),
                ..Default::default()
            })
            .collect(),
        max_lifetime_ms: opt_time_to_millis_i64(options.max_lifetime),
        ..Default::default()
    };
    if let Some(owner) = &options.owner {
        request.owner_principal_type = Some(owner.principal_type.clone());
        request.owner_principal_name = Some(owner.name.clone());
    }
    request
}

fn renew_request(hmac: &[u8], options: RenewDelegationTokenOptions) -> RenewDelegationTokenRequest {
    RenewDelegationTokenRequest {
        hmac: Bytes::copy_from_slice(hmac),
        renew_period_ms: opt_time_to_millis_i64(options.renew_time_period),
        ..Default::default()
    }
}

fn expire_request(
    hmac: &[u8],
    options: ExpireDelegationTokenOptions,
) -> ExpireDelegationTokenRequest {
    ExpireDelegationTokenRequest {
        hmac: Bytes::copy_from_slice(hmac),
        expiry_time_period_ms: opt_time_to_millis_i64(options.expiry_time_period),
        ..Default::default()
    }
}

/// The `DescribeDelegationToken` request of `options`, as Kafka's
/// `DescribeDelegationTokenRequest.Builder` builds it: a null owner list for
/// no owners, and the listed owners otherwise.
fn describe_request(options: &DescribeDelegationTokenOptions) -> DescribeDelegationTokenRequest {
    DescribeDelegationTokenRequest {
        owners: options.owners.as_ref().map(|owners| {
            owners
                .iter()
                .map(|owner| DescribeDelegationTokenOwner {
                    principal_type: owner.principal_type.clone(),
                    principal_name: owner.name.clone(),
                    ..Default::default()
                })
                .collect()
        }),
        ..Default::default()
    }
}

/// The token of a `CreateDelegationToken` answer, with the renewers that the
/// request named.
fn created_token(
    response: CreateDelegationTokenResponse,
    renewers: &[KafkaPrincipal],
) -> Result<DelegationToken, AdminError> {
    check("CreateDelegationToken", response.error_code)?;
    Ok(DelegationToken {
        token_id: response.token_id,
        hmac: response.hmac.to_vec(),
        owner: principal(response.principal_type, response.principal_name),
        token_requester: principal(
            response.token_requester_principal_type,
            response.token_requester_principal_name,
        ),
        renewers: renewers.to_vec(),
        issue_timestamp_ms: response.issue_timestamp_ms,
        expiry_timestamp_ms: response.expiry_timestamp_ms,
        max_timestamp_ms: response.max_timestamp_ms,
    })
}

fn described_tokens(
    response: DescribeDelegationTokenResponse,
) -> Result<Vec<DelegationToken>, AdminError> {
    check("DescribeDelegationToken", response.error_code)?;
    Ok(response.tokens.into_iter().map(described_token).collect())
}

fn described_token(token: DescribedDelegationToken) -> DelegationToken {
    DelegationToken {
        token_id: token.token_id,
        hmac: token.hmac.to_vec(),
        owner: principal(token.principal_type, token.principal_name),
        token_requester: principal(
            token.token_requester_principal_type,
            token.token_requester_principal_name,
        ),
        renewers: token
            .renewers
            .into_iter()
            .map(|renewer| principal(renewer.principal_type, renewer.principal_name))
            .collect(),
        issue_timestamp_ms: token.issue_timestamp,
        expiry_timestamp_ms: token.expiry_timestamp,
        max_timestamp_ms: token.max_timestamp,
    }
}

fn principal(principal_type: String, name: String) -> KafkaPrincipal {
    KafkaPrincipal {
        principal_type,
        name,
    }
}

/// `Ok` for a zero error code, and [`AdminError::Broker`] for any other.
fn check(api: &'static str, code: i16) -> Result<(), AdminError> {
    if code == 0 {
        return Ok(());
    }
    Err(AdminError::Broker {
        api,
        code,
        name: kafka_error_name(code),
        message: None,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use krabka_client_core::{ClientError, MockBroker};
    use krabka_protocol::{
        Decode, Encode,
        owned::{
            api_versions_request, create_delegation_token_request,
            describe_delegation_token_request,
            describe_delegation_token_response::DescribedDelegationTokenRenewer,
            expire_delegation_token_request,
            expire_delegation_token_response::ExpireDelegationTokenResponse,
            renew_delegation_token_request,
            renew_delegation_token_response::RenewDelegationTokenResponse,
        },
    };

    use super::*;
    use crate::partition_leaders::test_support::{
        api_versions, decode_request, encode_response, fast_admin,
    };

    fn user(name: &str) -> KafkaPrincipal {
        principal("User".to_owned(), name.to_owned())
    }

    fn group(name: &str) -> KafkaPrincipal {
        principal("Group".to_owned(), name.to_owned())
    }

    /// Every delegation-token API up to its latest version.
    const TOKEN_APIS: [(i16, i16, i16); 4] = [
        (create_delegation_token_request::API_KEY, 1, 3),
        (renew_delegation_token_request::API_KEY, 1, 2),
        (expire_delegation_token_request::API_KEY, 1, 2),
        (describe_delegation_token_request::API_KEY, 1, 3),
    ];

    /// A mock broker that advertises `apis`, answers each `api_key` request
    /// with `answer` encoded at the negotiated version, and records the
    /// requests it decodes. Every token API is flexible from v2.
    async fn token_broker<Req, Resp>(
        apis: Vec<(i16, i16, i16)>,
        api_key: i16,
        answer: Resp,
    ) -> (MockBroker, Arc<Mutex<Vec<Req>>>)
    where
        Req: for<'a> Decode<'a> + Send + 'static,
        Resp: Encode + Send + 'static,
    {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&requests);
        let broker = MockBroker::start(move |key, version, _, body| match key {
            api_versions_request::API_KEY => Some(api_versions(&apis)),
            key if key == api_key => {
                let flexible = version >= 2;
                seen.lock()
                    .expect("requests lock")
                    .push(decode_request::<Req>(body, version, flexible));
                Some(encode_response(&answer, version, flexible))
            }
            _ => None,
        })
        .await;
        (broker, requests)
    }

    type Outcome<T> = Result<T, (&'static str, i16, &'static str)>;

    fn outcome<T>(result: Result<T, AdminError>) -> Outcome<T> {
        result.map_err(|error| match error {
            AdminError::Broker {
                api,
                code,
                name,
                message: None,
            } => (api, code, name),
            other => panic!("unexpected error {other:?}"),
        })
    }

    fn created_response(error_code: i16) -> CreateDelegationTokenResponse {
        CreateDelegationTokenResponse {
            error_code,
            principal_type: "Group".to_owned(),
            principal_name: "ops".to_owned(),
            token_requester_principal_type: "User".to_owned(),
            token_requester_principal_name: "admin".to_owned(),
            issue_timestamp_ms: 1_000,
            expiry_timestamp_ms: 2_000,
            max_timestamp_ms: 9_000,
            token_id: "tok-1".to_owned(),
            hmac: Bytes::from_static(b"\xde\xad"),
            ..Default::default()
        }
    }

    /// Kafka's `createDelegationToken` sends the renewers and the maximum
    /// lifetime, sets the owner fields only for an owner, and builds the
    /// token from the answer and the requested renewers.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_delegation_token_sends_each_option_as_kafka_does() {
        let token = |renewers| DelegationToken {
            token_id: "tok-1".to_owned(),
            hmac: vec![0xde, 0xad],
            owner: group("ops"),
            token_requester: user("admin"),
            renewers,
            issue_timestamp_ms: 1_000,
            expiry_timestamp_ms: 2_000,
            max_timestamp_ms: 9_000,
        };
        let renewers = vec![user("bob"), group("ops")];
        let wire_renewers = vec![
            CreatableRenewers {
                principal_type: "User".to_owned(),
                principal_name: "bob".to_owned(),
                ..Default::default()
            },
            CreatableRenewers {
                principal_type: "Group".to_owned(),
                principal_name: "ops".to_owned(),
                ..Default::default()
            },
        ];
        for (name, options, request, error_code, expected) in [
            (
                "defaults",
                CreateDelegationTokenOptions::default(),
                CreateDelegationTokenRequest {
                    owner_principal_type: Some(String::new()),
                    owner_principal_name: Some(String::new()),
                    renewers: Vec::new(),
                    max_lifetime_ms: -1,
                    ..Default::default()
                },
                0,
                Ok(token(Vec::new())),
            ),
            (
                "owner of another principal type, renewers, lifetime",
                CreateDelegationTokenOptions {
                    owner: Some(group("ops")),
                    renewers: renewers.clone(),
                    max_lifetime: Some(krabka_units::secs(60)),
                },
                CreateDelegationTokenRequest {
                    owner_principal_type: Some("Group".to_owned()),
                    owner_principal_name: Some("ops".to_owned()),
                    renewers: wire_renewers.clone(),
                    max_lifetime_ms: 60_000,
                    ..Default::default()
                },
                0,
                Ok(token(renewers.clone())),
            ),
            (
                "zero lifetime reaches the broker as zero",
                CreateDelegationTokenOptions {
                    max_lifetime: Some(krabka_units::millis(0)),
                    ..CreateDelegationTokenOptions::default()
                },
                CreateDelegationTokenRequest {
                    max_lifetime_ms: 0,
                    ..Default::default()
                },
                0,
                Ok(token(Vec::new())),
            ),
            (
                "not authorized to create for the owner",
                CreateDelegationTokenOptions {
                    owner: Some(user("alice")),
                    ..CreateDelegationTokenOptions::default()
                },
                CreateDelegationTokenRequest {
                    owner_principal_type: Some("User".to_owned()),
                    owner_principal_name: Some("alice".to_owned()),
                    max_lifetime_ms: -1,
                    ..Default::default()
                },
                65,
                Err((
                    "CreateDelegationToken",
                    65,
                    "DELEGATION_TOKEN_AUTHORIZATION_FAILED",
                )),
            ),
            (
                "renewer of an invalid principal type",
                CreateDelegationTokenOptions {
                    renewers: renewers.clone(),
                    ..CreateDelegationTokenOptions::default()
                },
                CreateDelegationTokenRequest {
                    renewers: wire_renewers.clone(),
                    max_lifetime_ms: -1,
                    ..Default::default()
                },
                67,
                Err(("CreateDelegationToken", 67, "INVALID_PRINCIPAL_TYPE")),
            ),
        ] {
            let (broker, requests) = token_broker::<CreateDelegationTokenRequest, _>(
                TOKEN_APIS.to_vec(),
                create_delegation_token_request::API_KEY,
                created_response(error_code),
            )
            .await;
            let admin = fast_admin(broker.addr, krabka_units::secs(5)).await;

            let result = outcome(admin.create_delegation_token(&options).await);

            broker.stop();
            let requests = requests.lock().expect("requests lock").clone();
            assert2::assert!(
                (result, requests) == (expected, vec![request]),
                "case {name}"
            );
        }
    }

    /// Kafka's request refuses to write the owner fields below v3, so an
    /// owner needs v3. Without an owner an older version serves.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_delegation_token_needs_v3_only_for_an_owner() {
        for (name, owner, expected) in [
            ("no owner", None, Ok(1)),
            (
                "owner",
                Some(user("alice")),
                Err((create_delegation_token_request::API_KEY, 1, 2, 3, 3)),
            ),
        ] {
            let (broker, requests) = token_broker::<CreateDelegationTokenRequest, _>(
                vec![(create_delegation_token_request::API_KEY, 1, 2)],
                create_delegation_token_request::API_KEY,
                created_response(0),
            )
            .await;
            let admin = fast_admin(broker.addr, krabka_units::secs(5)).await;

            let result = admin
                .create_delegation_token(&CreateDelegationTokenOptions {
                    owner,
                    ..CreateDelegationTokenOptions::default()
                })
                .await
                .map(|_| requests.lock().expect("requests lock").len())
                .map_err(|error| match error {
                    AdminError::Transport(ClientError::IncompatibleVersion {
                        api_key,
                        broker_min,
                        broker_max,
                        client_min,
                        client_max,
                    }) => (api_key, broker_min, broker_max, client_min, client_max),
                    other => panic!("case {name}: unexpected error {other:?}"),
                });

            broker.stop();
            assert2::assert!(result == expected, "case {name}");
        }
    }

    /// Kafka's `renewDelegationToken` sends the renew period, `-1` by
    /// default, and returns the expiry of the answer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn renew_delegation_token_sends_the_period_and_returns_the_expiry() {
        for (name, renew_time_period, renew_period_ms, error_code, expected) in [
            ("broker default", None, -1, 0, Ok(5_000)),
            (
                "one hour",
                Some(krabka_units::secs(3_600)),
                3_600_000,
                0,
                Ok(5_000),
            ),
            (
                "unknown token",
                None,
                -1,
                62,
                Err(("RenewDelegationToken", 62, "DELEGATION_TOKEN_NOT_FOUND")),
            ),
            (
                "expired token",
                None,
                -1,
                66,
                Err(("RenewDelegationToken", 66, "DELEGATION_TOKEN_EXPIRED")),
            ),
        ] {
            let (broker, requests) = token_broker::<RenewDelegationTokenRequest, _>(
                TOKEN_APIS.to_vec(),
                renew_delegation_token_request::API_KEY,
                RenewDelegationTokenResponse {
                    error_code,
                    expiry_timestamp_ms: 5_000,
                    ..Default::default()
                },
            )
            .await;
            let admin = fast_admin(broker.addr, krabka_units::secs(5)).await;

            let result = outcome(
                admin
                    .renew_delegation_token(
                        b"\x01\x02",
                        RenewDelegationTokenOptions { renew_time_period },
                    )
                    .await,
            );

            broker.stop();
            let requests = requests.lock().expect("requests lock").clone();
            assert2::assert!(
                (result, requests)
                    == (
                        expected,
                        vec![RenewDelegationTokenRequest {
                            hmac: Bytes::from_static(b"\x01\x02"),
                            renew_period_ms,
                            ..Default::default()
                        }]
                    ),
                "case {name}"
            );
        }
    }

    /// Kafka's `expireDelegationToken` sends the expiry period, `-1` (now)
    /// by default, and returns the expiry of the answer, which
    /// `kafka-delegation-tokens` prints as the new expiry date.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn expire_delegation_token_sends_the_period_and_returns_the_expiry() {
        for (name, expiry_time_period, expiry_time_period_ms, error_code, expected) in [
            ("now", None, -1, 0, Ok(7_000)),
            ("in zero ms", Some(krabka_units::millis(0)), 0, 0, Ok(7_000)),
            (
                "in one minute",
                Some(krabka_units::secs(60)),
                60_000,
                0,
                Ok(7_000),
            ),
            (
                "not a renewer",
                None,
                -1,
                63,
                Err((
                    "ExpireDelegationToken",
                    63,
                    "DELEGATION_TOKEN_OWNER_MISMATCH",
                )),
            ),
        ] {
            let (broker, requests) = token_broker::<ExpireDelegationTokenRequest, _>(
                TOKEN_APIS.to_vec(),
                expire_delegation_token_request::API_KEY,
                ExpireDelegationTokenResponse {
                    error_code,
                    expiry_timestamp_ms: 7_000,
                    ..Default::default()
                },
            )
            .await;
            let admin = fast_admin(broker.addr, krabka_units::secs(5)).await;

            let result = outcome(
                admin
                    .expire_delegation_token(
                        b"\xaa",
                        ExpireDelegationTokenOptions { expiry_time_period },
                    )
                    .await,
            );

            broker.stop();
            let requests = requests.lock().expect("requests lock").clone();
            assert2::assert!(
                (result, requests)
                    == (
                        expected,
                        vec![ExpireDelegationTokenRequest {
                            hmac: Bytes::from_static(b"\xaa"),
                            expiry_time_period_ms,
                            ..Default::default()
                        }]
                    ),
                "case {name}"
            );
        }
    }

    /// Kafka's `describeDelegationToken` sends a null owner list for no
    /// owners, which asks for every token that the caller may describe, and
    /// an empty list for an empty list, which asks for none. It maps each
    /// described token with its requester and renewers.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn describe_delegation_token_sends_the_owner_filter_as_kafka_does() {
        let described = DescribedDelegationToken {
            principal_type: "User".to_owned(),
            principal_name: "alice".to_owned(),
            token_requester_principal_type: "User".to_owned(),
            token_requester_principal_name: "operator".to_owned(),
            issue_timestamp: 1_000,
            expiry_timestamp: 2_000,
            max_timestamp: 9_000,
            token_id: "tok-1".to_owned(),
            hmac: Bytes::from_static(b"\xbe\xef"),
            renewers: vec![DescribedDelegationTokenRenewer {
                principal_type: "Group".to_owned(),
                principal_name: "ops".to_owned(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let token = DelegationToken {
            token_id: "tok-1".to_owned(),
            hmac: vec![0xbe, 0xef],
            owner: user("alice"),
            token_requester: user("operator"),
            renewers: vec![group("ops")],
            issue_timestamp_ms: 1_000,
            expiry_timestamp_ms: 2_000,
            max_timestamp_ms: 9_000,
        };
        let owner = |principal_type: &str, principal_name: &str| DescribeDelegationTokenOwner {
            principal_type: principal_type.to_owned(),
            principal_name: principal_name.to_owned(),
            ..Default::default()
        };
        for (name, owners, wire_owners, (error_code, tokens), expected) in [
            (
                "every token",
                None,
                None,
                (0, vec![described.clone()]),
                Ok(vec![token.clone()]),
            ),
            (
                "no owner",
                Some(Vec::new()),
                Some(Vec::new()),
                (0, Vec::new()),
                Ok(Vec::new()),
            ),
            (
                "owners of two principal types",
                Some(vec![user("alice"), group("ops")]),
                Some(vec![owner("User", "alice"), owner("Group", "ops")]),
                (0, vec![described.clone()]),
                Ok(vec![token.clone()]),
            ),
            (
                "tokens disabled",
                None,
                None,
                (61, Vec::new()),
                Err((
                    "DescribeDelegationToken",
                    61,
                    "DELEGATION_TOKEN_AUTH_DISABLED",
                )),
            ),
        ] {
            let (broker, requests) = token_broker::<DescribeDelegationTokenRequest, _>(
                TOKEN_APIS.to_vec(),
                describe_delegation_token_request::API_KEY,
                DescribeDelegationTokenResponse {
                    error_code,
                    tokens,
                    ..Default::default()
                },
            )
            .await;
            let admin = fast_admin(broker.addr, krabka_units::secs(5)).await;

            let result = outcome(
                admin
                    .describe_delegation_token(&DescribeDelegationTokenOptions { owners })
                    .await,
            );

            broker.stop();
            let requests = requests.lock().expect("requests lock").clone();
            assert2::assert!(
                (result, requests)
                    == (
                        expected,
                        vec![DescribeDelegationTokenRequest {
                            owners: wire_owners,
                            ..Default::default()
                        }]
                    ),
                "case {name}"
            );
        }
    }
}
