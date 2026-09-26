//! Client-quota admin RPCs (KIP-546): `DescribeClientQuotas` (`api_key` 48)
//! and `AlterClientQuotas` (`api_key` 49), as Kafka's
//! `Admin.describeClientQuotas` and `alterClientQuotas`.
//!
//! An entity is a map from entity type to entity name. Kafka's brokers accept
//! the types [`ENTITY_USER`], [`ENTITY_CLIENT_ID`] and [`ENTITY_IP`], alone or
//! as the pair `(user, client-id)`. A `None` name is the default entity of its
//! type. As in Kafka's client, the broker validates the combination.
//!
//! The `KafkaUser` reconciler uses the per-user shortcuts
//! [`AdminClient::describe_user_quotas`] and
//! [`AdminClient::alter_user_quotas`].

use std::collections::BTreeMap;

use krabka_protocol::owned::{
    alter_client_quotas_request::{
        AlterClientQuotasRequest, EntityData as AlterEntity, EntryData as AlterEntry,
        OpData as AlterOp,
    },
    alter_client_quotas_response::AlterClientQuotasResponse,
    describe_client_quotas_request::{ComponentData, DescribeClientQuotasRequest},
    describe_client_quotas_response::DescribeClientQuotasResponse,
};

use crate::{
    AdminClient, AdminError, KafkaError, kafka_error_if, kafka_error_name, retry::ControllerRetry,
};

/// The entity type of a user principal, Kafka's `ClientQuotaEntity.USER`.
pub const ENTITY_USER: &str = "user";
/// The entity type of a client ID, Kafka's `ClientQuotaEntity.CLIENT_ID`.
pub const ENTITY_CLIENT_ID: &str = "client-id";
/// The entity type of a client IP address, Kafka's `ClientQuotaEntity.IP`.
pub const ENTITY_IP: &str = "ip";

/// Wire `match_type` constants of `DescribeClientQuotasRequest.json`.
const MATCH_TYPE_EXACT: i8 = 0;
const MATCH_TYPE_DEFAULT: i8 = 1;
const MATCH_TYPE_SPECIFIED: i8 = 2;

/// `UNKNOWN_SERVER_ERROR`.
const UNKNOWN_SERVER_ERROR: i16 = -1;

/// One mutation of a quota entity, as Kafka's `ClientQuotaAlteration.Op`.
#[derive(Debug, Clone, PartialEq)]
pub enum QuotaOp {
    /// Upsert `key` → `value`. `value` must be finite and non-negative;
    /// for `request_percentage` the broker also requires `value <= 100`.
    Set { key: String, value: f64 },
    /// Tombstone `key` for this entity. Kafka sends it for an `Op` with a
    /// null value, as `remove=true`.
    Remove { key: String },
}

/// Snapshot of the broker's quota state for a single user. An empty map means
/// no per-user quotas are configured.
pub type UserQuotaConfig = BTreeMap<String, f64>;

/// A quota entity, as Kafka's `ClientQuotaEntity`: entity type → entity
/// name, where `None` is the default entity of that type.
pub type ClientQuotaEntity = BTreeMap<String, Option<String>>;

/// The quota values of each entity, as Kafka's `DescribeClientQuotasResult`.
pub type ClientQuotas = BTreeMap<ClientQuotaEntity, BTreeMap<String, f64>>;

/// What one filter component matches, as the three constructors of Kafka's
/// `ClientQuotaFilterComponent`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientQuotaMatch {
    /// The entity with this name (`ofEntity`).
    Exact(String),
    /// The default entity of the type (`ofDefaultEntity`).
    Default,
    /// Any entity of the type, default or not (`ofEntityType`).
    Specified,
}

/// One component of a [`ClientQuotaFilter`], as Kafka's
/// `ClientQuotaFilterComponent`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientQuotaFilterComponent {
    /// The entity type, such as [`ENTITY_USER`].
    pub entity_type: String,
    /// Which entities of that type match.
    pub matches: ClientQuotaMatch,
}

impl ClientQuotaFilterComponent {
    /// The entity of `entity_type` named `name`.
    #[must_use]
    pub fn of_entity(entity_type: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            entity_type: entity_type.into(),
            matches: ClientQuotaMatch::Exact(name.into()),
        }
    }

    /// The default entity of `entity_type`.
    #[must_use]
    pub fn of_default_entity(entity_type: impl Into<String>) -> Self {
        Self {
            entity_type: entity_type.into(),
            matches: ClientQuotaMatch::Default,
        }
    }

    /// Any entity of `entity_type`.
    #[must_use]
    pub fn of_entity_type(entity_type: impl Into<String>) -> Self {
        Self {
            entity_type: entity_type.into(),
            matches: ClientQuotaMatch::Specified,
        }
    }
}

/// Which entities a describe returns, as Kafka's `ClientQuotaFilter`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClientQuotaFilter {
    /// Each component that a matched entity must satisfy.
    pub components: Vec<ClientQuotaFilterComponent>,
    /// Whether a matched entity may have only the component types
    /// (`containsOnly`), not others as well (`contains`).
    pub strict: bool,
}

impl ClientQuotaFilter {
    /// The entities that satisfy every component and may have other entity
    /// types as well (`ClientQuotaFilter.contains`).
    #[must_use]
    pub const fn contains(components: Vec<ClientQuotaFilterComponent>) -> Self {
        Self {
            components,
            strict: false,
        }
    }

    /// The entities that satisfy every component and have no other entity
    /// type (`ClientQuotaFilter.containsOnly`).
    #[must_use]
    pub const fn contains_only(components: Vec<ClientQuotaFilterComponent>) -> Self {
        Self {
            components,
            strict: true,
        }
    }

    /// Every entity (`ClientQuotaFilter.all`).
    #[must_use]
    pub const fn all() -> Self {
        Self::contains(Vec::new())
    }
}

/// The mutations of one entity, as Kafka's `ClientQuotaAlteration`.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientQuotaAlteration {
    /// The entity to change.
    pub entity: ClientQuotaEntity,
    /// The changes of its quota values.
    pub ops: Vec<QuotaOp>,
}

/// The `DescribeClientQuotas` request of `filter`, as Kafka's
/// `DescribeClientQuotasRequest.Builder` builds it.
fn describe_request(filter: &ClientQuotaFilter) -> DescribeClientQuotasRequest {
    DescribeClientQuotasRequest {
        components: filter
            .components
            .iter()
            .map(|component| {
                let (match_type, match_) = match &component.matches {
                    ClientQuotaMatch::Exact(name) => (MATCH_TYPE_EXACT, Some(name.clone())),
                    ClientQuotaMatch::Default => (MATCH_TYPE_DEFAULT, None),
                    ClientQuotaMatch::Specified => (MATCH_TYPE_SPECIFIED, None),
                };
                ComponentData {
                    entity_type: component.entity_type.clone(),
                    match_type,
                    match_,
                    ..Default::default()
                }
            })
            .collect(),
        strict: filter.strict,
        ..Default::default()
    }
}

/// The quotas of a `DescribeClientQuotas` answer, as Kafka's
/// `DescribeClientQuotasResponse.complete` reads it. A top-level error fails
/// the call.
fn describe_result(response: DescribeClientQuotasResponse) -> Result<ClientQuotas, AdminError> {
    if response.error_code != 0 {
        return Err(AdminError::Broker {
            api: "DescribeClientQuotas",
            code: response.error_code,
            name: kafka_error_name(response.error_code),
            message: response.error_message,
        });
    }
    Ok(response
        .entries
        .unwrap_or_default()
        .into_iter()
        .map(|entry| {
            let entity = entry
                .entity
                .into_iter()
                .map(|entity| (entity.entity_type, entity.entity_name))
                .collect();
            let values = entry
                .values
                .into_iter()
                .map(|value| (value.key, value.value))
                .collect();
            (entity, values)
        })
        .collect())
}

/// The `AlterClientQuotas` request of `alterations`, as Kafka's
/// `AlterClientQuotasRequest.Builder` builds it.
fn alter_request(
    alterations: &[ClientQuotaAlteration],
    validate_only: bool,
) -> AlterClientQuotasRequest {
    AlterClientQuotasRequest {
        entries: alterations
            .iter()
            .map(|alteration| AlterEntry {
                entity: alteration
                    .entity
                    .iter()
                    .map(|(entity_type, entity_name)| AlterEntity {
                        entity_type: entity_type.clone(),
                        entity_name: entity_name.clone(),
                        ..Default::default()
                    })
                    .collect(),
                ops: alteration.ops.iter().map(op_to_wire).collect(),
                ..Default::default()
            })
            .collect(),
        validate_only,
        ..Default::default()
    }
}

/// The result of each entity of `alterations`, as Kafka's
/// `AlterClientQuotasResponse.complete` gives it. An entity that the answer
/// does not name gets `UNKNOWN_SERVER_ERROR`, and an entity that nobody asked
/// for is skipped.
fn alter_results(
    alterations: &[ClientQuotaAlteration],
    response: AlterClientQuotasResponse,
) -> BTreeMap<ClientQuotaEntity, Result<(), KafkaError>> {
    let mut out = BTreeMap::new();
    for entry in response.entries {
        let entity: ClientQuotaEntity = entry
            .entity
            .into_iter()
            .map(|entity| (entity.entity_type, entity.entity_name))
            .collect();
        if !alterations
            .iter()
            .any(|alteration| alteration.entity == entity)
        {
            tracing::warn!(
                ?entity,
                "the AlterClientQuotas response names an entity that is not in the request"
            );
            continue;
        }
        let result = kafka_error_if(entry.error_code, entry.error_message).map_or(Ok(()), Err);
        out.insert(entity, result);
    }
    for alteration in alterations {
        out.entry(alteration.entity.clone()).or_insert_with(|| {
            Err(KafkaError {
                code: UNKNOWN_SERVER_ERROR,
                name: kafka_error_name(UNKNOWN_SERVER_ERROR),
                message: Some(format!(
                    "the AlterClientQuotas response did not contain a result for entity {:?}",
                    alteration.entity
                )),
            })
        });
    }
    out
}

/// The single-user entity `{user: name}`.
fn user_entity(username: &str) -> ClientQuotaEntity {
    BTreeMap::from([(ENTITY_USER.to_owned(), Some(username.to_owned()))])
}

impl AdminClient {
    /// Describes the quotas of every entity that `filter` matches, as
    /// Kafka's `describeClientQuotas` operation does.
    ///
    /// The client sends one `DescribeClientQuotas` request on its connection
    /// and stops at `default.api.timeout.ms` (60 s).
    ///
    /// # Errors
    /// Returns [`AdminError::Broker`] for a top-level error, such as
    /// `CLUSTER_AUTHORIZATION_FAILED` (31) or `INVALID_REQUEST` (42) for a
    /// filter the broker refuses, and with `REQUEST_TIMED_OUT` (7) at the
    /// deadline, or a transport error.
    pub async fn describe_client_quotas(
        &self,
        filter: &ClientQuotaFilter,
    ) -> Result<ClientQuotas, AdminError> {
        let retry = ControllerRetry::new("DescribeClientQuotas", self.retry);
        let response = retry
            .bounded(self.conn.send(describe_request(filter)))
            .await?;
        describe_result(response)
    }

    /// Changes the quotas of each entity of `alterations`, as Kafka's
    /// `alterClientQuotas` operation does. With `validate_only` the broker
    /// validates the changes and applies none.
    ///
    /// The result has one entry for each entity: `Ok(())`, or the error the
    /// broker gave for it, such as `INVALID_REQUEST` (42) for an entity type
    /// combination it does not accept.
    ///
    /// # Errors
    /// Returns a transport error, and [`AdminError::Broker`] with
    /// `REQUEST_TIMED_OUT` (7) at `default.api.timeout.ms` (60 s).
    pub async fn alter_client_quotas(
        &self,
        alterations: &[ClientQuotaAlteration],
        validate_only: bool,
    ) -> Result<BTreeMap<ClientQuotaEntity, Result<(), KafkaError>>, AdminError> {
        let retry = ControllerRetry::new("AlterClientQuotas", self.retry);
        let response = retry
            .bounded(self.conn.send(alter_request(alterations, validate_only)))
            .await?;
        Ok(alter_results(alterations, response))
    }

    /// Reads the quotas of the user entity `{user: username}`.
    ///
    /// The filter is strict on the single component `("user", username)`, as
    /// `ClientQuotaFilter.containsOnly`: an entry whose entity also has a
    /// `client-id` does not match.
    ///
    /// # Errors
    /// See [`Self::describe_client_quotas`].
    pub async fn describe_user_quotas(
        &mut self,
        username: &str,
    ) -> Result<UserQuotaConfig, AdminError> {
        let filter = ClientQuotaFilter::contains_only(vec![ClientQuotaFilterComponent::of_entity(
            ENTITY_USER,
            username,
        )]);
        Ok(self
            .describe_client_quotas(&filter)
            .await?
            .into_values()
            .flatten()
            .collect())
    }

    /// Applies `ops` to the user entity `{user: username}`. Returns the
    /// error the broker gave for the entity, or `None` on success. No
    /// request is sent for an empty `ops`.
    ///
    /// # Errors
    /// See [`Self::alter_client_quotas`].
    pub async fn alter_user_quotas(
        &mut self,
        username: &str,
        ops: &[QuotaOp],
        validate_only: bool,
    ) -> Result<Option<KafkaError>, AdminError> {
        if ops.is_empty() {
            return Ok(None);
        }
        let alteration = ClientQuotaAlteration {
            entity: user_entity(username),
            ops: ops.to_vec(),
        };
        Ok(self
            .alter_client_quotas(std::slice::from_ref(&alteration), validate_only)
            .await?
            .into_values()
            .next()
            .and_then(Result::err))
    }
}

fn op_to_wire(op: &QuotaOp) -> AlterOp {
    match op {
        QuotaOp::Set { key, value } => AlterOp {
            key: key.clone(),
            value: *value,
            ..Default::default()
        },
        QuotaOp::Remove { key } => AlterOp {
            key: key.clone(),
            remove: true,
            ..Default::default()
        },
    }
}

/// Pure function. It diffs the desired key-set against the current key-set and
/// produces the minimal `(set, remove)` op stream. Floats compare bit-equal,
/// so it does not re-issue a no-op `Set` with the same value.
#[must_use]
pub fn diff_user_quotas(current: &UserQuotaConfig, desired: &UserQuotaConfig) -> Vec<QuotaOp> {
    let mut ops = Vec::new();
    for (k, v) in desired {
        match current.get(k) {
            Some(cur) if cur.to_bits() == v.to_bits() => {}
            _ => ops.push(QuotaOp::Set {
                key: k.clone(),
                value: *v,
            }),
        }
    }
    for k in current.keys() {
        if !desired.contains_key(k) {
            ops.push(QuotaOp::Remove { key: k.clone() });
        }
    }
    ops
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use bytes::{Buf, BytesMut};
    use krabka_client_core::MockBroker;
    use krabka_protocol::{
        Decode, Encode, UnknownTaggedFields,
        owned::{
            alter_client_quotas_request,
            alter_client_quotas_response::{AlterClientQuotasResponse, EntityData, EntryData},
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            describe_client_quotas_request,
            describe_client_quotas_response::{
                DescribeClientQuotasResponse, EntityData as DescribeEntityData,
                EntryData as DescribeEntryData, ValueData as DescribeValueData,
            },
        },
    };

    use super::*;

    fn encode_v0(resp: &impl Encode) -> Vec<u8> {
        let mut buf = BytesMut::new();
        resp.encode(&mut buf, 0).unwrap();
        buf.to_vec()
    }

    fn api_versions_response(api_key: i16, version: i16) -> Vec<u8> {
        encode_v0(&ApiVersionsResponse {
            api_keys: vec![
                ApiVersion {
                    api_key: api_versions_request::API_KEY,
                    min_version: 0,
                    max_version: 0,
                    ..Default::default()
                },
                ApiVersion {
                    api_key,
                    min_version: version,
                    max_version: version,
                    ..Default::default()
                },
            ],
            ..Default::default()
        })
    }

    fn request_body_after_header(mut body: &[u8], flexible_header: bool) -> &[u8] {
        let client_id_len = body.get_i16();
        assert2::assert!(client_id_len >= 0);
        body.advance(usize::try_from(client_id_len).expect("client id length is non-negative"));
        if flexible_header {
            assert2::assert!(body.get_u8() == 0);
        }
        body
    }

    #[test]
    fn diff_no_change_returns_empty() {
        let mut c = UserQuotaConfig::new();
        c.insert("producer_byte_rate".into(), 1_048_576.0);
        let d = c.clone();
        assert2::assert!(diff_user_quotas(&c, &d).is_empty());
    }

    #[test]
    fn diff_set_added_keys() {
        let c = UserQuotaConfig::new();
        let mut d = UserQuotaConfig::new();
        d.insert("producer_byte_rate".into(), 1_048_576.0);
        d.insert("request_percentage".into(), 25.0);
        let ops = diff_user_quotas(&c, &d);
        // `desired` is a BTreeMap, so `Set` ops come out in key order.
        assert2::assert!(
            ops == vec![
                QuotaOp::Set {
                    key: "producer_byte_rate".to_string(),
                    value: 1_048_576.0,
                },
                QuotaOp::Set {
                    key: "request_percentage".to_string(),
                    value: 25.0,
                },
            ]
        );
    }

    #[test]
    fn diff_remove_dropped_keys() {
        let mut c = UserQuotaConfig::new();
        c.insert("producer_byte_rate".into(), 1.0);
        c.insert("consumer_byte_rate".into(), 2.0);
        let mut d = UserQuotaConfig::new();
        d.insert("producer_byte_rate".into(), 1.0);
        let ops = diff_user_quotas(&c, &d);
        assert2::assert!(
            ops == vec![QuotaOp::Remove {
                key: "consumer_byte_rate".into()
            }]
        );
    }

    #[test]
    fn diff_value_change_is_a_set() {
        let mut c = UserQuotaConfig::new();
        c.insert("producer_byte_rate".into(), 1.0);
        let mut d = UserQuotaConfig::new();
        d.insert("producer_byte_rate".into(), 2.0);
        let ops = diff_user_quotas(&c, &d);
        assert2::assert!(
            ops == vec![QuotaOp::Set {
                key: "producer_byte_rate".into(),
                value: 2.0,
            }]
        );
    }

    #[test]
    fn diff_mixed_add_change_remove() {
        let mut c = UserQuotaConfig::new();
        c.insert("producer_byte_rate".into(), 1.0);
        c.insert("consumer_byte_rate".into(), 2.0);
        let mut d = UserQuotaConfig::new();
        d.insert("producer_byte_rate".into(), 5.0); // change
        d.insert("request_percentage".into(), 25.0); // add
        // consumer_byte_rate dropped
        let ops = diff_user_quotas(&c, &d);
        // Sets come first (in `desired` key order), then removes (in
        // `current` key order) — both maps are BTreeMaps.
        assert2::assert!(
            ops == vec![
                QuotaOp::Set {
                    key: "producer_byte_rate".to_string(),
                    value: 5.0,
                },
                QuotaOp::Set {
                    key: "request_percentage".to_string(),
                    value: 25.0,
                },
                QuotaOp::Remove {
                    key: "consumer_byte_rate".to_string(),
                },
            ]
        );
    }

    #[test]
    fn op_to_wire_set() {
        let op = QuotaOp::Set {
            key: "producer_byte_rate".into(),
            value: 1.0,
        };
        let w = op_to_wire(&op);
        assert2::assert!(
            w == AlterOp {
                key: "producer_byte_rate".to_string(),
                value: 1.0,
                remove: false,
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    #[test]
    fn op_to_wire_remove_sends_zero_value_and_flag() {
        let op = QuotaOp::Remove {
            key: "producer_byte_rate".into(),
        };
        let w = op_to_wire(&op);
        assert2::assert!(
            w == AlterOp {
                key: "producer_byte_rate".to_string(),
                value: 0.0,
                remove: true,
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    fn entity(parts: &[(&str, Option<&str>)]) -> ClientQuotaEntity {
        parts
            .iter()
            .map(|(entity_type, name)| ((*entity_type).to_owned(), name.map(str::to_owned)))
            .collect()
    }

    fn component(entity_type: &str, match_type: i8, match_: Option<&str>) -> ComponentData {
        ComponentData {
            entity_type: entity_type.to_owned(),
            match_type,
            match_: match_.map(str::to_owned),
            ..Default::default()
        }
    }

    /// Kafka's `DescribeClientQuotasRequest.Builder` maps `ofEntity`,
    /// `ofDefaultEntity` and `ofEntityType` to match types 0, 1 and 2, and
    /// `containsOnly` to `strict`.
    #[test]
    fn describe_request_maps_each_filter_component() {
        for (name, filter, expected) in [
            (
                "all entities",
                ClientQuotaFilter::all(),
                DescribeClientQuotasRequest::default(),
            ),
            (
                "one user and any client id",
                ClientQuotaFilter::contains_only(vec![
                    ClientQuotaFilterComponent::of_entity(ENTITY_USER, "alice"),
                    ClientQuotaFilterComponent::of_entity_type(ENTITY_CLIENT_ID),
                ]),
                DescribeClientQuotasRequest {
                    components: vec![
                        component("user", 0, Some("alice")),
                        component("client-id", 2, None),
                    ],
                    strict: true,
                    ..Default::default()
                },
            ),
            (
                "the default ip",
                ClientQuotaFilter::contains(vec![ClientQuotaFilterComponent::of_default_entity(
                    ENTITY_IP,
                )]),
                DescribeClientQuotasRequest {
                    components: vec![component("ip", 1, None)],
                    strict: false,
                    ..Default::default()
                },
            ),
        ] {
            assert2::assert!(describe_request(&filter) == expected, "case {name}");
        }
    }

    /// Kafka's `AlterClientQuotasRequest.Builder` sends each entity with its
    /// entity types and ops, a default entity as a null name, and a removed
    /// key as `remove=true`.
    #[test]
    fn alter_request_carries_each_entity_type() {
        let alterations = [
            ClientQuotaAlteration {
                entity: entity(&[("user", Some("alice")), ("client-id", None)]),
                ops: vec![QuotaOp::Set {
                    key: "producer_byte_rate".to_owned(),
                    value: 1024.0,
                }],
            },
            ClientQuotaAlteration {
                entity: entity(&[("ip", Some("10.0.0.1"))]),
                ops: vec![QuotaOp::Remove {
                    key: "connection_creation_rate".to_owned(),
                }],
            },
        ];
        let alter_entity = |entity_type: &str, name: Option<&str>| AlterEntity {
            entity_type: entity_type.to_owned(),
            entity_name: name.map(str::to_owned),
            ..Default::default()
        };
        assert2::assert!(
            alter_request(&alterations, true)
                == AlterClientQuotasRequest {
                    entries: vec![
                        AlterEntry {
                            entity: vec![
                                alter_entity("client-id", None),
                                alter_entity("user", Some("alice")),
                            ],
                            ops: vec![AlterOp {
                                key: "producer_byte_rate".to_owned(),
                                value: 1024.0,
                                ..Default::default()
                            }],
                            ..Default::default()
                        },
                        AlterEntry {
                            entity: vec![alter_entity("ip", Some("10.0.0.1"))],
                            ops: vec![AlterOp {
                                key: "connection_creation_rate".to_owned(),
                                remove: true,
                                ..Default::default()
                            }],
                            ..Default::default()
                        },
                    ],
                    validate_only: true,
                    ..Default::default()
                }
        );
    }

    /// Kafka's `describeClientQuotas` returns the values of each entity of
    /// every type, retries a lost connection, and fails the call on a
    /// top-level error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn describe_client_quotas_maps_every_entity_type() {
        use krabka_client_core::MockReply;

        use crate::partition_leaders::test_support::{encode_response, scripted_broker};

        let entries = vec![
            DescribeEntryData {
                entity: vec![
                    DescribeEntityData {
                        entity_type: "user".into(),
                        entity_name: Some("alice".into()),
                        ..Default::default()
                    },
                    DescribeEntityData {
                        entity_type: "client-id".into(),
                        entity_name: None,
                        ..Default::default()
                    },
                ],
                values: vec![DescribeValueData {
                    key: "producer_byte_rate".into(),
                    value: 1024.0,
                    ..Default::default()
                }],
                ..Default::default()
            },
            DescribeEntryData {
                entity: vec![DescribeEntityData {
                    entity_type: "ip".into(),
                    entity_name: Some("10.0.0.1".into()),
                    ..Default::default()
                }],
                values: vec![DescribeValueData {
                    key: "connection_creation_rate".into(),
                    value: 5.0,
                    ..Default::default()
                }],
                ..Default::default()
            },
        ];
        let quotas = ClientQuotas::from([
            (
                entity(&[("user", Some("alice")), ("client-id", None)]),
                BTreeMap::from([("producer_byte_rate".to_owned(), 1024.0)]),
            ),
            (
                entity(&[("ip", Some("10.0.0.1"))]),
                BTreeMap::from([("connection_creation_rate".to_owned(), 5.0)]),
            ),
        ]);
        for (name, close_first, error_code, expected) in [
            ("success", false, 0, (Ok(quotas.clone()), 1)),
            (
                "a lost connection is retried",
                true,
                0,
                (Ok(quotas.clone()), 2),
            ),
            (
                "a top-level error fails the call",
                false,
                31,
                (Err(("DescribeClientQuotas", 31)), 1),
            ),
        ] {
            let requests = Arc::new(Mutex::new(0_usize));
            let handler_requests = Arc::clone(&requests);
            let entries = entries.clone();
            let broker = scripted_broker(
                vec![(describe_client_quotas_request::API_KEY, 0, 1)],
                move |api_key, version, _, _| {
                    if api_key != describe_client_quotas_request::API_KEY {
                        return MockReply::Silent;
                    }
                    let mut requests = handler_requests.lock().expect("requests lock");
                    *requests += 1;
                    if close_first && *requests == 1 {
                        return MockReply::Close;
                    }
                    MockReply::Respond(encode_response(
                        &DescribeClientQuotasResponse {
                            error_code,
                            entries: (error_code == 0).then(|| entries.clone()),
                            ..Default::default()
                        },
                        version,
                        true,
                    ))
                },
            )
            .await;
            let admin = AdminClient::connect(&[broker.addr.to_string()])
                .await
                .expect("admin connects");

            let result = admin
                .describe_client_quotas(&ClientQuotaFilter::all())
                .await
                .map_err(|error| match error {
                    AdminError::Broker { api, code, .. } => (api, code),
                    other => panic!("case {name}: unexpected error {other:?}"),
                });

            broker.stop();
            assert2::assert!(
                (result, *requests.lock().expect("requests lock")) == expected,
                "case {name}"
            );
        }
    }

    /// Kafka's `AlterClientQuotasResponse.complete` gives each entity its own
    /// result. An entity the answer leaves out gets `UNKNOWN_SERVER_ERROR`.
    #[test]
    fn alter_results_give_each_entity_its_result() {
        let alice = entity(&[("user", Some("alice")), ("client-id", Some("app"))]);
        let ip = entity(&[("ip", None)]);
        let alterations = [
            ClientQuotaAlteration {
                entity: alice.clone(),
                ops: Vec::new(),
            },
            ClientQuotaAlteration {
                entity: ip.clone(),
                ops: Vec::new(),
            },
        ];
        let answer = |entity_type: &str, entity_name: Option<&str>, error_code| EntryData {
            error_code,
            error_message: (error_code != 0).then(|| "bad".to_owned()),
            entity: vec![EntityData {
                entity_type: entity_type.to_owned(),
                entity_name: entity_name.map(str::to_owned),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut alice_answer = answer("user", Some("alice"), 0);
        alice_answer.entity.push(EntityData {
            entity_type: "client-id".to_owned(),
            entity_name: Some("app".to_owned()),
            ..Default::default()
        });
        let unknown = Err(KafkaError {
            code: -1,
            name: "UNKNOWN_SERVER_ERROR",
            message: Some(format!(
                "the AlterClientQuotas response did not contain a result for entity {ip:?}"
            )),
        });
        for (name, entries, expected) in [
            (
                "each entity has its result",
                vec![alice_answer.clone(), answer("ip", None, 42)],
                BTreeMap::from([
                    (alice.clone(), Ok(())),
                    (
                        ip.clone(),
                        Err(KafkaError {
                            code: 42,
                            name: "INVALID_REQUEST",
                            message: Some("bad".to_owned()),
                        }),
                    ),
                ]),
            ),
            (
                "a missing entity and an unasked entity",
                vec![alice_answer.clone(), answer("user", Some("bob"), 0)],
                BTreeMap::from([(alice.clone(), Ok(())), (ip.clone(), unknown)]),
            ),
        ] {
            let response = AlterClientQuotasResponse {
                entries,
                ..Default::default()
            };
            assert2::assert!(
                alter_results(&alterations, response) == expected,
                "case {name}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn describe_user_quotas_sends_strict_user_component() {
        let seen_request = Arc::new(Mutex::new(None));
        let captured_request = Arc::clone(&seen_request);
        let mock = MockBroker::start(move |api_key, version, _corr_id, body| {
            if api_key == api_versions_request::API_KEY {
                return Some(api_versions_response(
                    describe_client_quotas_request::API_KEY,
                    0,
                ));
            }
            if api_key == describe_client_quotas_request::API_KEY {
                let mut body = request_body_after_header(
                    body,
                    version >= describe_client_quotas_request::FLEXIBLE_MIN,
                );
                let request = DescribeClientQuotasRequest::decode(&mut body, version)
                    .expect("describe quotas request decodes");
                assert2::assert!(body.is_empty());
                *captured_request.lock().expect("request capture lock") = Some(request);
                return Some(encode_v0(&DescribeClientQuotasResponse {
                    entries: Some(vec![DescribeEntryData {
                        entity: vec![DescribeEntityData {
                            entity_type: "user".into(),
                            entity_name: Some("alice".into()),
                            ..Default::default()
                        }],
                        values: vec![DescribeValueData {
                            key: "producer_byte_rate".into(),
                            value: 1024.0,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }]),
                    ..Default::default()
                }));
            }
            None
        })
        .await;
        let mut admin = AdminClient::connect(&[mock.addr.to_string()])
            .await
            .expect("admin connects to mock broker");

        let quotas = admin
            .describe_user_quotas("alice")
            .await
            .expect("describe quotas response maps");

        assert2::assert!(quotas.get("producer_byte_rate") == Some(&1024.0));
        let request = seen_request
            .lock()
            .expect("request capture lock")
            .take()
            .expect("describe quotas request was captured");
        assert2::assert!(
            request
                == DescribeClientQuotasRequest {
                    components: vec![ComponentData {
                        entity_type: "user".into(),
                        match_type: MATCH_TYPE_EXACT,
                        match_: Some("alice".into()),
                        ..Default::default()
                    }],
                    strict: true,
                    ..Default::default()
                }
        );
        mock.stop();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn alter_user_quotas_surfaces_broker_entry_error() {
        let seen_request = Arc::new(Mutex::new(None));
        let captured_request = Arc::clone(&seen_request);
        let mock = MockBroker::start(move |api_key, version, _corr_id, body| {
            if api_key == api_versions_request::API_KEY {
                return Some(api_versions_response(
                    alter_client_quotas_request::API_KEY,
                    0,
                ));
            }
            if api_key == alter_client_quotas_request::API_KEY {
                let mut body = request_body_after_header(
                    body,
                    version >= alter_client_quotas_request::FLEXIBLE_MIN,
                );
                let request = AlterClientQuotasRequest::decode(&mut body, version)
                    .expect("alter quotas request decodes");
                assert2::assert!(body.is_empty());
                *captured_request.lock().expect("request capture lock") = Some(request);
                return Some(encode_v0(&AlterClientQuotasResponse {
                    entries: vec![EntryData {
                        error_code: 40,
                        error_message: Some("invalid quota".into()),
                        entity: vec![EntityData {
                            entity_type: "user".into(),
                            entity_name: Some("alice".into()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }));
            }
            None
        })
        .await;
        let mut admin = AdminClient::connect(&[mock.addr.to_string()])
            .await
            .expect("admin connects to mock broker");

        let error = admin
            .alter_user_quotas(
                "alice",
                &[
                    QuotaOp::Set {
                        key: "producer_byte_rate".into(),
                        value: 1024.0,
                    },
                    QuotaOp::Remove {
                        key: "consumer_byte_rate".into(),
                    },
                ],
                true,
            )
            .await
            .expect("alter quotas maps broker entry")
            .expect("non-zero broker error is returned");

        assert2::assert!((error.code, error.message.as_deref()) == (40, Some("invalid quota")));
        let request = seen_request
            .lock()
            .expect("request capture lock")
            .take()
            .expect("alter quotas request was captured");
        assert2::assert!(
            request
                == AlterClientQuotasRequest {
                    entries: vec![AlterEntry {
                        entity: vec![AlterEntity {
                            entity_type: "user".into(),
                            entity_name: Some("alice".into()),
                            ..Default::default()
                        }],
                        ops: vec![
                            AlterOp {
                                key: "producer_byte_rate".into(),
                                value: 1024.0,
                                ..Default::default()
                            },
                            AlterOp {
                                key: "consumer_byte_rate".into(),
                                remove: true,
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    }],
                    validate_only: true,
                    ..Default::default()
                }
        );
        mock.stop();
    }
}
