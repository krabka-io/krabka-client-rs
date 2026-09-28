//! [`AdminClient::list_consumer_groups`], Apache Kafka's older
//! `listConsumerGroups`.
//!
//! Kafka's `listConsumerGroups` sends `ListGroups` to every broker with the
//! state and type filters of its options, and keeps each listed group whose
//! protocol type is `consumer` or empty. That is
//! [`AdminClient::list_groups`] with the protocol types `""` and
//! `"consumer"`, so this call runs it and reads each listing as Kafka's
//! `ConsumerGroupListing`.

use std::collections::BTreeSet;

use crate::{
    AdminClient, AdminError,
    groups::{GroupListing, GroupState, GroupType, ListGroupsError, ListGroupsOptions},
};

/// The protocol type of a classic consumer group
/// (`ConsumerProtocol.PROTOCOL_TYPE`).
const CONSUMER_PROTOCOL_TYPE: &str = "consumer";

/// The options of [`AdminClient::list_consumer_groups`], as Apache Kafka's
/// `ListConsumerGroupsOptions` holds them. An empty set does not filter.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListConsumerGroupsOptions {
    /// The broker lists only groups in these states (`states_filter`).
    pub group_states: BTreeSet<GroupState>,
    /// The broker lists only groups of these types (`types_filter`).
    pub types: BTreeSet<GroupType>,
}

/// One group of [`AdminClient::list_consumer_groups`], as Apache Kafka's
/// `ConsumerGroupListing` holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupListing {
    pub group_id: String,
    /// Whether the group has no protocol type.
    pub is_simple_consumer_group: bool,
    /// `None` when the broker sends no state (`ListGroups` v3 or lower).
    pub group_state: Option<GroupState>,
    /// `None` when the broker sends no type (`ListGroups` v4 or lower).
    pub group_type: Option<GroupType>,
}

impl From<GroupListing> for ConsumerGroupListing {
    fn from(listing: GroupListing) -> Self {
        Self {
            is_simple_consumer_group: listing.protocol_type.is_empty(),
            group_id: listing.group_id,
            group_state: listing.group_state,
            group_type: listing.group_type,
        }
    }
}

/// The result of [`AdminClient::list_consumer_groups`], as Apache Kafka's
/// `ListConsumerGroupsResult` holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListConsumerGroupsResult {
    /// The consumer groups of every broker that answered, one entry for each
    /// group id, in group id order.
    pub valid: Vec<ConsumerGroupListing>,
    /// One entry for each broker that failed, in metadata order.
    pub errors: Vec<ListGroupsError>,
}

impl ListConsumerGroupsResult {
    /// The groups when no broker failed, as Kafka's
    /// `ListConsumerGroupsResult.all` gives them.
    ///
    /// # Errors
    /// Returns the first broker failure.
    pub fn all(self) -> Result<Vec<ConsumerGroupListing>, ListGroupsError> {
        match self.errors.into_iter().next() {
            Some(error) => Err(error),
            None => Ok(self.valid),
        }
    }
}

impl AdminClient {
    /// Lists the consumer groups of the whole cluster, as Apache Kafka's
    /// `listConsumerGroups` does.
    ///
    /// The call sends `ListGroups` to every broker with the state and type
    /// filters of `options`, and keeps each group whose protocol type is
    /// `consumer` or empty. See [`AdminClient::list_groups`] for the fan-out,
    /// the retries, the per-broker errors and the version that each filter
    /// needs.
    ///
    /// # Errors
    /// Returns an error when the `Metadata` request fails, or when the
    /// metadata names no broker after the timeout. A failure on one broker is
    /// an entry in [`ListConsumerGroupsResult::errors`].
    pub async fn list_consumer_groups(
        &self,
        options: &ListConsumerGroupsOptions,
    ) -> Result<ListConsumerGroupsResult, AdminError> {
        let result = self
            .list_groups(&ListGroupsOptions {
                group_states: options.group_states.clone(),
                types: options.types.clone(),
                protocol_types: BTreeSet::from([String::new(), CONSUMER_PROTOCOL_TYPE.to_owned()]),
            })
            .await?;
        Ok(ListConsumerGroupsResult {
            valid: result
                .valid
                .into_iter()
                .map(ConsumerGroupListing::from)
                .collect(),
            errors: result.errors,
        })
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_client_core::MockReply;
    use krabka_protocol::owned::{
        list_groups_request::{self, ListGroupsRequest},
        list_groups_response::{ListGroupsResponse, ListedGroup},
        metadata_request,
        metadata_response::{MetadataResponse, MetadataResponseBroker},
    };

    use super::*;
    use crate::partition_leaders::test_support::{
        decode_request, encode_response, fast_admin, scripted_broker,
    };

    fn listed(group_id: &str, protocol_type: &str, group_type: &str) -> ListedGroup {
        ListedGroup {
            group_id: group_id.to_owned(),
            protocol_type: protocol_type.to_owned(),
            group_state: "Stable".to_owned(),
            group_type: group_type.to_owned(),
            ..Default::default()
        }
    }

    /// Kafka's `listConsumerGroups` sends the state and type filters and
    /// keeps the groups whose protocol type is `consumer` or empty, marking a
    /// group with no protocol type as simple.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn list_consumer_groups_keeps_consumer_protocol_groups() {
        let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = std::sync::Arc::clone(&requests);
        let broker = scripted_broker(
            vec![
                (list_groups_request::API_KEY, 0, 5),
                (metadata_request::API_KEY, 12, 12),
            ],
            move |api_key, version, body, own| match api_key {
                metadata_request::API_KEY => MockReply::Respond(encode_response(
                    &MetadataResponse {
                        brokers: vec![MetadataResponseBroker {
                            node_id: 1,
                            host: own.ip().to_string(),
                            port: i32::from(own.port()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    version,
                    true,
                )),
                list_groups_request::API_KEY => {
                    let flexible = version >= list_groups_request::FLEXIBLE_MIN;
                    let request: ListGroupsRequest = decode_request(body, version, flexible);
                    seen.lock().expect("requests lock").push((version, request));
                    MockReply::Respond(encode_response(
                        &ListGroupsResponse {
                            groups: vec![
                                listed("kip848", "consumer", "Consumer"),
                                listed("simple", "", "Classic"),
                                listed("connect", "connect", "Classic"),
                                listed("share", "share", "Share"),
                            ],
                            ..Default::default()
                        },
                        version,
                        flexible,
                    ))
                }
                _ => MockReply::Silent,
            },
        )
        .await;
        let admin = fast_admin(broker.addr, krabka_units::secs(5)).await;

        let result = admin
            .list_consumer_groups(&ListConsumerGroupsOptions {
                group_states: BTreeSet::from([GroupState::Stable]),
                types: BTreeSet::new(),
            })
            .await
            .expect("list_consumer_groups succeeds");

        broker.stop();
        let listing = |group_id: &str, is_simple_consumer_group, group_type| ConsumerGroupListing {
            group_id: group_id.to_owned(),
            is_simple_consumer_group,
            group_state: Some(GroupState::Stable),
            group_type: Some(group_type),
        };
        assert!(
            (result, requests.lock().expect("requests lock").clone())
                == (
                    ListConsumerGroupsResult {
                        valid: vec![
                            listing("kip848", false, GroupType::Consumer),
                            listing("simple", true, GroupType::Classic),
                        ],
                        errors: Vec::new(),
                    },
                    vec![(
                        5,
                        ListGroupsRequest {
                            states_filter: vec!["Stable".to_owned()],
                            ..Default::default()
                        }
                    )],
                )
        );
    }
}
