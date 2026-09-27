//! Config resource listing (`ListConfigResources`, KIP-1142), as Kafka's
//! `Admin.listConfigResources` and `Admin.listClientMetricsResources`.
//!
//! `ListConfigResources` v0 is the former `ListClientMetricsResources`
//! (KIP-714): it takes no resource types and lists client-metrics
//! subscriptions only. v1 adds the resource types.

use std::collections::BTreeSet;

use krabka_protocol::owned::{
    list_config_resources_request::ListConfigResourcesRequest,
    list_config_resources_response::ListConfigResourcesResponse,
};

use crate::{AdminClient, AdminError, kafka_error_name, retry::ControllerRetry};

/// The type of a config resource, as Kafka's `ConfigResource.Type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ConfigResourceType {
    /// A type that Kafka does not define (`UNKNOWN`, id 0 or any other id).
    Unknown,
    /// A topic (id 2).
    Topic,
    /// A broker (id 4).
    Broker,
    /// The loggers of a broker (id 8).
    BrokerLogger,
    /// A client-metrics subscription (id 16, KIP-714).
    ClientMetrics,
    /// A group (id 32, KIP-848).
    Group,
}

impl ConfigResourceType {
    /// The wire id of the type.
    #[must_use]
    pub const fn id(self) -> i8 {
        match self {
            Self::Unknown => 0,
            Self::Topic => 2,
            Self::Broker => 4,
            Self::BrokerLogger => 8,
            Self::ClientMetrics => 16,
            Self::Group => 32,
        }
    }

    /// The type of a wire id, [`Self::Unknown`] for an id Kafka does not
    /// define, as `ConfigResource.Type.forId`.
    #[must_use]
    pub const fn from_id(id: i8) -> Self {
        match id {
            2 => Self::Topic,
            4 => Self::Broker,
            8 => Self::BrokerLogger,
            16 => Self::ClientMetrics,
            32 => Self::Group,
            _ => Self::Unknown,
        }
    }
}

/// One config resource, as Kafka's `ConfigResource`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConfigResourceListing {
    /// The type of the resource.
    pub resource_type: ConfigResourceType,
    /// The name of the resource.
    pub name: String,
}

/// Whether a request for `types` can go as `ListConfigResources` v0, which
/// Kafka's `ListConfigResourcesRequest.Builder` allows for exactly
/// `{CLIENT_METRICS}`.
fn fits_v0(types: &BTreeSet<ConfigResourceType>) -> bool {
    types.len() == 1 && types.contains(&ConfigResourceType::ClientMetrics)
}

fn list_request(types: &BTreeSet<ConfigResourceType>) -> ListConfigResourcesRequest {
    ListConfigResourcesRequest {
        resource_types: types.iter().map(|kind| kind.id()).collect(),
        ..Default::default()
    }
}

/// The resources of an answer, as Kafka's `ListConfigResourcesResponse`
/// reads it. A top-level error fails the call.
fn listings(
    response: ListConfigResourcesResponse,
) -> Result<Vec<ConfigResourceListing>, AdminError> {
    if response.error_code != 0 {
        return Err(AdminError::Broker {
            api: "ListConfigResources",
            code: response.error_code,
            name: kafka_error_name(response.error_code),
            message: None,
        });
    }
    Ok(response
        .config_resources
        .into_iter()
        .map(|resource| ConfigResourceListing {
            resource_type: ConfigResourceType::from_id(resource.resource_type),
            name: resource.resource_name,
        })
        .collect())
}

impl AdminClient {
    /// Lists the config resources of `types`, as Kafka's
    /// `listConfigResources` operation does. An empty set asks for every type
    /// the broker supports.
    ///
    /// The client sends one `ListConfigResources` request on its connection.
    /// Exactly `{ClientMetrics}` may go as v0, as Kafka's builder allows;
    /// every other set needs v1 (KIP-1142).
    ///
    /// # Errors
    /// Returns [`AdminError::Broker`] for a top-level error, such as
    /// `CLUSTER_AUTHORIZATION_FAILED` (31) or `UNSUPPORTED_VERSION` (35) for
    /// a type the broker does not list, and with `REQUEST_TIMED_OUT` (7) at
    /// `default.api.timeout.ms` (60 s). Returns [`AdminError::Transport`]
    /// when the connection fails, or when the broker supports only v0 and
    /// `types` is not `{ClientMetrics}`.
    pub async fn list_config_resources(
        &self,
        types: &BTreeSet<ConfigResourceType>,
    ) -> Result<Vec<ConfigResourceListing>, AdminError> {
        let request = list_request(types);
        let retry = ControllerRetry::new("ListConfigResources", self.retry);
        let response = if fits_v0(types) {
            retry.bounded(self.conn.send(request)).await?
        } else {
            retry.bounded(self.conn.send_at_least(request, 1)).await?
        };
        listings(response)
    }

    /// Lists the names of the client-metrics subscriptions (KIP-714), as
    /// Kafka's `listClientMetricsResources` operation does: a
    /// `ListConfigResources` request for `{ClientMetrics}`, keeping only the
    /// resources of that type.
    ///
    /// # Errors
    /// See [`Self::list_config_resources`].
    pub async fn list_client_metrics_resources(&self) -> Result<Vec<String>, AdminError> {
        let types = BTreeSet::from([ConfigResourceType::ClientMetrics]);
        Ok(self
            .list_config_resources(&types)
            .await?
            .into_iter()
            .filter(|listing| listing.resource_type == ConfigResourceType::ClientMetrics)
            .map(|listing| listing.name)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use krabka_client_core::{ClientError, MockReply};
    use krabka_protocol::owned::{
        list_config_resources_request, list_config_resources_response::ConfigResource,
    };

    use super::*;
    use crate::partition_leaders::test_support::{
        decode_request, encode_response, fast_admin, scripted_broker,
    };

    #[test]
    fn resource_type_ids_are_kafkas() {
        for (kind, id) in [
            (ConfigResourceType::Unknown, 0),
            (ConfigResourceType::Topic, 2),
            (ConfigResourceType::Broker, 4),
            (ConfigResourceType::BrokerLogger, 8),
            (ConfigResourceType::ClientMetrics, 16),
            (ConfigResourceType::Group, 32),
        ] {
            assert2::assert!((kind.id(), ConfigResourceType::from_id(id)) == (id, kind));
        }
        for id in [1, 3, 64, -1] {
            assert2::assert!(ConfigResourceType::from_id(id) == ConfigResourceType::Unknown);
        }
    }

    fn resource(resource_type: i8, name: &str) -> ConfigResource {
        ConfigResource {
            resource_name: name.to_owned(),
            resource_type,
            ..Default::default()
        }
    }

    /// What one `ListConfigResources` call did: its result, and the version
    /// and request of each request the broker got.
    type Outcome<T> = (Result<T, String>, Vec<(i16, ListConfigResourcesRequest)>);

    /// Runs `call` against a broker that advertises `ListConfigResources`
    /// `min..=max` and gives the `n`th request `answers[n]`: a `Close`, or an
    /// error code with the resources of `resources`.
    async fn run<T, F>(
        max_version: i16,
        answers: Vec<Option<i16>>,
        resources: Vec<ConfigResource>,
        call: F,
    ) -> Outcome<T>
    where
        F: AsyncFnOnce(&AdminClient) -> Result<T, AdminError>,
    {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let handler_requests = Arc::clone(&requests);
        let broker = scripted_broker(
            vec![(list_config_resources_request::API_KEY, 0, max_version)],
            move |api_key, version, body, _| {
                if api_key != list_config_resources_request::API_KEY {
                    return MockReply::Silent;
                }
                let mut requests = handler_requests.lock().expect("requests lock");
                let answer = answers[requests.len().min(answers.len() - 1)];
                let request: ListConfigResourcesRequest = decode_request(body, version, true);
                requests.push((version, request));
                let Some(error_code) = answer else {
                    return MockReply::Close;
                };
                MockReply::Respond(encode_response(
                    &ListConfigResourcesResponse {
                        error_code,
                        config_resources: resources.clone(),
                        ..Default::default()
                    },
                    version,
                    true,
                ))
            },
        )
        .await;
        let admin = fast_admin(broker.addr, krabka_units::secs(5)).await;
        let result = call(&admin).await.map_err(|error| match error {
            AdminError::Broker { code, .. } => format!("broker {code}"),
            AdminError::Transport(ClientError::IncompatibleVersion { .. }) => {
                "incompatible version".to_owned()
            }
            other => panic!("unexpected error {other:?}"),
        });
        broker.stop();
        let requests = requests.lock().expect("requests lock").clone();
        (result, requests)
    }

    fn request(types: &[i8]) -> ListConfigResourcesRequest {
        ListConfigResourcesRequest {
            resource_types: types.to_vec(),
            ..Default::default()
        }
    }

    /// Kafka's `listConfigResources` sends the type ids, maps each resource
    /// with `ConfigResource.Type.forId`, retries a lost connection, fails the
    /// call on a top-level error, and needs v1 for any set other than
    /// `{CLIENT_METRICS}`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn list_config_resources_maps_types_and_gates_v0() {
        let listed = vec![
            ConfigResourceListing {
                resource_type: ConfigResourceType::Topic,
                name: "orders".to_owned(),
            },
            ConfigResourceListing {
                resource_type: ConfigResourceType::Group,
                name: "billing".to_owned(),
            },
            ConfigResourceListing {
                resource_type: ConfigResourceType::Unknown,
                name: "future".to_owned(),
            },
        ];
        let resources = vec![
            resource(2, "orders"),
            resource(32, "billing"),
            resource(64, "future"),
        ];
        let topic_and_group =
            BTreeSet::from([ConfigResourceType::Topic, ConfigResourceType::Group]);
        for (name, max_version, types, answers, expected) in [
            (
                "v1 sends the types",
                1,
                topic_and_group.clone(),
                vec![Some(0)],
                (Ok(listed.clone()), vec![(1, request(&[2, 32]))]),
            ),
            (
                "an empty set asks for every type",
                1,
                BTreeSet::new(),
                vec![Some(0)],
                (Ok(listed.clone()), vec![(1, request(&[]))]),
            ),
            (
                "a lost connection is retried",
                1,
                topic_and_group.clone(),
                vec![None, Some(0)],
                (
                    Ok(listed.clone()),
                    vec![(1, request(&[2, 32])), (1, request(&[2, 32]))],
                ),
            ),
            (
                "a top-level error fails the call",
                1,
                topic_and_group.clone(),
                vec![Some(31)],
                (Err("broker 31".to_owned()), vec![(1, request(&[2, 32]))]),
            ),
            (
                "a v0 broker cannot list other types",
                0,
                topic_and_group,
                vec![Some(0)],
                (Err("incompatible version".to_owned()), vec![]),
            ),
        ] {
            let outcome = run(max_version, answers, resources.clone(), async |admin| {
                admin.list_config_resources(&types).await
            })
            .await;
            assert2::assert!(outcome == expected, "case {name}");
        }
    }

    /// Kafka's `listClientMetricsResources` asks for `{CLIENT_METRICS}`,
    /// which a v0 broker serves too, and keeps only that type.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn list_client_metrics_resources_keeps_client_metrics_only() {
        let resources = vec![resource(16, "all-metrics"), resource(2, "orders")];
        for (name, max_version) in [("v0 broker", 0), ("v1 broker", 1)] {
            let outcome = run(
                max_version,
                vec![Some(0)],
                resources.clone(),
                async |admin| admin.list_client_metrics_resources().await,
            )
            .await;
            // A v0 request carries no types, so it decodes as an empty list.
            let sent = if max_version == 0 {
                request(&[])
            } else {
                request(&[16])
            };
            let expected = if max_version == 0 {
                // A v0 answer has no type field, so each resource reads as the
                // schema default, CLIENT_METRICS.
                vec!["all-metrics".to_owned(), "orders".to_owned()]
            } else {
                vec!["all-metrics".to_owned()]
            };
            assert2::assert!(
                outcome == (Ok(expected), vec![(max_version, sent)]),
                "case {name}"
            );
        }
    }
}
