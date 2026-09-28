//! Config reads and changes of any config resource, as Kafka's
//! `Admin.describeConfigs` and `Admin.incrementalAlterConfigs`.
//!
//! Both calls route each resource as `KafkaAdminClient` does (`nodeFor`). A
//! `BROKER` resource that names a broker id, and every `BROKER_LOGGER`
//! resource, go to that node in a request of their own
//! (`ConstantNodeIdProvider`). Every other resource, including the empty-name
//! `BROKER` resource of the cluster-wide defaults, goes in one request on the
//! client's connection (`LeastLoadedBrokerOrActiveKController`). With a
//! controller bootstrap (KIP-919), `incrementalAlterConfigs` sends a `BROKER`
//! resource with the others to the active controller too.
//!
//! Each resource gets a result of its own: its config or change, or the
//! Kafka error of the resource or of the request that carried it.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use krabka_protocol::owned::{
    describe_configs_request::{DescribeConfigsRequest, DescribeConfigsResource},
    describe_configs_response::{
        DescribeConfigsResourceResult, DescribeConfigsResponse, DescribeConfigsResult,
    },
    incremental_alter_configs_request::{
        AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
    },
    incremental_alter_configs_response::IncrementalAlterConfigsResponse,
};

use crate::{
    AdminClient, AdminError, KafkaError, NOT_CONTROLLER,
    config_resources::{ConfigResource, ConfigResourceType},
    groups::list_groups_kafka_error,
    kafka_error_name,
    log_dirs::{NodeTarget, call_node},
    retry::{
        ControllerRetry, CoordinatorRetry, REQUEST_TIMED_OUT, call_timeout_error,
        is_connection_failure,
    },
};

/// `UNKNOWN_SERVER_ERROR`: Kafka's error for an exception that is not an
/// `ApiException`.
const UNKNOWN_SERVER_ERROR: i16 = -1;
/// `NOT_LEADER_OR_FOLLOWER`, which a follower controller gives (KIP-919).
const NOT_LEADER_OR_FOLLOWER: i16 = 6;
/// The first `DescribeConfigs` version that carries `IncludeDocumentation`.
const DESCRIBE_CONFIGS_DOCUMENTATION_VERSION: i16 = 3;

impl ConfigResource {
    /// The resource of `resource_type` named `name`.
    #[must_use]
    pub fn new(resource_type: ConfigResourceType, name: impl Into<String>) -> Self {
        Self {
            resource_type,
            name: name.into(),
        }
    }

    /// The topic `name`.
    #[must_use]
    pub fn topic(name: impl Into<String>) -> Self {
        Self::new(ConfigResourceType::Topic, name)
    }

    /// The broker `broker_id`.
    #[must_use]
    pub fn broker(broker_id: i32) -> Self {
        Self::new(ConfigResourceType::Broker, broker_id.to_string())
    }

    /// The cluster-wide default broker config: the `BROKER` resource with an
    /// empty name.
    #[must_use]
    pub fn broker_default() -> Self {
        Self::new(ConfigResourceType::Broker, "")
    }

    /// The loggers of broker `broker_id`.
    #[must_use]
    pub fn broker_logger(broker_id: i32) -> Self {
        Self::new(ConfigResourceType::BrokerLogger, broker_id.to_string())
    }

    /// The client-metrics subscription `name` (KIP-714).
    #[must_use]
    pub fn client_metrics(name: impl Into<String>) -> Self {
        Self::new(ConfigResourceType::ClientMetrics, name)
    }

    /// The group `name` (KIP-848).
    #[must_use]
    pub fn group(name: impl Into<String>) -> Self {
        Self::new(ConfigResourceType::Group, name)
    }

    /// Whether the resource is a default one, that is has an empty name, as
    /// Kafka's `ConfigResource.isDefault`.
    #[must_use]
    pub fn is_default(&self) -> bool {
        self.name.is_empty()
    }

    /// The source of the configs that are set on this resource itself, as
    /// `kafka-configs --describe` filters them: `DYNAMIC_TOPIC_CONFIG` for a
    /// topic, `DYNAMIC_BROKER_CONFIG` for a broker,
    /// `DYNAMIC_DEFAULT_BROKER_CONFIG` for the default broker,
    /// `DYNAMIC_BROKER_LOGGER_CONFIG` for broker loggers,
    /// `DYNAMIC_CLIENT_METRICS_CONFIG` and `DYNAMIC_GROUP_CONFIG`. `None` for
    /// an unknown type.
    #[must_use]
    pub fn dynamic_config_source(&self) -> Option<ConfigSource> {
        match self.resource_type {
            ConfigResourceType::Topic => Some(ConfigSource::DynamicTopicConfig),
            ConfigResourceType::Broker if self.is_default() => {
                Some(ConfigSource::DynamicDefaultBrokerConfig)
            }
            ConfigResourceType::Broker => Some(ConfigSource::DynamicBrokerConfig),
            ConfigResourceType::BrokerLogger => Some(ConfigSource::DynamicBrokerLoggerConfig),
            ConfigResourceType::ClientMetrics => Some(ConfigSource::DynamicClientMetricsConfig),
            ConfigResourceType::Group => Some(ConfigSource::DynamicGroupConfig),
            ConfigResourceType::Unknown => None,
        }
    }
}

/// Where the value of a config comes from, as Kafka's
/// `ConfigEntry.ConfigSource`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ConfigSource {
    /// Set on the topic (wire id 1).
    DynamicTopicConfig,
    /// Set on the loggers of the broker (wire id 6).
    DynamicBrokerLoggerConfig,
    /// Set on the broker (wire id 2).
    DynamicBrokerConfig,
    /// Set as the default of every broker of the cluster (wire id 3).
    DynamicDefaultBrokerConfig,
    /// Set on the client-metrics subscription (wire id 7, KIP-714).
    DynamicClientMetricsConfig,
    /// Set on the group (wire id 8, KIP-848).
    DynamicGroupConfig,
    /// The broker's static config, such as `server.properties` (wire id 4).
    StaticBrokerConfig,
    /// The built-in default (wire id 5).
    DefaultConfig,
    /// A source that Kafka does not define (wire id 0, or an id above 8).
    Unknown,
}

impl ConfigSource {
    /// The wire id of the source, as `DescribeConfigsResponse.ConfigSource`.
    #[must_use]
    pub const fn id(self) -> i8 {
        match self {
            Self::Unknown => 0,
            Self::DynamicTopicConfig => 1,
            Self::DynamicBrokerConfig => 2,
            Self::DynamicDefaultBrokerConfig => 3,
            Self::StaticBrokerConfig => 4,
            Self::DefaultConfig => 5,
            Self::DynamicBrokerLoggerConfig => 6,
            Self::DynamicClientMetricsConfig => 7,
            Self::DynamicGroupConfig => 8,
        }
    }

    /// The source of a wire id, as `DescribeConfigsResponse.ConfigSource.forId`:
    /// [`Self::Unknown`] for an id above 8, and `None` for a negative id,
    /// which Kafka refuses.
    #[must_use]
    pub const fn from_id(id: i8) -> Option<Self> {
        Some(match id {
            i8::MIN..=-1 => return None,
            1 => Self::DynamicTopicConfig,
            2 => Self::DynamicBrokerConfig,
            3 => Self::DynamicDefaultBrokerConfig,
            4 => Self::StaticBrokerConfig,
            5 => Self::DefaultConfig,
            6 => Self::DynamicBrokerLoggerConfig,
            7 => Self::DynamicClientMetricsConfig,
            8 => Self::DynamicGroupConfig,
            _ => Self::Unknown,
        })
    }
}

/// The data type of a config, as Kafka's `ConfigEntry.ConfigType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ConfigType {
    /// Not reported (`DescribeConfigs` below v3) or not defined (wire id 0,
    /// or an id above 9).
    Unknown,
    /// Wire id 1.
    Boolean,
    /// Wire id 2.
    String,
    /// Wire id 3.
    Int,
    /// Wire id 4.
    Short,
    /// Wire id 5.
    Long,
    /// Wire id 6.
    Double,
    /// Wire id 7.
    List,
    /// Wire id 8.
    Class,
    /// Wire id 9.
    Password,
}

impl ConfigType {
    /// The wire id of the type, as `DescribeConfigsResponse.ConfigType`.
    #[must_use]
    pub const fn id(self) -> i8 {
        match self {
            Self::Unknown => 0,
            Self::Boolean => 1,
            Self::String => 2,
            Self::Int => 3,
            Self::Short => 4,
            Self::Long => 5,
            Self::Double => 6,
            Self::List => 7,
            Self::Class => 8,
            Self::Password => 9,
        }
    }

    /// The type of a wire id, as `DescribeConfigsResponse.ConfigType.forId`:
    /// [`Self::Unknown`] for an id above 9, and `None` for a negative id,
    /// which Kafka refuses.
    #[must_use]
    pub const fn from_id(id: i8) -> Option<Self> {
        Some(match id {
            i8::MIN..=-1 => return None,
            1 => Self::Boolean,
            2 => Self::String,
            3 => Self::Int,
            4 => Self::Short,
            5 => Self::Long,
            6 => Self::Double,
            7 => Self::List,
            8 => Self::Class,
            9 => Self::Password,
            _ => Self::Unknown,
        })
    }
}

/// One value that a config takes from one source, as Kafka's
/// `ConfigEntry.ConfigSynonym`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigSynonym {
    /// The name of the config at that source, such as `log.retention.ms`
    /// for the topic config `retention.ms`.
    pub name: String,
    /// The value at that source. `None` when the broker withholds it, as it
    /// does for a sensitive config.
    pub value: Option<String>,
    /// The source.
    pub source: ConfigSource,
}

/// One config of a resource, as Kafka's `ConfigEntry`.
///
/// `Debug` prints a sensitive value as `Redacted`, as Kafka's `toString`
/// does.
#[derive(Clone, PartialEq, Eq)]
pub struct ConfigEntry {
    /// The name of the config.
    pub name: String,
    /// The value, as the broker gave it. The broker gives `None` for a
    /// sensitive config and for a config with no value.
    pub value: Option<String>,
    /// Where the value comes from.
    pub source: ConfigSource,
    /// Whether the config is sensitive, such as a password.
    pub is_sensitive: bool,
    /// Whether the config cannot be changed.
    pub is_read_only: bool,
    /// The value of the config at each source, most specific first. Empty
    /// unless [`DescribeConfigsOptions::include_synonyms`] is set.
    pub synonyms: Vec<ConfigSynonym>,
    /// The data type. [`ConfigType::Unknown`] below `DescribeConfigs` v3.
    pub config_type: ConfigType,
    /// The documentation. `None` unless
    /// [`DescribeConfigsOptions::include_documentation`] is set.
    pub documentation: Option<String>,
}

impl ConfigEntry {
    /// Whether the value is the built-in default, as Kafka's
    /// `ConfigEntry.isDefault`.
    #[must_use]
    pub fn is_default(&self) -> bool {
        self.source == ConfigSource::DefaultConfig
    }
}

impl fmt::Debug for ConfigEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value: &dyn fmt::Debug = if self.is_sensitive {
            &"Redacted"
        } else {
            &self.value
        };
        f.debug_struct("ConfigEntry")
            .field("name", &self.name)
            .field("value", value)
            .field("source", &self.source)
            .field("is_sensitive", &self.is_sensitive)
            .field("is_read_only", &self.is_read_only)
            .field("synonyms", &self.synonyms)
            .field("config_type", &self.config_type)
            .field("documentation", &self.documentation)
            .finish()
    }
}

/// The configs of one resource, as Kafka's `Config`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Config {
    /// Each config by name. A name the broker lists twice keeps its last
    /// entry, as Kafka's map does.
    pub entries: BTreeMap<String, ConfigEntry>,
}

impl Config {
    /// The config `name`.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&ConfigEntry> {
        self.entries.get(name)
    }

    /// The name and value of each config that is set on `resource` itself:
    /// the entries whose source is [`ConfigResource::dynamic_config_source`]
    /// and that have a value. This is what `kafka-configs --describe` lists
    /// without `--all`, less the sensitive configs, whose value the broker
    /// withholds.
    #[must_use]
    pub fn dynamic_overrides(&self, resource: &ConfigResource) -> BTreeMap<String, String> {
        let Some(source) = resource.dynamic_config_source() else {
            return BTreeMap::new();
        };
        self.entries
            .values()
            .filter(|entry| entry.source == source)
            .filter_map(|entry| Some((entry.name.clone(), entry.value.clone()?)))
            .collect()
    }
}

/// What [`AdminClient::describe_configs`] asks for, as Kafka's
/// `DescribeConfigsOptions`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DescribeConfigsOptions {
    /// Ask for the value of each config at each source
    /// ([`ConfigEntry::synonyms`]).
    pub include_synonyms: bool,
    /// Ask for the documentation of each config. Needs `DescribeConfigs` v3.
    pub include_documentation: bool,
}

/// What an [`AlterConfigOp`] does, as Kafka's `AlterConfigOp.OpType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AlterConfigOpType {
    /// Set the value (wire id 0).
    Set,
    /// Revert to the default value (wire id 1).
    Delete,
    /// Add the values to a list config (wire id 2).
    Append,
    /// Remove the values from a list config (wire id 3).
    Subtract,
}

impl AlterConfigOpType {
    /// The wire id of the operation.
    #[must_use]
    pub const fn id(self) -> i8 {
        match self {
            Self::Set => 0,
            Self::Delete => 1,
            Self::Append => 2,
            Self::Subtract => 3,
        }
    }
}

/// One change of one config, as Kafka's `AlterConfigOp`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConfigOp {
    /// The name of the config.
    pub name: String,
    /// The value that the operation sends. A `Delete` needs none.
    pub value: Option<String>,
    /// The operation.
    pub op_type: AlterConfigOpType,
}

impl AlterConfigOp {
    /// Set `name` to `value`.
    #[must_use]
    pub fn set(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: Some(value.into()),
            op_type: AlterConfigOpType::Set,
        }
    }

    /// Revert `name` to its default value. The operation sends no value.
    #[must_use]
    pub fn delete(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: None,
            op_type: AlterConfigOpType::Delete,
        }
    }

    /// Add the comma-separated `values` to the list config `name`.
    #[must_use]
    pub fn append(name: impl Into<String>, values: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: Some(values.into()),
            op_type: AlterConfigOpType::Append,
        }
    }

    /// Remove the comma-separated `values` from the list config `name`.
    #[must_use]
    pub fn subtract(name: impl Into<String>, values: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: Some(values.into()),
            op_type: AlterConfigOpType::Subtract,
        }
    }
}

/// What [`AdminClient::incremental_alter_configs`] asks for, as Kafka's
/// `AlterConfigsOptions`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IncrementalAlterConfigsOptions {
    /// Validate the changes and apply none.
    pub validate_only: bool,
}

/// The result of each resource of a [`AdminClient::describe_configs`] call.
pub type DescribeConfigsResults = BTreeMap<ConfigResource, Result<Config, KafkaError>>;

/// The result of each resource of an
/// [`AdminClient::incremental_alter_configs`] call.
pub type AlterConfigsResults = BTreeMap<ConfigResource, Result<(), KafkaError>>;

/// The node that a resource goes to, as `KafkaAdminClient.nodeFor`: the
/// broker id of a `BROKER` resource with a name and of a `BROKER_LOGGER`
/// resource, and `None` for every other resource.
fn node_for(resource: &ConfigResource) -> Result<Option<i32>, AdminError> {
    let by_id = match resource.resource_type {
        ConfigResourceType::Broker => !resource.is_default(),
        ConfigResourceType::BrokerLogger => true,
        _ => false,
    };
    if !by_id {
        return Ok(None);
    }
    resource.name.parse().map(Some).map_err(|_| {
        AdminError::InvalidArgument(format!(
            "the name of a {:?} config resource must be a broker id, found {:?}",
            resource.resource_type, resource.name
        ))
    })
}

/// The Kafka error of a request that failed as a whole. A connection that
/// fails until the deadline gives `REQUEST_TIMED_OUT` (7), as Kafka's
/// `Call.handleTimeoutFailure` does.
fn call_error(api: &str, error: &AdminError) -> KafkaError {
    if is_connection_failure(error) {
        return KafkaError {
            code: REQUEST_TIMED_OUT,
            name: kafka_error_name(REQUEST_TIMED_OUT),
            message: Some(format!("{api} timed out: {error}")),
        };
    }
    list_groups_kafka_error(error)
}

/// `UNKNOWN_SERVER_ERROR` with `message`: Kafka's error for a failure that is
/// not an `ApiException`.
fn unknown_server_error(message: String) -> KafkaError {
    KafkaError {
        code: UNKNOWN_SERVER_ERROR,
        name: kafka_error_name(UNKNOWN_SERVER_ERROR),
        message: Some(message),
    }
}

fn describe_request(
    resources: &BTreeSet<ConfigResource>,
    options: DescribeConfigsOptions,
) -> DescribeConfigsRequest {
    DescribeConfigsRequest {
        resources: resources
            .iter()
            .map(|resource| DescribeConfigsResource {
                resource_type: resource.resource_type.id(),
                resource_name: resource.name.clone(),
                configuration_keys: None,
                ..Default::default()
            })
            .collect(),
        include_synonyms: options.include_synonyms,
        include_documentation: options.include_documentation,
        ..Default::default()
    }
}

/// The lowest `DescribeConfigs` version that carries `options`.
/// `IncludeDocumentation` is not ignorable, so Kafka refuses to send it below
/// v3.
fn describe_min_version(options: DescribeConfigsOptions) -> Option<i16> {
    options
        .include_documentation
        .then_some(DESCRIBE_CONFIGS_DOCUMENTATION_VERSION)
}

/// The source of a wire id, or Kafka's `IllegalArgumentException` message.
fn source_of(id: i8) -> Result<ConfigSource, String> {
    ConfigSource::from_id(id).ok_or_else(|| format!("id should be positive, id: {id}"))
}

/// The config of one resource of a `DescribeConfigs` answer, as Kafka's
/// `describeConfigResult`. A negative source or type id gives Kafka's
/// `IllegalArgumentException` message.
fn config_of(configs: Vec<DescribeConfigsResourceResult>) -> Result<Config, String> {
    let mut entries = BTreeMap::new();
    for config in configs {
        let synonyms = config
            .synonyms
            .into_iter()
            .map(|synonym| {
                Ok(ConfigSynonym {
                    name: synonym.name,
                    value: synonym.value,
                    source: source_of(synonym.source)?,
                })
            })
            .collect::<Result<_, String>>()?;
        let entry = ConfigEntry {
            source: source_of(config.config_source)?,
            config_type: ConfigType::from_id(config.config_type)
                .ok_or_else(|| format!("id should be positive, id: {}", config.config_type))?,
            name: config.name,
            value: config.value,
            is_sensitive: config.is_sensitive,
            is_read_only: config.read_only,
            synonyms,
            documentation: config.documentation,
        };
        entries.insert(entry.name.clone(), entry);
    }
    Ok(Config { entries })
}

/// The result of each resource of `asked` from the answer to one
/// `DescribeConfigs` request, as Kafka's `describeConfigs` call handles it:
///
/// - A resource with an error code gets that error.
/// - A resource that the answer does not name gets `UNKNOWN_SERVER_ERROR`
///   (`completeUnrealizedFutures`). A resource that the request did not name
///   is skipped.
/// - A request that failed, an answer that names a resource twice, and an
///   answer with a negative source or type id fail every resource of the
///   request, as Kafka's `handleFailure` does.
fn describe_results(
    asked: &BTreeSet<ConfigResource>,
    answer: Result<DescribeConfigsResponse, KafkaError>,
) -> DescribeConfigsResults {
    let fail_all = |error: KafkaError| {
        asked
            .iter()
            .map(|resource| (resource.clone(), Err(error.clone())))
            .collect()
    };
    let response = match answer {
        Ok(response) => response,
        Err(error) => return fail_all(error),
    };
    let mut answered = BTreeMap::<ConfigResource, DescribeConfigsResult>::new();
    for result in response.results {
        let resource = ConfigResource::new(
            ConfigResourceType::from_id(result.resource_type),
            result.resource_name.clone(),
        );
        if answered.insert(resource.clone(), result).is_some() {
            return fail_all(unknown_server_error(format!(
                "Duplicate key for config resource {resource:?}"
            )));
        }
    }
    let mut out = BTreeMap::new();
    for resource in asked {
        let result = match answered.remove(resource) {
            None => Err(unknown_server_error(format!(
                "The node response did not contain a result for config resource {resource:?}"
            ))),
            Some(result) if result.error_code != 0 => Err(KafkaError {
                code: result.error_code,
                name: kafka_error_name(result.error_code),
                message: result.error_message,
            }),
            Some(result) => match config_of(result.configs) {
                Ok(config) => Ok(config),
                Err(message) => return fail_all(unknown_server_error(message)),
            },
        };
        out.insert(resource.clone(), result);
    }
    for resource in answered.into_keys() {
        tracing::warn!(
            ?resource,
            "the DescribeConfigs response names a resource that was not asked for"
        );
    }
    out
}

fn alter_request(
    resources: &BTreeMap<ConfigResource, &[AlterConfigOp]>,
    options: IncrementalAlterConfigsOptions,
) -> IncrementalAlterConfigsRequest {
    IncrementalAlterConfigsRequest {
        resources: resources
            .iter()
            .map(|(resource, ops)| AlterConfigsResource {
                resource_type: resource.resource_type.id(),
                resource_name: resource.name.clone(),
                configs: ops
                    .iter()
                    .map(|op| AlterableConfig {
                        name: op.name.clone(),
                        config_operation: op.op_type.id(),
                        value: op.value.clone(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        validate_only: options.validate_only,
        ..Default::default()
    }
}

/// Whether an `IncrementalAlterConfigs` answer makes Kafka find the
/// controller again and retry (`handleNotControllerError`): any
/// `NOT_CONTROLLER` (41), or with a controller bootstrap any
/// `NOT_LEADER_OR_FOLLOWER` (6).
fn names_not_controller(response: &IncrementalAlterConfigsResponse, controllers: bool) -> bool {
    response.responses.iter().any(|result| {
        result.error_code == NOT_CONTROLLER
            || (controllers && result.error_code == NOT_LEADER_OR_FOLLOWER)
    })
}

/// The result of each resource of `asked` from the answer to one
/// `IncrementalAlterConfigs` request. A resource that the answer does not
/// name gets `UNKNOWN_SERVER_ERROR`. A request that failed fails every
/// resource.
fn alter_results<'a>(
    asked: impl Iterator<Item = &'a ConfigResource>,
    answer: Result<IncrementalAlterConfigsResponse, KafkaError>,
) -> AlterConfigsResults {
    let response = match answer {
        Ok(response) => response,
        Err(error) => {
            return asked
                .map(|resource| (resource.clone(), Err(error.clone())))
                .collect();
        }
    };
    let answered = response
        .responses
        .into_iter()
        .map(|result| {
            (
                ConfigResource::new(
                    ConfigResourceType::from_id(result.resource_type),
                    result.resource_name,
                ),
                (result.error_code, result.error_message),
            )
        })
        .collect::<BTreeMap<_, _>>();
    asked
        .map(|resource| {
            let result = match answered.get(resource) {
                None => Err(unknown_server_error(format!(
                    "The node response did not contain a result for config resource {resource:?}"
                ))),
                Some((0, _)) => Ok(()),
                Some((code, message)) => Err(KafkaError {
                    code: *code,
                    name: kafka_error_name(*code),
                    message: message.clone(),
                }),
            };
            (resource.clone(), result)
        })
        .collect()
}

impl AdminClient {
    /// Describes the configs of each resource of `resources`, as Kafka's
    /// `describeConfigs` operation does.
    ///
    /// The client sends one `DescribeConfigs` request to each broker that a
    /// `BROKER` resource with a broker id or a `BROKER_LOGGER` resource
    /// names, and one request with every other resource on its connection,
    /// all at the same time. Each request asks for every config of its
    /// resources. The result has one entry for each resource:
    ///
    /// - Its [`Config`], or the error the broker gave for it, such as
    ///   `UNKNOWN_TOPIC_OR_PARTITION` (3).
    /// - `UNKNOWN_SERVER_ERROR` (-1) when the answer has no result for it.
    /// - The error of its request when that request fails: `REQUEST_TIMED_OUT`
    ///   (7) at `default.api.timeout.ms` (60 s), or `UNSUPPORTED_VERSION` (35)
    ///   when the node does not support `DescribeConfigs` v3 and
    ///   `include_documentation` is set. A broker is found by id in fresh
    ///   metadata, and a missing broker or a lost connection is tried again
    ///   until the deadline, as in [`Self::describe_log_dirs`].
    ///
    /// # Errors
    /// Returns [`AdminError::InvalidArgument`], and sends nothing, when the
    /// name of a `BROKER` resource is neither empty nor a broker id, or the
    /// name of a `BROKER_LOGGER` resource is not a broker id, as Kafka's
    /// `nodeFor` throws a `NumberFormatException`.
    pub async fn describe_configs(
        &self,
        resources: &[ConfigResource],
        options: DescribeConfigsOptions,
    ) -> Result<DescribeConfigsResults, AdminError> {
        let mut by_node = BTreeMap::<Option<i32>, BTreeSet<ConfigResource>>::new();
        for resource in resources {
            by_node
                .entry(node_for(resource)?)
                .or_default()
                .insert(resource.clone());
        }
        let start = self.retry.start();
        let min_version = describe_min_version(options);
        let answers = futures_util::future::join_all(by_node.iter().map(|(node, asked)| {
            let request = describe_request(asked, options);
            async move {
                let answer = if let Some(node_id) = *node {
                    call_node(
                        &self.conn,
                        NodeTarget {
                            node_id,
                            supports_controllers: true,
                            min_version,
                        },
                        &self.options,
                        request,
                        CoordinatorRetry::from_deadline(start),
                    )
                    .await
                } else {
                    let retry = ControllerRetry::new("DescribeConfigs", self.retry);
                    let sent = match min_version {
                        Some(min_version) => {
                            retry
                                .bounded(self.conn.send_at_least(request, min_version))
                                .await
                        }
                        None => retry.bounded(self.conn.send(request)).await,
                    };
                    sent.map_err(|error| call_error("DescribeConfigs", &error))
                };
                describe_results(asked, answer)
            }
        }))
        .await;
        Ok(answers.into_iter().flatten().collect())
    }

    /// Changes the configs of each resource of `configs`, as Kafka's
    /// `incrementalAlterConfigs` operation does. With
    /// [`IncrementalAlterConfigsOptions::validate_only`] the broker
    /// validates the changes and applies none.
    ///
    /// A `BROKER` resource with a broker id goes to that broker, and a
    /// `BROKER_LOGGER` resource to its broker, each in a request of its own.
    /// With a controller bootstrap only a `BROKER_LOGGER` resource goes to
    /// its node. Every other resource goes in one request on the client's
    /// connection. A `NOT_CONTROLLER` (41) answer, or with a controller
    /// bootstrap a `NOT_LEADER_OR_FOLLOWER` (6) answer, makes the client find
    /// the controller again and send the request again until the deadline.
    /// The result has one entry for each resource:
    ///
    /// - `Ok(())`, or the error the broker gave for it, such as
    ///   `INVALID_CONFIG` (40).
    /// - `UNKNOWN_SERVER_ERROR` (-1) when the answer has no result for it.
    /// - The error of its request when that request fails, with
    ///   `REQUEST_TIMED_OUT` (7) at `default.api.timeout.ms` (60 s).
    ///
    /// # Errors
    /// Returns [`AdminError::InvalidArgument`], and sends nothing, for a
    /// resource name that [`Self::describe_configs`] refuses.
    pub async fn incremental_alter_configs(
        &mut self,
        configs: &BTreeMap<ConfigResource, Vec<AlterConfigOp>>,
        options: IncrementalAlterConfigsOptions,
    ) -> Result<AlterConfigsResults, AdminError> {
        let controllers = self.conn.uses_controller_bootstrap();
        let mut unified = BTreeMap::new();
        let mut by_node = Vec::new();
        for (resource, ops) in configs {
            let node = node_for(resource)?.filter(|_| {
                !controllers || resource.resource_type == ConfigResourceType::BrokerLogger
            });
            match node {
                Some(node_id) => by_node.push((node_id, resource, ops.as_slice())),
                None => {
                    unified.insert(resource.clone(), ops.as_slice());
                }
            }
        }
        let start = self.retry.start();
        let (conn, client_options) = (&self.conn, &self.options);
        let node_answers =
            futures_util::future::join_all(by_node.iter().map(|(node_id, resource, ops)| {
                let request = alter_request(&BTreeMap::from([((*resource).clone(), *ops)]), options);
                let target = NodeTarget {
                    node_id: *node_id,
                    supports_controllers: true,
                    min_version: None,
                };
                async move {
                    let mut deadline = start;
                    let mut attempts = 1;
                    let answer = loop {
                        let answer = call_node(
                            conn,
                            target,
                            client_options,
                            request.clone(),
                            CoordinatorRetry::from_deadline(deadline),
                        )
                        .await;
                        if !matches!(&answer, Ok(response) if names_not_controller(response, controllers))
                        {
                            break answer;
                        }
                        let timed_out = || {
                            Err(call_timeout_error(
                                "IncrementalAlterConfigs",
                                attempts,
                                "NOT_CONTROLLER",
                            ))
                        };
                        if deadline.exhausted() {
                            break timed_out();
                        }
                        deadline.backoff().await;
                        if deadline.expired() {
                            break timed_out();
                        }
                        attempts += 1;
                    };
                    alter_results(std::iter::once(*resource), answer)
                }
            }))
            .await;
        let mut out: AlterConfigsResults = node_answers.into_iter().flatten().collect();
        if !unified.is_empty() {
            let answer = self.alter_unified(&unified, options, controllers).await;
            out.extend(alter_results(unified.keys(), answer));
        }
        Ok(out)
    }

    /// The `IncrementalAlterConfigs` request of the resources that go on the
    /// client's connection, sent again after each `NOT_CONTROLLER` answer
    /// until the deadline.
    async fn alter_unified(
        &mut self,
        resources: &BTreeMap<ConfigResource, &[AlterConfigOp]>,
        options: IncrementalAlterConfigsOptions,
        controllers: bool,
    ) -> Result<IncrementalAlterConfigsResponse, KafkaError> {
        const API: &str = "IncrementalAlterConfigs";
        let request = alter_request(resources, options);
        let mut retry = ControllerRetry::new(API, self.retry);
        loop {
            let response = retry
                .bounded(self.conn.send(request.clone()))
                .await
                .map_err(|error| call_error(API, &error))?;
            if !names_not_controller(&response, controllers) {
                return Ok(response);
            }
            retry
                .after_not_controller(self)
                .await
                .map_err(|error| call_error(API, &error))?;
        }
    }
}

#[cfg(test)]
mod tests;
