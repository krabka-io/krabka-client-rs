//! The part of the OpenTelemetry metrics protocol (OTLP) that a KIP-714
//! client pushes.
//!
//! The `metrics` field of a `PushTelemetry` request holds a serialized
//! `opentelemetry.proto.metrics.v1.MetricsData`. Kafka's client builds it
//! with the generated protobuf classes. A client only needs number gauges and
//! number sums with string attributes, so this module encodes and decodes that
//! subset of the protobuf wire format by hand. Fields outside the subset are
//! skipped on decode.

use thiserror::Error;

/// `opentelemetry.proto.metrics.v1.MetricsData`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MetricsData {
    /// Field 1.
    pub resource_metrics: Vec<ResourceMetrics>,
}

/// `opentelemetry.proto.metrics.v1.ResourceMetrics`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ResourceMetrics {
    /// Field 1. Kafka always sets it, and an empty resource encodes as an
    /// empty message.
    pub resource: Option<Resource>,
    /// Field 2.
    pub scope_metrics: Vec<ScopeMetrics>,
}

/// `opentelemetry.proto.resource.v1.Resource`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Resource {
    /// Field 1.
    pub attributes: Vec<KeyValue>,
}

/// `opentelemetry.proto.metrics.v1.ScopeMetrics`, without the
/// instrumentation scope, which Kafka does not set.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ScopeMetrics {
    /// Field 2.
    pub metrics: Vec<Metric>,
}

/// `opentelemetry.proto.metrics.v1.Metric` with a gauge or a sum.
#[derive(Clone, Debug, PartialEq)]
pub struct Metric {
    /// Field 1.
    pub name: String,
    /// Field 5 (`gauge`) or field 7 (`sum`). `None` for a metric of another
    /// kind, which this module does not decode.
    pub data: Option<MetricData>,
}

/// The data of a [`Metric`].
#[derive(Clone, Debug, PartialEq)]
pub enum MetricData {
    /// `opentelemetry.proto.metrics.v1.Gauge`.
    Gauge {
        /// Field 1.
        data_points: Vec<NumberDataPoint>,
    },
    /// `opentelemetry.proto.metrics.v1.Sum`.
    Sum {
        /// Field 1.
        data_points: Vec<NumberDataPoint>,
        /// Field 2.
        aggregation_temporality: AggregationTemporality,
        /// Field 3.
        is_monotonic: bool,
    },
}

/// `opentelemetry.proto.metrics.v1.AggregationTemporality`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggregationTemporality {
    /// `AGGREGATION_TEMPORALITY_UNSPECIFIED` (0).
    Unspecified,
    /// `AGGREGATION_TEMPORALITY_DELTA` (1).
    Delta,
    /// `AGGREGATION_TEMPORALITY_CUMULATIVE` (2).
    Cumulative,
}

impl AggregationTemporality {
    const fn number(self) -> u64 {
        match self {
            Self::Unspecified => 0,
            Self::Delta => 1,
            Self::Cumulative => 2,
        }
    }

    const fn from_number(number: u64) -> Option<Self> {
        match number {
            0 => Some(Self::Unspecified),
            1 => Some(Self::Delta),
            2 => Some(Self::Cumulative),
            _ => None,
        }
    }
}

/// `opentelemetry.proto.metrics.v1.NumberDataPoint`.
#[derive(Clone, Debug, PartialEq)]
pub struct NumberDataPoint {
    /// Field 7.
    pub attributes: Vec<KeyValue>,
    /// Field 2. Zero when unset.
    pub start_time_unix_nano: u64,
    /// Field 3.
    pub time_unix_nano: u64,
    /// Field 4 (`as_double`) or field 6 (`as_int`).
    pub value: Option<NumberValue>,
}

/// The `value` oneof of a [`NumberDataPoint`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum NumberValue {
    /// `as_double`.
    Double(f64),
    /// `as_int`.
    Int(i64),
}

/// `opentelemetry.proto.common.v1.KeyValue` with a string value, the only
/// kind of attribute that Kafka writes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyValue {
    /// Field 1.
    pub key: String,
    /// The `string_value` (field 1) of the `AnyValue` in field 2.
    pub value: String,
}

impl KeyValue {
    /// A key and a string value.
    #[must_use]
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// A payload that is not a well-formed `MetricsData`.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("malformed OTLP metrics payload: {0}")]
pub struct OtlpDecodeError(&'static str);

const WIRE_VARINT: u8 = 0;
const WIRE_FIXED64: u8 = 1;
const WIRE_LEN: u8 = 2;
const WIRE_FIXED32: u8 = 5;

fn put_varint(buf: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        buf.push(u8::try_from(value & 0x7f).unwrap_or(0) | 0x80);
        value >>= 7;
    }
    buf.push(u8::try_from(value).unwrap_or(0));
}

fn put_key(buf: &mut Vec<u8>, field: u32, wire: u8) {
    put_varint(buf, (u64::from(field) << 3) | u64::from(wire));
}

fn put_bytes(buf: &mut Vec<u8>, field: u32, bytes: &[u8]) {
    put_key(buf, field, WIRE_LEN);
    put_varint(buf, u64::try_from(bytes.len()).unwrap_or(u64::MAX));
    buf.extend_from_slice(bytes);
}

fn put_message(buf: &mut Vec<u8>, field: u32, encode: impl FnOnce(&mut Vec<u8>)) {
    let mut inner = Vec::new();
    encode(&mut inner);
    put_bytes(buf, field, &inner);
}

/// A proto3 string: an empty one is the default and is not written.
fn put_string(buf: &mut Vec<u8>, field: u32, value: &str) {
    if !value.is_empty() {
        put_bytes(buf, field, value.as_bytes());
    }
}

fn put_fixed64(buf: &mut Vec<u8>, field: u32, value: u64) {
    put_key(buf, field, WIRE_FIXED64);
    buf.extend_from_slice(&value.to_le_bytes());
}

impl MetricsData {
    /// The protobuf encoding of this message.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        for resource_metrics in &self.resource_metrics {
            put_message(&mut buf, 1, |buf| resource_metrics.encode(buf));
        }
        buf
    }

    /// Decode a protobuf `MetricsData`.
    ///
    /// # Errors
    ///
    /// Returns [`OtlpDecodeError`] for a truncated message, a bad wire type,
    /// a string that is not UTF-8, or an unknown aggregation temporality.
    pub fn decode(bytes: &[u8]) -> Result<Self, OtlpDecodeError> {
        let mut out = Self::default();
        for field in Fields(bytes) {
            if let (1, Value::Len(bytes)) = field? {
                out.resource_metrics.push(ResourceMetrics::decode(bytes)?);
            }
        }
        Ok(out)
    }
}

impl ResourceMetrics {
    fn encode(&self, buf: &mut Vec<u8>) {
        if let Some(resource) = &self.resource {
            put_message(buf, 1, |buf| {
                for attribute in &resource.attributes {
                    put_message(buf, 1, |buf| attribute.encode(buf));
                }
            });
        }
        for scope_metrics in &self.scope_metrics {
            put_message(buf, 2, |buf| {
                for metric in &scope_metrics.metrics {
                    put_message(buf, 2, |buf| metric.encode(buf));
                }
            });
        }
    }

    fn decode(bytes: &[u8]) -> Result<Self, OtlpDecodeError> {
        let mut out = Self::default();
        for field in Fields(bytes) {
            match field? {
                (1, Value::Len(bytes)) => {
                    let mut resource = Resource::default();
                    for field in Fields(bytes) {
                        if let (1, Value::Len(bytes)) = field? {
                            resource.attributes.push(KeyValue::decode(bytes)?);
                        }
                    }
                    out.resource = Some(resource);
                }
                (2, Value::Len(bytes)) => {
                    let mut scope_metrics = ScopeMetrics::default();
                    for field in Fields(bytes) {
                        if let (2, Value::Len(bytes)) = field? {
                            scope_metrics.metrics.push(Metric::decode(bytes)?);
                        }
                    }
                    out.scope_metrics.push(scope_metrics);
                }
                _ => {}
            }
        }
        Ok(out)
    }
}

impl Metric {
    fn encode(&self, buf: &mut Vec<u8>) {
        put_string(buf, 1, &self.name);
        match &self.data {
            Some(MetricData::Gauge { data_points }) => put_message(buf, 5, |buf| {
                for point in data_points {
                    put_message(buf, 1, |buf| point.encode(buf));
                }
            }),
            Some(MetricData::Sum {
                data_points,
                aggregation_temporality,
                is_monotonic,
            }) => put_message(buf, 7, |buf| {
                for point in data_points {
                    put_message(buf, 1, |buf| point.encode(buf));
                }
                if *aggregation_temporality != AggregationTemporality::Unspecified {
                    put_key(buf, 2, WIRE_VARINT);
                    put_varint(buf, aggregation_temporality.number());
                }
                if *is_monotonic {
                    put_key(buf, 3, WIRE_VARINT);
                    put_varint(buf, 1);
                }
            }),
            None => {}
        }
    }

    fn decode(bytes: &[u8]) -> Result<Self, OtlpDecodeError> {
        let mut name = String::new();
        let mut data = None;
        for field in Fields(bytes) {
            match field? {
                (1, Value::Len(bytes)) => name = utf8(bytes)?,
                (5, Value::Len(bytes)) => {
                    let mut data_points = Vec::new();
                    for field in Fields(bytes) {
                        if let (1, Value::Len(bytes)) = field? {
                            data_points.push(NumberDataPoint::decode(bytes)?);
                        }
                    }
                    data = Some(MetricData::Gauge { data_points });
                }
                (7, Value::Len(bytes)) => {
                    let mut data_points = Vec::new();
                    let mut aggregation_temporality = AggregationTemporality::Unspecified;
                    let mut is_monotonic = false;
                    for field in Fields(bytes) {
                        match field? {
                            (1, Value::Len(bytes)) => {
                                data_points.push(NumberDataPoint::decode(bytes)?);
                            }
                            (2, Value::Varint(number)) => {
                                aggregation_temporality =
                                    AggregationTemporality::from_number(number)
                                        .ok_or(OtlpDecodeError("unknown temporality"))?;
                            }
                            (3, Value::Varint(flag)) => is_monotonic = flag != 0,
                            _ => {}
                        }
                    }
                    data = Some(MetricData::Sum {
                        data_points,
                        aggregation_temporality,
                        is_monotonic,
                    });
                }
                _ => {}
            }
        }
        Ok(Self { name, data })
    }
}

impl NumberDataPoint {
    /// Protobuf-java writes the fields in field-number order.
    fn encode(&self, buf: &mut Vec<u8>) {
        if self.start_time_unix_nano != 0 {
            put_fixed64(buf, 2, self.start_time_unix_nano);
        }
        if self.time_unix_nano != 0 {
            put_fixed64(buf, 3, self.time_unix_nano);
        }
        // A oneof member is written even when it holds the default value.
        match self.value {
            Some(NumberValue::Double(value)) => put_fixed64(buf, 4, value.to_bits()),
            Some(NumberValue::Int(value)) => {
                put_fixed64(buf, 6, u64::from_le_bytes(value.to_le_bytes()));
            }
            None => {}
        }
        for attribute in &self.attributes {
            put_message(buf, 7, |buf| attribute.encode(buf));
        }
    }

    fn decode(bytes: &[u8]) -> Result<Self, OtlpDecodeError> {
        let mut out = Self {
            attributes: Vec::new(),
            start_time_unix_nano: 0,
            time_unix_nano: 0,
            value: None,
        };
        for field in Fields(bytes) {
            match field? {
                (2, Value::Fixed64(time)) => out.start_time_unix_nano = time,
                (3, Value::Fixed64(time)) => out.time_unix_nano = time,
                (4, Value::Fixed64(bits)) => {
                    out.value = Some(NumberValue::Double(f64::from_bits(bits)));
                }
                (6, Value::Fixed64(bits)) => {
                    out.value = Some(NumberValue::Int(i64::from_le_bytes(bits.to_le_bytes())));
                }
                (7, Value::Len(bytes)) => out.attributes.push(KeyValue::decode(bytes)?),
                _ => {}
            }
        }
        Ok(out)
    }
}

impl KeyValue {
    fn encode(&self, buf: &mut Vec<u8>) {
        put_string(buf, 1, &self.key);
        // `string_value` is a oneof member of `AnyValue`: written even when
        // empty.
        put_message(buf, 2, |buf| put_bytes(buf, 1, self.value.as_bytes()));
    }

    fn decode(bytes: &[u8]) -> Result<Self, OtlpDecodeError> {
        let mut out = Self::new("", "");
        for field in Fields(bytes) {
            match field? {
                (1, Value::Len(bytes)) => out.key = utf8(bytes)?,
                (2, Value::Len(bytes)) => {
                    for field in Fields(bytes) {
                        if let (1, Value::Len(bytes)) = field? {
                            out.value = utf8(bytes)?;
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(out)
    }
}

fn utf8(bytes: &[u8]) -> Result<String, OtlpDecodeError> {
    String::from_utf8(bytes.to_vec()).map_err(|_| OtlpDecodeError("string is not UTF-8"))
}

/// The value of one protobuf field.
enum Value<'a> {
    Varint(u64),
    Fixed64(u64),
    Fixed32,
    Len(&'a [u8]),
}

/// The fields of one protobuf message, in wire order.
struct Fields<'a>(&'a [u8]);

impl<'a> Fields<'a> {
    fn varint(&mut self) -> Result<u64, OtlpDecodeError> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let (&byte, rest) = self
                .0
                .split_first()
                .ok_or(OtlpDecodeError("truncated varint"))?;
            self.0 = rest;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(OtlpDecodeError("varint longer than 10 bytes"))
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], OtlpDecodeError> {
        if self.0.len() < len {
            return Err(OtlpDecodeError("truncated field"));
        }
        let (head, rest) = self.0.split_at(len);
        self.0 = rest;
        Ok(head)
    }

    fn field(&mut self) -> Result<(u32, Value<'a>), OtlpDecodeError> {
        let key = self.varint()?;
        let field = u32::try_from(key >> 3).map_err(|_| OtlpDecodeError("field number"))?;
        let value = match u8::try_from(key & 0x7).unwrap_or(u8::MAX) {
            WIRE_VARINT => Value::Varint(self.varint()?),
            WIRE_FIXED64 => {
                let bytes: [u8; 8] = self
                    .take(8)?
                    .try_into()
                    .map_err(|_| OtlpDecodeError("truncated fixed64"))?;
                Value::Fixed64(u64::from_le_bytes(bytes))
            }
            WIRE_LEN => {
                let len = usize::try_from(self.varint()?)
                    .map_err(|_| OtlpDecodeError("length does not fit"))?;
                Value::Len(self.take(len)?)
            }
            WIRE_FIXED32 => {
                self.take(4)?;
                Value::Fixed32
            }
            _ => return Err(OtlpDecodeError("unsupported wire type")),
        };
        Ok((field, value))
    }
}

impl<'a> Iterator for Fields<'a> {
    type Item = Result<(u32, Value<'a>), OtlpDecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_empty() {
            return None;
        }
        let field = self.field();
        if field.is_err() {
            // Stop after the first error.
            self.0 = &[];
        }
        Some(field)
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn point(value: NumberValue, attributes: Vec<KeyValue>) -> NumberDataPoint {
        NumberDataPoint {
            attributes,
            start_time_unix_nano: 1_700_000_000_000_000_000,
            time_unix_nano: 1_700_000_001_000_000_000,
            value: Some(value),
        }
    }

    #[test]
    fn metrics_data_round_trips() {
        let cases = [
            ("empty", MetricsData::default()),
            (
                "gauge with an empty resource",
                MetricsData {
                    resource_metrics: vec![ResourceMetrics {
                        resource: Some(Resource::default()),
                        scope_metrics: vec![ScopeMetrics {
                            metrics: vec![Metric {
                                name: "org.apache.kafka.producer.connection.count".into(),
                                data: Some(MetricData::Gauge {
                                    data_points: vec![point(NumberValue::Double(0.0), vec![])],
                                }),
                            }],
                        }],
                    }],
                },
            ),
            (
                "sums with attributes",
                MetricsData {
                    resource_metrics: vec![ResourceMetrics {
                        resource: Some(Resource {
                            attributes: vec![KeyValue::new("transactional_id", "tx")],
                        }),
                        scope_metrics: vec![ScopeMetrics {
                            metrics: vec![
                                Metric {
                                    name: "org.apache.kafka.producer.request.total".into(),
                                    data: Some(MetricData::Sum {
                                        data_points: vec![point(
                                            NumberValue::Double(42.5),
                                            vec![KeyValue::new("node_id", "node-1")],
                                        )],
                                        aggregation_temporality: AggregationTemporality::Delta,
                                        is_monotonic: true,
                                    }),
                                },
                                Metric {
                                    name: "negative".into(),
                                    data: Some(MetricData::Sum {
                                        data_points: vec![point(NumberValue::Int(-7), vec![])],
                                        aggregation_temporality: AggregationTemporality::Cumulative,
                                        is_monotonic: false,
                                    }),
                                },
                            ],
                        }],
                    }],
                },
            ),
        ];
        for (name, data) in cases {
            let decoded = MetricsData::decode(&data.encode());
            assert!(decoded == Ok(data), "{name}");
        }
    }

    /// The bytes of a small message, as protobuf-java writes them.
    #[test]
    fn encoding_matches_the_protobuf_wire_format() {
        let data = MetricsData {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource::default()),
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "m".into(),
                        data: Some(MetricData::Sum {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![KeyValue::new("k", "v")],
                                start_time_unix_nano: 0,
                                time_unix_nano: 1,
                                value: Some(NumberValue::Double(1.0)),
                            }],
                            aggregation_temporality: AggregationTemporality::Cumulative,
                            is_monotonic: true,
                        }),
                    }],
                }],
            }],
        };
        let point = [
            &[0x19][..],
            &1u64.to_le_bytes(),
            &[0x21],
            &1.0f64.to_bits().to_le_bytes(),
            &[0x3a, 0x08, 0x0a, 0x01, b'k', 0x12, 0x03, 0x0a, 0x01, b'v'],
        ]
        .concat();
        let sum = [&[0x0a, 0x1c][..], &point, &[0x10, 0x02, 0x18, 0x01]].concat();
        let metric = [&[0x0a, 0x01, b'm', 0x3a, 0x22][..], &sum].concat();
        let scope = [&[0x12, 0x27][..], &metric].concat();
        let resource_metrics = [&[0x0a, 0x00, 0x12, 0x29][..], &scope].concat();
        let expected = [&[0x0a, 0x2d][..], &resource_metrics].concat();
        assert!(data.encode() == expected);
    }

    #[test]
    fn decode_skips_unknown_fields_and_rejects_malformed_input() {
        // A `MetricsData` with an unknown varint field 9 and an unknown
        // fixed32 field 10 around an empty resource metrics.
        let unknown = [0x48, 0x01, 0x0a, 0x00, 0x55, 1, 2, 3, 4];
        assert!(
            MetricsData::decode(&unknown)
                == Ok(MetricsData {
                    resource_metrics: vec![ResourceMetrics::default()],
                })
        );
        let cases: [(&str, &[u8], &str); 4] = [
            ("truncated length", &[0x0a, 0x05, 0x00], "truncated field"),
            ("truncated varint", &[0x08, 0x80], "truncated varint"),
            ("group wire type", &[0x0b], "unsupported wire type"),
            (
                "unknown temporality",
                &[0x0a, 0x08, 0x12, 0x06, 0x12, 0x04, 0x3a, 0x02, 0x10, 0x07],
                "unknown temporality",
            ),
        ];
        for (name, bytes, reason) in cases {
            assert!(
                MetricsData::decode(bytes) == Err(OtlpDecodeError(reason)),
                "{name}"
            );
        }
    }
}
