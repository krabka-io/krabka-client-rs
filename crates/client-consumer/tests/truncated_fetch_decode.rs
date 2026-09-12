//! Regression: a truncated trailing record batch must not fail the decode.
//!
//! Apache Kafka fills a Fetch response partition up to the partition byte
//! budget and then stops mid-batch. The last batch on the wire is therefore
//! often a fragment. The response decoder must keep every complete batch and
//! drop the fragment. If it failed the whole response instead, the consumer
//! would stall on a response that the JVM client accepts.
//!
//! This suite proves the `krabka-protocol` decode path that
//! `Consumer::poll` uses. It needs no broker and no container, so it carries
//! no `#[ignore]` and CI runs it on every build.

use assert2::assert;
use bytes::{Bytes, BytesMut};
use krabka_protocol::{
    Decode, Encode,
    owned::fetch_response::{FLEXIBLE_MIN, FetchResponse, FetchableTopicResponse, PartitionData},
    records::{Record, RecordBatch, RecordsPayload},
};

/// The Fetch version under test.
///
/// This case is not table-driven, because the generated codec admits exactly
/// one version that is both flexible and still carries the topic name. The
/// name field spans versions 0 to 12 and `FLEXIBLE_MIN` is 12, so version 12
/// is the only member of that set. Version 13 and above replace the name with
/// `topic_id` (KIP-516).
const VERSION: i16 = FLEXIBLE_MIN;

/// Bytes of a partial trailing batch.
///
/// A v2 batch header is 61 bytes. Any shorter run makes the batch decoder
/// report `HeaderTooShort`, which is the truncation the lenient path must
/// absorb.
const PARTIAL_BATCH: [u8; 9] = [0u8; 9];

/// One complete v2 batch that holds a single record.
fn batch(base_offset: i64, value: &[u8]) -> RecordBatch {
    RecordBatch {
        base_offset,
        last_offset_delta: 0,
        records: vec![Record {
            offset_delta: 0,
            value: Some(Bytes::copy_from_slice(value)),
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[test]
fn fetch_response_with_truncated_trailing_batch_decodes_complete_batches() {
    // Build the records-field bytes by hand: one complete batch, then the
    // start of a second batch that the broker cut short.
    let complete = batch(0, b"hello");
    let mut field = BytesMut::new();
    complete.encode(&mut field).expect("encode complete batch");
    field.extend_from_slice(&PARTIAL_BATCH);

    let response = FetchResponse {
        responses: vec![FetchableTopicResponse {
            topic: "t".into(),
            partitions: vec![PartitionData {
                partition_index: 0,
                high_watermark: 2,
                records: Some(RecordsPayload::Raw(field.freeze())),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    // Encode, then decode. The decode path is the one the consumer uses, and
    // it must tolerate the truncated tail.
    let mut wire = BytesMut::new();
    response
        .encode(&mut wire, VERSION)
        .expect("encode response");
    let mut cursor: &[u8] = &wire;
    let decoded = FetchResponse::decode(&mut cursor, VERSION).expect("lenient decode");

    let partition = &decoded.responses[0].partitions[0];
    let batches = partition
        .records
        .as_ref()
        .expect("records field present")
        .as_v2()
        .expect("v2 batches");
    // The decoder keeps the complete batch whole and drops the fragment. The
    // comparison covers the whole batch, not the offset alone.
    let expected = vec![complete];
    assert!(batches == expected.as_slice());
}
