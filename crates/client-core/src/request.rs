use bytes::BufMut;
use krabka_protocol::{
    Encode, ProtocolError,
    owned::{
        offset_commit_request::{self, OffsetCommitRequest},
        offset_commit_response::OffsetCommitResponse,
    },
};

pub use krabka_protocol::ProtocolRequest;

/// An `OffsetCommit` request that names its topics, capped at v9.
///
/// `OffsetCommit` v10 names each topic by id only. Apache Kafka's
/// `OffsetCommitRequest.Builder.forTopicNames` allows v9 at most. This
/// type gives the same cap to version negotiation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicNameOffsetCommit(pub OffsetCommitRequest);

impl Encode for TopicNameOffsetCommit {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        self.0.encode(buf, version)
    }

    fn encoded_len(&self, version: i16) -> usize {
        self.0.encoded_len(version)
    }
}

impl ProtocolRequest for TopicNameOffsetCommit {
    const API_KEY: i16 = offset_commit_request::API_KEY;
    const MIN_VERSION: i16 = offset_commit_request::MIN_VERSION;
    /// The last `OffsetCommit` version that carries topic names.
    const MAX_VERSION: i16 = 9;
    /// The cap is a released version, so it is also the stable maximum.
    const LATEST_STABLE_VERSION: i16 = Self::MAX_VERSION;
    const FLEXIBLE_MIN: i16 = offset_commit_request::FLEXIBLE_MIN;
    type Response = OffsetCommitResponse;
}
