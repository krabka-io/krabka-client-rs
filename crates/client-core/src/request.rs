use bytes::BufMut;
pub use krabka_protocol::ProtocolRequest;
use krabka_protocol::{
    Encode, ProtocolError,
    owned::{leave_group_request, offset_commit_request, offset_fetch_request},
};

/// A protocol request with a narrower version range than its wire type.
///
/// The lower bound is raised to at least the wrapped request's minimum, and
/// the upper bound is capped at its maximum. The stable version follows the
/// wrapped request unless the cap lowers it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionBoundedRequest<Request, const MIN_VERSION: i16, const MAX_VERSION: i16> {
    request: Request,
}

impl<Request, const MIN_VERSION: i16, const MAX_VERSION: i16>
    VersionBoundedRequest<Request, MIN_VERSION, MAX_VERSION>
{
    /// Wrap a request in the configured version range.
    pub fn new(request: Request) -> Self {
        Self { request }
    }
}

impl<Request, const MIN_VERSION: i16, const MAX_VERSION: i16> Encode
    for VersionBoundedRequest<Request, MIN_VERSION, MAX_VERSION>
where
    Request: ProtocolRequest,
{
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        self.request.encode(buf, version)
    }

    fn encoded_len(&self, version: i16) -> usize {
        self.request.encoded_len(version)
    }
}

impl<Request, const MIN_VERSION: i16, const MAX_VERSION: i16> ProtocolRequest
    for VersionBoundedRequest<Request, MIN_VERSION, MAX_VERSION>
where
    Request: ProtocolRequest,
{
    const API_KEY: i16 = Request::API_KEY;
    const MIN_VERSION: i16 = if Request::MIN_VERSION > MIN_VERSION {
        Request::MIN_VERSION
    } else {
        MIN_VERSION
    };
    const MAX_VERSION: i16 = if Request::MAX_VERSION < MAX_VERSION {
        Request::MAX_VERSION
    } else {
        MAX_VERSION
    };
    const LATEST_STABLE_VERSION: i16 = if Request::LATEST_STABLE_VERSION < Self::MAX_VERSION {
        Request::LATEST_STABLE_VERSION
    } else {
        Self::MAX_VERSION
    };
    const FLEXIBLE_MIN: i16 = Request::FLEXIBLE_MIN;
    type Response = Request::Response;
}

/// An `OffsetCommit` request that names its topics, capped at v9.
pub type TopicNameOffsetCommit = VersionBoundedRequest<
    offset_commit_request::OffsetCommitRequest,
    { offset_commit_request::MIN_VERSION },
    9,
>;

/// An `OffsetFetch` request that names its topics, capped at v9.
pub type TopicNameOffsetFetch = VersionBoundedRequest<
    offset_fetch_request::OffsetFetchRequest,
    { offset_fetch_request::MIN_VERSION },
    9,
>;

/// An admin `OffsetFetch` request for all topics, which needs v2 through v9.
pub type AllTopicsOffsetFetch =
    VersionBoundedRequest<offset_fetch_request::OffsetFetchRequest, 2, 9>;

/// A `LeaveGroup` request that carries its member list, needing v3 or newer.
pub type MembersOnlyLeaveGroup = VersionBoundedRequest<
    leave_group_request::LeaveGroupRequest,
    3,
    { leave_group_request::MAX_VERSION },
>;
