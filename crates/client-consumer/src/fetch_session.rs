//! Incremental fetch sessions (KIP-227), one per broker.
//!
//! This is Kafka's `FetchSessionHandler` and `FetchMetadata`. The first request
//! to a broker is a full request with session id 0 and epoch 0. When the
//! broker answers with a session id, the next requests are incremental: they
//! name only the partitions that are new or changed, and they forget the
//! partitions that are no longer fetched.

use std::collections::BTreeMap;

use krabka_protocol::primitives::uuid::Uuid as WireUuid;

/// Kafka's `FetchMetadata.INVALID_SESSION_ID`.
pub(crate) const INVALID_SESSION_ID: i32 = 0;
/// Kafka's `FetchMetadata.INITIAL_EPOCH`.
const INITIAL_EPOCH: i32 = 0;
/// Kafka's `FetchMetadata.FINAL_EPOCH`.
pub(crate) const FINAL_EPOCH: i32 = -1;
/// `FETCH_SESSION_ID_NOT_FOUND`.
const FETCH_SESSION_ID_NOT_FOUND: i16 = 70;

/// Kafka's `FetchMetadata`: the session id and epoch of the next request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FetchMetadata {
    pub session_id: i32,
    pub epoch: i32,
}

impl FetchMetadata {
    /// Kafka's `FetchMetadata.INITIAL`: create a new session.
    const INITIAL: Self = Self {
        session_id: INVALID_SESSION_ID,
        epoch: INITIAL_EPOCH,
    };

    /// A full request sends every partition.
    fn is_full(self) -> bool {
        self.epoch == INITIAL_EPOCH || self.epoch == FINAL_EPOCH
    }

    /// Close the session and create a new one with a full request.
    fn close_existing_attempt_new(self) -> Self {
        Self {
            session_id: self.session_id,
            epoch: INITIAL_EPOCH,
        }
    }

    fn next_epoch(epoch: i32) -> i32 {
        if epoch < 0 {
            FINAL_EPOCH
        } else if epoch == i32::MAX {
            1
        } else {
            epoch + 1
        }
    }
}

/// The fields of one partition in a fetch session. Kafka's
/// `FetchRequest.PartitionData`; a change of any field makes the partition part
/// of the next incremental request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SessionPartition {
    pub topic_id: WireUuid,
    pub fetch_offset: i64,
    pub current_leader_epoch: i32,
    pub last_fetched_epoch: i32,
    pub partition_max_bytes: i32,
}

/// The session fields and partitions of one Fetch request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SessionRequest {
    pub session_id: i32,
    pub session_epoch: i32,
    /// The partitions to send: all of them in a full request, the new and the
    /// changed ones in an incremental request.
    pub partitions: BTreeMap<(String, i32), SessionPartition>,
    /// The partitions to forget, with their topic ids.
    pub forgotten: Vec<((String, i32), WireUuid)>,
}

/// The fetch session of one broker. Kafka's `FetchSessionHandler`.
#[derive(Debug)]
pub(crate) struct FetchSession {
    next: FetchMetadata,
    partitions: BTreeMap<(String, i32), SessionPartition>,
}

impl Default for FetchSession {
    fn default() -> Self {
        Self {
            next: FetchMetadata::INITIAL,
            partitions: BTreeMap::new(),
        }
    }
}

impl FetchSession {
    /// The session id of the next request, or 0 when there is no session.
    pub(crate) fn session_id(&self) -> i32 {
        self.next.session_id
    }

    /// Build the next request for the partitions in `wanted`, and record them
    /// as the partitions of the session. Kafka's `FetchSessionHandler.Builder.build`.
    pub(crate) fn build(
        &mut self,
        wanted: BTreeMap<(String, i32), SessionPartition>,
    ) -> SessionRequest {
        // Kafka forgets a partition whose topic id changed and sends it again.
        // The forget goes by topic id only from Fetch v13, and the client does
        // not know the version here. A full request that closes the session and
        // creates a new one is correct at every version.
        let replaced = wanted.iter().any(|(key, next)| {
            self.partitions.get(key).is_some_and(|previous| {
                previous.topic_id != next.topic_id
                    && previous.topic_id != WireUuid::default()
                    && next.topic_id != WireUuid::default()
            })
        });
        if replaced && !self.next.is_full() {
            self.next = self.next.close_existing_attempt_new();
        }
        let metadata = self.next;
        if metadata.is_full() {
            self.partitions.clone_from(&wanted);
            return SessionRequest {
                session_id: metadata.session_id,
                session_epoch: metadata.epoch,
                partitions: wanted,
                forgotten: Vec::new(),
            };
        }
        let forgotten = self
            .partitions
            .iter()
            .filter(|(key, _)| !wanted.contains_key(*key))
            .map(|(key, partition)| (key.clone(), partition.topic_id))
            .collect();
        let changed = wanted
            .iter()
            .filter(|(key, partition)| self.partitions.get(*key) != Some(*partition))
            .map(|(key, partition)| (key.clone(), *partition))
            .collect();
        self.partitions = wanted;
        SessionRequest {
            session_id: metadata.session_id,
            session_epoch: metadata.epoch,
            partitions: changed,
            forgotten,
        }
    }

    /// Update the session after a response. Return `false` when the consumer
    /// must not use the partition data of the response. Kafka's
    /// `FetchSessionHandler.handleResponse`.
    pub(crate) fn handle_response(
        &mut self,
        error_code: i16,
        response_session_id: i32,
        response_partitions: usize,
        throttle_time_ms: i32,
    ) -> bool {
        if error_code != 0 {
            self.next = if error_code == FETCH_SESSION_ID_NOT_FOUND {
                FetchMetadata::INITIAL
            } else {
                self.next.close_existing_attempt_new()
            };
            return false;
        }
        if self.next.is_full() {
            if response_partitions == 0 && throttle_time_ms > 0 {
                // KIP-219: an empty full response throttles the client.
                self.next = FetchMetadata::INITIAL;
                return false;
            }
            self.next = if response_session_id == INVALID_SESSION_ID {
                FetchMetadata::INITIAL
            } else {
                FetchMetadata {
                    session_id: response_session_id,
                    epoch: FetchMetadata::next_epoch(INITIAL_EPOCH),
                }
            };
        } else {
            self.next = if response_session_id == INVALID_SESSION_ID {
                FetchMetadata::INITIAL
            } else {
                FetchMetadata {
                    session_id: self.next.session_id,
                    epoch: FetchMetadata::next_epoch(self.next.epoch),
                }
            };
        }
        true
    }

    /// Update the session after the request failed. Kafka's
    /// `FetchSessionHandler.handleError`.
    pub(crate) fn handle_error(&mut self) {
        self.next = self.next.close_existing_attempt_new();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOPIC_ID: WireUuid = WireUuid([1; 16]);

    fn partition(fetch_offset: i64) -> SessionPartition {
        SessionPartition {
            topic_id: TOPIC_ID,
            fetch_offset,
            current_leader_epoch: 3,
            last_fetched_epoch: 2,
            partition_max_bytes: 1024,
        }
    }

    fn key(index: i32) -> (String, i32) {
        ("orders".to_owned(), index)
    }

    /// One step of a session case: the partitions to fetch, and the response
    /// `(error_code, session_id, partitions, throttle_time_ms)` or a transport
    /// error.
    enum Step {
        Fetch(Vec<((String, i32), SessionPartition)>),
        Respond(i16, i32, usize, i32),
        Fail,
    }

    /// The request and response handling of Kafka's `FetchSessionHandler`.
    #[test]
    fn fetch_session_builds_requests_as_kafka_does() {
        use Step::{Fail, Fetch, Respond};
        let request = |session_id,
                       session_epoch,
                       partitions: Vec<((String, i32), SessionPartition)>,
                       forgotten: Vec<(String, i32)>| {
            SessionRequest {
                session_id,
                session_epoch,
                partitions: partitions.into_iter().collect(),
                forgotten: forgotten.into_iter().map(|key| (key, TOPIC_ID)).collect(),
            }
        };
        let both = vec![(key(0), partition(10)), (key(1), partition(20))];
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, steps, expected) in [
            (
                "full, then incremental with no change",
                vec![
                    Fetch(both.clone()),
                    Respond(0, 77, 2, 0),
                    Fetch(both.clone()),
                    Respond(0, 77, 0, 0),
                    Fetch(both.clone()),
                ],
                vec![
                    request(0, 0, both.clone(), Vec::new()),
                    request(77, 1, Vec::new(), Vec::new()),
                    request(77, 2, Vec::new(), Vec::new()),
                ],
            ),
            (
                "incremental sends a changed offset and forgets a removed partition",
                vec![
                    Fetch(both.clone()),
                    Respond(0, 77, 2, 0),
                    Fetch(vec![(key(0), partition(15))]),
                ],
                vec![
                    request(0, 0, both.clone(), Vec::new()),
                    request(77, 1, vec![(key(0), partition(15))], vec![key(1)]),
                ],
            ),
            (
                "session id not found starts a new session",
                vec![
                    Fetch(both.clone()),
                    Respond(0, 77, 2, 0),
                    Fetch(both.clone()),
                    Respond(70, 0, 0, 0),
                    Fetch(both.clone()),
                ],
                vec![
                    request(0, 0, both.clone(), Vec::new()),
                    request(77, 1, Vec::new(), Vec::new()),
                    request(0, 0, both.clone(), Vec::new()),
                ],
            ),
            (
                "invalid session epoch closes the session and creates a new one",
                vec![
                    Fetch(both.clone()),
                    Respond(0, 77, 2, 0),
                    Fetch(both.clone()),
                    Respond(71, 0, 0, 0),
                    Fetch(both.clone()),
                ],
                vec![
                    request(0, 0, both.clone(), Vec::new()),
                    request(77, 1, Vec::new(), Vec::new()),
                    request(77, 0, both.clone(), Vec::new()),
                ],
            ),
            (
                "a transport error closes the session and creates a new one",
                vec![
                    Fetch(both.clone()),
                    Respond(0, 77, 2, 0),
                    Fetch(both.clone()),
                    Fail,
                    Fetch(both.clone()),
                ],
                vec![
                    request(0, 0, both.clone(), Vec::new()),
                    request(77, 1, Vec::new(), Vec::new()),
                    request(77, 0, both.clone(), Vec::new()),
                ],
            ),
            (
                "a broker without sessions answers session id 0",
                vec![
                    Fetch(both.clone()),
                    Respond(0, 0, 2, 0),
                    Fetch(both.clone()),
                ],
                vec![
                    request(0, 0, both.clone(), Vec::new()),
                    request(0, 0, both.clone(), Vec::new()),
                ],
            ),
            (
                "a changed topic id sends a full request",
                vec![
                    Fetch(both.clone()),
                    Respond(0, 77, 2, 0),
                    Fetch(vec![(
                        key(0),
                        SessionPartition {
                            topic_id: WireUuid([2; 16]),
                            ..partition(10)
                        },
                    )]),
                ],
                vec![
                    request(0, 0, both.clone(), Vec::new()),
                    request(
                        77,
                        0,
                        vec![(
                            key(0),
                            SessionPartition {
                                topic_id: WireUuid([2; 16]),
                                ..partition(10)
                            },
                        )],
                        Vec::new(),
                    ),
                ],
            ),
            (
                "an empty throttled full response keeps the next request full",
                vec![
                    Fetch(both.clone()),
                    Respond(0, 77, 0, 100),
                    Fetch(both.clone()),
                ],
                vec![
                    request(0, 0, both.clone(), Vec::new()),
                    request(0, 0, both.clone(), Vec::new()),
                ],
            ),
        ] {
            let mut session = FetchSession::default();
            let mut requests = Vec::new();
            for step in steps {
                match step {
                    Fetch(partitions) => {
                        requests.push(session.build(partitions.into_iter().collect()));
                    }
                    Respond(error_code, session_id, partitions, throttle) => {
                        session.handle_response(error_code, session_id, partitions, throttle);
                    }
                    Fail => session.handle_error(),
                }
            }
            actual.push((name, requests));
            wanted.push((name, expected));
        }
        assert2::assert!(actual == wanted);
    }
}
