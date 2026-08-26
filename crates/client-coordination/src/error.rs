//! The error type of the coordination client.

use crabka_client_admin::AdminError;
use crabka_client_core::ClientError;
use crabka_client_producer::ProducerError;
use thiserror::Error;

use crate::record::{MemberId, RecordError, Role};

/// A fault that the coordination client reports.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CoordinationError {
    /// A record of the coordination topic did not decode, or a name did not
    /// pass its constructor.
    #[error("record: {0}")]
    Record(#[from] RecordError),

    /// The connection layer failed.
    #[error("client: {0}")]
    Client(#[from] ClientError),

    /// The producer failed for a reason other than a fence.
    #[error("producer: {0}")]
    Producer(#[from] ProducerError),

    /// The admin client failed.
    #[error("admin: {0}")]
    Admin(#[from] AdminError),

    /// The broker fenced this member, so another member holds the role now.
    ///
    /// This is the expected end of a leadership. The caller stops the work of
    /// the role at once. The epoch it held is superseded, so the cluster
    /// already rejects every write it makes.
    #[error("fenced: another member holds role {role}")]
    Fenced {
        /// The role that this member no longer holds.
        role: Role,
    },

    /// The caller asked a leadership handle to act after it resigned.
    #[error("member {member} no longer holds role {role}")]
    NotHeld {
        /// The role that the handle named.
        role: Role,
        /// The member that the handle named.
        member: MemberId,
    },

    /// The acquire call reached its deadline before this member won the role.
    ///
    /// Another member held the role for the whole wait. A caller that must
    /// keep waiting calls acquire again.
    #[error("did not win role {role} before the deadline")]
    AcquireTimeout {
        /// The role that this member competed for.
        role: Role,
    },

    /// A configuration value is outside the range that the client accepts.
    #[error("invalid config: {0}")]
    InvalidConfig(String),
}

impl CoordinationError {
    /// Report whether the broker fenced this member.
    ///
    /// A caller uses this to separate the loss of a role, which is an ordinary
    /// event, from a fault that a retry could clear.
    #[must_use]
    pub fn is_fenced(&self) -> bool {
        matches!(
            self,
            Self::Fenced { .. } | Self::Producer(ProducerError::FencedProducer)
        )
    }
}
