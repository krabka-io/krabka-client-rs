//! The seam between the succession rules and the cluster.
//!
//! [`CoordinationTransport`] holds every operation that the succession rules
//! need from a Kafka cluster, and it holds nothing else. The rules decide who
//! registers, who challenges, and when. This trait performs the five requests
//! that carry those decisions to the brokers.
//!
//! The seam exists so a test drives the rules without a broker.
//! `#[mockall::automock]` generates `MockCoordinationTransport` under
//! `cfg(test)`, and a unit test scripts each method with the record sequence
//! it wants. [`crate::broker::BrokerTransport`] is the one implementation that
//! talks to a cluster.
//!
//! # Which calls carry authority
//!
//! [`CoordinationTransport::acquire_epoch`] and
//! [`CoordinationTransport::write_lease`] are the guarded pair.
//! `acquire_epoch` mints the epoch of the role and fences the member that held
//! it before. `write_lease` writes under that epoch, and the broker rejects
//! the write when a later member has taken the role. A deposed holder learns
//! that it lost the role from [`CoordinationError::Fenced`], and from nothing
//! else.
//!
//! [`CoordinationTransport::register`] carries no authority. A candidate holds
//! no epoch when it announces itself, so the registration is a plain append.
//! [`CoordinationTransport::read_role_records`] and
//! [`CoordinationTransport::describe`] read.

use async_trait::async_trait;

use crate::{
    error::CoordinationError,
    record::{CoordinationKey, CoordinationRecord, FencingToken, Lease, MemberId, Role},
};

/// The records of one role's partition, in offset order.
///
/// Each entry pairs the offset of the record with its decoded key and value.
/// The offset is the join sequence of a registration, because compaction keeps
/// the offset of every record it retains. The succession rules rank candidates
/// on that offset.
pub type RoleRecords = Vec<(i64, CoordinationKey, CoordinationRecord)>;

/// The cluster operations that the coordination client performs.
///
/// A caller that wants the real cluster uses
/// [`crate::broker::BrokerTransport`]. A test uses the generated
/// `MockCoordinationTransport`.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait CoordinationTransport: Send + Sync {
    /// Mints a new epoch for `role` and fences the member that held it.
    ///
    /// The transaction coordinator picks the epoch, so the value is
    /// quorum-minted and monotonic. The implementation keeps the writer that
    /// the epoch binds, and [`Self::write_lease`] uses it.
    ///
    /// # Errors
    ///
    /// Returns [`CoordinationError::Producer`] when the coordinator refuses
    /// the call, and [`CoordinationError::Client`] when the connection to the
    /// coordinator fails.
    async fn acquire_epoch(&self, role: &Role) -> Result<FencingToken, CoordinationError>;

    /// Reads the whole partition of `role` and returns the records in offset
    /// order.
    ///
    /// The read takes committed records only, so an aborted lease write is
    /// invisible. The result holds the records of `role` and drops every
    /// record of another role that shares the partition.
    ///
    /// # Errors
    ///
    /// Returns [`CoordinationError::Client`] when a fetch fails, and
    /// [`CoordinationError::Record`] when a record of `role` does not decode.
    async fn read_role_records(&self, role: &Role) -> Result<RoleRecords, CoordinationError>;

    /// Appends the registration of `member` to the partition of `role`.
    ///
    /// A candidate holds no epoch, so this append sits outside a transaction
    /// and carries no authority. Its offset is the join sequence that the
    /// succession rules rank on.
    ///
    /// # Errors
    ///
    /// Returns [`CoordinationError::Producer`] when the append fails, and
    /// [`CoordinationError::Client`] when the connection fails.
    async fn register(&self, role: &Role, member: &MemberId) -> Result<(), CoordinationError>;

    /// Writes the lease of `role` in a transaction under `token`.
    ///
    /// The broker rejects the write when a later member has taken `role`, and
    /// that rejection is how a deposed holder learns it lost the role.
    ///
    /// # Errors
    ///
    /// Returns [`CoordinationError::Fenced`] when the broker rejects `token`.
    /// Returns [`CoordinationError::NotHeld`] when this transport never minted
    /// `token`. Returns [`CoordinationError::Producer`] for every other
    /// failure of the write.
    async fn write_lease(
        &self,
        role: &Role,
        token: FencingToken,
        lease: &Lease,
    ) -> Result<(), CoordinationError>;

    /// Asks the transaction coordinator which token holds `role` now.
    ///
    /// The result is `None` when no member has ever held `role`. A third party
    /// calls this to check the authority of a writer. It joins no group and it
    /// takes no epoch.
    ///
    /// # Errors
    ///
    /// Returns [`CoordinationError::Admin`] when the coordinator lookup fails
    /// or the coordinator reports a fault other than an unknown role.
    async fn describe(&self, role: &Role) -> Result<Option<FencingToken>, CoordinationError>;
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::record::{CoordinationRecord, Registration};

    fn role() -> Role {
        Role::new("controller").expect("a valid role")
    }

    fn member() -> MemberId {
        MemberId::new("node-1").expect("a valid member id")
    }

    fn lease(token: FencingToken) -> Lease {
        Lease {
            member: member(),
            token,
            granted_at: 1_700_000_000_000,
            deadline: 1_700_000_030_000,
        }
    }

    fn token() -> FencingToken {
        FencingToken::new(4242, 7).expect("a valid token")
    }

    /// The mock stands in for a cluster, so the succession rules and the
    /// client layer both test without a broker. This test drives every method
    /// of the seam through the mock.
    #[tokio::test]
    async fn the_mock_transport_answers_every_call_of_the_seam() {
        let mut transport = MockCoordinationTransport::new();
        transport
            .expect_acquire_epoch()
            .returning(|_role| Ok(token()));
        transport
            .expect_register()
            .returning(|_role, _member| Ok(()));
        transport
            .expect_write_lease()
            .returning(|_role, _token, _lease| Ok(()));
        transport
            .expect_describe()
            .returning(|_role| Ok(Some(token())));
        transport.expect_read_role_records().returning(|_role| {
            Ok(vec![(
                7,
                CoordinationKey::registration(role(), member()),
                CoordinationRecord::Registration(Registration {
                    member: member(),
                    registered_at: 1_700_000_000_000,
                }),
            )])
        });

        check!(transport.acquire_epoch(&role()).await.unwrap() == token());
        check!(transport.register(&role(), &member()).await.is_ok());
        check!(
            transport
                .write_lease(&role(), token(), &lease(token()))
                .await
                .is_ok()
        );
        check!(transport.describe(&role()).await.unwrap() == Some(token()));
        let records = transport.read_role_records(&role()).await.unwrap();
        check!(records.len() == 1);
        check!(records[0].0 == 7);
    }

    /// A deposed holder learns that it lost the role from the error of
    /// `write_lease`. The mock reproduces that shape, so the succession rules
    /// test the loss path without a broker.
    #[tokio::test]
    async fn the_mock_transport_reports_a_fence_on_a_lease_write() {
        let mut transport = MockCoordinationTransport::new();
        transport
            .expect_write_lease()
            .returning(|role, _token, _lease| {
                Err(CoordinationError::Fenced { role: role.clone() })
            });

        let outcome = transport
            .write_lease(&role(), token(), &lease(token()))
            .await;
        let error = outcome.expect_err("the write reports a fence");
        assert!(let CoordinationError::Fenced { .. } = &error);
        check!(error.is_fenced());
        check!(error.to_string() == "fenced: another member holds role controller");
    }
}
