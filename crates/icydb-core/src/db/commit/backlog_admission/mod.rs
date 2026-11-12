//! Module: db::commit::backlog_admission
//! Responsibility: enforce the exact database-backlog tuple before marker publication.
//! Does not own: individual commit admission, scheduling, or convergence execution.
//! Boundary: persisted tail controls + marker-owned encoded envelopes -> Gate-2 admission.

use crate::{
    db::{
        commit::{CommitMarker, MAX_COMMIT_BYTES, MAX_PERSISTED_STORE_ALLOCATIONS},
        journal::{JournalTailControl, JournalTailStore, MAX_JOURNAL_BATCH_RECORDS},
    },
    error::InternalError,
};

use super::store::EncodedCommitControlSlot;

use serde::Deserialize;

use crate::db::{
    commit::{
        PersistedStoreAllocationState, current_commit_memory_allocation,
        current_commit_memory_allocation_if_configured,
    },
    registry::{StoreAllocationIdentity, StoreRegistry},
};
use std::{cell::RefCell, thread::LocalKey};

type RuntimeJournalTail = (
    super::memory::CommitMemoryAllocation,
    StoreAllocationIdentity,
    &'static LocalKey<RefCell<JournalTailStore>>,
);

thread_local! {
    static RUNTIME_JOURNAL_TAILS: RefCell<Vec<RuntimeJournalTail>> =
        const { RefCell::new(Vec::new()) };
}

#[cfg(test)]
use crate::db::journal::{decode_journal_batch, encode_journal_batch};

/// Exact database-wide retained or proposed journal contribution.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, serde::Serialize)]
#[cfg_attr(
    all(feature = "wire-schema", not(target_family = "wasm")),
    derive(ic_wire::Wire)
)]
pub struct ExactBacklogMeasurement {
    batch_count: u64,
    record_count: u64,
    encoded_batch_bytes: u64,
}

impl ExactBacklogMeasurement {
    /// Empty database-wide journal debt.
    pub(crate) const EMPTY: Self = Self {
        batch_count: 0,
        record_count: 0,
        encoded_batch_bytes: 0,
    };

    /// Construct one owner-validated contribution or bounded observation.
    #[must_use]
    pub(crate) const fn new(batch_count: u64, record_count: u64, encoded_batch_bytes: u64) -> Self {
        Self {
            batch_count,
            record_count,
            encoded_batch_bytes,
        }
    }

    /// Sum current owner-local control values with checked arithmetic.
    pub(in crate::db) fn from_tail_controls(
        controls: &[JournalTailControl],
    ) -> Result<Self, InternalError> {
        if controls.len() > MAX_PERSISTED_STORE_ALLOCATIONS {
            return Err(InternalError::store_corruption());
        }
        controls.iter().try_fold(Self::EMPTY, |total, control| {
            total.checked_add(
                Self::new(
                    control.batch_count(),
                    control.record_count(),
                    control.encoded_batch_bytes(),
                ),
                InternalError::store_corruption,
            )
        })
    }

    /// Measure the exact envelopes already prepared for marker publication.
    pub(super) fn from_prepared_marker(
        marker: &CommitMarker,
        encoded: &EncodedCommitControlSlot,
    ) -> Result<Self, InternalError> {
        marker.journal_batches().iter().enumerate().try_fold(
            Self::EMPTY,
            |total, (ordinal, batch)| {
                if batch.journal_sequence().get() == 0 {
                    return Ok(total);
                }
                let bytes = encoded.journal_batch_bytes(ordinal)?;
                total.checked_add(
                    Self::new(
                        1,
                        u64::try_from(batch.records().len())
                            .map_err(|_| InternalError::store_unsupported())?,
                        u64::try_from(bytes.len())
                            .map_err(|_| InternalError::store_unsupported())?,
                    ),
                    InternalError::store_unsupported,
                )
            },
        )
    }

    /// Number of retained engine journal batches, not application calls.
    #[must_use]
    pub const fn batch_count(self) -> u64 {
        self.batch_count
    }

    /// Number of encoded journal records across those batches.
    #[must_use]
    pub const fn record_count(self) -> u64 {
        self.record_count
    }

    /// Total complete encoded batch-envelope bytes, not store or heap bytes.
    #[must_use]
    pub const fn encoded_batch_bytes(self) -> u64 {
        self.encoded_batch_bytes
    }

    fn checked_add(
        self,
        other: Self,
        overflow: fn() -> InternalError,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            batch_count: self
                .batch_count
                .checked_add(other.batch_count)
                .ok_or_else(overflow)?,
            record_count: self
                .record_count
                .checked_add(other.record_count)
                .ok_or_else(overflow)?,
            encoded_batch_bytes: self
                .encoded_batch_bytes
                .checked_add(other.encoded_batch_bytes)
                .ok_or_else(overflow)?,
        })
    }
}

/// Fixed engine-owned Gate-2 policy input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct BacklogLimits(ExactBacklogMeasurement);

impl BacklogLimits {
    #[must_use]
    pub(in crate::db) const fn new(
        batch_count: u64,
        record_count: u64,
        encoded_batch_bytes: u64,
    ) -> Self {
        Self(ExactBacklogMeasurement::new(
            batch_count,
            record_count,
            encoded_batch_bytes,
        ))
    }
}

/// Deterministic cumulative-pressure dimension order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum BacklogPressureDimension {
    BatchCount,
    RecordCount,
    EncodedBatchBytes,
}

/// One typed cumulative-capacity rejection from Gate 2.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct BacklogPressure {
    dimension: BacklogPressureDimension,
    current: u64,
    proposed: u64,
    limit: u64,
}

/// Gate-2 decision returned before marker publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum BacklogAdmission {
    Admitted { projected: ExactBacklogMeasurement },
    Pressure(BacklogPressure),
}

/// Evaluate the frozen cumulative tuple after individual admission succeeds.
pub(in crate::db) fn admit_backlog(
    current: ExactBacklogMeasurement,
    proposed: ExactBacklogMeasurement,
    limits: BacklogLimits,
) -> Result<BacklogAdmission, InternalError> {
    let projected = current.checked_add(proposed, InternalError::store_corruption)?;
    let dimensions = [
        (
            BacklogPressureDimension::BatchCount,
            current.batch_count,
            proposed.batch_count,
            projected.batch_count,
            limits.0.batch_count,
        ),
        (
            BacklogPressureDimension::RecordCount,
            current.record_count,
            proposed.record_count,
            projected.record_count,
            limits.0.record_count,
        ),
        (
            BacklogPressureDimension::EncodedBatchBytes,
            current.encoded_batch_bytes,
            proposed.encoded_batch_bytes,
            projected.encoded_batch_bytes,
            limits.0.encoded_batch_bytes,
        ),
    ];
    for (dimension, current, proposed, projected, limit) in dimensions {
        if projected > limit {
            return Ok(BacklogAdmission::Pressure(BacklogPressure {
                dimension,
                current,
                proposed,
                limit,
            }));
        }
    }
    Ok(BacklogAdmission::Admitted { projected })
}

/// Maximum retained batches independently proved by convergence evidence.
pub(super) const MAX_RETAINED_JOURNAL_BATCHES: u64 = 64;

/// Frozen production tuple proved by convergence evidence.
pub(in crate::db) const BACKLOG_LIMITS: BacklogLimits = BacklogLimits::new(
    MAX_RETAINED_JOURNAL_BATCHES,
    MAX_JOURNAL_BATCH_RECORDS as u64,
    MAX_COMMIT_BYTES as u64,
);

pub(in crate::db) fn register_runtime_journal_tails_for_backlog(registry: &StoreRegistry) {
    let Some(commit_allocation) = current_commit_memory_allocation_if_configured() else {
        return;
    };
    let tails = registry
        .iter()
        .filter_map(|(_, handle)| {
            Some((handle.journal_allocation()?, handle.journal_tail_store()?))
        })
        .collect::<Vec<_>>();
    RUNTIME_JOURNAL_TAILS.with(|registered| {
        let mut registered = registered.borrow_mut();
        registered.retain(|(existing, _, _)| *existing != commit_allocation);
        registered.extend(
            tails
                .into_iter()
                .map(|(identity, tail)| (commit_allocation, identity, tail)),
        );
    });
}

pub(in crate::db) fn current_database_backlog() -> Result<ExactBacklogMeasurement, InternalError> {
    let commit_allocation = current_commit_memory_allocation()?;
    let allocations =
        super::store::with_commit_store(super::store::CommitStore::persisted_store_allocations)?;
    let controls = RUNTIME_JOURNAL_TAILS.with(|registered| {
        let registered = registered.borrow();
        allocations
            .iter()
            .map(|allocation| match allocation.state() {
                PersistedStoreAllocationState::Dormant => {
                    let control =
                        crate::db::database_format::dormant_journal_tail_control(allocation)?;
                    if !control.is_empty() {
                        return Err(InternalError::store_corruption());
                    }
                    Ok(control)
                }
                PersistedStoreAllocationState::Active => {
                    let identity = allocation.journal();
                    registered
                        .iter()
                        .find(|(owner, candidate, _)| {
                            *owner == commit_allocation
                                && candidate.memory_id() == identity.memory_id()
                                && candidate.stable_key() == identity.stable_key()
                        })
                        .ok_or_else(InternalError::store_invariant)?
                        .2
                        .with_borrow(JournalTailStore::current_tail_control)
                }
            })
            .collect::<Result<Vec<_>, _>>()
    })?;
    ExactBacklogMeasurement::from_tail_controls(&controls)
}

impl BacklogPressure {
    /// Convert exact pressure into the retryable public mutation boundary.
    pub(in crate::db) fn into_error(self) -> InternalError {
        let resource = match self.dimension {
            BacklogPressureDimension::BatchCount => {
                icydb_diagnostic_code::DiagnosticBacklogResource::Batches
            }
            BacklogPressureDimension::RecordCount => {
                icydb_diagnostic_code::DiagnosticBacklogResource::Records
            }
            BacklogPressureDimension::EncodedBatchBytes => {
                icydb_diagnostic_code::DiagnosticBacklogResource::EncodedBytes
            }
        };
        InternalError::convergence_backlog_pressure(
            resource,
            self.current,
            self.proposed,
            self.limit,
        )
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod helper_tests;

#[cfg(test)]
pub(super) use helper_tests::PreparedBacklogProposal;
