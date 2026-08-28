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

#[cfg(not(test))]
use crate::db::database_format::open_registered_store_memory;

#[cfg(test)]
use crate::{
    db::{
        Db,
        commit::{
            PersistedStoreAllocationState, current_commit_memory_allocation,
            current_commit_memory_allocation_if_configured,
        },
        registry::StoreAllocationIdentity,
    },
    traits::CanisterKind,
};
#[cfg(test)]
use std::{cell::RefCell, thread::LocalKey};

#[cfg(test)]
type TestRuntimeJournalTail = (
    super::memory::CommitMemoryAllocation,
    StoreAllocationIdentity,
    &'static LocalKey<RefCell<JournalTailStore>>,
);

#[cfg(test)]
thread_local! {
    static TEST_RUNTIME_JOURNAL_TAILS: RefCell<Vec<TestRuntimeJournalTail>> =
        const { RefCell::new(Vec::new()) };
}

#[cfg(test)]
use crate::db::journal::{decode_journal_batch, encode_journal_batch};

/// Exact database-wide retained or proposed journal contribution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct ExactBacklogMeasurement {
    batch_count: u64,
    record_count: u64,
    encoded_batch_bytes: u64,
}

impl ExactBacklogMeasurement {
    /// Empty database-wide journal debt.
    pub(in crate::db) const EMPTY: Self = Self {
        batch_count: 0,
        record_count: 0,
        encoded_batch_bytes: 0,
    };

    /// Construct an exact synthetic measurement for focused harnesses.
    #[must_use]
    pub(in crate::db) const fn new(
        batch_count: u64,
        record_count: u64,
        encoded_batch_bytes: u64,
    ) -> Self {
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

    #[must_use]
    pub(in crate::db) const fn batch_count(self) -> u64 {
        self.batch_count
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn record_count(self) -> u64 {
        self.record_count
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn encoded_batch_bytes(self) -> u64 {
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

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn from_measurement(measurement: ExactBacklogMeasurement) -> Self {
        Self(measurement)
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

impl BacklogPressure {
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn dimension(self) -> BacklogPressureDimension {
        self.dimension
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn current(self) -> u64 {
        self.current
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn proposed(self) -> u64 {
        self.proposed
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn limit(self) -> u64 {
        self.limit
    }
}

/// Gate-2 decision returned before marker publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum BacklogAdmission {
    Admitted { projected: ExactBacklogMeasurement },
    Pressure(BacklogPressure),
}

/// Own exact encoded journal envelopes once for measurement and later reuse.
#[cfg(test)]
pub(super) struct PreparedBacklogProposal {
    marker: CommitMarker,
    encoded_batches: Vec<Vec<u8>>,
    contribution: ExactBacklogMeasurement,
}

#[cfg(test)]
impl PreparedBacklogProposal {
    pub(super) fn from_marker(marker: CommitMarker) -> Result<Self, InternalError> {
        let mut encoded_batches = Vec::with_capacity(marker.journal_batches().len());
        let mut contribution = ExactBacklogMeasurement::EMPTY;
        for batch in marker.journal_batches() {
            let encoded = encode_journal_batch(batch)?;
            let batch_contribution = ExactBacklogMeasurement::new(
                1,
                u64::try_from(batch.records().len())
                    .map_err(|_| InternalError::store_unsupported())?,
                u64::try_from(encoded.len()).map_err(|_| InternalError::store_unsupported())?,
            );
            contribution =
                contribution.checked_add(batch_contribution, InternalError::store_unsupported)?;
            encoded_batches.push(encoded);
        }
        Ok(Self {
            marker,
            encoded_batches,
            contribution,
        })
    }

    pub(super) const fn contribution(&self) -> ExactBacklogMeasurement {
        self.contribution
    }

    pub(super) fn exact_batches(
        &self,
    ) -> impl Iterator<Item = (&crate::db::journal::JournalBatch, &[u8])> {
        self.marker
            .journal_batches()
            .iter()
            .zip(self.encoded_batches.iter().map(Vec::as_slice))
    }
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

/// Sum exact owner-local controls from the bounded persisted registry.
#[cfg(not(test))]
pub(in crate::db) fn current_database_backlog() -> Result<ExactBacklogMeasurement, InternalError> {
    let allocations =
        super::store::with_commit_store(super::store::CommitStore::persisted_store_allocations)?;
    let controls = allocations
        .iter()
        .map(|allocation| {
            let journal = allocation.journal();
            let memory = open_registered_store_memory(journal.memory_id(), journal.stable_key())?;
            JournalTailStore::init(memory).current_tail_control()
        })
        .collect::<Result<Vec<_>, _>>()?;
    ExactBacklogMeasurement::from_tail_controls(&controls)
}

#[cfg(test)]
pub(in crate::db) fn register_runtime_journal_tails_for_backlog<C: CanisterKind>(db: &Db<C>) {
    let Some(commit_allocation) = current_commit_memory_allocation_if_configured() else {
        return;
    };
    let tails = db.with_store_registry(|registry| {
        registry
            .iter()
            .filter_map(|(_, handle)| {
                Some((handle.journal_allocation()?, handle.journal_tail_store()?))
            })
            .collect::<Vec<_>>()
    });
    TEST_RUNTIME_JOURNAL_TAILS.with(|registered| {
        let mut registered = registered.borrow_mut();
        registered.retain(|(existing, _, _)| *existing != commit_allocation);
        registered.extend(
            tails
                .into_iter()
                .map(|(identity, tail)| (commit_allocation, identity, tail)),
        );
    });
}

#[cfg(test)]
pub(in crate::db) fn current_database_backlog() -> Result<ExactBacklogMeasurement, InternalError> {
    let commit_allocation = current_commit_memory_allocation()?;
    let allocations =
        super::store::with_commit_store(super::store::CommitStore::persisted_store_allocations)?;
    let controls = TEST_RUNTIME_JOURNAL_TAILS.with(|registered| {
        let registered = registered.borrow();
        allocations
            .iter()
            .map(|allocation| match allocation.state() {
                PersistedStoreAllocationState::Retired => Ok(JournalTailControl::empty()),
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
mod tests {
    use super::*;
    use crate::{
        db::{
            commit::CommitMarker,
            data::{DecodedDataStoreKey, RawDataStoreKey},
            journal::{
                DatabaseCommitSequence, JournalBatch, JournalRecord, JournalSequence,
                JournalTailStore,
            },
            key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        },
        testing::test_memory,
        types::EntityTag,
    };

    const MARKER_ID: [u8; 16] = [0xA1; 16];

    fn row_key(value: u64) -> RawDataStoreKey {
        DecodedDataStoreKey::new_primary_key_value(
            EntityTag::new(1),
            &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(value)),
        )
        .to_raw()
        .expect("test row key should encode")
    }

    fn row_record(value: u64) -> JournalRecord {
        JournalRecord::row_put("tests::Backlog", row_key(value), Vec::new(), [0xB2; 16])
            .expect("test row record should admit")
    }

    fn batch(ordinal: u8, records: Vec<JournalRecord>) -> JournalBatch {
        JournalBatch::new_with_database_commit_sequence(
            [ordinal; 16],
            MARKER_ID,
            JournalSequence::new(1),
            DatabaseCommitSequence::new(1),
            records,
        )
        .expect("test backlog batch should admit")
    }

    fn proposal(batches: Vec<JournalBatch>) -> PreparedBacklogProposal {
        PreparedBacklogProposal::from_marker(
            CommitMarker::from_parts(MARKER_ID, batches).expect("test marker should admit"),
        )
        .expect("test proposal should prepare")
    }

    #[test]
    fn exact_prepared_bytes_and_tail_controls_share_one_measurement() {
        let first = batch(1, vec![row_record(1)]);
        let second = batch(2, vec![row_record(2), row_record(3)]);
        let prepared = proposal(vec![first.clone(), second.clone()]);
        let exact_bytes = prepared
            .exact_batches()
            .map(|(batch, bytes)| {
                assert_eq!(decode_journal_batch(bytes).unwrap(), *batch);
                bytes.len() as u64
            })
            .sum::<u64>();
        assert_eq!(prepared.contribution().batch_count(), 2);
        assert_eq!(prepared.contribution().record_count(), 3);
        assert_eq!(prepared.contribution().encoded_batch_bytes(), exact_bytes);

        let mut first_tail = JournalTailStore::init(test_memory(248));
        first_tail.initialize_current_tail_control().unwrap();
        first_tail.append_batch(&first).unwrap();
        let mut second_tail = JournalTailStore::init(test_memory(249));
        second_tail.initialize_current_tail_control().unwrap();
        second_tail.append_batch(&second).unwrap();
        let current = ExactBacklogMeasurement::from_tail_controls(&[
            first_tail.current_tail_control().unwrap(),
            second_tail.current_tail_control().unwrap(),
        ])
        .unwrap();
        assert_eq!(current, prepared.contribution());
    }

    #[test]
    fn every_pressure_dimension_is_typed_and_drain_restores_admission() {
        let proposed = ExactBacklogMeasurement::new(1, 2, 3);
        let cases = [
            (
                ExactBacklogMeasurement::new(1, 0, 0),
                BacklogLimits::new(1, u64::MAX, u64::MAX),
                BacklogPressureDimension::BatchCount,
            ),
            (
                ExactBacklogMeasurement::new(0, 1, 0),
                BacklogLimits::new(u64::MAX, 2, u64::MAX),
                BacklogPressureDimension::RecordCount,
            ),
            (
                ExactBacklogMeasurement::new(0, 0, 1),
                BacklogLimits::new(u64::MAX, u64::MAX, 3),
                BacklogPressureDimension::EncodedBatchBytes,
            ),
        ];
        for (current, limits, expected_dimension) in cases {
            let BacklogAdmission::Pressure(pressure) =
                admit_backlog(current, proposed, limits).unwrap()
            else {
                panic!("retained debt should exceed the isolated candidate dimension");
            };
            assert_eq!(pressure.dimension(), expected_dimension);
            assert!(pressure.current() > 0);
            assert_eq!(
                pressure.proposed(),
                match expected_dimension {
                    BacklogPressureDimension::BatchCount => 1,
                    BacklogPressureDimension::RecordCount => 2,
                    BacklogPressureDimension::EncodedBatchBytes => 3,
                }
            );
            assert!(pressure.limit() > 0);
        }

        let limits = BacklogLimits::from_measurement(proposed);
        assert_eq!(
            admit_backlog(ExactBacklogMeasurement::EMPTY, proposed, limits,).unwrap(),
            BacklogAdmission::Admitted {
                projected: proposed,
            },
        );
    }

    #[test]
    fn maximum_zero_index_multi_store_gate_one_shape_fits_at_zero_debt() {
        let mut next_row = 0_u64;
        let batches = (0..MAX_PERSISTED_STORE_ALLOCATIONS)
            .map(|store_ordinal| {
                let rows = 4_096 / MAX_PERSISTED_STORE_ALLOCATIONS;
                let records = (0..rows)
                    .map(|_| {
                        next_row += 1;
                        row_record(next_row)
                    })
                    .collect();
                batch(u8::try_from(store_ordinal + 1).unwrap(), records)
            })
            .collect();
        let prepared = proposal(batches);
        assert_eq!(prepared.contribution().batch_count(), 16);
        assert_eq!(prepared.contribution().record_count(), 4_096);
        assert_eq!(
            admit_backlog(
                ExactBacklogMeasurement::EMPTY,
                prepared.contribution(),
                BacklogLimits::from_measurement(prepared.contribution()),
            )
            .unwrap(),
            BacklogAdmission::Admitted {
                projected: prepared.contribution(),
            },
        );
    }

    #[test]
    fn current_sum_and_projection_overflow_fail_closed() {
        let malformed = ExactBacklogMeasurement::new(u64::MAX, 0, 0);
        assert!(
            admit_backlog(
                malformed,
                ExactBacklogMeasurement::new(1, 0, 0),
                BacklogLimits::new(u64::MAX, u64::MAX, u64::MAX),
            )
            .is_err(),
        );
        let too_many_controls = vec![JournalTailControl::empty(); 17];
        assert!(ExactBacklogMeasurement::from_tail_controls(&too_many_controls).is_err());
    }
}
