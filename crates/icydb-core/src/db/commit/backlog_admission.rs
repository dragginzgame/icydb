//! Module: db::commit::backlog_admission
//! Responsibility: evaluate the dormant exact database-backlog tuple.
//! Does not own: frozen limits, production admission, scheduling, or convergence execution.
//! Boundary: exact tail controls + immutable prepared envelopes -> measurement candidate.

use crate::{
    db::{
        commit::{CommitMarker, MAX_PERSISTED_STORE_ALLOCATIONS},
        journal::{JournalTailControl, decode_journal_batch, encode_journal_batch},
    },
    error::InternalError,
};

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

    /// Construct an exact synthetic measurement for dormant harnesses.
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

    #[must_use]
    pub(in crate::db) const fn batch_count(self) -> u64 {
        self.batch_count
    }

    #[must_use]
    pub(in crate::db) const fn record_count(self) -> u64 {
        self.record_count
    }

    #[must_use]
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

/// Patch-5 input candidate; no production limit is frozen by Patch 4.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct CandidateBacklogLimits(ExactBacklogMeasurement);

impl CandidateBacklogLimits {
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

/// One typed cumulative-capacity rejection from the dormant evaluator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct DormantBacklogPressure {
    dimension: BacklogPressureDimension,
    current: u64,
    proposed: u64,
    limit: u64,
}

impl DormantBacklogPressure {
    #[must_use]
    pub(in crate::db) const fn dimension(self) -> BacklogPressureDimension {
        self.dimension
    }

    #[must_use]
    pub(in crate::db) const fn current(self) -> u64 {
        self.current
    }

    #[must_use]
    pub(in crate::db) const fn proposed(self) -> u64 {
        self.proposed
    }

    #[must_use]
    pub(in crate::db) const fn limit(self) -> u64 {
        self.limit
    }
}

/// Dormant Gate-2 decision. It is not a production mutation result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum DormantBacklogAdmission {
    Admitted { projected: ExactBacklogMeasurement },
    Pressure(DormantBacklogPressure),
}

/// Own exact encoded journal envelopes once for measurement and later reuse.
struct PreparedBacklogProposal {
    marker: CommitMarker,
    encoded_batches: Vec<Vec<u8>>,
    contribution: ExactBacklogMeasurement,
}

impl PreparedBacklogProposal {
    fn from_marker(marker: CommitMarker) -> Result<Self, InternalError> {
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

    const fn contribution(&self) -> ExactBacklogMeasurement {
        self.contribution
    }

    fn exact_batches(&self) -> impl Iterator<Item = (&crate::db::journal::JournalBatch, &[u8])> {
        self.marker
            .journal_batches()
            .iter()
            .zip(self.encoded_batches.iter().map(Vec::as_slice))
    }
}

/// Evaluate the candidate tuple after individual admission has already succeeded.
pub(in crate::db) fn admit_dormant_backlog(
    current: ExactBacklogMeasurement,
    proposed: ExactBacklogMeasurement,
    limits: CandidateBacklogLimits,
) -> Result<DormantBacklogAdmission, InternalError> {
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
            return Ok(DormantBacklogAdmission::Pressure(DormantBacklogPressure {
                dimension,
                current,
                proposed,
                limit,
            }));
        }
    }
    Ok(DormantBacklogAdmission::Admitted { projected })
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
                CandidateBacklogLimits::new(1, u64::MAX, u64::MAX),
                BacklogPressureDimension::BatchCount,
            ),
            (
                ExactBacklogMeasurement::new(0, 1, 0),
                CandidateBacklogLimits::new(u64::MAX, 2, u64::MAX),
                BacklogPressureDimension::RecordCount,
            ),
            (
                ExactBacklogMeasurement::new(0, 0, 1),
                CandidateBacklogLimits::new(u64::MAX, u64::MAX, 3),
                BacklogPressureDimension::EncodedBatchBytes,
            ),
        ];
        for (current, limits, expected_dimension) in cases {
            let DormantBacklogAdmission::Pressure(pressure) =
                admit_dormant_backlog(current, proposed, limits).unwrap()
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

        let limits = CandidateBacklogLimits::from_measurement(proposed);
        assert_eq!(
            admit_dormant_backlog(ExactBacklogMeasurement::EMPTY, proposed, limits,).unwrap(),
            DormantBacklogAdmission::Admitted {
                projected: proposed,
            },
        );
    }

    #[test]
    fn maximum_zero_index_multi_store_gate_one_shape_fits_at_zero_debt() {
        let mut next_row = 0_u64;
        let batches = (0..MAX_PERSISTED_STORE_ALLOCATIONS)
            .map(|store_ordinal| {
                let rows = 4_096 / MAX_PERSISTED_STORE_ALLOCATIONS
                    + usize::from(store_ordinal < 4_096 % MAX_PERSISTED_STORE_ALLOCATIONS);
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
        assert_eq!(prepared.contribution().batch_count(), 38);
        assert_eq!(prepared.contribution().record_count(), 4_096);
        assert_eq!(
            admit_dormant_backlog(
                ExactBacklogMeasurement::EMPTY,
                prepared.contribution(),
                CandidateBacklogLimits::from_measurement(prepared.contribution()),
            )
            .unwrap(),
            DormantBacklogAdmission::Admitted {
                projected: prepared.contribution(),
            },
        );
    }

    #[test]
    fn current_sum_and_projection_overflow_fail_closed() {
        let malformed = ExactBacklogMeasurement::new(u64::MAX, 0, 0);
        assert!(
            admit_dormant_backlog(
                malformed,
                ExactBacklogMeasurement::new(1, 0, 0),
                CandidateBacklogLimits::new(u64::MAX, u64::MAX, u64::MAX),
            )
            .is_err(),
        );
        let too_many_controls = vec![JournalTailControl::empty(); 39];
        assert!(ExactBacklogMeasurement::from_tail_controls(&too_many_controls).is_err());
    }
}
