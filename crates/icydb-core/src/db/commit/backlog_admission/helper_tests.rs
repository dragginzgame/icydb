//! Test-only constructors and adapters for the parent module.

use super::*;

impl BacklogLimits {
    #[must_use]
    pub(in crate::db) const fn from_measurement(measurement: ExactBacklogMeasurement) -> Self {
        Self(measurement)
    }
}

impl BacklogPressure {
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

/// Own exact encoded journal envelopes once for measurement and later reuse.
pub(in crate::db::commit) struct PreparedBacklogProposal {
    pub(super) marker: CommitMarker,
    pub(super) encoded_batches: Vec<Vec<u8>>,
    pub(super) contribution: ExactBacklogMeasurement,
}

impl PreparedBacklogProposal {
    pub(in crate::db::commit) fn from_marker(marker: CommitMarker) -> Result<Self, InternalError> {
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

    pub(in crate::db::commit) const fn contribution(&self) -> ExactBacklogMeasurement {
        self.contribution
    }

    pub(in crate::db::commit) fn exact_batches(
        &self,
    ) -> impl Iterator<Item = (&crate::db::journal::JournalBatch, &[u8])> {
        self.marker
            .journal_batches()
            .iter()
            .zip(self.encoded_batches.iter().map(Vec::as_slice))
    }
}
