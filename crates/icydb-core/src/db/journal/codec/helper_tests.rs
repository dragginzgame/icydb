//! Test-only constructors and adapters for the parent module.

use super::*;

impl JournalBatch {
    pub(in crate::db) fn new(
        batch_id: JournalBatchId,
        commit_marker_id: JournalCommitMarkerId,
        journal_sequence: JournalSequence,
        records: Vec<JournalRecord>,
    ) -> Result<Self, InternalError> {
        Self::new_with_database_commit_sequence(
            batch_id,
            commit_marker_id,
            journal_sequence,
            DatabaseCommitSequence::new(journal_sequence.get().max(1)),
            records,
        )
    }
}
