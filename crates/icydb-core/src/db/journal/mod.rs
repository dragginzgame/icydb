//! Module: db::journal
//! Responsibility: journaled cached-stable batch codec and journal-tail storage.
//! Does not own: runtime data/index/schema wrappers, recovery replay, or fold policy.
//! Boundary: generated journal memory -> journal tail -> future recovery/fold consumers.

mod codec;
mod store;
#[cfg(test)]
mod tests;

pub(in crate::db) use codec::JournalRecord;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use codec::MAX_ACCEPTED_SCHEMA_INDEX_KEYS_PER_RECORD;
pub(in crate::db) use codec::MAX_JOURNAL_BATCH_RECORDS;
#[cfg(any(test, feature = "migration"))]
pub(in crate::db) use codec::journal_record_payload_len;
pub(in crate::db) use codec::{
    DatabaseCommitSequence, JournalBatch, JournalSequence, decode_journal_batch,
    encode_journal_batch, journal_batch_encoded_len,
};
#[cfg(test)]
pub(in crate::db) use store::JOURNAL_TAIL_CHUNK_BYTES;
pub use store::JournalTailStore;
pub(in crate::db) use store::{
    FoldWatermark, JournalInspectionCheckpoint, JournalInspectionLimits, JournalIntegrityIssue,
    JournalTailControl, JournalTailProofIdentity, PreparedEntityMutationRevision,
};
