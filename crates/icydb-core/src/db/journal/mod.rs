//! Module: db::journal
//! Responsibility: journaled cached-stable batch codec and journal-tail storage.
//! Does not own: runtime data/index/schema wrappers, recovery replay, or fold policy.
//! Boundary: generated journal memory -> journal tail -> future recovery/fold consumers.

mod codec;
mod store;
#[cfg(test)]
mod tests;

pub(in crate::db) use codec::JournalRecord;
pub(in crate::db) use codec::{
    JournalBatch, JournalSequence, decode_journal_batch, encode_journal_batch,
    journal_batch_encoded_len,
};
#[cfg(feature = "sql")]
pub(in crate::db) use codec::{
    journal_batch_encoded_len_for_record_payloads, journal_row_delete_record_payload_len,
    journal_row_put_record_payload_len,
};
pub(in crate::db) use store::FoldWatermark;
pub use store::JournalTailStore;
