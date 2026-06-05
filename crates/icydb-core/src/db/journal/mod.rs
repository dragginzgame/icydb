//! Module: db::journal
//! Responsibility: journaled cached-stable batch codec and journal-tail storage.
//! Does not own: runtime data/index/schema wrappers, recovery replay, or fold policy.
//! Boundary: generated journal memory -> journal tail -> future recovery/fold consumers.

#![expect(
    dead_code,
    reason = "recovery and fold will consume the remaining journal-tail surface after 0.174.3"
)]

mod codec;
mod store;
#[cfg(test)]
mod tests;

pub(in crate::db) use codec::JournalRecord;
pub(in crate::db) use codec::{
    JournalBatch, JournalSequence, decode_journal_batch, encode_journal_batch,
    journal_batch_encoded_len,
};
pub use store::JournalTailStore;
pub(in crate::db) use store::{FoldWatermark, JournalTailVisit};
