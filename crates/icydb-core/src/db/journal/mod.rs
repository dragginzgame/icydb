//! Module: db::journal
//! Responsibility: journaled cached-stable batch codec and journal-tail storage.
//! Does not own: runtime data/index/schema wrappers, recovery replay, or fold policy.
//! Boundary: generated journal memory -> journal tail -> future recovery/fold consumers.

#![allow(
    dead_code,
    reason = "0.174.2 lands the journal codec/tail boundary before runtime wrappers consume it"
)]

mod codec;
mod store;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub(in crate::db) use codec::JournalRecord;
pub(in crate::db) use codec::{
    JournalBatch, JournalSequence, decode_journal_batch, encode_journal_batch,
    journal_batch_encoded_len,
};
pub use store::JournalTailStore;
#[cfg(test)]
pub(in crate::db) use store::JournalTailVisit;
