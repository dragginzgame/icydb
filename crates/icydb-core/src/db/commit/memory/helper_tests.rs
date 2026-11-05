//! Test-only helpers for the parent module.

use super::*;

/// Select a raw fixture allocation without a generated readiness owner.
pub(in crate::db) fn select_commit_memory_allocation(memory_id: u64, stable_key: &'static str) {
    CURRENT_COMMIT_STORE.with(|cell| {
        cell.set(Some(CommitMemorySelection {
            allocation: CommitMemoryAllocation {
                memory_id,
                stable_key,
            },
            readiness_record_key: || Ok(None),
        }));
    });
}
