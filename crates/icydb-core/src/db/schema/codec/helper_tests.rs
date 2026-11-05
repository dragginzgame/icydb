//! Test-only helpers for the parent module.

use super::*;

pub(in crate::db) fn reset_persisted_schema_snapshot_decode_count_for_tests() {
    PERSISTED_SCHEMA_SNAPSHOT_DECODE_CALLS.with(|calls| calls.set(0));
}

pub(in crate::db) fn persisted_schema_snapshot_decode_count_for_tests() -> u64 {
    PERSISTED_SCHEMA_SNAPSHOT_DECODE_CALLS.with(Cell::get)
}

/// Encode an intentionally malformed domain fixture for decode-boundary tests.
pub(in crate::db) fn encode_unchecked_persisted_schema_snapshot_for_tests(
    snapshot: &PersistedSchemaSnapshot,
) -> Result<Vec<u8>, InternalError> {
    encode_snapshot(snapshot, snapshot.version())
}

thread_local! {
    pub(super) static PERSISTED_SCHEMA_SNAPSHOT_DECODE_CALLS: Cell<u64> = const { Cell::new(0) };
}
