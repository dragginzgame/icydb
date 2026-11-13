mod wakeup;

use super::*;
use crate::db::{
    commit::{commit_memory_handle, select_commit_memory_allocation},
    database_format::initialize_current_database_control_for_tests,
};

#[test]
fn missing_startup_recovery_wakeup_rejects_before_marker_publication() {
    const MEMORY_ID: u8 = 242;
    const STABLE_KEY: &str = "icydb.test.commit_guard_missing_wakeup.v1";

    select_commit_memory_allocation(MEMORY_ID.into(), STABLE_KEY);
    let allocation = super::super::memory::current_commit_memory_allocation()
        .expect("commit allocation should resolve");
    let memory = commit_memory_handle(allocation).expect("commit memory should open");
    initialize_current_database_control_for_tests(&memory);

    let previous = STARTUP_RECOVERY_WAKEUP.with(|installed| installed.replace(None));
    let _restore = core::mem::DropGuard::new(previous, |previous| {
        STARTUP_RECOVERY_WAKEUP.with(|installed| installed.set(previous));
    });
    let marker = CommitMarker::from_parts([0x6d; 16], Vec::new())
        .expect("empty control marker should admit");

    let error = begin_commit(&marker).expect_err("missing wake-up wiring must fail closed");
    assert_eq!(
        error.diagnostic().error_code(),
        icydb_diagnostic_code::ErrorCode::RUNTIME_INVARIANT_VIOLATION,
    );
    assert!(
        with_commit_store(super::super::store::CommitStore::marker_is_empty)
            .expect("commit marker should remain observable"),
        "failed admission must not publish durable recovery work",
    );
}
