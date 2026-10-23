//! Published commit guards retain the captured recovery wake-up obligation.

use super::*;
use crate::{
    db::{
        commit::{
            memory::current_commit_memory_allocation, next_database_commit_sequence,
            store::CommitStore,
        },
        data::DecodedDataStoreKey,
        journal::{DatabaseCommitSequence, JournalBatch, JournalRecord, JournalSequence},
        key_taxonomy::PrimaryKeyComponent,
        schema::{AcceptedSchemaRevision, empty_accepted_schema_candidate_for_tests},
    },
    types::EntityTag,
};

const MEMORY_ID: u8 = 243;
const STABLE_KEY: &str = "icydb.test.commit_guard_wakeup.v1";

thread_local! {
    static CAPTURED_WAKEUPS: Cell<u32> = const { Cell::new(0) };
    static REPLACEMENT_WAKEUPS: Cell<u32> = const { Cell::new(0) };
}

fn captured_wakeup() {
    CAPTURED_WAKEUPS.with(|count| count.set(count.get() + 1));
}

fn replacement_wakeup() {
    REPLACEMENT_WAKEUPS.with(|count| count.set(count.get() + 1));
}

fn initialize_wakeup_test() -> StartupRecoveryWakeupRestore {
    select_commit_memory_allocation(MEMORY_ID, STABLE_KEY);
    let memory = commit_memory_handle(current_commit_memory_allocation().unwrap()).unwrap();
    initialize_current_database_control_for_tests(&memory);
    CAPTURED_WAKEUPS.with(|count| count.set(0));
    REPLACEMENT_WAKEUPS.with(|count| count.set(0));
    StartupRecoveryWakeupRestore(
        STARTUP_RECOVERY_WAKEUP.with(|hook| hook.replace(Some(captured_wakeup))),
    )
}

#[test]
fn abandoned_published_commit_uses_its_captured_wakeup_and_retains_marker() {
    let _restore = initialize_wakeup_test();
    let marker = CommitMarker::from_parts([0x6e; 16], Vec::new()).unwrap();
    let guard = begin_commit(&marker).unwrap();
    install_startup_recovery_wakeup(replacement_wakeup);
    assert_eq!(CAPTURED_WAKEUPS.with(Cell::get), 0);
    drop(guard);

    assert_eq!(CAPTURED_WAKEUPS.with(Cell::get), 1);
    assert_eq!(REPLACEMENT_WAKEUPS.with(Cell::get), 0);
    let retained = with_commit_store(CommitStore::load).unwrap().unwrap();
    assert_eq!(retained.id, marker.id);
    assert!(retained.journal_batches.is_empty());
    assert!(retained.database_control.is_empty());
}

#[test]
fn successful_commit_without_journal_debt_disarms_its_wakeup() {
    let _restore = initialize_wakeup_test();
    let marker = CommitMarker::from_parts([0x6f; 16], Vec::new()).unwrap();
    let guard = begin_commit(&marker).unwrap();
    finish_commit(guard, |_| Ok(())).unwrap();

    assert_eq!(CAPTURED_WAKEUPS.with(Cell::get), 0);
    assert!(with_commit_store(CommitStore::marker_is_empty).unwrap());
}

#[test]
fn schema_publication_retains_wakeup_with_and_without_journal_debt() {
    let _restore = initialize_wakeup_test();
    for sequence in [0, 1] {
        CAPTURED_WAKEUPS.with(|count| count.set(0));
        let candidate = empty_accepted_schema_candidate_for_tests(
            "test::Store",
            AcceptedSchemaRevision::new(2),
        );
        let record = JournalRecord::accepted_schema_publish(
            "test::Store",
            AcceptedSchemaRevision::INITIAL,
            candidate.encoded_bundle().to_vec(),
            candidate.encoded_root().to_vec(),
        )
        .unwrap();
        let batch = JournalBatch::new_with_database_commit_sequence(
            [0x72; 16],
            [0x73; 16],
            JournalSequence::new(sequence),
            DatabaseCommitSequence::new(next_database_commit_sequence().unwrap()),
            vec![record],
        )
        .unwrap();
        let marker = CommitMarker::from_parts([0x73; 16], vec![batch]).unwrap();
        let guard = begin_commit(&marker).unwrap();
        assert_eq!(CAPTURED_WAKEUPS.with(Cell::get), 0);
        finish_commit(guard, |_| Ok(())).unwrap();
        assert_eq!(CAPTURED_WAKEUPS.with(Cell::get), 1);
        assert!(with_commit_store(CommitStore::marker_is_empty).unwrap());
    }
}

#[test]
fn row_only_commit_wakes_only_for_journal_debt() {
    let _restore = initialize_wakeup_test();
    for sequence in [0, 1] {
        CAPTURED_WAKEUPS.with(|count| count.set(0));
        let key =
            DecodedDataStoreKey::new(EntityTag::new(1), &PrimaryKeyComponent::Nat64(1).into())
                .to_raw()
                .unwrap();
        let record = JournalRecord::row_delete("test::Entity", key, [0x22; 16]).unwrap();
        let batch = JournalBatch::new_with_database_commit_sequence(
            [0x74; 16],
            [0x75; 16],
            JournalSequence::new(sequence),
            DatabaseCommitSequence::new(next_database_commit_sequence().unwrap()),
            vec![record],
        )
        .unwrap();
        let marker = CommitMarker::from_parts([0x75; 16], vec![batch]).unwrap();
        finish_commit(begin_commit(&marker).unwrap(), |_| Ok(())).unwrap();
        assert_eq!(CAPTURED_WAKEUPS.with(Cell::get), u32::from(sequence != 0));
        assert!(with_commit_store(CommitStore::marker_is_empty).unwrap());
    }
}

#[test]
fn failed_apply_retains_marker_and_requests_one_wakeup() {
    let _restore = initialize_wakeup_test();
    let marker = CommitMarker::from_parts([0x70; 16], Vec::new()).unwrap();
    let guard = begin_commit(&marker).unwrap();
    let expected = InternalError::query_executor_invariant();
    let error =
        finish_commit(guard, |_| Err(InternalError::query_executor_invariant())).unwrap_err();

    assert_eq!(error.diagnostic_code(), expected.diagnostic_code());
    assert_eq!(error.origin(), expected.origin());
    assert_eq!(CAPTURED_WAKEUPS.with(Cell::get), 1);
    let retained = with_commit_store(CommitStore::load).unwrap().unwrap();
    assert_eq!(retained.id, marker.id);
}

#[test]
fn failed_clear_retains_marker_and_requests_one_wakeup() {
    let _restore = initialize_wakeup_test();
    let marker = CommitMarker::from_parts([0x71; 16], Vec::new()).unwrap();
    let guard = begin_commit(&marker).unwrap();
    // The clear path must not open a store after marker publication. Selecting
    // an unopened allocation exercises that typed failure without corrupting IO.
    select_commit_memory_allocation(244, "icydb.test.commit_guard_uninitialized_clear.v1");
    let error = finish_commit(guard, |_| Ok(())).unwrap_err();
    select_commit_memory_allocation(MEMORY_ID, STABLE_KEY);

    let expected = InternalError::commit_store_uninitialized();
    assert_eq!(error.diagnostic_code(), expected.diagnostic_code());
    assert_eq!(error.origin(), expected.origin());
    assert_eq!(CAPTURED_WAKEUPS.with(Cell::get), 1);
    let retained = with_commit_store(CommitStore::load).unwrap().unwrap();
    assert_eq!(retained.id, marker.id);
}
