//! Module: db::migration::tests
//! Covers migration planning and migration-state invariants.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use super::{
    PersistedMigrationState, decode_persisted_migration_state, encode_persisted_migration_state,
};
use crate::{
    db::{
        Db, EntityRuntimeHooks,
        commit::{
            CommitMarker, CommitRowOp, begin_commit, begin_commit_with_migration_state,
            clear_commit_marker_for_tests, clear_migration_state_bytes, commit_marker_present,
            init_commit_store_for_tests, prepare_row_commit_for_entity_with_structural_readers,
        },
        data::{DataKey, DataStore, RawDataKey, RawRow},
        index::IndexStore,
        migration::{
            MigrationCursor, MigrationPlan, MigrationRunState, MigrationStep,
            encode_durable_cursor_state, execute_migration_plan,
        },
        registry::{StoreHandle, StoreRegistry},
        relation::validate_delete_strong_relations_for_source,
        schema::commit_schema_fingerprint_for_entity,
    },
    error::{ErrorClass, ErrorOrigin},
    model::field::FieldKind,
    testing::test_memory,
    traits::{EntityKind, Path},
    types::Ulid,
};
use icydb_derive::{FieldProjection, PersistedRow};
use serde::Deserialize;
use std::cell::RefCell;

type MigrationStoreSnapshot = (Vec<(Vec<u8>, Vec<u8>)>, Vec<(Vec<u8>, Vec<u8>)>);

//
// MigrationTestCanister
//

crate::test_canister! {
    ident = MigrationTestCanister,
    commit_memory_id = crate::testing::test_commit_memory_id(),
}

//
// MigrationTestStore
//

crate::test_store! {
    ident = MigrationTestStore,
    canister = MigrationTestCanister,
}

///
/// MigrationEntity
///
/// Minimal migration test entity with one indexed-free primary key.
/// This entity keeps migration tests focused on commit-marker row semantics.
///

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct MigrationEntity {
    id: Ulid,
    rank: u32,
}

crate::test_entity_schema! {
    ident = MigrationEntity,
    id = Ulid,
    id_field = id,
    entity_name = "MigrationEntity",
    entity_tag = crate::testing::MIGRATION_ENTITY_TAG,
    pk_index = 0,
    fields = [("id", FieldKind::Ulid), ("rank", FieldKind::Uint)],
    indexes = [],
    store = MigrationTestStore,
    canister = MigrationTestCanister,
}

static ENTITY_RUNTIME_HOOKS: &[EntityRuntimeHooks<MigrationTestCanister>] =
    &[EntityRuntimeHooks::new(
        MigrationEntity::ENTITY_TAG,
        <MigrationEntity as crate::traits::EntitySchema>::MODEL,
        MigrationEntity::PATH,
        MigrationTestStore::PATH,
        prepare_row_commit_for_entity_with_structural_readers::<MigrationEntity>,
        validate_delete_strong_relations_for_source::<MigrationEntity>,
    )];

thread_local! {
    static DATA_STORE: RefCell<DataStore> = RefCell::new(DataStore::init(test_memory(41)));
    static INDEX_STORE: RefCell<IndexStore> = RefCell::new(IndexStore::init(test_memory(42)));
    static STORE_REGISTRY: StoreRegistry = {
        let mut reg = StoreRegistry::new();
        reg.register_store(MigrationTestStore::PATH, &DATA_STORE, &INDEX_STORE)
            .expect("migration test store registration should succeed");
        reg
    };
}

static DB: Db<MigrationTestCanister> = Db::new_with_hooks(&STORE_REGISTRY, ENTITY_RUNTIME_HOOKS);

fn with_migration_store<R>(f: impl FnOnce(StoreHandle) -> R) -> R {
    DB.with_store_registry(|reg| reg.try_get_store(MigrationTestStore::PATH).map(f))
        .expect("migration test store access should succeed")
}

// Reset marker + data/index stores to isolate migration tests.
fn reset_migration_state() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    clear_commit_marker_for_tests().expect("commit marker reset should succeed");
    clear_migration_state_bytes().expect("migration state reset should succeed");

    with_migration_store(|store| {
        store.with_data_mut(DataStore::clear);
        store.with_index_mut(IndexStore::clear);
    });
}

fn migration_data_key(id: Ulid) -> RawDataKey {
    DataKey::try_new::<MigrationEntity>(id)
        .expect("migration test data key should build")
        .to_raw()
        .expect("migration test data key should encode")
}

fn migration_row_bytes(entity: &MigrationEntity) -> Vec<u8> {
    RawRow::from_entity(entity)
        .expect("migration test row should encode")
        .as_bytes()
        .to_vec()
}

fn insert_row_op(entity: &MigrationEntity) -> CommitRowOp {
    CommitRowOp::new(
        MigrationEntity::PATH,
        migration_data_key(entity.id),
        None,
        Some(migration_row_bytes(entity)),
        commit_schema_fingerprint_for_entity::<MigrationEntity>(),
    )
}

fn row_bytes_for(id: Ulid) -> Option<Vec<u8>> {
    let key = migration_data_key(id);
    with_migration_store(|store| {
        store.with_data(|data_store| data_store.get(&key).map(|row| row.as_bytes().to_vec()))
    })
}

fn migration_store_snapshot() -> MigrationStoreSnapshot {
    with_migration_store(|store| {
        let mut data_rows = store.with_data(|data_store| {
            data_store
                .iter()
                .map(|entry| {
                    (
                        entry.key().as_bytes().to_vec(),
                        entry.value().as_bytes().to_vec(),
                    )
                })
                .collect::<Vec<_>>()
        });
        let mut index_rows = store.with_index(|index_store| {
            index_store
                .entries()
                .into_iter()
                .map(|(raw_key, raw_entry)| {
                    (raw_key.as_bytes().to_vec(), raw_entry.as_bytes().to_vec())
                })
                .collect::<Vec<_>>()
        });
        data_rows.sort();
        index_rows.sort();

        (data_rows, index_rows)
    })
}

fn two_step_migration_plan() -> MigrationPlan {
    let first = MigrationEntity {
        id: Ulid::from_u128(1_001),
        rank: 1,
    };
    let second = MigrationEntity {
        id: Ulid::from_u128(1_002),
        rank: 2,
    };
    MigrationPlan::new(
        "migration_two_step_insert",
        1,
        vec![
            MigrationStep::new("seed_first", vec![insert_row_op(&first)])
                .expect("first migration step should build"),
            MigrationStep::new("seed_second", vec![insert_row_op(&second)])
                .expect("second migration step should build"),
        ],
    )
    .expect("migration plan should build")
}

#[test]
fn migration_plan_contract_rejects_empty_labels_and_steps() {
    let step = MigrationStep::new(
        "seed",
        vec![insert_row_op(&MigrationEntity {
            id: Ulid::from_u128(2_001),
            rank: 7,
        })],
    )
    .expect("seed step should build");

    let empty_id =
        MigrationPlan::new("", 1, vec![step.clone()]).expect_err("empty migration id must fail");
    assert_eq!(empty_id.class, ErrorClass::Unsupported);
    assert_eq!(empty_id.origin, ErrorOrigin::Store);

    let zero_version = MigrationPlan::new("migration_zero_version", 0, vec![step])
        .expect_err("zero migration version must fail");
    assert_eq!(zero_version.class, ErrorClass::Unsupported);
    assert_eq!(zero_version.origin, ErrorOrigin::Store);

    let no_steps =
        MigrationPlan::new("migration_empty", 1, Vec::new()).expect_err("empty steps must fail");
    assert_eq!(no_steps.class, ErrorClass::Unsupported);
    assert_eq!(no_steps.origin, ErrorOrigin::Store);

    let empty_step_name = MigrationStep::new(
        "",
        vec![insert_row_op(&MigrationEntity {
            id: Ulid::from_u128(2_002),
            rank: 8,
        })],
    )
    .expect_err("empty step name must fail");
    assert_eq!(empty_step_name.class, ErrorClass::Unsupported);
    assert_eq!(empty_step_name.origin, ErrorOrigin::Store);

    let empty_step_ops =
        MigrationStep::new("empty_ops", Vec::new()).expect_err("empty step row ops must fail");
    assert_eq!(empty_step_ops.class, ErrorClass::Unsupported);
    assert_eq!(empty_step_ops.origin, ErrorOrigin::Store);
}

#[test]
fn migration_state_binary_codec_round_trips_and_rejects_trailing_bytes() {
    // Phase 1: round-trip one in-progress migration-state payload.
    let state = PersistedMigrationState {
        migration_id: "migration_binary_codec".to_string(),
        migration_version: 7,
        step_index: 3,
        last_applied_row_key: Some(vec![1, 2, 3, 4, 5]),
    };
    let bytes = encode_persisted_migration_state(&state)
        .expect("migration state payload should encode with the binary codec");
    let decoded = decode_persisted_migration_state(&bytes)
        .expect("migration state payload should decode with the binary codec");
    assert_eq!(
        decoded, state,
        "migration state binary codec must round-trip the full persisted state",
    );

    // Phase 2: reject trailing bytes so the payload stays exact and single-version.
    let mut trailing = bytes;
    trailing.push(0xAA);
    let err = decode_persisted_migration_state(&trailing)
        .expect_err("trailing bytes must fail migration state decode");
    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Serialize);
    assert!(
        err.message.contains("trailing bytes"),
        "decode error should explain the exact migration state codec failure: {err:?}",
    );
}

#[test]
fn migration_execution_is_deterministic_across_resume_boundaries() {
    let plan = two_step_migration_plan();

    // Phase 1: execute the full plan in one run and capture resulting stores.
    reset_migration_state();
    let full_outcome = execute_migration_plan(&DB, &plan, usize::MAX)
        .expect("full migration execution should succeed");
    assert_eq!(full_outcome.state(), MigrationRunState::Complete);
    assert_eq!(full_outcome.applied_steps(), 2);
    assert_eq!(full_outcome.applied_row_ops(), 2);
    let full_snapshot = migration_store_snapshot();

    // Phase 2: execute one step and resume from durable internal cursor state.
    reset_migration_state();
    let first_pass = execute_migration_plan(&DB, &plan, 1)
        .expect("bounded first migration execution should succeed");
    assert_eq!(first_pass.state(), MigrationRunState::NeedsResume);
    assert_eq!(first_pass.cursor().next_step(), 1);
    assert_eq!(first_pass.applied_steps(), 1);
    assert_eq!(first_pass.applied_row_ops(), 1);

    let second_pass = execute_migration_plan(&DB, &plan, usize::MAX)
        .expect("resumed migration execution should succeed");
    assert_eq!(second_pass.state(), MigrationRunState::Complete);
    assert_eq!(second_pass.cursor().next_step(), plan.len());
    let resumed_snapshot = migration_store_snapshot();

    assert_eq!(
        resumed_snapshot, full_snapshot,
        "full execution and resume-bounded execution must converge on identical store state",
    );
}

#[test]
fn migration_execution_recovers_pending_marker_before_running_steps() {
    reset_migration_state();

    let replay_seed = MigrationEntity {
        id: Ulid::from_u128(3_001),
        rank: 41,
    };
    let pending_marker =
        CommitMarker::new(vec![insert_row_op(&replay_seed)]).expect("pending marker should build");
    begin_commit(pending_marker).expect("begin_commit should persist pending marker");
    assert!(
        commit_marker_present().expect("marker presence check should succeed"),
        "pending marker must be present before migration execution starts",
    );

    let plan = MigrationPlan::new(
        "migration_after_recovery",
        1,
        vec![
            MigrationStep::new(
                "insert_after_recovery",
                vec![insert_row_op(&MigrationEntity {
                    id: Ulid::from_u128(3_002),
                    rank: 42,
                })],
            )
            .expect("migration step should build"),
        ],
    )
    .expect("migration plan should build");

    let outcome = execute_migration_plan(&DB, &plan, usize::MAX)
        .expect("migration execution should run after recovery replay");
    assert_eq!(outcome.state(), MigrationRunState::Complete);
    assert!(
        row_bytes_for(replay_seed.id).is_some(),
        "recovery replay should materialize pending-marker rows before migration execution",
    );
    assert!(
        row_bytes_for(Ulid::from_u128(3_002)).is_some(),
        "migration step should run after recovery replay completes",
    );
    assert!(
        !commit_marker_present().expect("marker presence check should succeed"),
        "marker should be cleared after recovery + migration execution succeed",
    );
}

#[test]
fn migration_step_failure_is_classified_and_keeps_marker_authority() {
    reset_migration_state();

    let valid = MigrationEntity {
        id: Ulid::from_u128(4_001),
        rank: 51,
    };
    let invalid_path = "migration::UnknownEntity";
    let invalid = CommitRowOp::new(
        invalid_path,
        migration_data_key(Ulid::from_u128(4_002)),
        None,
        Some(migration_row_bytes(&MigrationEntity {
            id: Ulid::from_u128(4_002),
            rank: 52,
        })),
        [0u8; 16],
    );
    let plan = MigrationPlan::new(
        "migration_failure_path",
        1,
        vec![
            MigrationStep::new("mixed_valid_invalid", vec![insert_row_op(&valid), invalid])
                .expect("migration failure step should build"),
        ],
    )
    .expect("migration failure plan should build");

    let err = execute_migration_plan(&DB, &plan, usize::MAX)
        .expect_err("migration with invalid row op path must fail closed");
    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Store);
    assert!(
        err.message
            .contains("migration 'migration_failure_path' step 0"),
        "migration failure should include plan/step observability context: {err:?}",
    );
    assert!(
        commit_marker_present().expect("marker presence check should succeed"),
        "failed migration step must keep marker authority for explicit recovery",
    );
    assert_eq!(
        row_bytes_for(valid.id),
        None,
        "pre-apply step preparation must prevent partial row writes on migration-step failure",
    );

    // Cleanup so unrelated tests do not observe this intentionally-failed marker.
    clear_commit_marker_for_tests().expect("commit marker cleanup should succeed");
    clear_migration_state_bytes().expect("migration state cleanup should succeed");
}

#[test]
fn migration_rejects_different_plan_while_persisted_cursor_is_in_progress() {
    reset_migration_state();

    let in_progress_plan = two_step_migration_plan();
    let first_pass = execute_migration_plan(&DB, &in_progress_plan, 1)
        .expect("first bounded pass should persist in-progress migration state");
    assert_eq!(first_pass.state(), MigrationRunState::NeedsResume);

    let different_plan = MigrationPlan::new(
        "migration_different_plan",
        1,
        vec![
            MigrationStep::new(
                "different_seed",
                vec![insert_row_op(&MigrationEntity {
                    id: Ulid::from_u128(5_001),
                    rank: 61,
                })],
            )
            .expect("different migration step should build"),
        ],
    )
    .expect("different migration plan should build");

    let err = execute_migration_plan(&DB, &different_plan, usize::MAX)
        .expect_err("different migration id must fail while persisted state is in progress");
    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Store);
    assert!(
        err.message.contains("migration_different_plan"),
        "error should include requested migration id: {err:?}",
    );
    assert!(
        err.message.contains(in_progress_plan.id()),
        "error should include in-progress migration id: {err:?}",
    );

    clear_migration_state_bytes().expect("migration state cleanup should succeed");
}

#[test]
fn migration_rejects_different_plan_version_for_same_id_while_in_progress() {
    reset_migration_state();

    let in_progress_plan = two_step_migration_plan();
    let first_pass = execute_migration_plan(&DB, &in_progress_plan, 1)
        .expect("first bounded pass should persist in-progress migration state");
    assert_eq!(first_pass.state(), MigrationRunState::NeedsResume);

    let different_version_plan = MigrationPlan::new(
        "migration_two_step_insert",
        2,
        vec![
            MigrationStep::new(
                "different_version_seed",
                vec![insert_row_op(&MigrationEntity {
                    id: Ulid::from_u128(6_001),
                    rank: 71,
                })],
            )
            .expect("different-version migration step should build"),
        ],
    )
    .expect("different-version migration plan should build");

    let err = execute_migration_plan(&DB, &different_version_plan, usize::MAX).expect_err(
        "same migration id with different version must fail while persisted state is in progress",
    );
    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Store);
    assert!(
        err.message.contains("migration_two_step_insert@2"),
        "error should include requested migration id@version: {err:?}",
    );
    assert!(
        err.message.contains("migration_two_step_insert@1"),
        "error should include in-progress migration id@version: {err:?}",
    );

    clear_migration_state_bytes().expect("migration state cleanup should succeed");
}

#[test]
fn migration_recovery_does_not_reexecute_step_when_marker_already_bound_progress() {
    reset_migration_state();

    let seeded = MigrationEntity {
        id: Ulid::from_u128(7_001),
        rank: 81,
    };
    let plan = MigrationPlan::new(
        "migration_marker_bound_progress",
        1,
        vec![
            MigrationStep::new("seed_once", vec![insert_row_op(&seeded)])
                .expect("migration step should build"),
        ],
    )
    .expect("migration plan should build");

    let step = plan
        .step_at(0)
        .expect("first migration step should resolve");
    let marker =
        CommitMarker::new(step.row_ops.clone()).expect("migration marker payload should build");
    let next_cursor = MigrationCursor::start().advance();
    let state_bytes = encode_durable_cursor_state(&plan, next_cursor, step.row_ops.last())
        .expect("durable next-step migration state should encode");

    begin_commit_with_migration_state(marker, state_bytes)
        .expect("begin commit with migration state should persist marker + progress");
    assert!(
        commit_marker_present().expect("marker presence check should succeed"),
        "migration marker should be present before simulated crash recovery",
    );

    let outcome = execute_migration_plan(&DB, &plan, usize::MAX)
        .expect("recovery-backed migration execution should succeed");
    assert_eq!(
        outcome.state(),
        MigrationRunState::Complete,
        "migration should complete after recovery replay",
    );
    assert_eq!(
        outcome.applied_steps(),
        0,
        "recovered marker step must not be executed a second time",
    );
    assert!(
        row_bytes_for(seeded.id).is_some(),
        "recovery should apply the marker row op exactly once",
    );
    assert!(
        !commit_marker_present().expect("marker presence check should succeed"),
        "recovery should clear the in-flight marker after replay",
    );
}

#[test]
fn migration_recovery_resumes_remaining_steps_after_marker_bound_crash() {
    reset_migration_state();

    let plan = two_step_migration_plan();
    let step_zero = plan
        .step_at(0)
        .expect("first migration step should resolve for crash simulation");
    let marker =
        CommitMarker::new(step_zero.row_ops.clone()).expect("step-zero marker should build");
    let state_bytes = encode_durable_cursor_state(
        &plan,
        MigrationCursor::start().advance(),
        step_zero.row_ops.last(),
    )
    .expect("step-one durable migration state should encode");

    begin_commit_with_migration_state(marker, state_bytes)
        .expect("begin commit with migration state should persist crash fixture marker");

    let resumed = execute_migration_plan(&DB, &plan, usize::MAX)
        .expect("recovery should replay step 0 and execute only remaining steps");
    assert_eq!(resumed.state(), MigrationRunState::Complete);
    assert_eq!(
        resumed.applied_steps(),
        1,
        "resume run should apply only step 1 after replaying persisted step 0 marker",
    );
    assert_eq!(resumed.cursor().next_step(), plan.len());

    let (data_rows, _) = migration_store_snapshot();
    assert_eq!(
        data_rows.len(),
        2,
        "both migration rows should be present once"
    );
}
