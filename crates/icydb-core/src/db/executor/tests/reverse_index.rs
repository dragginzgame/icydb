//! Module: db::executor::tests::reverse_index
//! Responsibility: module-local ownership and contracts for strong-relation delete validation and reverse-index recovery.
//! Does not own: unrelated executor orchestration outside reverse-index state transitions.
//! Boundary: exposes this module API while keeping implementation details internal.

use super::*;
use crate::db::data::{DataKey, RawDataKey};
use canic_cdk::structures::Storable;
use std::borrow::Cow;

// Build canonical persisted row bytes for relation-source replay markers.
fn relation_source_row_bytes(entity: &RelationSourceEntity) -> Vec<u8> {
    crate::db::data::RawRow::from_entity(entity)
        .expect("relation source row should serialize canonically")
        .as_bytes()
        .to_vec()
}

#[test]
fn delete_blocks_when_target_has_strong_referrer() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_id = Ulid::from_u128(9_001);
    let source_id = Ulid::from_u128(9_002);

    let target_save = SaveExecutor::<RelationTargetEntity>::new(REL_DB, false);
    target_save
        .insert(RelationTargetEntity { id: target_id })
        .expect("target save should succeed");

    let source_save = SaveExecutor::<RelationSourceEntity>::new(REL_DB, false);
    source_save
        .insert(RelationSourceEntity {
            id: source_id,
            target: target_id,
        })
        .expect("source save should succeed");

    let target_delete = DeleteExecutor::<RelationTargetEntity>::new(REL_DB);
    let delete_plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_id)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target delete plan should build");
    let err = target_delete
        .execute(delete_plan)
        .expect_err("target delete should be blocked by strong relation");
    assert_eq!(
        err.class,
        crate::error::ErrorClass::Unsupported,
        "blocked strong-relation delete should classify as unsupported",
    );
    assert_eq!(
        err.origin,
        crate::error::ErrorOrigin::Executor,
        "blocked strong-relation delete should originate from executor validation",
    );

    assert!(
        err.message.contains("delete blocked by strong relation"),
        "unexpected error: {err:?}"
    );
    assert!(
        err.message
            .contains(&format!("source_entity={}", RelationSourceEntity::PATH)),
        "diagnostic should include source entity path: {err:?}",
    );
    assert!(
        err.message.contains("source_field=target"),
        "diagnostic should include relation field name: {err:?}",
    );
    assert!(
        err.message
            .contains(&format!("target_entity={}", RelationTargetEntity::PATH)),
        "diagnostic should include target entity path: {err:?}",
    );
    assert!(
        err.message
            .contains("action=delete source rows or retarget relation before deleting target"),
        "diagnostic should include operator action hint: {err:?}",
    );

    let target_rows = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_data(|data_store| data_store.iter().count()))
        })
        .expect("target store access should succeed");
    let source_rows = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationSourceStore::PATH)
                .map(|store| store.with_data(|data_store| data_store.iter().count()))
        })
        .expect("source store access should succeed");
    assert_eq!(target_rows, 1, "blocked delete must keep target row");
    assert_eq!(source_rows, 1, "blocked delete must keep source row");
}

#[test]
fn delete_target_succeeds_after_strong_referrer_is_removed() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_id = Ulid::from_u128(9_101);
    let source_id = Ulid::from_u128(9_102);

    let target_save = SaveExecutor::<RelationTargetEntity>::new(REL_DB, false);
    target_save
        .insert(RelationTargetEntity { id: target_id })
        .expect("target save should succeed");

    let source_save = SaveExecutor::<RelationSourceEntity>::new(REL_DB, false);
    source_save
        .insert(RelationSourceEntity {
            id: source_id,
            target: target_id,
        })
        .expect("source save should succeed");

    let source_delete = DeleteExecutor::<RelationSourceEntity>::new(REL_DB);
    let source_delete_plan = Query::<RelationSourceEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(source_id)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("source delete plan should build");
    let deleted_sources = source_delete
        .execute(source_delete_plan)
        .expect("source delete should succeed");
    assert_eq!(deleted_sources.len(), 1, "source row should be removed");

    let target_delete = DeleteExecutor::<RelationTargetEntity>::new(REL_DB);
    let target_delete_plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_id)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target delete plan should build");
    let deleted_targets = target_delete
        .execute(target_delete_plan)
        .expect("target delete should succeed once referrer is removed");
    assert_eq!(deleted_targets.len(), 1, "target row should be removed");
}

#[test]
fn delete_allows_target_with_weak_single_referrer() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_id = Ulid::from_u128(9_111);
    let source_id = Ulid::from_u128(9_112);

    SaveExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .insert(RelationTargetEntity { id: target_id })
        .expect("target save should succeed");
    SaveExecutor::<WeakSingleRelationSourceEntity>::new(REL_DB, false)
        .insert(WeakSingleRelationSourceEntity {
            id: source_id,
            target: target_id,
        })
        .expect("weak source save should succeed");

    let reverse_rows_before_delete = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_before_delete, 0,
        "weak relation should not create reverse strong-relation index entries",
    );

    let target_delete_plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_id)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target delete plan should build");
    let deleted_targets = DeleteExecutor::<RelationTargetEntity>::new(REL_DB)
        .execute(target_delete_plan)
        .expect("target delete should succeed for weak referrer");
    assert_eq!(deleted_targets.len(), 1, "target row should be removed");

    let source_plan = Query::<WeakSingleRelationSourceEntity>::new(MissingRowPolicy::Ignore)
        .by_id(source_id)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("source load plan should build");
    let remaining_source = LoadExecutor::<WeakSingleRelationSourceEntity>::new(REL_DB, false)
        .execute(source_plan)
        .expect("source load should succeed");
    assert_eq!(remaining_source.len(), 1, "weak source row should remain");
    assert_eq!(
        remaining_source[0].entity_ref().target,
        target_id,
        "weak source relation value should be preserved",
    );
}

#[test]
fn delete_allows_target_with_weak_optional_referrer() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_id = Ulid::from_u128(9_121);
    let source_id = Ulid::from_u128(9_122);

    SaveExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .insert(RelationTargetEntity { id: target_id })
        .expect("target save should succeed");
    SaveExecutor::<WeakOptionalRelationSourceEntity>::new(REL_DB, false)
        .insert(WeakOptionalRelationSourceEntity {
            id: source_id,
            target: Some(target_id),
        })
        .expect("weak optional source save should succeed");

    let reverse_rows_before_delete = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_before_delete, 0,
        "weak optional relation should not create reverse strong-relation index entries",
    );

    let target_delete_plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_id)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target delete plan should build");
    let deleted_targets = DeleteExecutor::<RelationTargetEntity>::new(REL_DB)
        .execute(target_delete_plan)
        .expect("target delete should succeed for weak optional referrer");
    assert_eq!(deleted_targets.len(), 1, "target row should be removed");

    let source_plan = Query::<WeakOptionalRelationSourceEntity>::new(MissingRowPolicy::Ignore)
        .by_id(source_id)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("source load plan should build");
    let remaining_source = LoadExecutor::<WeakOptionalRelationSourceEntity>::new(REL_DB, false)
        .execute(source_plan)
        .expect("source load should succeed");
    assert_eq!(
        remaining_source.len(),
        1,
        "weak optional source row should remain"
    );
    assert_eq!(
        remaining_source[0].entity_ref().target,
        Some(target_id),
        "weak optional source relation value should be preserved",
    );
}

#[test]
fn delete_allows_target_with_weak_list_referrer() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_id = Ulid::from_u128(9_131);
    let source_id = Ulid::from_u128(9_132);

    SaveExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .insert(RelationTargetEntity { id: target_id })
        .expect("target save should succeed");
    SaveExecutor::<WeakListRelationSourceEntity>::new(REL_DB, false)
        .insert(WeakListRelationSourceEntity {
            id: source_id,
            targets: vec![target_id],
        })
        .expect("weak list source save should succeed");

    let reverse_rows_before_delete = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_before_delete, 0,
        "weak list relation should not create reverse strong-relation index entries",
    );

    let target_delete_plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_id)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target delete plan should build");
    let deleted_targets = DeleteExecutor::<RelationTargetEntity>::new(REL_DB)
        .execute(target_delete_plan)
        .expect("target delete should succeed for weak list referrer");
    assert_eq!(deleted_targets.len(), 1, "target row should be removed");

    let source_plan = Query::<WeakListRelationSourceEntity>::new(MissingRowPolicy::Ignore)
        .by_id(source_id)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("source load plan should build");
    let remaining_source = LoadExecutor::<WeakListRelationSourceEntity>::new(REL_DB, false)
        .execute(source_plan)
        .expect("source load should succeed");
    assert_eq!(
        remaining_source.len(),
        1,
        "weak list source row should remain"
    );
    assert_eq!(
        remaining_source[0].entity_ref().targets,
        vec![target_id],
        "weak list source relation values should be preserved",
    );
}

#[test]
fn strong_relation_reverse_index_tracks_source_lifecycle() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_id = Ulid::from_u128(9_201);
    let source_id = Ulid::from_u128(9_202);

    let target_save = SaveExecutor::<RelationTargetEntity>::new(REL_DB, false);
    target_save
        .insert(RelationTargetEntity { id: target_id })
        .expect("target save should succeed");

    let source_save = SaveExecutor::<RelationSourceEntity>::new(REL_DB, false);
    source_save
        .insert(RelationSourceEntity {
            id: source_id,
            target: target_id,
        })
        .expect("source save should succeed");

    let reverse_rows_after_insert = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_after_insert, 1,
        "target index store should contain one reverse-relation entry after source insert",
    );

    let source_delete = DeleteExecutor::<RelationSourceEntity>::new(REL_DB);
    let source_delete_plan = Query::<RelationSourceEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(source_id)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("source delete plan should build");
    source_delete
        .execute(source_delete_plan)
        .expect("source delete should succeed");

    let reverse_rows_after_delete = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_after_delete, 0,
        "target index store reverse entry should be removed after source delete",
    );
}

#[test]
fn strong_relation_reverse_index_moves_on_fk_update() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_a = Ulid::from_u128(9_301);
    let target_b = Ulid::from_u128(9_302);
    let source_id = Ulid::from_u128(9_303);

    let target_save = SaveExecutor::<RelationTargetEntity>::new(REL_DB, false);
    target_save
        .insert(RelationTargetEntity { id: target_a })
        .expect("target A save should succeed");
    target_save
        .insert(RelationTargetEntity { id: target_b })
        .expect("target B save should succeed");

    let source_save = SaveExecutor::<RelationSourceEntity>::new(REL_DB, false);
    source_save
        .insert(RelationSourceEntity {
            id: source_id,
            target: target_a,
        })
        .expect("source insert should succeed");

    source_save
        .replace(RelationSourceEntity {
            id: source_id,
            target: target_b,
        })
        .expect("source replace should move relation target");

    let reverse_rows_after_update = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_after_update, 1,
        "reverse index should remove old target entry and keep only the new one",
    );

    let old_target_delete_plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_a)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target A delete plan should build");
    let deleted_a = DeleteExecutor::<RelationTargetEntity>::new(REL_DB)
        .execute(old_target_delete_plan)
        .expect("old target should be deletable after relation retarget");
    assert_eq!(deleted_a.len(), 1, "old target should delete cleanly");

    let protected_target_delete_plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_b)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target B delete plan should build");
    let err = DeleteExecutor::<RelationTargetEntity>::new(REL_DB)
        .execute(protected_target_delete_plan)
        .expect_err("new target should remain protected by strong relation");
    assert_eq!(
        err.class,
        crate::error::ErrorClass::Unsupported,
        "blocked strong-relation delete should classify as unsupported",
    );
    assert_eq!(
        err.origin,
        crate::error::ErrorOrigin::Executor,
        "blocked strong-relation delete should originate from executor validation",
    );
    assert!(
        err.message.contains("delete blocked by strong relation"),
        "unexpected error: {err:?}",
    );
}

#[test]
fn recovery_replays_reverse_relation_index_mutations() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_id = Ulid::from_u128(9_401);
    let source_id = Ulid::from_u128(9_402);

    let target_save = SaveExecutor::<RelationTargetEntity>::new(REL_DB, false);
    target_save
        .insert(RelationTargetEntity { id: target_id })
        .expect("target save should succeed");

    let source = RelationSourceEntity {
        id: source_id,
        target: target_id,
    };
    let raw_key = DataKey::try_new::<RelationSourceEntity>(source.id)
        .expect("source data key should build")
        .to_raw()
        .expect("source data key should encode");
    let row_bytes = crate::db::data::RawRow::from_entity(&source)
        .expect("source row should serialize")
        .as_bytes()
        .to_vec();

    let marker = CommitMarker::new(vec![crate::db::commit::CommitRowOp::new(
        RelationSourceEntity::PATH,
        raw_key,
        None,
        Some(row_bytes),
        crate::db::schema::commit_schema_fingerprint_for_entity::<RelationSourceEntity>(),
    )])
    .expect("commit marker creation should succeed");

    begin_commit(marker).expect("begin_commit should persist marker");
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be present before recovery replay",
    );

    ensure_recovered(&REL_DB).expect("recovery replay should succeed");
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after recovery replay",
    );

    let reverse_rows_after_replay = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_after_replay, 1,
        "recovery replay should materialize reverse relation index entries",
    );

    let target_delete = DeleteExecutor::<RelationTargetEntity>::new(REL_DB);
    let delete_plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_id)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target delete plan should build");
    let err = target_delete
        .execute(delete_plan)
        .expect_err("target delete should be blocked after replayed reverse index insert");
    assert_eq!(
        err.class,
        crate::error::ErrorClass::Unsupported,
        "blocked strong-relation delete should classify as unsupported",
    );
    assert_eq!(
        err.origin,
        crate::error::ErrorOrigin::Executor,
        "blocked strong-relation delete should originate from executor validation",
    );
    assert!(
        err.message.contains("delete blocked by strong relation"),
        "unexpected error: {err:?}",
    );
}

/// Tests the startup rebuild path with direct store tampering, so the setup stays deliberately verbose.
#[expect(clippy::too_many_lines)]
#[test]
fn recovery_startup_rebuild_drops_orphan_reverse_relation_entries() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    // Phase 1: seed two valid targets and two source refs to build two reverse entries.
    let target_live = Ulid::from_u128(9_410);
    let target_orphan = Ulid::from_u128(9_411);
    let source_live = Ulid::from_u128(9_412);
    let source_orphan = Ulid::from_u128(9_413);
    let target_save = SaveExecutor::<RelationTargetEntity>::new(REL_DB, false);
    target_save
        .insert(RelationTargetEntity { id: target_live })
        .expect("live target save should succeed");
    target_save
        .insert(RelationTargetEntity { id: target_orphan })
        .expect("orphan target save should succeed");

    let source_save = SaveExecutor::<RelationSourceEntity>::new(REL_DB, false);
    source_save
        .insert(RelationSourceEntity {
            id: source_live,
            target: target_live,
        })
        .expect("live source save should succeed");
    source_save
        .insert(RelationSourceEntity {
            id: source_orphan,
            target: target_orphan,
        })
        .expect("orphan source save should succeed");

    // Phase 2: simulate stale reverse-index drift by removing one source row directly.
    let orphan_source_key = DataKey::try_new::<RelationSourceEntity>(source_orphan)
        .expect("orphan source key should build")
        .to_raw()
        .expect("orphan source key should encode");
    REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationSourceStore::PATH).map(|store| {
                let removed =
                    store.with_data_mut(|data_store| data_store.remove(&orphan_source_key));
                assert!(
                    removed.is_some(),
                    "orphan source row should exist before direct data-store removal",
                );
            })
        })
        .expect("relation source store access should succeed");

    let reverse_rows_before_recovery = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_before_recovery, 2,
        "stale reverse entry should remain until startup rebuild runs",
    );

    // Phase 3: force startup recovery rebuild and assert stale reverse entry is purged.
    let marker = CommitMarker::new(Vec::new()).expect("marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");
    ensure_recovered(&REL_DB).expect("startup recovery rebuild should succeed");
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after startup recovery rebuild",
    );

    let reverse_rows_after_recovery = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_after_recovery, 1,
        "startup rebuild should drop orphan reverse entries and keep live ones",
    );

    let delete_orphan_target = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_orphan)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("orphan target delete plan should build");
    let deleted_orphan_target = DeleteExecutor::<RelationTargetEntity>::new(REL_DB)
        .execute(delete_orphan_target)
        .expect("orphan target should be deletable after startup rebuild");
    assert_eq!(
        deleted_orphan_target.len(),
        1,
        "orphan target should delete after stale reverse entry is purged",
    );

    let delete_live_target = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_live)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("live target delete plan should build");
    let err = DeleteExecutor::<RelationTargetEntity>::new(REL_DB)
        .execute(delete_live_target)
        .expect_err("live target should remain protected by surviving relation");
    assert_eq!(
        err.class,
        crate::error::ErrorClass::Unsupported,
        "blocked strong-relation delete should classify as unsupported",
    );
    assert_eq!(
        err.origin,
        crate::error::ErrorOrigin::Executor,
        "blocked strong-relation delete should originate from executor validation",
    );
    assert!(
        err.message.contains("delete blocked by strong relation"),
        "unexpected error: {err:?}",
    );
}

#[test]
fn recovery_startup_rebuild_restores_missing_reverse_relation_entry() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    // Phase 1: seed one valid strong relation so forward+reverse state starts consistent.
    let target_id = Ulid::from_u128(9_420);
    let source_id = Ulid::from_u128(9_421);
    SaveExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .insert(RelationTargetEntity { id: target_id })
        .expect("target save should succeed");
    SaveExecutor::<RelationSourceEntity>::new(REL_DB, false)
        .insert(RelationSourceEntity {
            id: source_id,
            target: target_id,
        })
        .expect("source save should succeed");

    // Phase 2: simulate partial-commit drift by removing reverse index state only.
    REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index_mut(IndexStore::clear))
        })
        .expect("target index store access should succeed");
    let reverse_rows_before_recovery = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_before_recovery, 0,
        "simulated partial-commit state should have missing reverse entry",
    );

    // Phase 3: force startup recovery rebuild and verify reverse symmetry is restored.
    let marker = CommitMarker::new(Vec::new()).expect("marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");
    ensure_recovered(&REL_DB).expect("startup recovery rebuild should succeed");
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after startup recovery rebuild",
    );

    let reverse_rows_after_recovery = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_after_recovery, 1,
        "startup rebuild should restore missing reverse entry from authoritative source row",
    );

    let delete_target = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_id)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target delete plan should build");
    let err = DeleteExecutor::<RelationTargetEntity>::new(REL_DB)
        .execute(delete_target)
        .expect_err("restored reverse entry should block target delete");
    assert_eq!(
        err.class,
        crate::error::ErrorClass::Unsupported,
        "blocked strong-relation delete should classify as unsupported",
    );
    assert_eq!(
        err.origin,
        crate::error::ErrorOrigin::Executor,
        "blocked strong-relation delete should originate from executor validation",
    );
    assert!(
        err.message.contains("delete blocked by strong relation"),
        "unexpected error: {err:?}",
    );
}

/// Exercises a multi-marker replay sequence, so the setup is intentionally linear and explicit.
#[test]
#[expect(clippy::too_many_lines)]
fn recovery_replays_reverse_index_mixed_save_save_delete_sequence() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_id = Ulid::from_u128(9_451);
    let source_a = Ulid::from_u128(9_452);
    let source_b = Ulid::from_u128(9_453);

    SaveExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .insert(RelationTargetEntity { id: target_id })
        .expect("target save should succeed");

    let first_source_key = DataKey::try_new::<RelationSourceEntity>(source_a)
        .expect("first source key should build")
        .to_raw()
        .expect("first source key should encode");
    let second_source_key = DataKey::try_new::<RelationSourceEntity>(source_b)
        .expect("second source key should build")
        .to_raw()
        .expect("second source key should encode");
    let first_source_row_bytes = relation_source_row_bytes(&RelationSourceEntity {
        id: source_a,
        target: target_id,
    });
    let second_source_row_bytes = relation_source_row_bytes(&RelationSourceEntity {
        id: source_b,
        target: target_id,
    });

    // Phase 1: replay first save marker.
    let first_save_marker = CommitMarker::new(vec![crate::db::commit::CommitRowOp::new(
        RelationSourceEntity::PATH,
        first_source_key,
        None,
        Some(first_source_row_bytes.clone()),
        crate::db::schema::commit_schema_fingerprint_for_entity::<RelationSourceEntity>(),
    )])
    .expect("first save marker creation should succeed");
    begin_commit(first_save_marker).expect("begin_commit should persist marker");
    ensure_recovered(&REL_DB).expect("first save recovery replay should succeed");

    let reverse_rows_after_save_a = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_after_save_a, 1,
        "first save replay should create one reverse entry",
    );

    // Phase 2: replay second save marker targeting the same target key.
    let second_save_marker = CommitMarker::new(vec![crate::db::commit::CommitRowOp::new(
        RelationSourceEntity::PATH,
        second_source_key,
        None,
        Some(second_source_row_bytes),
        crate::db::schema::commit_schema_fingerprint_for_entity::<RelationSourceEntity>(),
    )])
    .expect("second save marker creation should succeed");
    begin_commit(second_save_marker).expect("begin_commit should persist marker");
    ensure_recovered(&REL_DB).expect("second save recovery replay should succeed");

    let reverse_rows_after_save_b = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_after_save_b, 1,
        "second save replay should merge into the existing reverse entry",
    );

    // Phase 3: replay delete marker for one source row.
    let delete_a_marker = CommitMarker::new(vec![crate::db::commit::CommitRowOp::new(
        RelationSourceEntity::PATH,
        first_source_key,
        Some(first_source_row_bytes),
        None,
        crate::db::schema::commit_schema_fingerprint_for_entity::<RelationSourceEntity>(),
    )])
    .expect("delete marker creation should succeed");
    begin_commit(delete_a_marker).expect("begin_commit should persist marker");
    ensure_recovered(&REL_DB).expect("delete recovery replay should succeed");

    let reverse_rows_after_delete_a = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_after_delete_a, 1,
        "delete replay should keep reverse entry while one referrer remains",
    );

    let target_delete_plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_id)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target delete plan should build");
    let err = DeleteExecutor::<RelationTargetEntity>::new(REL_DB)
        .execute(target_delete_plan)
        .expect_err("target delete should remain blocked by surviving source row");
    assert_eq!(
        err.class,
        crate::error::ErrorClass::Unsupported,
        "blocked strong-relation delete should classify as unsupported",
    );
    assert_eq!(
        err.origin,
        crate::error::ErrorOrigin::Executor,
        "blocked strong-relation delete should originate from executor validation",
    );
    assert!(
        err.message.contains("delete blocked by strong relation"),
        "unexpected error: {err:?}",
    );

    let source_delete_plan = Query::<RelationSourceEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(source_b)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("source B delete plan should build");
    DeleteExecutor::<RelationSourceEntity>::new(REL_DB)
        .execute(source_delete_plan)
        .expect("source B delete should succeed");

    let retry_target_delete_plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_id)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("retry target delete plan should build");
    let deleted_target = DeleteExecutor::<RelationTargetEntity>::new(REL_DB)
        .execute(retry_target_delete_plan)
        .expect("target should delete once all referrers are removed");
    assert_eq!(deleted_target.len(), 1, "target row should be removed");
}

#[test]
fn recovery_replays_retarget_update_moves_reverse_index_membership() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_a = Ulid::from_u128(9_461);
    let target_b = Ulid::from_u128(9_462);
    let source_id = Ulid::from_u128(9_463);

    SaveExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .insert(RelationTargetEntity { id: target_a })
        .expect("target A save should succeed");
    SaveExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .insert(RelationTargetEntity { id: target_b })
        .expect("target B save should succeed");
    SaveExecutor::<RelationSourceEntity>::new(REL_DB, false)
        .insert(RelationSourceEntity {
            id: source_id,
            target: target_a,
        })
        .expect("source insert should succeed");

    let source_key = DataKey::try_new::<RelationSourceEntity>(source_id)
        .expect("source key should build")
        .to_raw()
        .expect("source key should encode");
    let before_row_bytes = relation_source_row_bytes(&RelationSourceEntity {
        id: source_id,
        target: target_a,
    });
    let after_row_bytes = relation_source_row_bytes(&RelationSourceEntity {
        id: source_id,
        target: target_b,
    });

    let marker = CommitMarker::new(vec![crate::db::commit::CommitRowOp::new(
        RelationSourceEntity::PATH,
        source_key,
        Some(before_row_bytes),
        Some(after_row_bytes),
        crate::db::schema::commit_schema_fingerprint_for_entity::<RelationSourceEntity>(),
    )])
    .expect("commit marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");
    ensure_recovered(&REL_DB).expect("recovery replay should succeed");

    let reverse_rows_after_retarget = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_after_retarget, 1,
        "retarget replay should keep one reverse entry mapped to the new target",
    );

    let delete_target_a = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_a)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target A delete plan should build");
    let removed_old_target = DeleteExecutor::<RelationTargetEntity>::new(REL_DB)
        .execute(delete_target_a)
        .expect("old target should be deletable after replayed retarget");
    assert_eq!(removed_old_target.len(), 1, "old target should be removed");

    let delete_target_b = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_b)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target B delete plan should build");
    let err = DeleteExecutor::<RelationTargetEntity>::new(REL_DB)
        .execute(delete_target_b)
        .expect_err("new target should remain blocked by relation referrer");
    assert_eq!(
        err.class,
        crate::error::ErrorClass::Unsupported,
        "blocked strong-relation delete should classify as unsupported",
    );
    assert_eq!(
        err.origin,
        crate::error::ErrorOrigin::Executor,
        "blocked strong-relation delete should originate from executor validation",
    );
    assert!(
        err.message.contains("delete blocked by strong relation"),
        "unexpected error: {err:?}",
    );
}

/// Covers the rollback path after a later malformed row op invalidates the marker replay.
#[expect(clippy::too_many_lines)]
#[test]
fn recovery_rollback_restores_reverse_index_state_on_prepare_error() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_a = Ulid::from_u128(9_471);
    let target_b = Ulid::from_u128(9_472);
    let source_id = Ulid::from_u128(9_473);

    SaveExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .insert(RelationTargetEntity { id: target_a })
        .expect("target A save should succeed");
    SaveExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .insert(RelationTargetEntity { id: target_b })
        .expect("target B save should succeed");
    SaveExecutor::<RelationSourceEntity>::new(REL_DB, false)
        .insert(RelationSourceEntity {
            id: source_id,
            target: target_a,
        })
        .expect("source insert should succeed");

    let source_key = DataKey::try_new::<RelationSourceEntity>(source_id)
        .expect("source key should build")
        .to_raw()
        .expect("source key should encode");
    let source_raw_key = source_key;
    let update_before_row_bytes = relation_source_row_bytes(&RelationSourceEntity {
        id: source_id,
        target: target_a,
    });
    let update_after_row_bytes = relation_source_row_bytes(&RelationSourceEntity {
        id: source_id,
        target: target_b,
    });

    let mut malformed_key = vec![0u8; DataKey::STORED_SIZE_USIZE];
    malformed_key[DataKey::ENTITY_TAG_SIZE_USIZE] = 0xFF;
    let malformed_raw_key = RawDataKey::from_bytes(Cow::Owned(malformed_key));

    let marker = CommitMarker::new(vec![
        crate::db::commit::CommitRowOp::new(
            RelationSourceEntity::PATH,
            source_key,
            Some(update_before_row_bytes),
            Some(update_after_row_bytes),
            crate::db::schema::commit_schema_fingerprint_for_entity::<RelationSourceEntity>(),
        ),
        crate::db::commit::CommitRowOp::new(
            RelationSourceEntity::PATH,
            malformed_raw_key,
            None,
            Some(vec![1]),
            crate::db::schema::commit_schema_fingerprint_for_entity::<RelationSourceEntity>(),
        ),
    ])
    .expect("commit marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");

    let err =
        ensure_recovered(&REL_DB).expect_err("recovery should fail when a later row op is invalid");
    assert_eq!(
        err.class,
        crate::error::ErrorClass::Corruption,
        "prepare failure should surface corruption for malformed key shape",
    );
    assert_eq!(
        err.origin,
        crate::error::ErrorOrigin::Recovery,
        "malformed key bytes should surface recovery-boundary origin",
    );

    let marker_still_present = match commit_marker_present() {
        Ok(present) => present,
        Err(err) => {
            assert_eq!(
                err.class,
                crate::error::ErrorClass::Corruption,
                "invalid marker payload should fail decode as corruption",
            );
            assert_eq!(
                err.origin,
                crate::error::ErrorOrigin::Store,
                "invalid marker payload should fail at store decode boundary",
            );
            true
        }
    };
    // Clear the intentionally-bad marker to avoid contaminating later tests.
    crate::db::commit::clear_commit_marker_for_tests().expect("marker cleanup should succeed");
    assert!(
        marker_still_present,
        "failed replay should keep the marker persisted until cleanup",
    );

    let source_after_failure = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationSourceStore::PATH)
                .map(|store| store.with_data(|data_store| data_store.get(&source_raw_key)))
        })
        .expect("source store access should succeed")
        .expect("source row should still exist after rollback");
    let source_after_failure = source_after_failure
        .try_decode::<RelationSourceEntity>()
        .expect("source row decode should succeed after rollback");
    assert_eq!(
        source_after_failure.target, target_a,
        "rollback should restore original source relation target",
    );

    let delete_target_a = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_a)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target A delete plan should build");
    let err = DeleteExecutor::<RelationTargetEntity>::new(REL_DB)
        .execute(delete_target_a)
        .expect_err("target A should remain protected after rollback");
    assert_eq!(
        err.class,
        crate::error::ErrorClass::Unsupported,
        "blocked strong-relation delete should classify as unsupported",
    );
    assert_eq!(
        err.origin,
        crate::error::ErrorOrigin::Executor,
        "blocked strong-relation delete should originate from executor validation",
    );
    assert!(
        err.message.contains("delete blocked by strong relation"),
        "unexpected target A error after rollback: {err:?}",
    );

    let delete_target_b = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_b)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target B delete plan should build");
    let removed_free_target = DeleteExecutor::<RelationTargetEntity>::new(REL_DB)
        .execute(delete_target_b)
        .expect("target B should remain deletable after rollback");
    assert_eq!(removed_free_target.len(), 1, "target B should be removed");
}

/// Exercises a two-row mixed retarget replay, so the marker setup intentionally stays explicit.
#[test]
#[expect(clippy::too_many_lines)]
fn recovery_partial_fk_update_preserves_reverse_index_invariants() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    // Phase 1: seed two targets and two source rows that both reference target A.
    let target_a = Ulid::from_u128(9_501);
    let target_b = Ulid::from_u128(9_502);
    let source_1 = Ulid::from_u128(9_503);
    let source_2 = Ulid::from_u128(9_504);

    let target_save = SaveExecutor::<RelationTargetEntity>::new(REL_DB, false);
    target_save
        .insert(RelationTargetEntity { id: target_a })
        .expect("target A save should succeed");
    target_save
        .insert(RelationTargetEntity { id: target_b })
        .expect("target B save should succeed");

    let source_save = SaveExecutor::<RelationSourceEntity>::new(REL_DB, false);
    source_save
        .insert(RelationSourceEntity {
            id: source_1,
            target: target_a,
        })
        .expect("source 1 save should succeed");
    source_save
        .insert(RelationSourceEntity {
            id: source_2,
            target: target_a,
        })
        .expect("source 2 save should succeed");

    let seeded_reverse_rows = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        seeded_reverse_rows, 1,
        "initially both referrers share one reverse entry on target A",
    );

    // Phase 2: persist a marker with a partial update in one block:
    // - source 1 moves A -> B
    // - source 2 stays on A (before==after relation value)
    let source_1_key = DataKey::try_new::<RelationSourceEntity>(source_1)
        .expect("source 1 key should build")
        .to_raw()
        .expect("source 1 key should encode");
    let source_2_key = DataKey::try_new::<RelationSourceEntity>(source_2)
        .expect("source 2 key should build")
        .to_raw()
        .expect("source 2 key should encode");

    let source_1_before_row_bytes = relation_source_row_bytes(&RelationSourceEntity {
        id: source_1,
        target: target_a,
    });
    let source_1_after_row_bytes = relation_source_row_bytes(&RelationSourceEntity {
        id: source_1,
        target: target_b,
    });
    let source_2_same_row_bytes = relation_source_row_bytes(&RelationSourceEntity {
        id: source_2,
        target: target_a,
    });

    let marker = CommitMarker::new(vec![
        crate::db::commit::CommitRowOp::new(
            RelationSourceEntity::PATH,
            source_1_key,
            Some(source_1_before_row_bytes),
            Some(source_1_after_row_bytes),
            crate::db::schema::commit_schema_fingerprint_for_entity::<RelationSourceEntity>(),
        ),
        crate::db::commit::CommitRowOp::new(
            RelationSourceEntity::PATH,
            source_2_key,
            Some(source_2_same_row_bytes.clone()),
            Some(source_2_same_row_bytes),
            crate::db::schema::commit_schema_fingerprint_for_entity::<RelationSourceEntity>(),
        ),
    ])
    .expect("commit marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be present before recovery replay",
    );

    // Phase 3: recovery replays row ops and reverse mutations from the marker.
    ensure_recovered(&REL_DB).expect("recovery replay should succeed");
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after recovery replay",
    );

    let reverse_rows_after_replay = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_after_replay, 2,
        "partial FK update should split reverse entries across old/new targets",
    );

    let delete_target_a = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_a)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target A delete plan should build");
    let blocked_delete_err = DeleteExecutor::<RelationTargetEntity>::new(REL_DB)
        .execute(delete_target_a)
        .expect_err("target A should remain blocked by source 2");
    assert_eq!(
        blocked_delete_err.class,
        crate::error::ErrorClass::Unsupported,
        "blocked strong-relation delete should classify as unsupported",
    );
    assert_eq!(
        blocked_delete_err.origin,
        crate::error::ErrorOrigin::Executor,
        "blocked strong-relation delete should originate from executor validation",
    );
    assert!(
        blocked_delete_err
            .message
            .contains("delete blocked by strong relation"),
        "unexpected target A error: {blocked_delete_err:?}",
    );

    let delete_target_b = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_b)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target B delete plan should build");
    let blocked_delete_err = DeleteExecutor::<RelationTargetEntity>::new(REL_DB)
        .execute(delete_target_b)
        .expect_err("target B should be blocked by moved source 1");
    assert_eq!(
        blocked_delete_err.class,
        crate::error::ErrorClass::Unsupported,
        "blocked strong-relation delete should classify as unsupported",
    );
    assert_eq!(
        blocked_delete_err.origin,
        crate::error::ErrorOrigin::Executor,
        "blocked strong-relation delete should originate from executor validation",
    );
    assert!(
        blocked_delete_err
            .message
            .contains("delete blocked by strong relation"),
        "unexpected target B error: {blocked_delete_err:?}",
    );

    // Phase 4: remove remaining refs and ensure no orphan reverse entries remain.
    let delete_source_2 = Query::<RelationSourceEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(source_2)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("source 2 delete plan should build");
    DeleteExecutor::<RelationSourceEntity>::new(REL_DB)
        .execute(delete_source_2)
        .expect("source 2 delete should succeed");

    let retry_delete_plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_a)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target A delete plan should build");
    DeleteExecutor::<RelationTargetEntity>::new(REL_DB)
        .execute(retry_delete_plan)
        .expect("target A should delete once source 2 is gone");

    let delete_source_1 = Query::<RelationSourceEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(source_1)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("source 1 delete plan should build");
    DeleteExecutor::<RelationSourceEntity>::new(REL_DB)
        .execute(delete_source_1)
        .expect("source 1 delete should succeed");

    let retry_delete_plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_b)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target B delete plan should build");
    DeleteExecutor::<RelationTargetEntity>::new(REL_DB)
        .execute(retry_delete_plan)
        .expect("target B should delete once source 1 is gone");

    let final_reverse_rows = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        final_reverse_rows, 0,
        "reverse index should be empty after all source refs are removed",
    );
}
