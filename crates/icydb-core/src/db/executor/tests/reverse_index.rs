//! Module: db::executor::tests::reverse_index
//! Covers strong-relation delete validation and reverse-index recovery.
//! Does not own: unrelated executor orchestration outside reverse-index state transitions.
//! Boundary: exposes this module API while keeping implementation details internal.

use super::support::*;
use crate::db::{
    commit::{CommitRowOp, CommitSchemaFingerprint},
    data::{DataStore, DecodedDataStoreKey},
    journal::{JournalBatch, JournalRecord},
    schema::{accepted_commit_schema_fingerprint, ensure_accepted_schema_snapshot},
};

// Build canonical persisted row bytes for relation-source replay markers.
fn relation_source_row_bytes(entity: &RelationSourceEntity) -> Vec<u8> {
    crate::db::data::CanonicalRow::from_generated_entity_for_test(entity)
        .expect("relation source row should serialize canonically")
        .into_raw_row()
        .as_bytes()
        .to_vec()
}

fn relation_source_accepted_commit_schema_fingerprint() -> CommitSchemaFingerprint {
    let store = REL_DB
        .recovered_store(RelationSourceStore::PATH)
        .expect("relation source store should be recovered");
    let accepted = store
        .with_schema_mut(|schema_store| {
            ensure_accepted_schema_snapshot(
                schema_store,
                RelationSourceEntity::ENTITY_TAG,
                RelationSourceEntity::PATH,
                <RelationSourceEntity as crate::traits::EntitySchema>::MODEL,
            )
        })
        .expect("relation source accepted schema should initialize");

    accepted_commit_schema_fingerprint(&accepted)
        .expect("relation source accepted commit fingerprint should derive")
}

fn relation_source_recovery_marker(row_ops: Vec<CommitRowOp>) -> crate::db::commit::CommitMarker {
    let marker_id =
        crate::db::commit::generate_commit_id().expect("relation recovery marker id should build");
    let records = row_ops
        .iter()
        .map(relation_source_journal_record_for_row_op)
        .collect::<Result<Vec<_>, _>>()
        .expect("relation recovery journal records should build");
    let source_store = REL_DB
        .with_store_registry(|reg| reg.try_get_store(RelationSourceStore::PATH))
        .expect("relation source store should resolve");
    let sequence = source_store
        .journal_tail_store()
        .expect("relation source store should have a journal tail")
        .with_borrow(crate::db::journal::JournalTailStore::next_append_sequence)
        .expect("relation source journal sequence should allocate");
    let batch = JournalBatch::new(marker_id, marker_id, sequence, records)
        .expect("relation recovery journal batch should build");

    crate::db::commit::CommitMarker::from_parts(marker_id, Vec::new(), vec![batch])
        .expect("relation recovery marker should build")
}

fn relation_source_journal_record_for_row_op(
    row_op: &CommitRowOp,
) -> Result<JournalRecord, crate::error::InternalError> {
    match row_op.after.as_ref() {
        Some(after) => JournalRecord::row_put(
            row_op.entity_path.as_ref(),
            row_op.key.clone(),
            after.clone(),
            row_op.schema_fingerprint,
        ),
        None => JournalRecord::row_delete(
            row_op.entity_path.as_ref(),
            row_op.key.clone(),
            row_op.schema_fingerprint,
        ),
    }
}

fn assert_relation_delete_blocked_diagnostic(err: &crate::error::InternalError, context: &str) {
    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeUnsupported,
        "{context}: compact blocked-relation diagnostic drifted: {err:?}",
    );
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
        .map(crate::db::executor::PreparedExecutionPlan::from)
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

    assert_relation_delete_blocked_diagnostic(&err, "delete protected target");

    let target_rows = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_data(DataStore::len))
        })
        .expect("target store access should succeed");
    let source_rows = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationSourceStore::PATH)
                .map(|store| store.with_data(DataStore::len))
        })
        .expect("source store access should succeed");
    assert_eq!(target_rows, 1, "blocked delete must keep target row");
    assert_eq!(source_rows, 1, "blocked delete must keep source row");
}

#[test]
fn save_composite_relation_target_validates_existing_target_tuple() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_key = CompositeRelationTargetKey {
        tenant_id: 17,
        local_id: 42,
    };
    let source_id = Ulid::from_u128(9_051);

    SaveExecutor::<CompositeRelationTargetEntity>::new(REL_DB, false)
        .insert(CompositeRelationTargetEntity {
            tenant_id: target_key.tenant_id,
            local_id: target_key.local_id,
            label: "target".to_string(),
        })
        .expect("composite target save should succeed");

    SaveExecutor::<CompositeRelationSourceEntity>::new(REL_DB, false)
        .insert(CompositeRelationSourceEntity {
            id: source_id,
            target_tenant_id: target_key.tenant_id,
            target_local_id: target_key.local_id,
        })
        .expect("source save should validate existing composite target tuple");

    let reverse_rows = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows, 1,
        "composite relation source should create one reverse-index entry"
    );
}

#[test]
fn save_composite_relation_target_rejects_missing_target_tuple() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let err = SaveExecutor::<CompositeRelationSourceEntity>::new(REL_DB, false)
        .insert(CompositeRelationSourceEntity {
            id: Ulid::from_u128(9_061),
            target_tenant_id: 17,
            target_local_id: 404,
        })
        .expect_err("source save should reject missing composite target tuple");

    assert_eq!(
        err.class,
        crate::error::ErrorClass::Unsupported,
        "missing composite relation target should classify as unsupported",
    );
    assert_eq!(
        err.origin,
        crate::error::ErrorOrigin::Executor,
        "missing composite relation target should originate from executor validation",
    );
}

#[test]
fn delete_blocks_composite_relation_target_with_strong_referrer() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_key = CompositeRelationTargetKey {
        tenant_id: 22,
        local_id: 77,
    };

    SaveExecutor::<CompositeRelationTargetEntity>::new(REL_DB, false)
        .insert(CompositeRelationTargetEntity {
            tenant_id: target_key.tenant_id,
            local_id: target_key.local_id,
            label: "target".to_string(),
        })
        .expect("composite target save should succeed");
    SaveExecutor::<CompositeRelationSourceEntity>::new(REL_DB, false)
        .insert(CompositeRelationSourceEntity {
            id: Ulid::from_u128(9_071),
            target_tenant_id: target_key.tenant_id,
            target_local_id: target_key.local_id,
        })
        .expect("source save should validate composite target tuple");

    let delete_plan = Query::<CompositeRelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_key)
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("composite target delete plan should build");
    let err = DeleteExecutor::<CompositeRelationTargetEntity>::new(REL_DB)
        .execute(delete_plan)
        .expect_err("target delete should be blocked by composite strong relation");

    assert_eq!(
        err.class,
        crate::error::ErrorClass::Unsupported,
        "blocked composite relation delete should classify as unsupported",
    );
    assert_eq!(
        err.origin,
        crate::error::ErrorOrigin::Executor,
        "blocked composite relation delete should originate from executor validation",
    );
}

#[test]
fn save_optional_composite_relation_target_accepts_empty_tuple_without_reverse_entry() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    SaveExecutor::<OptionalCompositeRelationSourceEntity>::new(REL_DB, false)
        .insert(OptionalCompositeRelationSourceEntity {
            id: Ulid::from_u128(9_081),
            target_tenant_id: None,
            target_local_id: None,
        })
        .expect("all-null optional composite relation tuple should be accepted");

    let reverse_rows = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows, 0,
        "all-null optional composite relation tuple must not create reverse-index membership"
    );
}

#[test]
fn save_optional_composite_relation_target_rejects_partial_tuple() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    SaveExecutor::<OptionalCompositeRelationSourceEntity>::new(REL_DB, false)
        .insert(OptionalCompositeRelationSourceEntity {
            id: Ulid::from_u128(9_082),
            target_tenant_id: Some(17),
            target_local_id: None,
        })
        .expect_err("partial optional composite relation tuple should reject");
}

#[test]
fn delete_blocks_composite_relation_target_with_composite_source_key_referrer() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_key = CompositeRelationTargetKey {
        tenant_id: 33,
        local_id: 88,
    };
    let source_key = CompositePkRelationSourceKey {
        tenant_id: 44,
        source_local_id: 99,
    };

    SaveExecutor::<CompositeRelationTargetEntity>::new(REL_DB, false)
        .insert(CompositeRelationTargetEntity {
            tenant_id: target_key.tenant_id,
            local_id: target_key.local_id,
            label: "target".to_string(),
        })
        .expect("composite target save should succeed");
    SaveExecutor::<CompositePkRelationSourceEntity>::new(REL_DB, false)
        .insert(CompositePkRelationSourceEntity {
            tenant: source_key.tenant_id,
            source_local: source_key.source_local_id,
            target_tenant: target_key.tenant_id,
            target_local: target_key.local_id,
        })
        .expect("composite source save should validate composite target tuple");

    let reverse_rows = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows, 1,
        "composite source and composite target should create one reverse-index entry"
    );

    let delete_plan = Query::<CompositeRelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_key)
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("composite target delete plan should build");
    let err = DeleteExecutor::<CompositeRelationTargetEntity>::new(REL_DB)
        .execute(delete_plan)
        .expect_err("target delete should be blocked by composite source key relation");

    assert_eq!(
        err.class,
        crate::error::ErrorClass::Unsupported,
        "blocked composite source relation delete should classify as unsupported",
    );
    assert_eq!(
        err.origin,
        crate::error::ErrorOrigin::Executor,
        "blocked composite source relation delete should originate from executor validation",
    );
}

fn assert_composite_relation_target_identity_does_not_collide(
    unreferenced_target: CompositeRelationTargetKey,
    referenced_target: CompositeRelationTargetKey,
    source_id: Ulid,
) {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    for target in [unreferenced_target, referenced_target] {
        SaveExecutor::<CompositeRelationTargetEntity>::new(REL_DB, false)
            .insert(CompositeRelationTargetEntity {
                tenant_id: target.tenant_id,
                local_id: target.local_id,
                label: "target".to_string(),
            })
            .expect("composite target save should succeed");
    }

    SaveExecutor::<CompositeRelationSourceEntity>::new(REL_DB, false)
        .insert(CompositeRelationSourceEntity {
            id: source_id,
            target_tenant_id: referenced_target.tenant_id,
            target_local_id: referenced_target.local_id,
        })
        .expect("source save should validate referenced composite target tuple");

    let unreferenced_delete_plan =
        Query::<CompositeRelationTargetEntity>::new(MissingRowPolicy::Ignore)
            .delete()
            .by_id(unreferenced_target)
            .plan()
            .map(crate::db::executor::PreparedExecutionPlan::from)
            .expect("unreferenced composite target delete plan should build");
    let deleted = DeleteExecutor::<CompositeRelationTargetEntity>::new(REL_DB)
        .execute(unreferenced_delete_plan)
        .expect("unreferenced composite target should delete without reverse-index collision");
    assert_eq!(
        deleted.len(),
        1,
        "unreferenced target sharing one component with a referenced target should not be blocked"
    );

    let referenced_delete_plan =
        Query::<CompositeRelationTargetEntity>::new(MissingRowPolicy::Ignore)
            .delete()
            .by_id(referenced_target)
            .plan()
            .map(crate::db::executor::PreparedExecutionPlan::from)
            .expect("referenced composite target delete plan should build");
    let err = DeleteExecutor::<CompositeRelationTargetEntity>::new(REL_DB)
        .execute(referenced_delete_plan)
        .expect_err("referenced composite target should still be blocked");
    assert_eq!(
        err.class,
        crate::error::ErrorClass::Unsupported,
        "blocked referenced composite target should classify as unsupported",
    );
}

#[test]
fn delete_does_not_collide_composite_targets_sharing_later_component() {
    assert_composite_relation_target_identity_does_not_collide(
        CompositeRelationTargetKey {
            tenant_id: 41,
            local_id: 7,
        },
        CompositeRelationTargetKey {
            tenant_id: 42,
            local_id: 7,
        },
        Ulid::from_u128(9_083),
    );
}

#[test]
fn delete_does_not_collide_composite_targets_sharing_first_component() {
    assert_composite_relation_target_identity_does_not_collide(
        CompositeRelationTargetKey {
            tenant_id: 51,
            local_id: 7,
        },
        CompositeRelationTargetKey {
            tenant_id: 51,
            local_id: 8,
        },
        Ulid::from_u128(9_084),
    );
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
        .map(crate::db::executor::PreparedExecutionPlan::from)
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
        .map(crate::db::executor::PreparedExecutionPlan::from)
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
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("target delete plan should build");
    let deleted_targets = DeleteExecutor::<RelationTargetEntity>::new(REL_DB)
        .execute(target_delete_plan)
        .expect("target delete should succeed for weak referrer");
    assert_eq!(deleted_targets.len(), 1, "target row should be removed");

    let source_plan = Query::<WeakSingleRelationSourceEntity>::new(MissingRowPolicy::Ignore)
        .by_id(source_id)
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("source load plan should build");
    let remaining_source = LoadExecutor::<WeakSingleRelationSourceEntity>::new(REL_DB, false)
        .execute(source_plan)
        .expect("source load should succeed");
    assert_eq!(remaining_source.len(), 1, "weak source row should remain");
    assert_eq!(
        remaining_source.as_slice()[0].entity_ref().target,
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
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("target delete plan should build");
    let deleted_targets = DeleteExecutor::<RelationTargetEntity>::new(REL_DB)
        .execute(target_delete_plan)
        .expect("target delete should succeed for weak optional referrer");
    assert_eq!(deleted_targets.len(), 1, "target row should be removed");

    let source_plan = Query::<WeakOptionalRelationSourceEntity>::new(MissingRowPolicy::Ignore)
        .by_id(source_id)
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
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
        remaining_source.as_slice()[0].entity_ref().target,
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
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("target delete plan should build");
    let deleted_targets = DeleteExecutor::<RelationTargetEntity>::new(REL_DB)
        .execute(target_delete_plan)
        .expect("target delete should succeed for weak list referrer");
    assert_eq!(deleted_targets.len(), 1, "target row should be removed");

    let source_plan = Query::<WeakListRelationSourceEntity>::new(MissingRowPolicy::Ignore)
        .by_id(source_id)
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
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
        remaining_source.as_slice()[0].entity_ref().targets,
        vec![target_id],
        "weak list source relation values should be preserved",
    );
}

#[test]
fn delete_allows_target_with_weak_set_referrer() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_id = Ulid::from_u128(9_141);
    let source_id = Ulid::from_u128(9_142);

    SaveExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .insert(RelationTargetEntity { id: target_id })
        .expect("target save should succeed");
    SaveExecutor::<WeakSetRelationSourceEntity>::new(REL_DB, false)
        .insert(WeakSetRelationSourceEntity {
            id: source_id,
            targets: vec![target_id],
        })
        .expect("weak set source save should succeed");

    let reverse_rows_before_delete = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_before_delete, 0,
        "weak set relation should not create reverse strong-relation index entries",
    );

    let target_delete_plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_id)
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("target delete plan should build");
    let deleted_targets = DeleteExecutor::<RelationTargetEntity>::new(REL_DB)
        .execute(target_delete_plan)
        .expect("target delete should succeed for weak set referrer");
    assert_eq!(deleted_targets.len(), 1, "target row should be removed");

    let source_plan = Query::<WeakSetRelationSourceEntity>::new(MissingRowPolicy::Ignore)
        .by_id(source_id)
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("source load plan should build");
    let remaining_source = LoadExecutor::<WeakSetRelationSourceEntity>::new(REL_DB, false)
        .execute(source_plan)
        .expect("source load should succeed");
    assert_eq!(
        remaining_source.len(),
        1,
        "weak set source row should remain"
    );
    assert_eq!(
        remaining_source.as_slice()[0].entity_ref().targets,
        vec![target_id],
        "weak set source relation values should be preserved",
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
        .map(crate::db::executor::PreparedExecutionPlan::from)
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
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("target A delete plan should build");
    let deleted_a = DeleteExecutor::<RelationTargetEntity>::new(REL_DB)
        .execute(old_target_delete_plan)
        .expect("old target should be deletable after relation retarget");
    assert_eq!(deleted_a.len(), 1, "old target should delete cleanly");

    let protected_target_delete_plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_b)
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
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
    assert_relation_delete_blocked_diagnostic(&err, "retargeted relation delete");
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
    let raw_key = DecodedDataStoreKey::try_new::<RelationSourceEntity>(source.id)
        .expect("source data key should build")
        .to_raw()
        .expect("source data key should encode");
    let row_bytes = crate::db::data::CanonicalRow::from_generated_entity_for_test(&source)
        .expect("source row should serialize")
        .into_raw_row()
        .as_bytes()
        .to_vec();

    let marker = relation_source_recovery_marker(vec![crate::db::commit::CommitRowOp::new(
        RelationSourceEntity::PATH,
        raw_key,
        None,
        Some(row_bytes),
        relation_source_accepted_commit_schema_fingerprint(),
    )]);

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
        .map(crate::db::executor::PreparedExecutionPlan::from)
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
    assert_relation_delete_blocked_diagnostic(&err, "replayed reverse index delete");
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

    // Fold the seeded rows and reverse indexes first, so the stale-entry setup
    // below mutates durable data truth rather than only the live projection.
    let seed_marker = CommitMarker::new(Vec::new()).expect("marker creation should succeed");
    begin_commit(seed_marker).expect("begin_commit should persist seed marker");
    ensure_recovered(&REL_DB).expect("seed recovery rebuild should succeed");

    // Phase 2: simulate stale reverse-index drift by removing one source row
    // from durable row truth while leaving derived reverse-index state behind.
    let orphan_source_key = DecodedDataStoreKey::try_new::<RelationSourceEntity>(source_orphan)
        .expect("orphan source key should build")
        .to_raw()
        .expect("orphan source key should encode");
    REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationSourceStore::PATH).map(|store| {
                let removed = store.with_data_mut(|data_store| {
                    data_store
                        .fold_recovered_journal_delete(&orphan_source_key)
                        .expect("canonical orphan source row removal should succeed")
                });
                assert!(
                    removed.is_some(),
                    "orphan source row should exist before canonical data-store removal",
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
        .map(crate::db::executor::PreparedExecutionPlan::from)
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
        .map(crate::db::executor::PreparedExecutionPlan::from)
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
    assert_relation_delete_blocked_diagnostic(&err, "startup rebuild surviving relation delete");
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
        .map(crate::db::executor::PreparedExecutionPlan::from)
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
    assert_relation_delete_blocked_diagnostic(&err, "startup rebuild restored relation delete");
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

    let first_source_key = DecodedDataStoreKey::try_new::<RelationSourceEntity>(source_a)
        .expect("first source key should build")
        .to_raw()
        .expect("first source key should encode");
    let second_source_key = DecodedDataStoreKey::try_new::<RelationSourceEntity>(source_b)
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
    let first_save_marker =
        relation_source_recovery_marker(vec![crate::db::commit::CommitRowOp::new(
            RelationSourceEntity::PATH,
            first_source_key.clone(),
            None,
            Some(first_source_row_bytes.clone()),
            relation_source_accepted_commit_schema_fingerprint(),
        )]);
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
    let second_save_marker =
        relation_source_recovery_marker(vec![crate::db::commit::CommitRowOp::new(
            RelationSourceEntity::PATH,
            second_source_key,
            None,
            Some(second_source_row_bytes),
            relation_source_accepted_commit_schema_fingerprint(),
        )]);
    begin_commit(second_save_marker).expect("begin_commit should persist marker");
    ensure_recovered(&REL_DB).expect("second save recovery replay should succeed");

    let reverse_rows_after_save_b = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_after_save_b, 2,
        "second save replay should create a second key-owned reverse entry",
    );

    // Phase 3: replay delete marker for one source row.
    let delete_a_marker =
        relation_source_recovery_marker(vec![crate::db::commit::CommitRowOp::new(
            RelationSourceEntity::PATH,
            first_source_key,
            Some(first_source_row_bytes),
            None,
            relation_source_accepted_commit_schema_fingerprint(),
        )]);
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
        .map(crate::db::executor::PreparedExecutionPlan::from)
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
    assert_relation_delete_blocked_diagnostic(&err, "mixed replay surviving source delete");

    let source_delete_plan = Query::<RelationSourceEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(source_b)
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("source B delete plan should build");
    DeleteExecutor::<RelationSourceEntity>::new(REL_DB)
        .execute(source_delete_plan)
        .expect("source B delete should succeed");

    let retry_target_delete_plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_id)
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
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

    let source_key = DecodedDataStoreKey::try_new::<RelationSourceEntity>(source_id)
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

    let marker = relation_source_recovery_marker(vec![crate::db::commit::CommitRowOp::new(
        RelationSourceEntity::PATH,
        source_key,
        Some(before_row_bytes),
        Some(after_row_bytes),
        relation_source_accepted_commit_schema_fingerprint(),
    )]);
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
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("target A delete plan should build");
    let removed_old_target = DeleteExecutor::<RelationTargetEntity>::new(REL_DB)
        .execute(delete_target_a)
        .expect("old target should be deletable after replayed retarget");
    assert_eq!(removed_old_target.len(), 1, "old target should be removed");

    let delete_target_b = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_b)
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
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
    assert_relation_delete_blocked_diagnostic(&err, "retarget replay protected target");
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

    let source_key = DecodedDataStoreKey::try_new::<RelationSourceEntity>(source_id)
        .expect("source key should build")
        .to_raw()
        .expect("source key should encode");
    let source_raw_key = source_key.clone();
    let update_before_row_bytes = relation_source_row_bytes(&RelationSourceEntity {
        id: source_id,
        target: target_a,
    });
    let update_after_row_bytes = relation_source_row_bytes(&RelationSourceEntity {
        id: source_id,
        target: target_b,
    });

    let mismatched_entity_key = DecodedDataStoreKey::try_new::<RelationTargetEntity>(target_b)
        .expect("mismatched target key should build")
        .to_raw()
        .expect("mismatched target key should encode");

    let marker = relation_source_recovery_marker(vec![
        crate::db::commit::CommitRowOp::new(
            RelationSourceEntity::PATH,
            source_key,
            Some(update_before_row_bytes),
            Some(update_after_row_bytes),
            relation_source_accepted_commit_schema_fingerprint(),
        ),
        crate::db::commit::CommitRowOp::new(
            RelationSourceEntity::PATH,
            mismatched_entity_key,
            None,
            Some(vec![1]),
            relation_source_accepted_commit_schema_fingerprint(),
        ),
    ]);
    crate::db::commit::persist_raw_commit_marker_for_tests(&marker)
        .expect("raw corrupt marker fixture should persist");

    let err = ensure_recovered(&REL_DB)
        .expect_err("recovery should fail when a later row op targets the wrong entity key");
    assert_eq!(
        err.class,
        crate::error::ErrorClass::Corruption,
        "prepare failure should surface corruption for mismatched entity key shape",
    );
    assert_eq!(
        err.origin,
        crate::error::ErrorOrigin::Recovery,
        "mismatched entity key bytes should surface recovery-boundary origin",
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
        .try_decode_with_generated_model_for_test::<RelationSourceEntity>()
        .expect("source row decode should succeed after rollback");
    assert_eq!(
        source_after_failure.target, target_a,
        "rollback should restore original source relation target",
    );

    let delete_target_a = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_a)
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
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
    assert_relation_delete_blocked_diagnostic(&err, "rollback restored target A protection");

    let delete_target_b = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_b)
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
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
        seeded_reverse_rows, 2,
        "initially both referrers use separate key-owned reverse entries on target A",
    );

    // Phase 2: persist a marker with a partial update in one block:
    // - source 1 moves A -> B
    // - source 2 stays on A (before==after relation value)
    let source_1_key = DecodedDataStoreKey::try_new::<RelationSourceEntity>(source_1)
        .expect("source 1 key should build")
        .to_raw()
        .expect("source 1 key should encode");
    let source_2_key = DecodedDataStoreKey::try_new::<RelationSourceEntity>(source_2)
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

    let marker = relation_source_recovery_marker(vec![
        crate::db::commit::CommitRowOp::new(
            RelationSourceEntity::PATH,
            source_1_key,
            Some(source_1_before_row_bytes),
            Some(source_1_after_row_bytes),
            relation_source_accepted_commit_schema_fingerprint(),
        ),
        crate::db::commit::CommitRowOp::new(
            RelationSourceEntity::PATH,
            source_2_key,
            Some(source_2_same_row_bytes.clone()),
            Some(source_2_same_row_bytes),
            relation_source_accepted_commit_schema_fingerprint(),
        ),
    ]);
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
        .map(crate::db::executor::PreparedExecutionPlan::from)
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
    assert_relation_delete_blocked_diagnostic(
        &blocked_delete_err,
        "multi-source target A protection",
    );

    let delete_target_b = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_b)
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
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
    assert_relation_delete_blocked_diagnostic(
        &blocked_delete_err,
        "multi-source target B protection",
    );

    // Phase 4: remove remaining refs and ensure no orphan reverse entries remain.
    let delete_source_2 = Query::<RelationSourceEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(source_2)
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("source 2 delete plan should build");
    DeleteExecutor::<RelationSourceEntity>::new(REL_DB)
        .execute(delete_source_2)
        .expect("source 2 delete should succeed");

    let retry_delete_plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_a)
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("target A delete plan should build");
    DeleteExecutor::<RelationTargetEntity>::new(REL_DB)
        .execute(retry_delete_plan)
        .expect("target A should delete once source 2 is gone");

    let delete_source_1 = Query::<RelationSourceEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(source_1)
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("source 1 delete plan should build");
    DeleteExecutor::<RelationSourceEntity>::new(REL_DB)
        .execute(delete_source_1)
        .expect("source 1 delete should succeed");

    let retry_delete_plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_b)
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
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
