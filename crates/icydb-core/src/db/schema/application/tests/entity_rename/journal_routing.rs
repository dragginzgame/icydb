//! Retained rows straddle a rename while their accepted entity identity stays fixed.

use super::*;
use crate::db::executor::{MutationCommitInterruption, interrupt_next_mutation_commit_for_tests};

fn insert_patch(id: u64) -> DynamicStructuralPatch {
    DynamicStructuralPatch::new(vec![
        ("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))),
        (
            "key".into(),
            DynamicWriteCell::Value(InputValue::nat64(id + 100)),
        ),
        (
            "label".into(),
            DynamicWriteCell::Value(InputValue::nat64(id + 200)),
        ),
        (
            "parent_id".into(),
            DynamicWriteCell::Value(InputValue::null()),
        ),
    ])
}

fn rename(db: &Db<MigrationExecutionCanister>) {
    let candidate = proposal(&schema_application_target(db).unwrap(), true, false);
    assert_eq!(
        advance(db, &candidate).unwrap().phase(),
        SchemaMigrationPhase::Applied
    );
}

pub(super) fn assert_current_rows(
    session: &DbSession<MigrationExecutionCanister>,
    expected: &[u64],
) {
    let query = DynamicQuery::new("CatalogItem").select(["id"]);
    let mut rows = Vec::new();
    let mut continuation = None;
    for _ in 0..4 {
        let page = session
            .execute_trusted_live_page(&query, continuation.as_deref())
            .unwrap();
        rows.extend(page.rows);
        continuation = page.continuation;
        if continuation.is_none() {
            break;
        }
    }
    assert!(continuation.is_none());
    assert_eq!(
        rows,
        expected
            .iter()
            .map(|id| vec![OutputValue::nat64(*id)])
            .collect::<Vec<_>>()
    );
    for id in expected {
        let page = session
            .execute_trusted_live_page(
                &query.clone().filter(FieldRef::new("key").eq(*id + 100)),
                None,
            )
            .unwrap();
        assert_eq!(page.rows, vec![vec![OutputValue::nat64(*id)]]);
    }
}

#[test]
fn online_fold_routes_rows_on_both_sides_of_entity_rename() {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = Db::<MigrationExecutionCanister>::new(&MIGRATION_EXECUTION_REGISTRY, root.scope());
    drive_startup_recovery_to_completion(&db);
    apply_schema(
        &db,
        &proposal(&schema_application_target(&db).unwrap(), false, false),
    )
    .unwrap();
    let session = DbSession::new(&MIGRATION_EXECUTION_REGISTRY, &root);
    for id in 1..=2 {
        session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
                entity: "Item".into(),
                patch: insert_patch(id),
            })
            .unwrap();
    }
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Delete {
            entity: "Item".into(),
            key: InputValue::nat64(2),
        })
        .unwrap();
    assert_eq!(
        advance(
            &db,
            &proposal(&schema_application_target(&db).unwrap(), true, false)
        )
        .unwrap()
        .phase(),
        SchemaMigrationPhase::Applied
    );
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
            entity: "CatalogItem".into(),
            patch: insert_patch(3),
        })
        .unwrap();
    assert_current_rows(&session, &[1, 3]);
    // Keep volatile recovery ownership: this exercises ordinary online folding.
    drive_startup_recovery_to_completion(&db);
    assert_current_rows(&session, &[1, 3]);
    forget_recovered_domain_for_tests(&db).unwrap();
    drive_startup_recovery_to_completion(&db);
    assert_current_rows(&session, &[1, 3]);
}

fn assert_post_rename_marker_recovers(interruption: MutationCommitInterruption) {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = initialize(&root, false);
    let session = DbSession::new(&MIGRATION_EXECUTION_REGISTRY, &root);
    rename(&db);
    interrupt_next_mutation_commit_for_tests(interruption);
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
            entity: "CatalogItem".into(),
            patch: insert_patch(3),
        })
        .expect_err("retain the post-rename marker before completion");

    // Upgrade loses every live projection before the marker is routed. The
    // canonical catalog still names Item until the earlier rename batch folds.
    let handle = db.store_handle(MIGRATION_EXECUTION_STORE_PATH).unwrap();
    let generation = handle.with_data_mut(|data| {
        data.reset_journaled_live_projection().unwrap();
        data.generation()
    });
    let watermark = handle
        .journal_tail_store()
        .unwrap()
        .with_borrow(JournalTailStore::fold_watermark)
        .unwrap();
    handle
        .with_index_mut(|index| index.reset_journaled_live_projection(generation, watermark))
        .unwrap();
    handle
        .with_schema_mut(SchemaStore::reset_journaled_live_projection)
        .unwrap();
    forget_recovered_domain_for_tests(&db).unwrap();
    drive_startup_recovery_to_completion(&db);
    assert_current_rows(&session, &[1, 2, 3]);
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Delete {
            entity: "CatalogItem".into(),
            key: InputValue::nat64(3),
        })
        .unwrap();
    drive_startup_recovery_to_completion(&db);
    assert_current_rows(&session, &[1, 2]);
}

#[test]
fn renamed_entity_marker_recovers_before_journal_publication() {
    assert_post_rename_marker_recovers(MutationCommitInterruption::MarkerPersisted);
}

#[test]
fn renamed_entity_marker_recovers_after_row_publication() {
    assert_post_rename_marker_recovers(MutationCommitInterruption::RowsPublished);
}
