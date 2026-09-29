//! Replay keeps relation source and target contracts at the same fold boundary.

use super::*;

fn update_reference(
    session: &DbSession<MigrationExecutionCanister>,
    entity: &str,
    id: u64,
    field: &str,
    target: Option<u64>,
) {
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Update {
            entity: entity.into(),
            key: InputValue::nat64(id),
            patch: DynamicStructuralPatch::new(vec![(
                field.into(),
                DynamicWriteCell::Value(target.map_or_else(InputValue::null, InputValue::nat64)),
            )]),
        })
        .unwrap();
}

fn delete(session: &DbSession<MigrationExecutionCanister>, entity: &str, id: u64) {
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Delete {
            entity: entity.into(),
            key: InputValue::nat64(id),
        })
        .unwrap();
}

fn insert_child(
    session: &DbSession<MigrationExecutionCanister>,
    entity: &str,
    id: u64,
    parent: u64,
) {
    insert(
        session,
        entity,
        vec![
            ("id", InputValue::nat64(id)),
            ("key", InputValue::nat64(id + 100)),
            ("label", InputValue::nat64(id + 200)),
            ("parent_id", InputValue::nat64(parent)),
        ],
    );
}

fn assert_restricted(session: &DbSession<MigrationExecutionCanister>, id: u64) {
    let error = session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Delete {
            entity: "CatalogItem".into(),
            key: InputValue::nat64(id),
        })
        .expect_err("recovered reverse references must restrict deletion");
    assert!(error.diagnostic_facts().contains(&(
        icydb_diagnostic_code::DiagnosticFactTag::ConstraintKind,
        icydb_diagnostic_code::DiagnosticConstraintKind::Relation.raw(),
    )));
}

// A restart drops every volatile projection, while online folding retains the
// newest catalog and must still prepare old journal rows canonically.
fn cold_restart(db: &Db<MigrationExecutionCanister>) {
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
    forget_recovered_domain_for_tests(db).unwrap();
    drive_startup_recovery_to_completion(db);
}

fn assert_retained_relations(inbound: bool, restart: bool) {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = initialize(&root, inbound);
    let session = DbSession::new(&MIGRATION_EXECUTION_REGISTRY, &root);
    insert_child(&session, "Item", 3, 1);
    update_reference(&session, "Item", 2, "parent_id", Some(3));
    if inbound {
        update_reference(&session, "Holder", 10, "item_id", Some(3));
    }
    delete(&session, "Item", 2);
    let candidate = proposal(&schema_application_target(&db).unwrap(), true, inbound);
    assert_eq!(
        advance(&db, &candidate).unwrap().phase(),
        SchemaMigrationPhase::Applied
    );
    insert_child(&session, "CatalogItem", 4, 3);
    if inbound {
        insert(
            &session,
            "Holder",
            vec![
                ("id", InputValue::nat64(11)),
                ("item_id", InputValue::nat64(4)),
            ],
        );
        delete(&session, "Holder", 10);
    }
    if restart {
        cold_restart(&db);
    } else {
        drive_startup_recovery_to_completion(&db);
    }
    journal_routing::assert_current_rows(&session, &[1, 3, 4]);
    assert_restricted(&session, 1);
    assert_restricted(&session, 3);
    if inbound {
        assert_restricted(&session, 4);
        let page = session
            .execute_trusted_live_page(&DynamicQuery::new("Holder").select(["id", "item_id"]), None)
            .unwrap();
        assert_eq!(
            page.rows,
            vec![vec![OutputValue::nat64(11), OutputValue::nat64(4)]]
        );
    }
    // Normal writes still validate the live target after replay completes.
    let error = session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Update {
            entity: "CatalogItem".into(),
            key: InputValue::nat64(4),
            patch: DynamicStructuralPatch::new(vec![(
                "parent_id".into(),
                DynamicWriteCell::Value(InputValue::nat64(999)),
            )]),
        })
        .expect_err("missing live targets must reject");
    assert!(error.diagnostic_facts().contains(&(
        icydb_diagnostic_code::DiagnosticFactTag::ConstraintKind,
        icydb_diagnostic_code::DiagnosticConstraintKind::Relation.raw(),
    )));
    update_reference(&session, "CatalogItem", 4, "parent_id", None);
    delete(&session, "CatalogItem", 3);
    delete(&session, "CatalogItem", 1);
    if inbound {
        delete(&session, "Holder", 11);
    }
    delete(&session, "CatalogItem", 4);
    drive_startup_recovery_to_completion(&db);
    cold_restart(&db);
    journal_routing::assert_current_rows(&session, &[]);
    let state = physical_state(&db);
    assert!(state.rows.is_empty());
    assert!(
        state.indexes.is_empty(),
        "deleted references must leave no reverse keys"
    );
}

#[test]
fn retained_self_relations_fold_across_entity_rename() {
    assert_retained_relations(false, false);
}

#[test]
fn retained_inbound_relations_fold_across_entity_rename() {
    assert_retained_relations(true, false);
}

#[test]
fn retained_self_relations_restart_across_entity_rename() {
    assert_retained_relations(false, true);
}

#[test]
fn retained_inbound_relations_restart_across_entity_rename() {
    assert_retained_relations(true, true);
}
