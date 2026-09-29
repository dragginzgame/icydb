//! Generated allocation survives dense field-ID changes without reissuing values.

use super::*;
use crate::{db::DynamicQuery, value::OutputValue};
use icydb_schema::SchemaRemoval;

fn proposal(db: &Db<MigrationExecutionCanister>, stage: usize) -> SchemaProposal {
    let target = schema_application_target(db).unwrap();
    let source = EntitySourceKey::try_new("SequenceRow").unwrap();
    let mut fields = ["archived", "extra"]
        .into_iter()
        .skip(stage)
        .map(|field| {
            FieldFragment::new(
                name(field),
                FieldType::Scalar(ScalarType::Bool),
                false,
                FieldInsertPolicy::Default(ScalarLiteral::Bool(false)),
                None,
            )
        })
        .collect::<Vec<_>>();
    fields.push(FieldFragment::new(
        name("id"),
        FieldType::Scalar(ScalarType::Nat8),
        false,
        FieldInsertPolicy::Generated,
        None,
    ));
    SchemaProposal::try_compose(
        vec![SchemaCapability::GENERATED_VALUES],
        target.database_identity(),
        SchemaSubmissionKey::try_new(format!("identity-removal-{stage}")).unwrap(),
        target.accepted_head().clone(),
        vec![
            SchemaFragment::try_new(
                vec![
                    EntityFragment::try_new(
                        name("SequenceRow"),
                        version_one(),
                        fields,
                        vec![FieldSourceKey::try_new("id").unwrap()],
                        Vec::new(),
                        Vec::new(),
                        Vec::new(),
                    )
                    .unwrap(),
                ],
                Vec::new(),
            )
            .unwrap(),
        ],
        vec![EntityStoreAssignment::new(
            source.clone(),
            target.stores()[0].identity(),
        )],
        stage
            .checked_sub(1)
            .map(|index| SchemaRemoval::Field {
                entity: source,
                field: FieldSourceKey::try_new(["archived", "extra"][index]).unwrap(),
            })
            .into_iter()
            .collect(),
        None,
    )
    .unwrap()
}

fn insert_and_assert(session: &DbSession<MigrationExecutionCanister>, expected: u8) {
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
            entity: "SequenceRow".into(),
            patch: DynamicStructuralPatch::new(Vec::new()),
        })
        .unwrap();
    let page = session
        .execute_trusted_live_page(&DynamicQuery::new("SequenceRow").select(["id"]), None)
        .unwrap();
    assert_eq!(
        page.rows,
        vec![vec![OutputValue::nat64(u64::from(expected))]]
    );
}

fn delete(session: &DbSession<MigrationExecutionCanister>, id: u8) {
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Delete {
            entity: "SequenceRow".into(),
            key: InputValue::nat64(u64::from(id)),
        })
        .unwrap();
}

fn restart(db: &Db<MigrationExecutionCanister>) {
    let store = db.store_handle(MIGRATION_EXECUTION_STORE_PATH).unwrap();
    let generation = store.with_data_mut(|data| {
        data.reset_journaled_live_projection().unwrap();
        data.generation()
    });
    let watermark = store
        .journal_tail_store()
        .unwrap()
        .with_borrow(JournalTailStore::fold_watermark)
        .unwrap();
    store
        .with_index_mut(|index| index.reset_journaled_live_projection(generation, watermark))
        .unwrap();
    store
        .with_schema_mut(SchemaStore::reset_journaled_live_projection)
        .unwrap();
    forget_recovered_domain_for_tests(db).unwrap();
    drive_startup_recovery_to_completion(db);
}

fn assert_repeated_removal(restart_after_publication: bool) {
    let root = crate::db::RequestExecutionRoot::__new_runtime_root();
    let db = Db::<MigrationExecutionCanister>::new(&MIGRATION_EXECUTION_REGISTRY, root.scope());
    drive_startup_recovery_to_completion(&db);
    apply_schema(&db, &proposal(&db, 0)).unwrap();
    let session = DbSession::new(&MIGRATION_EXECUTION_REGISTRY, &root);
    for id in 1..=3 {
        insert_and_assert(&session, id);
        delete(&session, id);
    }
    for stage in 1..=2 {
        // Empty-domain admission remains required. Retained Identity ranges must
        // fold before the schema move and retain their committed high-water.
        let store = db.store_handle(MIGRATION_EXECUTION_STORE_PATH).unwrap();
        drive_startup_recovery_to_completion(&db);
        drive_cardinality_to_ready(store);
        let pending_id = u8::try_from(stage * 2 + 2).unwrap();
        insert_and_assert(&session, pending_id);
        delete(&session, pending_id);
        apply_schema(&db, &proposal(&db, stage)).unwrap();
        if restart_after_publication {
            restart(&db);
        }
        let id = pending_id + 1;
        insert_and_assert(&session, id);
        delete(&session, id);
        drive_startup_recovery_to_completion(&db);
        let inventory = store
            .with_schema(|schema| {
                schema.identity_state_inventory_for_integrity(database_incarnation_id().unwrap())
            })
            .unwrap();
        assert_eq!(
            inventory.len(),
            1,
            "a retained owner must not create retirement records"
        );
        assert_eq!(
            inventory[0].owner().field_id().get(),
            u32::try_from(3 - stage).unwrap()
        );
        assert_eq!(inventory[0].materialized_high_water(), u128::from(id));
    }
    restart(&db);
    insert_and_assert(&session, 8);
}

#[test]
fn identity_counter_survives_repeated_field_removal_online() {
    assert_repeated_removal(false);
}

#[test]
fn identity_counter_survives_repeated_field_removal_restart() {
    assert_repeated_removal(true);
}
