//! Nonterminal migrations retain their predecessor head across ordinary schema work.

use super::*;
use crate::db::{
    commit::publish_accepted_schema_candidate,
    schema::{
        application::{application_authorities, lower_application_candidates},
        load_schema_migration_record,
    },
};
use icydb_diagnostic_code::{DiagnosticDetail, SchemaMigrationCode};

fn in_progress_detail() -> DiagnosticDetail {
    DiagnosticDetail::SchemaMigration {
        reason: SchemaMigrationCode::MigrationInProgress,
    }
}

// A valid default-only change still changes the database-wide head and must
// not strand the prepared Source migration.
fn ordinary_default_change(db: &Db<MigrationExecutionCanister>) -> SchemaProposal {
    let target = schema_application_target(db).expect("target should issue");
    let base = proposal(
        false,
        target.accepted_head().clone(),
        target.database_identity(),
        target.stores()[0].identity(),
    );
    let entities = base.fragments()[0]
        .entities()
        .iter()
        .map(|entity| {
            let fields = entity
                .fields()
                .iter()
                .map(|field| {
                    if field.name().as_str() == "stable" {
                        FieldFragment::new(
                            field.name().clone(),
                            field.field_type().clone(),
                            field.nullable(),
                            FieldInsertPolicy::Default(ScalarLiteral::Nat(3)),
                            field.management(),
                        )
                    } else {
                        field.clone()
                    }
                })
                .collect();
            EntityFragment::try_new(
                entity.name().clone(),
                entity.version(),
                fields,
                entity.primary_key().to_vec(),
                entity.indexes().to_vec(),
                entity.relations().to_vec(),
                entity.constraints().to_vec(),
            )
            .expect("default-only entity should admit")
        })
        .collect();
    SchemaProposal::try_compose(
        vec![
            SchemaCapability::RESTRICTIVE_RELATIONS,
            SchemaCapability::INSERT_DEFAULTS,
        ],
        base.target_database(),
        SchemaSubmissionKey::try_new("ordinary-default-change").unwrap(),
        base.expected_head().clone(),
        vec![SchemaFragment::try_new(entities, Vec::new()).unwrap()],
        base.assignments().to_vec(),
        Vec::new(),
        None,
    )
    .expect("ordinary proposal should compose")
}

#[test]
fn prepared_migration_blocks_schema_application_and_direct_publication_until_abort() {
    let (db, session, migration) = initialize(2);
    let ordinary = ordinary_default_change(&db);
    let target = schema_application_target(&db).unwrap();
    let authorities = application_authorities(&db);
    let lowered = lower_application_candidates::<true>(&target, &ordinary, &authorities)
        .expect("ordinary default change should lower before migration preparation");
    assert_eq!(lowered.candidates.len(), 1);
    assert_eq!(
        advance(&db, &migration).unwrap().phase(),
        SchemaMigrationPhase::Prepared
    );
    let record = load_schema_migration_record().unwrap();
    let rows = row_bytes(&db);
    let error = apply_schema(&db, &ordinary).expect_err("prepared head must be reserved");
    assert_eq!(error.diagnostic().detail(), Some(&in_progress_detail()));
    let candidate = &lowered.candidates[0];
    let store = db.store_handle(candidate.store_path()).unwrap();
    let revision = store
        .with_schema(SchemaStore::current_accepted_schema_bundle)
        .unwrap()
        .unwrap()
        .revision();
    let error = publish_accepted_schema_candidate(
        MIGRATION_EXECUTION_STORE_PATH,
        store,
        revision,
        candidate,
    )
    .expect_err("publication must independently preserve the reserved head");
    assert_eq!(error.diagnostic().detail(), Some(&in_progress_detail()));
    assert_eq!(
        schema_application_target(&db).unwrap().accepted_head(),
        target.accepted_head()
    );
    assert_eq!(load_schema_migration_record().unwrap(), record);
    assert_eq!(row_bytes(&db), rows);

    // Prepared still admits ordinary row writes and accepted-schema reads.
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Update {
            entity: "Source".to_string(),
            key: InputValue::nat64(10),
            patch: DynamicStructuralPatch::new(vec![(
                "backup".to_string(),
                DynamicWriteCell::Value(nested_value(1)),
            )]),
        })
        .expect("prepared migration should permit row writes");
    assert_ne!(row_bytes(&db), rows);
    assert_edges(&db, &accepted_relations(&db), 1);
    assert_eq!(
        advance(&db, &migration).unwrap().phase(),
        SchemaMigrationPhase::Validating
    );
    let aborted = migrate_schema(
        &db,
        &migration,
        SchemaMigrationCommand::Abort {
            expected_database: migration.target_database(),
            expected_head: migration.expected_head().clone(),
            expected_plan: migration.migration().unwrap().digest(),
        },
    )
    .expect("blocked schema edit must leave migration abortable");
    assert_eq!(aborted.phase(), SchemaMigrationPhase::Aborted);
    assert!(matches!(
        apply_schema(&db, &ordinary).unwrap().outcome(),
        SchemaChangeOutcome::Applied { .. }
    ));
}

#[cfg(feature = "sql")]
#[test]
fn prepared_migration_blocks_sql_ddl_across_recovery_and_allows_it_after_publication() {
    let (db, session, migration) = initialize(2);
    let ddl =
        "ALTER TABLE Target EXPECT SCHEMA VERSION 1 SET SCHEMA VERSION 2 ADD COLUMN note nat64";
    assert_eq!(
        advance(&db, &migration).unwrap().phase(),
        SchemaMigrationPhase::Prepared
    );
    let record = load_schema_migration_record().unwrap();
    let rows = row_bytes(&db);
    for _ in 0..2 {
        let error = session
            .execute_admin_sql_ddl(ddl)
            .expect_err("DDL must preserve the prepared head");
        assert_eq!(error.diagnostic().detail(), Some(&in_progress_detail()));
        assert_eq!(
            schema_application_target(&db).unwrap().accepted_head(),
            migration.expected_head()
        );
        assert_eq!(load_schema_migration_record().unwrap(), record);
        assert_eq!(row_bytes(&db), rows);
        forget_recovered_domain_for_tests(&db).unwrap();
        drive_startup_recovery_to_completion(&db);
    }
    let mut completed = false;
    for _ in 0..32 {
        let page = advance(&db, &migration).expect("migration must still advance and publish");
        if page.phase() == SchemaMigrationPhase::Applied {
            completed = true;
            break;
        }
        let error = session
            .execute_admin_sql_ddl(ddl)
            .expect_err("active migration must reject DDL");
        assert_eq!(error.diagnostic().detail(), Some(&in_progress_detail()));
    }
    assert!(completed);
    assert_edges(&db, &accepted_relations(&db), 2);
    session
        .execute_admin_sql_ddl(ddl)
        .expect("the same DDL should publish after migration completion");
}
