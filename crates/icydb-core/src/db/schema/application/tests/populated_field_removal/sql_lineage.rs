//! SQL catalog edits preserve source meaning while exact migration heads stay checked.

use super::*;
use crate::db::schema::{
    SchemaMigrationStatusRequest,
    application::{preflight_ordinary_source_application, schema_migration_status},
    live_schema_checkpoint::{load_entity_source_lineage_catalog, load_schema_migration_record},
};
use icydb_diagnostic_code::{DiagnosticDetail, SchemaMigrationCode};

fn execute_sql_edit(db: &Db<MigrationExecutionCanister>, alteration: &str) {
    let store = db.store_handle(MIGRATION_EXECUTION_STORE_PATH).unwrap();
    let bundle = store
        .with_schema(SchemaStore::current_accepted_schema_bundle)
        .unwrap()
        .unwrap();
    let version = bundle
        .entity_snapshots()
        .values()
        .find(|snapshot| snapshot.entity_path() == "Remapped")
        .unwrap()
        .version()
        .get();
    let root = RequestExecutionRoot::__new_runtime_root();
    let session =
        DbSession::<MigrationExecutionCanister>::new(&MIGRATION_EXECUTION_REGISTRY, &root);
    session.execute_admin_sql_ddl(&format!(
        "ALTER TABLE Remapped EXPECT SCHEMA VERSION {version} SET SCHEMA VERSION {} {alteration}",
        version + 1,
    )).unwrap();
}

fn assert_sql_values(session: &DbSession<MigrationExecutionCanister>) {
    for (id, expected) in [(1, 88), (2, 77), (3, 77)] {
        let page = session
            .execute_trusted_live_page(
                &DynamicQuery::new("Remapped")
                    .select(["note"])
                    .filter(FieldRef::new("id").eq(InputValue::nat64(id))),
                None,
            )
            .unwrap();
        assert_eq!(page.rows, vec![vec![OutputValue::nat64(expected)]]);
    }
}

fn assert_stale_heads_reject(
    db: &Db<MigrationExecutionCanister>,
    stale: &SchemaProposal,
    current: &SchemaProposal,
) {
    let before = load_entity_source_lineage_catalog().unwrap();
    for (proposal, command) in [
        (stale, advance_command(stale)),
        (stale, advance_command(current)),
        (current, advance_command(stale)),
        (
            stale,
            SchemaMigrationCommand::Adopt {
                expected_database: current.target_database(),
                expected_head: current.expected_head().clone(),
            },
        ),
        (
            stale,
            SchemaMigrationCommand::Abort {
                expected_database: current.target_database(),
                expected_head: current.expected_head().clone(),
                expected_plan: current.migration().unwrap().digest(),
            },
        ),
    ] {
        let error = migrate_schema(db, proposal, command).unwrap_err();
        assert_eq!(
            error.diagnostic().detail(),
            Some(&DiagnosticDetail::SchemaMigration {
                reason: SchemaMigrationCode::StaleAcceptedHead,
            })
        );
        assert_eq!(
            schema_application_target(db).unwrap().accepted_head(),
            current.expected_head()
        );
        assert_eq!(load_entity_source_lineage_catalog().unwrap(), before);
        assert!(load_schema_migration_record().unwrap().is_none());
    }
}

fn finish_migration(
    db: &Db<MigrationExecutionCanister>,
    candidate: &SchemaProposal,
    interrupted: bool,
) {
    let mut recovered = false;
    let mut applied = false;
    for _ in 0..32 {
        let before = load_schema_migration_record()
            .unwrap()
            .map(|record| record.phase());
        let phase = migrate_schema(db, candidate, advance_command(candidate))
            .unwrap_or_else(|error| panic!("advance from {before:?} failed: {error:?}"))
            .phase();
        if phase == SchemaMigrationPhase::Applied {
            applied = true;
            break;
        }
        if interrupted && !recovered && phase == SchemaMigrationPhase::RewritingRows {
            interrupt_next_migration_rewrite_at(MigrationRewriteInterruption::JournalPublished);
            assert!(migrate_schema(db, candidate, advance_command(candidate)).is_err());
            restart(db);
            recovered = true;
        }
    }
    assert!(applied);
    assert_eq!(recovered, interrupted);
    // Exact terminal retry keeps its original predecessor and result.
    assert_eq!(
        migrate_schema(db, candidate, advance_command(candidate))
            .unwrap()
            .phase(),
        SchemaMigrationPhase::Applied
    );
}

// A newly appended non-null field reuses a numeric ID vacated by the migration.
// Its catalog-owned name must be available without retiring retained constraints.
fn qualify_post_migration_field_addition(db: &Db<MigrationExecutionCanister>) {
    execute_sql_edit(db, "ADD COLUMN extra nat64 DEFAULT 77 NOT NULL");
    restart(db);
    let root = RequestExecutionRoot::__new_runtime_root();
    let session =
        DbSession::<MigrationExecutionCanister>::new(&MIGRATION_EXECUTION_REGISTRY, &root);
    assert_sql_values(&session);
    for id in 1..=3 {
        let page = session
            .execute_trusted_live_page(
                &DynamicQuery::new("Remapped")
                    .select(["extra"])
                    .filter(FieldRef::new("id").eq(InputValue::nat64(id))),
                None,
            )
            .unwrap();
        assert_eq!(page.rows, vec![vec![OutputValue::nat64(77)]]);
    }
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
            entity: "Remapped".into(),
            patch: DynamicStructuralPatch::new(vec![
                ("id".into(), DynamicWriteCell::Value(InputValue::nat64(4))),
                (
                    "state".into(),
                    DynamicWriteCell::Value(InputValue::nat64(40)),
                ),
                (
                    "z_value".into(),
                    DynamicWriteCell::Value(InputValue::nat64(24)),
                ),
            ]),
        })
        .unwrap();
    restart(db);
    let page = session
        .execute_trusted_live_page(
            &DynamicQuery::new("Remapped")
                .select(["note", "extra"])
                .filter(FieldRef::new("id").eq(InputValue::nat64(4))),
            None,
        )
        .unwrap();
    assert_eq!(
        page.rows,
        vec![vec![OutputValue::nat64(99), OutputValue::nat64(77)]]
    );
}

fn run_sql_then_source_migration(interrupted: bool) {
    let (db, _, stale) = initialize(false);
    let lineage = load_entity_source_lineage_catalog().unwrap();
    execute_sql_edit(&db, "ADD COLUMN note nat64 DEFAULT 77 NOT NULL");
    assert_eq!(load_entity_source_lineage_catalog().unwrap(), lineage);
    if interrupted {
        restart(&db);
    }

    let root = RequestExecutionRoot::__new_runtime_root();
    let db = Db::new(&MIGRATION_EXECUTION_REGISTRY, root.scope());
    let session = DbSession::new(&MIGRATION_EXECUTION_REGISTRY, &root);
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Update {
            entity: "Remapped".into(),
            key: InputValue::nat64(1),
            patch: DynamicStructuralPatch::new(vec![(
                "note".into(),
                DynamicWriteCell::Value(InputValue::nat64(88)),
            )]),
        })
        .unwrap();
    let candidate = proposal(&db, true, false);
    assert_ne!(candidate.expected_head(), stale.expected_head());
    assert_stale_heads_reject(&db, &stale, &candidate);
    assert_rows(&session, false);
    assert_sql_values(&session);
    finish_migration(&db, &candidate, interrupted);
    assert_rows(&session, true);
    assert_sql_values(&session);
    restart(&db);
    assert_rows(&session, true);
    assert_sql_values(&session);

    // Later SQL edits must not make the already-applied source plan pending again.
    let lineage = load_entity_source_lineage_catalog().unwrap();
    execute_sql_edit(&db, "ALTER COLUMN note SET DEFAULT 99");
    restart(&db);
    assert_eq!(load_entity_source_lineage_catalog().unwrap(), lineage);
    let current = proposal(&db, true, false);
    let status =
        schema_migration_status(&db, &current, &SchemaMigrationStatusRequest::default()).unwrap();
    assert_eq!(status.phase(), SchemaMigrationPhase::Applied);
    // Qualify source readiness without resubmitting the one-time removal.
    preflight_ordinary_source_application(&db, &current, &schema_application_target(&db).unwrap())
        .unwrap();
    assert_sql_values(&session);
    qualify_post_migration_field_addition(&db);
}

#[test]
fn sql_ddl_then_populated_source_migration_preserves_values_and_stale_head_checks() {
    run_sql_then_source_migration(false);
}

#[test]
fn sql_ddl_then_populated_source_migration_recovers_and_preserves_applied_status() {
    run_sql_then_source_migration(true);
}
