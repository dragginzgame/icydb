//! Candidate staging and abort preserve retained ordinary journal effects.

use super::*;
use crate::{
    db::{
        DynamicQuery, FieldRef, RequestExecutionRoot, SqlStatementResult,
        index::{IndexEntryValue, RawIndexStoreKey},
        schema::{
            SchemaApplicationTarget, SchemaMigrationCommand, SchemaMigrationPhase, migrate_schema,
        },
    },
    value::OutputValue,
};
use std::ops::Bound;

fn source(value: &str) -> FieldSourceKey {
    FieldSourceKey::try_new(value).unwrap()
}

fn proposal(target: &SchemaApplicationTarget, current: bool) -> SchemaProposal {
    let entity = EntitySourceKey::try_new("StagingItem").unwrap();
    let value_name = if current { "w_value" } else { "w_old" };
    let fields = [
        ("id", ScalarType::Nat64),
        ("status", ScalarType::Nat64),
        (
            value_name,
            if current {
                ScalarType::Nat8
            } else {
                ScalarType::Int64
            },
        ),
    ]
    .into_iter()
    .map(|(field, kind)| {
        FieldFragment::new(
            name(field),
            FieldType::Scalar(kind),
            false,
            FieldInsertPolicy::Required,
            None,
        )
    })
    .collect();
    let mut indexes = vec![
        IndexFragment::try_new(
            name("status_lookup"),
            vec![IndexKeyFragment::Field(source("status"))],
            false,
            None,
        )
        .unwrap(),
    ];
    if current {
        indexes.push(
            IndexFragment::try_new(
                name("value_unique"),
                vec![IndexKeyFragment::Field(source(value_name))],
                true,
                None,
            )
            .unwrap(),
        );
    }
    let fragment = EntityFragment::try_new(
        name("StagingItem"),
        DeclaredEntityVersion::try_new(if current { 2 } else { 1 }).unwrap(),
        fields,
        vec![source("id")],
        indexes,
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let migration = current.then(|| {
        SchemaMigrationPlan::try_new(vec![
            EntityMigration::try_new(
                entity.clone(),
                version_one(),
                None,
                Vec::new(),
                vec![SchemaMigrationTransform::CheckedCast {
                    from: source("w_old"),
                    to: source("w_value"),
                    target: ScalarType::Nat8,
                }],
            )
            .unwrap(),
        ])
        .unwrap()
    });
    let mut capabilities = vec![SchemaCapability::SECONDARY_INDEXES];
    if current {
        capabilities.push(SchemaCapability::VERSIONED_MIGRATIONS);
    }
    SchemaProposal::try_compose(
        capabilities,
        target.database_identity(),
        SchemaSubmissionKey::try_new(if current { "staging-v2" } else { "staging-v1" }).unwrap(),
        target.accepted_head().clone(),
        vec![SchemaFragment::try_new(vec![fragment], Vec::new()).unwrap()],
        vec![EntityStoreAssignment::new(
            entity.clone(),
            target.stores()[0].identity(),
        )],
        current
            .then_some(icydb_schema::SchemaRemoval::Field {
                entity,
                field: source("w_old"),
            })
            .into_iter()
            .collect(),
        migration,
    )
    .unwrap()
}

fn insert(session: &DbSession<MigrationExecutionCanister>, id: u64, status: u64) {
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
            entity: "StagingItem".into(),
            patch: DynamicStructuralPatch::new(vec![
                ("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))),
                (
                    "status".into(),
                    DynamicWriteCell::Value(InputValue::nat64(status)),
                ),
                (
                    "w_old".into(),
                    DynamicWriteCell::Value(InputValue::int64(i64::try_from(id).unwrap())),
                ),
            ]),
        })
        .unwrap();
}

fn retain_writes(session: &DbSession<MigrationExecutionCanister>) {
    insert(session, 4, 40);
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Update {
            entity: "StagingItem".into(),
            key: InputValue::nat64(1),
            patch: DynamicStructuralPatch::new(vec![(
                "status".into(),
                DynamicWriteCell::Value(InputValue::nat64(11)),
            )]),
        })
        .unwrap();
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Delete {
            entity: "StagingItem".into(),
            key: InputValue::nat64(2),
        })
        .unwrap();
}

fn assert_indexed_rows(session: &DbSession<MigrationExecutionCanister>, deleted: bool) {
    for (status, expected) in [
        (10, None),
        (11, Some(1)),
        (20, None),
        (30, Some(3)),
        (40, if deleted { None } else { Some(4) }),
    ] {
        let query = DynamicQuery::new("StagingItem")
            .select(["id"])
            .filter(FieldRef::new("status").eq(InputValue::nat64(status)));
        let mut continuation = None;
        let mut actual = Vec::new();
        for _ in 0..16 {
            let page = session
                .execute_trusted_live_page(&query, continuation.as_deref())
                .unwrap();
            actual.extend(page.rows);
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        assert!(continuation.is_none());
        assert_eq!(
            actual,
            expected
                .into_iter()
                .map(|id| vec![OutputValue::nat64(id)])
                .collect::<Vec<_>>(),
            "status={status}"
        );
        let count = session
            .execute_trusted_sql_query(&format!(
                "SELECT COUNT(*) FROM StagingItem WHERE status = {status}"
            ))
            .unwrap();
        let SqlStatementResult::Projection { rows, .. } = count else {
            panic!("count projection");
        };
        assert_eq!(
            rows,
            vec![vec![OutputValue::nat64(u64::from(expected.is_some()))]]
        );
    }
}

fn canonical_keys(store: StoreHandle) -> Vec<(RawIndexStoreKey, IndexEntryValue)> {
    let mut keys = Vec::new();
    store
        .with_index(|index| {
            index.visit_canonical_raw_entries_in_range(
                (&Bound::Unbounded, &Bound::Unbounded),
                |key, value| {
                    keys.push((key.clone(), value.clone()));
                    Ok(false)
                },
            )
        })
        .unwrap();
    keys
}

// Each invocation has its own test thread and stable-memory fixture. Keep an
// ordinary insert, update and delete retained across the migration boundary.
fn assert_staging_preserves_retained_writes(validate: bool, restart: bool) {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = Db::<MigrationExecutionCanister>::new(&MIGRATION_EXECUTION_REGISTRY, root.scope());
    drive_startup_recovery_to_completion(&db);
    let initial = proposal(&schema_application_target(&db).unwrap(), false);
    apply_schema(&db, &initial).unwrap();
    let session =
        DbSession::<MigrationExecutionCanister>::new(&MIGRATION_EXECUTION_REGISTRY, &root);
    for (id, status) in [(1, 10), (2, 20), (3, 30)] {
        insert(&session, id, status);
    }
    drive_startup_recovery_to_completion(&db);
    let store = db.store_handle(MIGRATION_EXECUTION_STORE_PATH).unwrap();
    drive_cardinality_to_ready(store);
    let canonical = canonical_keys(store);
    let watermark = store
        .journal_tail_store()
        .unwrap()
        .with_borrow(JournalTailStore::fold_watermark)
        .unwrap();
    let next = proposal(&schema_application_target(&db).unwrap(), true);
    let advance = || SchemaMigrationCommand::Advance {
        expected_database: next.target_database(),
        expected_head: next.expected_head().clone(),
        expected_plan: next.migration().unwrap().digest(),
        acknowledged_finding_page: None,
    };
    if validate {
        retain_writes(&session);
    }
    assert_eq!(
        migrate_schema(&db, &next, advance()).unwrap().phase(),
        SchemaMigrationPhase::Prepared
    );
    if !validate {
        retain_writes(&session);
    }
    assert_indexed_rows(&session, false);
    if validate {
        assert_eq!(
            migrate_schema(&db, &next, advance()).unwrap().phase(),
            SchemaMigrationPhase::Validating
        );
        assert_eq!(
            migrate_schema(&db, &next, advance()).unwrap().phase(),
            SchemaMigrationPhase::ReadyToRewrite
        );
        assert!(canonical_keys(store).len() > canonical.len());
    }
    let abort = SchemaMigrationCommand::Abort {
        expected_database: next.target_database(),
        expected_head: next.expected_head().clone(),
        expected_plan: next.migration().unwrap().digest(),
    };
    assert_eq!(
        migrate_schema(&db, &next, abort).unwrap().phase(),
        SchemaMigrationPhase::Aborted
    );
    assert_indexed_rows(&session, false);
    assert_eq!(canonical_keys(store), canonical);
    assert_eq!(
        store
            .journal_tail_store()
            .unwrap()
            .with_borrow(JournalTailStore::fold_watermark)
            .unwrap(),
        watermark
    );
    if restart {
        forget_recovered_domain_for_tests(&db).unwrap();
    }
    drive_startup_recovery_to_completion(&db);
    assert_indexed_rows(&session, false);
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Delete {
            entity: "StagingItem".into(),
            key: InputValue::nat64(4),
        })
        .unwrap();
    drive_startup_recovery_to_completion(&db);
    assert_indexed_rows(&session, true);
    forget_recovered_domain_for_tests(&db).unwrap();
    drive_startup_recovery_to_completion(&db);
    assert_indexed_rows(&session, true);
}

#[test]
fn abort_prepared_preserves_retained_writes_and_online_fold() {
    assert_staging_preserves_retained_writes(false, false);
}

#[test]
fn abort_prepared_preserves_retained_writes_and_restart() {
    assert_staging_preserves_retained_writes(false, true);
}

#[test]
fn validation_staging_preserves_retained_writes_and_online_fold() {
    assert_staging_preserves_retained_writes(true, false);
}

#[test]
fn validation_staging_preserves_retained_writes_and_restart() {
    assert_staging_preserves_retained_writes(true, true);
}
