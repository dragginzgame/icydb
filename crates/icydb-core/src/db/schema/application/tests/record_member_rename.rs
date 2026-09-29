//! Populated record renames publish only after bounded row rewriting.

use super::*;
use crate::{
    db::{
        DynamicMutation, DynamicQuery, DynamicStructuralPatch, DynamicWriteCell, FieldRef,
        RequestExecutionRoot,
        data::StoreVisit,
        index::{IndexKey, IndexKeyKind, IndexStoreVisit},
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        schema::{
            MigrationRewriteInterruption, SchemaMigrationCommand, SchemaMigrationPhase,
            SchemaMigrationStatusPage, interrupt_next_migration_rewrite_at, migrate_schema,
        },
        session::DbSession,
    },
    value::{InputValue, OutputValue},
};
use icydb_schema::{
    EntityMigration, IndexFragment, IndexKeyFragment, RecordFieldFragment, RecordTypeFragment,
    RelationDeleteAction, RelationFragment, RelationPathStepFragment, RelationSourceFragment,
    SchemaMigrationPlan, SchemaMigrationRename,
};

fn field(value: &str) -> FieldSourceKey {
    FieldSourceKey::try_new(value).unwrap()
}

fn profile_type() -> TypeSourceKey {
    TypeSourceKey::try_new("Profile").unwrap()
}

fn record_type(label: &str, owner: &str) -> RecordTypeFragment {
    RecordTypeFragment::try_new(
        name("Profile"),
        vec![
            RecordFieldFragment::new(
                name(label),
                FieldType::Scalar(ScalarType::Text { max_len: Some(64) }),
                false,
            ),
            RecordFieldFragment::new(name(owner), FieldType::Scalar(ScalarType::Nat64), false),
        ],
    )
    .unwrap()
}

fn item_fragment(current: bool, owner: &str) -> EntityFragment {
    EntityFragment::try_new(
        name("Item"),
        DeclaredEntityVersion::try_new(if current { 2 } else { 1 }).unwrap(),
        [
            ("id", FieldType::Scalar(ScalarType::Nat64)),
            ("profile", FieldType::Named(profile_type())),
            (
                "copies",
                FieldType::List(Box::new(FieldType::Named(profile_type()))),
            ),
        ]
        .into_iter()
        .map(|(label, kind)| {
            FieldFragment::new(name(label), kind, false, FieldInsertPolicy::Required, None)
        })
        .collect(),
        vec![field("id")],
        vec![
            IndexFragment::try_new(
                name("id_lookup"),
                vec![IndexKeyFragment::Field(field("id"))],
                false,
                None,
            )
            .unwrap(),
        ],
        vec![
            RelationFragment::try_new(
                name("owner"),
                RelationSourceFragment::Nested {
                    root: field("profile"),
                    steps: vec![
                        RelationPathStepFragment::EnterNamed {
                            r#type: profile_type(),
                        },
                        RelationPathStepFragment::RecordMember {
                            record: profile_type(),
                            field: field(owner),
                        },
                    ],
                },
                EntitySourceKey::try_new("Item").unwrap(),
                vec![field("id")],
                RelationDeleteAction::Restrict,
            )
            .unwrap(),
        ],
        Vec::new(),
    )
    .unwrap()
}

fn record_renames() -> Vec<SchemaMigrationRename> {
    [("nickname", "zz_label"), ("owner_id", "aa_owner")]
        .into_iter()
        .map(|(from, to)| SchemaMigrationRename::RecordField {
            named_type: profile_type(),
            from: field(from),
            to: field(to),
        })
        .collect()
}

fn proposal(db: &Db<MigrationExecutionCanister>, current: bool) -> SchemaProposal {
    let target = schema_application_target(db).unwrap();
    let entity = EntitySourceKey::try_new("Item").unwrap();
    let mirror_name = if current { "RenamedMirror" } else { "Mirror" };
    let mirror = EntitySourceKey::try_new(mirror_name).unwrap();
    let (label, owner) = if current {
        ("zz_label", "aa_owner")
    } else {
        ("nickname", "owner_id")
    };
    let fragment = item_fragment(current, owner);
    let mirror_fragment = EntityFragment::try_new(
        name(mirror_name),
        fragment.version(),
        fragment.fields().to_vec(),
        vec![field("id")],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let record = record_type(label, owner);
    let migration = current.then(|| {
        SchemaMigrationPlan::try_new(vec![
            EntityMigration::try_new(
                entity.clone(),
                version_one(),
                None,
                record_renames(),
                Vec::new(),
            )
            .unwrap(),
            // Every entity sharing the record declares the same member rename.
            EntityMigration::try_new(
                mirror.clone(),
                version_one(),
                Some(EntitySourceKey::try_new("Mirror").unwrap()),
                record_renames(),
                Vec::new(),
            )
            .unwrap(),
        ])
        .unwrap()
    });
    let mut capabilities = vec![
        SchemaCapability::EXACT_COMPOSITE_TYPES,
        SchemaCapability::RESTRICTIVE_RELATIONS,
        SchemaCapability::SECONDARY_INDEXES,
    ];
    if current {
        capabilities.push(SchemaCapability::VERSIONED_MIGRATIONS);
    }
    SchemaProposal::try_compose(
        capabilities,
        target.database_identity(),
        SchemaSubmissionKey::try_new(if current { "record-v2" } else { "record-v1" }).unwrap(),
        target.accepted_head().clone(),
        vec![
            SchemaFragment::try_new(
                vec![fragment, mirror_fragment],
                vec![NamedTypeFragment::Record(record)],
            )
            .unwrap(),
        ],
        vec![
            EntityStoreAssignment::new(entity, target.stores()[0].identity()),
            EntityStoreAssignment::new(mirror, target.stores()[0].identity()),
        ],
        Vec::new(),
        migration,
    )
    .unwrap()
}

fn profile(current: bool, owner: u64) -> InputValue {
    InputValue::map(if current {
        vec![
            (
                InputValue::text("aa_owner".into()),
                InputValue::nat64(owner),
            ),
            (
                InputValue::text("zz_label".into()),
                InputValue::text("Ada".into()),
            ),
        ]
    } else {
        vec![
            (
                InputValue::text("nickname".into()),
                InputValue::text("Ada".into()),
            ),
            (
                InputValue::text("owner_id".into()),
                InputValue::nat64(owner),
            ),
        ]
    })
}

fn patch(current: bool, owner: u64) -> DynamicStructuralPatch {
    DynamicStructuralPatch::new(vec![
        (
            "profile".into(),
            DynamicWriteCell::Value(profile(current, owner)),
        ),
        (
            "copies".into(),
            DynamicWriteCell::Value(InputValue::list(vec![profile(current, owner)])),
        ),
    ])
}

fn initialize() -> (
    Db<MigrationExecutionCanister>,
    DbSession<MigrationExecutionCanister>,
    SchemaProposal,
) {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = Db::new(&MIGRATION_EXECUTION_REGISTRY, root.scope());
    drive_startup_recovery_to_completion(&db);
    apply_schema(&db, &proposal(&db, false)).unwrap();
    let session = DbSession::new(&MIGRATION_EXECUTION_REGISTRY, &root);
    for (entity, id) in [("Item", 1), ("Item", 2), ("Item", 3), ("Mirror", 10)] {
        session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
                entity: entity.into(),
                patch: DynamicStructuralPatch::new(vec![
                    ("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))),
                    ("profile".into(), DynamicWriteCell::Value(profile(false, 1))),
                    (
                        "copies".into(),
                        DynamicWriteCell::Value(InputValue::list(vec![profile(false, 1)])),
                    ),
                ]),
            })
            .unwrap();
    }
    drive_startup_recovery_to_completion(&db);
    let candidate = proposal(&db, true);
    (db, session, candidate)
}

fn advance(
    db: &Db<MigrationExecutionCanister>,
    proposal: &SchemaProposal,
) -> SchemaMigrationStatusPage {
    migrate_schema(
        db,
        proposal,
        SchemaMigrationCommand::Advance {
            expected_database: proposal.target_database(),
            expected_head: proposal.expected_head().clone(),
            expected_plan: proposal.migration().unwrap().digest(),
            acknowledged_finding_page: None,
        },
    )
    .unwrap()
}

fn rows(db: &Db<MigrationExecutionCanister>) -> Vec<Vec<u8>> {
    let mut rows = Vec::new();
    db.store_handle(MIGRATION_EXECUTION_STORE_PATH)
        .unwrap()
        .with_data(|data| {
            data.visit_entries(|_, row| {
                rows.push(row.as_bytes().to_vec());
                Ok::<_, crate::error::InternalError>(StoreVisit::Continue)
            })
        })
        .unwrap();
    rows
}

fn assert_index(db: &Db<MigrationExecutionCanister>, expected: &[u64]) {
    let entity = db.accepted_runtime_entity_for_path("Item").unwrap();
    let store = db.store_handle(MIGRATION_EXECUTION_STORE_PATH).unwrap();
    let selection = store
        .with_schema(|schema| {
            schema.current_accepted_catalog_selection(
                entity.entity_tag(),
                "Item",
                MIGRATION_EXECUTION_STORE_PATH,
            )
        })
        .unwrap()
        .unwrap();
    let generation = selection.snapshot().persisted_snapshot().indexes()[0].physical_generation();
    let mut actual = Vec::new();
    store
        .with_index(|index| {
            index.visit_entries(|raw, _| {
                let key = IndexKey::try_from_raw(raw).unwrap();
                if key.key_kind() == IndexKeyKind::User
                    && key.index_id().entity_tag() == entity.entity_tag()
                    && key.index_id().generation() == generation
                {
                    actual.push(key.primary_key_value().unwrap());
                }
                Ok::<_, crate::error::InternalError>(IndexStoreVisit::Continue)
            })
        })
        .unwrap();
    assert_eq!(
        actual,
        expected
            .iter()
            .map(|id| PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(*id)))
            .collect::<Vec<_>>()
    );
}

fn assert_row(session: &DbSession<MigrationExecutionCanister>, id: u64, current: bool, owner: u64) {
    let value = OutputValue::from_public(profile(current, owner).into_public());
    let page = session
        .execute_trusted_live_page(
            &DynamicQuery::new(if id == 10 {
                if current { "RenamedMirror" } else { "Mirror" }
            } else {
                "Item"
            })
            .filter(FieldRef::new("id").eq(id))
            .select(["profile", "copies"]),
            None,
        )
        .unwrap();
    assert_eq!(
        page.rows,
        vec![vec![
            value.clone(),
            OutputValue::list(vec![value.into_public()])
        ]]
    );
}

fn run_rename(interrupted: bool) {
    let (db, session, proposal) = initialize();
    let before = rows(&db);
    let mut interrupted_once = false;
    let mut applied = false;
    for _ in 0..24 {
        let phase = advance(&db, &proposal).phase();
        if phase == SchemaMigrationPhase::Applied {
            applied = true;
            break;
        }
        if matches!(
            phase,
            SchemaMigrationPhase::Prepared
                | SchemaMigrationPhase::Validating
                | SchemaMigrationPhase::ReadyToRewrite
        ) {
            assert_eq!(
                rows(&db),
                before,
                "validation must not rewrite accepted rows"
            );
        }
        if interrupted && !interrupted_once && phase == SchemaMigrationPhase::RewritingRows {
            interrupt_next_migration_rewrite_at(MigrationRewriteInterruption::JournalPublished);
            let error = migrate_schema(
                &db,
                &proposal,
                SchemaMigrationCommand::Advance {
                    expected_database: proposal.target_database(),
                    expected_head: proposal.expected_head().clone(),
                    expected_plan: proposal.migration().unwrap().digest(),
                    acknowledged_finding_page: None,
                },
            );
            assert!(error.is_err());
            forget_recovered_domain_for_tests(&db).unwrap();
            drive_startup_recovery_to_completion(&db);
            interrupted_once = true;
        }
    }
    assert!(applied);
    for id in 1..=3 {
        assert_row(&session, id, true, 1);
    }
    assert_row(&session, 10, true, 1);
    assert_index(&db, &[1, 2, 3]);
    assert_ne!(rows(&db), before);
    assert_eq!(interrupted_once, interrupted);
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Delete {
            entity: "Item".into(),
            key: InputValue::nat64(1),
        })
        .expect_err("retained reverse edges must restrict deletion");
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Update {
            entity: "Item".into(),
            key: InputValue::nat64(2),
            patch: patch(true, 3),
        })
        .unwrap();
    assert_row(&session, 2, true, 3);
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Delete {
            entity: "Item".into(),
            key: InputValue::nat64(2),
        })
        .unwrap();
    forget_recovered_domain_for_tests(&db).unwrap();
    drive_startup_recovery_to_completion(&db);
    assert_row(&session, 3, true, 1);
    assert_row(&session, 10, true, 1);
    assert_index(&db, &[1, 3]);
}

#[test]
fn record_member_rename_keeps_populated_rows_readable_and_writable() {
    run_rename(false);
}

#[test]
fn record_member_rename_recovers_interrupted_rewrite() {
    run_rename(true);
}

#[test]
fn record_member_rename_abort_preserves_predecessor_rows() {
    let (db, session, proposal) = initialize();
    let before = rows(&db);
    for phase in [
        SchemaMigrationPhase::Prepared,
        SchemaMigrationPhase::Validating,
        SchemaMigrationPhase::ReadyToRewrite,
    ] {
        assert_eq!(advance(&db, &proposal).phase(), phase);
    }
    migrate_schema(
        &db,
        &proposal,
        SchemaMigrationCommand::Abort {
            expected_database: proposal.target_database(),
            expected_head: proposal.expected_head().clone(),
            expected_plan: proposal.migration().unwrap().digest(),
        },
    )
    .unwrap();
    assert_eq!(rows(&db), before);
    for id in 1..=3 {
        assert_row(&session, id, false, 1);
    }
    assert_row(&session, 10, false, 1);
    assert_index(&db, &[1, 2, 3]);
    forget_recovered_domain_for_tests(&db).unwrap();
    drive_startup_recovery_to_completion(&db);
    assert_row(&session, 2, false, 1);
}
