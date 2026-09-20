//! Populated same-store entity rename through the maintained migration boundary.

mod admission;
mod recovery;

use super::*;
use crate::{
    db::{
        DynamicQuery, FieldRef, RequestExecutionRoot, TypedEntityDescriptor, TypedFieldDescriptor,
        TypedFieldType,
        data::StoreVisit,
        index::IndexStoreVisit,
        journal::{JournalRecord, JournalSequence, JournalTailStore},
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        schema::{
            PersistedSchemaSnapshot, SchemaMigrationCommand, SchemaMigrationPhase,
            SchemaMigrationStatusPage, application::load_entity_source_lineage_catalog,
            migrate_schema, migration_lineage::AcceptedEntitySourceLineageState,
        },
    },
    error::InternalError,
    value::OutputValue,
};
use icydb_schema::RelationSourceFragment;

const ITEM_DESCRIPTOR: TypedEntityDescriptor = TypedEntityDescriptor::new(
    "Item",
    &["id"],
    &[TypedFieldDescriptor::new(
        "id",
        TypedFieldType::Scalar(ScalarType::Nat64),
        false,
    )],
);

fn entity(value: &str) -> EntitySourceKey {
    EntitySourceKey::try_new(value).unwrap()
}

fn field(value: &str) -> FieldSourceKey {
    FieldSourceKey::try_new(value).unwrap()
}

fn scalar_field(value: &str, kind: ScalarType, nullable: bool) -> FieldFragment {
    FieldFragment::new(
        name(value),
        FieldType::Scalar(kind),
        nullable,
        if nullable {
            FieldInsertPolicy::Nullable
        } else {
            FieldInsertPolicy::Required
        },
        None,
    )
}

fn relation(source: &str, target: &str) -> RelationFragment {
    RelationFragment::try_new(
        name(source),
        RelationSourceFragment::direct(vec![field(source)]),
        entity(target),
        vec![field("id")],
        RelationDeleteAction::Restrict,
    )
    .unwrap()
}

fn proposal(
    target: &crate::db::schema::SchemaApplicationTarget,
    renamed: bool,
    inbound: bool,
) -> SchemaProposal {
    let item = if renamed { "CatalogItem" } else { "Item" };
    let mut entities = vec![
        EntityFragment::try_new(
            name(item),
            DeclaredEntityVersion::try_new(if renamed { 2 } else { 1 }).unwrap(),
            vec![
                scalar_field("id", ScalarType::Nat64, false),
                scalar_field("key", ScalarType::Nat64, false),
                scalar_field("label", ScalarType::Nat64, false),
                scalar_field("parent_id", ScalarType::Nat64, true),
            ],
            vec![field("id")],
            vec![
                IndexFragment::try_new(
                    name("key_lookup"),
                    vec![IndexKeyFragment::Field(field("key"))],
                    true,
                    None,
                )
                .unwrap(),
            ],
            vec![relation("parent_id", item)],
            Vec::new(),
        )
        .unwrap(),
    ];
    if inbound {
        entities.push(
            EntityFragment::try_new(
                name("Holder"),
                DeclaredEntityVersion::try_new(if renamed { 2 } else { 1 }).unwrap(),
                vec![
                    scalar_field("id", ScalarType::Nat64, false),
                    scalar_field("item_id", ScalarType::Nat64, false),
                ],
                vec![field("id")],
                Vec::new(),
                vec![relation("item_id", item)],
                Vec::new(),
            )
            .unwrap(),
        );
    }
    let assignments = entities
        .iter()
        .map(|entity| {
            EntityStoreAssignment::new(entity.source_key().clone(), target.stores()[0].identity())
        })
        .collect();
    let migration = renamed.then(|| {
        let mut transitions = vec![
            EntityMigration::try_new(
                entity(item),
                version_one(),
                Some(entity("Item")),
                Vec::new(),
                Vec::new(),
            )
            .unwrap(),
        ];
        if inbound {
            transitions.push(
                EntityMigration::try_new(
                    entity("Holder"),
                    version_one(),
                    None,
                    Vec::new(),
                    Vec::new(),
                )
                .unwrap(),
            );
        }
        SchemaMigrationPlan::try_new(transitions).unwrap()
    });
    let mut capabilities = vec![SchemaCapability::RESTRICTIVE_RELATIONS];
    if renamed {
        capabilities.push(SchemaCapability::VERSIONED_MIGRATIONS);
    }
    SchemaProposal::try_compose(
        capabilities,
        target.database_identity(),
        SchemaSubmissionKey::try_new(if renamed {
            "rename-item"
        } else {
            "initial-item"
        })
        .unwrap(),
        target.accepted_head().clone(),
        vec![SchemaFragment::try_new(entities, Vec::new()).unwrap()],
        assignments,
        Vec::new(),
        migration,
    )
    .unwrap()
}

fn insert(
    session: &DbSession<MigrationExecutionCanister>,
    name: &str,
    fields: Vec<(&str, InputValue)>,
) {
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
            entity: name.to_string(),
            patch: DynamicStructuralPatch::new(
                fields
                    .into_iter()
                    .map(|(name, value)| (name.to_string(), DynamicWriteCell::Value(value)))
                    .collect(),
            ),
        })
        .unwrap();
}

fn initialize(root: &RequestExecutionRoot, inbound: bool) -> Db<MigrationExecutionCanister> {
    let db = Db::<MigrationExecutionCanister>::new(&MIGRATION_EXECUTION_REGISTRY, root.scope());
    drive_startup_recovery_to_completion(&db);
    let target = schema_application_target(&db).unwrap();
    apply_schema(&db, &proposal(&target, false, inbound)).unwrap();
    let session = DbSession::new(&MIGRATION_EXECUTION_REGISTRY, root);
    for id in 1..=2 {
        insert(
            &session,
            "Item",
            vec![
                ("id", InputValue::nat64(id)),
                ("key", InputValue::nat64(id + 100)),
                ("label", InputValue::nat64(id + 200)),
                (
                    "parent_id",
                    if id == 1 {
                        InputValue::null()
                    } else {
                        InputValue::nat64(1)
                    },
                ),
            ],
        );
    }
    if inbound {
        insert(
            &session,
            "Holder",
            vec![
                ("id", InputValue::nat64(10)),
                ("item_id", InputValue::nat64(2)),
            ],
        );
    }
    db
}

fn advance(
    db: &Db<MigrationExecutionCanister>,
    candidate: &SchemaProposal,
) -> Result<SchemaMigrationStatusPage, InternalError> {
    migrate_schema(
        db,
        candidate,
        SchemaMigrationCommand::Advance {
            expected_database: candidate.target_database(),
            expected_head: candidate.expected_head().clone(),
            expected_plan: candidate.migration().unwrap().digest(),
            acknowledged_finding_page: None,
        },
    )
}

#[derive(Debug, Eq, PartialEq)]
struct PhysicalState {
    rows: Vec<(Vec<u8>, Vec<u8>)>,
    indexes: Vec<(Vec<u8>, Vec<u8>)>,
}

fn physical_state(db: &Db<MigrationExecutionCanister>) -> PhysicalState {
    let store = db.store_handle(MIGRATION_EXECUTION_STORE_PATH).unwrap();
    let mut state = PhysicalState {
        rows: Vec::new(),
        indexes: Vec::new(),
    };
    store
        .with_data(|data| {
            data.visit_entries(|key, row| {
                state
                    .rows
                    .push((key.as_bytes().to_vec(), row.as_bytes().to_vec()));
                Ok::<_, InternalError>(StoreVisit::Continue)
            })
        })
        .unwrap();
    store
        .with_index(|index| {
            index.visit_entries(|key, value| {
                state
                    .indexes
                    .push((key.as_bytes().to_vec(), value.as_bytes().to_vec()));
                Ok::<_, InternalError>(IndexStoreVisit::Continue)
            })
        })
        .unwrap();
    state
}

fn snapshot(db: &Db<MigrationExecutionCanister>, path: &str) -> PersistedSchemaSnapshot {
    let runtime = db.accepted_runtime_entity_for_path(path).unwrap();
    db.store_handle(MIGRATION_EXECUTION_STORE_PATH)
        .unwrap()
        .with_schema(|schema| {
            schema.current_accepted_catalog_selection(
                runtime.entity_tag(),
                path,
                MIGRATION_EXECUTION_STORE_PATH,
            )
        })
        .unwrap()
        .unwrap()
        .snapshot()
        .persisted_snapshot()
        .clone()
}

fn assert_snapshot_identity(before: &PersistedSchemaSnapshot, after: &PersistedSchemaSnapshot) {
    assert_eq!(
        before.primary_key_field_ids(),
        after.primary_key_field_ids()
    );
    assert_eq!(before.row_layout(), after.row_layout());
    assert_eq!(before.fields(), after.fields());
    assert_eq!(before.indexes(), after.indexes());
    assert_eq!(before.constraint_catalog(), after.constraint_catalog());
    assert_eq!(
        before.relation_id_allocator(),
        after.relation_id_allocator()
    );
    let expected = before
        .relations()
        .iter()
        .map(|relation| {
            relation.clone_with_metadata(relation.name().to_string(), "CatalogItem".to_string())
        })
        .collect::<Vec<_>>();
    assert_eq!(expected, after.relations());
}

fn item_query(path: &str) -> DynamicQuery {
    DynamicQuery::new(path)
        .select(["id", "key", "label", "parent_id"])
        .order_by(crate::db::asc("id"))
}

fn assert_schema_only_publication(db: &Db<MigrationExecutionCanister>, next: JournalSequence) {
    let store = db.store_handle(MIGRATION_EXECUTION_STORE_PATH).unwrap();
    let batch = store
        .journal_tail_store()
        .unwrap()
        .with_borrow(|journal| journal.next_batch_after(JournalSequence::new(next.get() - 1)))
        .unwrap()
        .unwrap();
    assert_eq!(batch.journal_sequence(), next);
    assert_eq!(batch.records().len(), 1);
    assert!(
        matches!(
            batch.records()[0],
            JournalRecord::AcceptedSchemaPublish { .. }
        ),
        "the rename journal must contain no row or index mutations"
    );
}

fn assert_lineage(db: &Db<MigrationExecutionCanister>, proposal: &SchemaProposal) {
    let lineage = load_entity_source_lineage_catalog().unwrap().unwrap();
    let head = schema_application_target(db).unwrap();
    for transition in proposal.migration().unwrap().transitions() {
        let runtime = db
            .accepted_runtime_entity_for_path(transition.entity().as_str())
            .unwrap();
        let entry = lineage
            .get(head.stores()[0].identity(), runtime.entity_tag())
            .unwrap();
        assert_eq!(entry.accepted_head(), head.accepted_head());
        let AcceptedEntitySourceLineageState::Adopted {
            version,
            source_digest,
        } = entry.state()
        else {
            panic!("published rename lineage must stay adopted");
        };
        assert_eq!(version.get(), 2);
        assert_eq!(
            *source_digest,
            proposal.entity_source_digest(transition.entity()).unwrap()
        );
    }
}

fn assert_populated_rename(inbound: bool) {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = initialize(&root, inbound);
    let session = DbSession::new(&MIGRATION_EXECUTION_REGISTRY, &root);
    let before = db.accepted_runtime_entity_for_path("Item").unwrap();
    let before_snapshot = snapshot(&db, "Item");
    let holder = inbound.then(|| snapshot(&db, "Holder"));
    let state = physical_state(&db);
    assert_eq!(state.rows.len(), if inbound { 3 } else { 2 });
    assert_eq!(state.indexes.len(), if inbound { 4 } else { 3 });
    let cursor = session
        .execute_trusted_live_page_with_result_bytes_limit_for_tests(
            &DynamicQuery::new("Item")
                .select(["id"])
                .order_by(crate::db::asc("id")),
            None,
            32,
        )
        .unwrap()
        .continuation
        .expect("two rows must produce a continuation");
    let binding = session
        .issue_typed_entity_binding(&ITEM_DESCRIPTOR)
        .unwrap();
    let target = schema_application_target(&db).unwrap();
    let candidate = proposal(&target, true, inbound);
    let store = db.store_handle(MIGRATION_EXECUTION_STORE_PATH).unwrap();
    let revision = store
        .journal_tail_store()
        .unwrap()
        .with_borrow(JournalTailStore::data_mutation_revision)
        .unwrap();
    let next = store
        .journal_tail_store()
        .unwrap()
        .with_borrow(JournalTailStore::next_mutation_append_sequence)
        .unwrap();
    let result = advance(&db, &candidate).expect("populated metadata rename should publish");
    assert_schema_only_publication(&db, next);
    assert_eq!(result.phase(), SchemaMigrationPhase::Applied);
    let after = db.accepted_runtime_entity_for_path("CatalogItem").unwrap();
    assert_eq!(before.entity_tag(), after.entity_tag());
    assert_snapshot_identity(&before_snapshot, &snapshot(&db, "CatalogItem"));
    if let Some(holder) = holder {
        assert_snapshot_identity(&holder, &snapshot(&db, "Holder"));
    }
    assert_eq!(state, physical_state(&db));
    assert_eq!(
        revision,
        store
            .journal_tail_store()
            .unwrap()
            .with_borrow(JournalTailStore::data_mutation_revision)
            .unwrap()
    );
    assert!(
        session
            .execute_public_exact_key_batch_for_typed_binding(
                &binding,
                &[PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(1))]
            )
            .unwrap()
            .is_none(),
        "a preserved entity tag must not authorize a stale binding"
    );
    let cursor_error = session
        .execute_trusted_live_page(
            &DynamicQuery::new("CatalogItem")
                .select(["id"])
                .order_by(crate::db::asc("id")),
            Some(&cursor),
        )
        .expect_err("pre-publication cursor must not authorize a renamed entity");
    assert_eq!(
        cursor_error.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::QueryInvalidContinuationCursor
    );
    assert_rows(&session);
    assert_constraints(&session, inbound);
    let published_target = schema_application_target(&db).unwrap();
    assert_eq!(
        published_target.database_identity(),
        target.database_identity()
    );
    assert_eq!(published_target.stores(), target.stores());
    for _ in 0..2 {
        assert_eq!(advance(&db, &candidate).unwrap(), result);
        forget_recovered_domain_for_tests(&db).unwrap();
        drive_startup_recovery_to_completion(&db);
        assert_eq!(schema_application_target(&db).unwrap(), published_target);
        assert_eq!(state, physical_state(&db));
        assert_lineage(&db, &candidate);
        assert_rows(&session);
    }
}

fn assert_rows(session: &DbSession<MigrationExecutionCanister>) {
    let page = session
        .execute_trusted_live_page(&item_query("CatalogItem").limit(2), None)
        .unwrap();
    assert_eq!(
        page.rows,
        vec![
            vec![
                OutputValue::nat64(1),
                OutputValue::nat64(101),
                OutputValue::nat64(201),
                OutputValue::null(),
            ],
            vec![
                OutputValue::nat64(2),
                OutputValue::nat64(102),
                OutputValue::nat64(202),
                OutputValue::nat64(1),
            ],
        ],
    );
    let indexed = session
        .execute_trusted_live_page(
            &DynamicQuery::new("CatalogItem")
                .filter(FieldRef::new("key").eq(InputValue::nat64(102)))
                .select(["id"]),
            None,
        )
        .unwrap();
    assert_eq!(indexed.rows, vec![vec![OutputValue::nat64(2)]]);
}

fn assert_constraints(session: &DbSession<MigrationExecutionCanister>, inbound: bool) {
    for id in if inbound { vec![1, 2] } else { vec![1] } {
        let error = session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Delete {
                entity: "CatalogItem".to_string(),
                key: InputValue::nat64(id),
            })
            .expect_err("populated reverse references must restrict deletion");
        assert!(error.diagnostic_facts().contains(&(
            icydb_diagnostic_code::DiagnosticFactTag::ConstraintKind,
            icydb_diagnostic_code::DiagnosticConstraintKind::Relation.raw(),
        )));
    }
    let error = session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
            entity: "CatalogItem".to_string(),
            patch: DynamicStructuralPatch::new(vec![
                (
                    "id".to_string(),
                    DynamicWriteCell::Value(InputValue::nat64(3)),
                ),
                (
                    "key".to_string(),
                    DynamicWriteCell::Value(InputValue::nat64(103)),
                ),
                (
                    "label".to_string(),
                    DynamicWriteCell::Value(InputValue::nat64(203)),
                ),
                (
                    "parent_id".to_string(),
                    DynamicWriteCell::Value(InputValue::nat64(999)),
                ),
            ]),
        })
        .expect_err("self relation must validate the current target");
    assert!(error.diagnostic_facts().contains(&(
        icydb_diagnostic_code::DiagnosticFactTag::ConstraintKind,
        icydb_diagnostic_code::DiagnosticConstraintKind::Relation.raw(),
    )));
}

#[test]
fn populated_entity_rename_preserves_self_relation() {
    assert_populated_rename(false);
}

#[test]
fn populated_entity_rename_preserves_inbound_relation() {
    assert_populated_rename(true);
}
