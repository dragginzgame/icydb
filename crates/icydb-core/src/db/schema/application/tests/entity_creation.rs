//! Entity creation preserves populated accepted domains and publishes one schema receipt.

#[cfg(feature = "migration")]
mod migrations;
mod named_types;
#[cfg(feature = "migration")]
mod recovery;

use super::*;
use crate::{
    db::{
        DynamicQuery, FieldRef, RequestExecutionRoot,
        data::StoreVisit,
        index::IndexStoreVisit,
        schema::{
            PersistedSchemaSnapshot, SchemaApplicationTarget, application::apply_generated_schema,
        },
    },
    error::InternalError,
    value::OutputValue,
};
use icydb_schema::{
    IndexFragment, IndexKeyFragment, RelationDeleteAction, RelationFragment, RelationSourceFragment,
};

fn entity(value: &str) -> EntitySourceKey {
    EntitySourceKey::try_new(value).unwrap()
}

fn field(value: &str) -> FieldSourceKey {
    FieldSourceKey::try_new(value).unwrap()
}

fn scalar(value: &str, kind: ScalarType) -> FieldFragment {
    FieldFragment::new(
        name(value),
        FieldType::Scalar(kind),
        false,
        FieldInsertPolicy::Required,
        None,
    )
}

fn declaration(value: &str, version: u32) -> EntityFragment {
    let quest = value == "Quest";
    let mut fields = vec![scalar("id", ScalarType::Nat64)];
    if quest {
        fields.extend([
            scalar("item_id", ScalarType::Nat64),
            scalar("score", ScalarType::Int64),
        ]);
    }
    EntityFragment::try_new(
        name(value),
        DeclaredEntityVersion::try_new(version).unwrap(),
        fields,
        vec![field("id")],
        vec![
            IndexFragment::try_new(
                name("id_lookup"),
                vec![IndexKeyFragment::Field(field("id"))],
                true,
                None,
            )
            .unwrap(),
        ],
        if quest {
            vec![
                RelationFragment::try_new(
                    name("item"),
                    RelationSourceFragment::direct(vec![field("item_id")]),
                    entity("Item"),
                    vec![field("id")],
                    RelationDeleteAction::Restrict,
                )
                .unwrap(),
            ]
        } else {
            Vec::new()
        },
        if quest {
            vec![ConstraintFragment::check(
                name("positive_score"),
                SourceCheckExpr::try_new(vec![
                    SourceCheckInstruction::Field(field("score")),
                    SourceCheckInstruction::Literal(ScalarLiteral::Int(0)),
                    SourceCheckInstruction::GreaterThanOrEqual,
                ])
                .unwrap(),
            )]
        } else {
            Vec::new()
        },
    )
    .unwrap()
}

fn proposal(
    target: &SchemaApplicationTarget,
    additions: &[(&str, u32)],
    key: &str,
) -> SchemaProposal {
    let mut entities = vec![declaration("Item", 1)];
    entities.extend(
        additions
            .iter()
            .map(|(name, version)| declaration(name, *version)),
    );
    let assignments = entities
        .iter()
        .map(|entity| {
            EntityStoreAssignment::new(entity.source_key().clone(), target.stores()[0].identity())
        })
        .collect();
    SchemaProposal::try_compose(
        vec![
            SchemaCapability::ACCEPTED_CHECKS,
            SchemaCapability::RESTRICTIVE_RELATIONS,
        ],
        target.database_identity(),
        SchemaSubmissionKey::try_new(key).unwrap(),
        target.accepted_head().clone(),
        vec![SchemaFragment::try_new(entities, Vec::new()).unwrap()],
        assignments,
        Vec::new(),
        None,
    )
    .unwrap()
}

fn insert(
    session: &DbSession<EvolutionCanister>,
    name: &str,
    values: Vec<(&str, InputValue)>,
) -> Result<(), InternalError> {
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
            entity: name.into(),
            patch: DynamicStructuralPatch::new(
                values
                    .into_iter()
                    .map(|(name, value)| (name.into(), DynamicWriteCell::Value(value)))
                    .collect(),
            ),
        })
        .map(|_| ())
}

fn initialize(root: &RequestExecutionRoot) -> Db<EvolutionCanister> {
    let db = Db::<EvolutionCanister>::new(&EVOLUTION_REGISTRY, root.scope());
    drive_startup_recovery_to_completion(&db);
    apply_schema(
        &db,
        &proposal(&schema_application_target(&db).unwrap(), &[], "initial"),
    )
    .unwrap();
    let session = DbSession::new(&EVOLUTION_REGISTRY, root);
    for id in 1..=2 {
        insert(&session, "Item", vec![("id", InputValue::nat64(id))]).unwrap();
    }
    db
}

fn snapshot(db: &Db<EvolutionCanister>, path: &str) -> PersistedSchemaSnapshot {
    let runtime = db.accepted_runtime_entity_for_path(path).unwrap();
    db.store_handle(EVOLUTION_STORE_PATH)
        .unwrap()
        .with_schema(|schema| {
            schema.current_accepted_catalog_selection(
                runtime.entity_tag(),
                path,
                EVOLUTION_STORE_PATH,
            )
        })
        .unwrap()
        .unwrap()
        .snapshot()
        .persisted_snapshot()
        .clone()
}

#[derive(Debug, Eq, PartialEq)]
struct PhysicalState {
    rows: Vec<(Vec<u8>, Vec<u8>)>,
    indexes: Vec<(Vec<u8>, Vec<u8>)>,
}

fn physical_state(db: &Db<EvolutionCanister>) -> PhysicalState {
    let store = db.store_handle(EVOLUTION_STORE_PATH).unwrap();
    let mut rows = Vec::new();
    let mut indexes = Vec::new();
    store
        .with_data(|data| {
            data.visit_entries(|key, row| {
                rows.push((key.as_bytes().to_vec(), row.as_bytes().to_vec()));
                Ok::<_, InternalError>(StoreVisit::Continue)
            })
        })
        .unwrap();
    store
        .with_index(|index| {
            index.visit_entries(|key, value| {
                indexes.push((key.as_bytes().to_vec(), value.as_bytes().to_vec()));
                Ok::<_, InternalError>(IndexStoreVisit::Continue)
            })
        })
        .unwrap();
    PhysicalState { rows, indexes }
}

fn assert_rows_and_constraints(root: &RequestExecutionRoot) {
    let session = DbSession::<EvolutionCanister>::new(&EVOLUTION_REGISTRY, root);
    let rows = session
        .execute_trusted_live_page(
            &DynamicQuery::new("Item")
                .select(["id"])
                .order_by(crate::db::asc("id")),
            None,
        )
        .unwrap();
    assert_eq!(
        rows.rows,
        vec![vec![OutputValue::nat64(1)], vec![OutputValue::nat64(2)]]
    );
    let values = |id, item, score| {
        vec![
            ("id", InputValue::nat64(id)),
            ("item_id", InputValue::nat64(item)),
            ("score", InputValue::int64(score)),
        ]
    };
    assert!(insert(&session, "Quest", values(10, 1, -1)).is_err());
    assert!(insert(&session, "Quest", values(10, 99, 1)).is_err());
    insert(&session, "Quest", values(10, 1, 1)).unwrap();
    assert!(insert(&session, "Quest", values(10, 1, 2)).is_err());
    let page = session
        .execute_trusted_live_page(
            &DynamicQuery::new("Quest")
                .select(["id"])
                .filter(FieldRef::new("id").eq(10_u64)),
            None,
        )
        .unwrap();
    assert_eq!(page.rows, vec![vec![OutputValue::nat64(10)]]);
    assert!(
        session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Delete {
                entity: "Item".into(),
                key: InputValue::nat64(1)
            })
            .is_err()
    );
}

fn assert_creation(generated: bool) {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = initialize(&root);
    let before = snapshot(&db, "Item");
    let tag = db
        .accepted_runtime_entity_for_path("Item")
        .unwrap()
        .entity_tag();
    let physical = physical_state(&db);
    assert_eq!(physical.rows.len(), 2);
    assert_eq!(physical.indexes.len(), 2);
    let candidate = proposal(
        &schema_application_target(&db).unwrap(),
        &[("Quest", 1)],
        "create-quest",
    );
    let receipt = if generated {
        apply_generated_schema(&db, &candidate)
    } else {
        apply_schema(&db, &candidate)
    }
    .unwrap();
    assert!(matches!(
        receipt.outcome(),
        SchemaChangeOutcome::Applied { .. }
    ));
    assert_eq!(apply_schema(&db, &candidate).unwrap(), receipt);
    assert_eq!(snapshot(&db, "Item"), before);
    assert_eq!(
        db.accepted_runtime_entity_for_path("Item")
            .unwrap()
            .entity_tag(),
        tag
    );
    assert_eq!(physical_state(&db), physical);
    let quest = db
        .accepted_runtime_entity_for_path("Quest")
        .unwrap()
        .entity_tag();
    assert_ne!(quest, tag);
    #[cfg(feature = "migration")]
    {
        use crate::db::schema::{
            application::load_entity_source_lineage_catalog,
            migration_lineage::AcceptedEntitySourceLineageState,
        };
        let lineage = load_entity_source_lineage_catalog().unwrap().unwrap();
        let target = schema_application_target(&db).unwrap();
        for (source, tag) in [("Item", tag), ("Quest", quest)] {
            let entry = lineage.get(target.stores()[0].identity(), tag).unwrap();
            assert_eq!(entry.publication_head(), target.accepted_head());
            let AcceptedEntitySourceLineageState::Adopted {
                version,
                source_digest,
            } = entry.state()
            else {
                panic!("creation must publish adopted lineage");
            };
            assert_eq!(version.get(), 1);
            assert_eq!(
                *source_digest,
                candidate.entity_source_digest(&entity(source)).unwrap()
            );
        }
    }
    assert_rows_and_constraints(&root);
}

#[test]
fn populated_entity_creation_publishes_and_replays() {
    assert_creation(false);
}

#[test]
fn generated_entity_creation_preserves_populated_domains() {
    assert_creation(true);
}

#[test]
fn new_entity_requires_version_one_without_publication() {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = initialize(&root);
    let before = schema_application_target(&db).unwrap();
    let physical = physical_state(&db);
    assert!(apply_schema(&db, &proposal(&before, &[("Quest", 2)], "invalid-version")).is_err());
    assert_eq!(
        schema_application_target(&db).unwrap().accepted_head(),
        before.accepted_head()
    );
    assert_eq!(physical_state(&db), physical);
    assert!(db.accepted_runtime_entity_for_path("Quest").is_err());
}

#[test]
fn stale_creation_head_rejects_without_losing_existing_data() {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = initialize(&root);
    let target = schema_application_target(&db).unwrap();
    apply_schema(&db, &proposal(&target, &[("Quest", 1)], "first-creation")).unwrap();
    let current = schema_application_target(&db).unwrap();
    let physical = physical_state(&db);
    let error = apply_schema(
        &db,
        &proposal(&target, &[("Achievement", 1)], "stale-creation"),
    )
    .unwrap_err();
    assert_eq!(
        error.diagnostic_code(),
        InternalError::schema_application_conflict().diagnostic_code()
    );
    assert_eq!(
        schema_application_target(&db).unwrap().accepted_head(),
        current.accepted_head()
    );
    assert_eq!(physical_state(&db), physical);
}

#[test]
fn creation_rejects_unaccepted_store_placement() {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = initialize(&root);
    let target = schema_application_target(&db).unwrap();
    let base = proposal(&target, &[("Quest", 1)], "wrong-store");
    let assignments = base
        .assignments()
        .iter()
        .map(|assignment| {
            if assignment.entity() == &entity("Quest") {
                EntityStoreAssignment::new(
                    entity("Quest"),
                    TargetStoreIdentity::from_bytes([0x55; 32]),
                )
            } else {
                assignment.clone()
            }
        })
        .collect();
    let candidate = SchemaProposal::try_compose(
        base.capabilities().to_vec(),
        base.target_database(),
        base.submission_key().clone(),
        base.expected_head().clone(),
        base.fragments().to_vec(),
        assignments,
        Vec::new(),
        None,
    )
    .unwrap();
    let physical = physical_state(&db);
    let error = apply_schema(&db, &candidate).unwrap_err();
    assert_eq!(
        error.diagnostic_code(),
        InternalError::store_unsupported().diagnostic_code()
    );
    assert_eq!(
        schema_application_target(&db).unwrap().accepted_head(),
        target.accepted_head()
    );
    assert_eq!(physical_state(&db), physical);
}

#[test]
fn creation_with_new_named_type_preserves_populated_domains() {
    use icydb_schema::{RecordFieldFragment, RecordTypeFragment};
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = initialize(&root);
    let target = schema_application_target(&db).unwrap();
    let item = declaration("Item", 1);
    let extra = TypeSourceKey::try_new("Extra").unwrap();
    let added = EntityFragment::try_new(
        name("Achievement"),
        version_one(),
        vec![
            scalar("id", ScalarType::Nat64),
            FieldFragment::new(
                name("extra"),
                FieldType::Named(extra),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
        ],
        vec![field("id")],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let entities = vec![item, added];
    let assignments = entities
        .iter()
        .map(|entity| {
            EntityStoreAssignment::new(entity.source_key().clone(), target.stores()[0].identity())
        })
        .collect();
    let candidate = SchemaProposal::try_compose(
        vec![SchemaCapability::EXACT_COMPOSITE_TYPES],
        target.database_identity(),
        SchemaSubmissionKey::try_new("new-named-type").unwrap(),
        target.accepted_head().clone(),
        vec![
            SchemaFragment::try_new(
                entities,
                vec![NamedTypeFragment::Record(
                    RecordTypeFragment::try_new(
                        name("Extra"),
                        vec![RecordFieldFragment::new(
                            name("value"),
                            FieldType::Scalar(ScalarType::Nat64),
                            false,
                        )],
                    )
                    .unwrap(),
                )],
            )
            .unwrap(),
        ],
        assignments,
        Vec::new(),
        None,
    )
    .unwrap();
    let physical = physical_state(&db);
    apply_schema(&db, &candidate).unwrap();
    assert_ne!(
        schema_application_target(&db).unwrap().accepted_head(),
        target.accepted_head()
    );
    assert_eq!(physical_state(&db), physical);
    assert!(db.accepted_runtime_entity_for_path("Achievement").is_ok());
}

#[test]
fn occupied_new_row_domain_rejects_creation_before_publication() {
    use crate::{db::data::RawDataStoreKey, types::EntityTag};
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = initialize(&root);
    let target = schema_application_target(&db).unwrap();
    let tag = EntityTag::new(
        db.accepted_runtime_entity_for_path("Item")
            .unwrap()
            .entity_tag()
            .value()
            + 1,
    );
    let store = db.store_handle(EVOLUTION_STORE_PATH).unwrap();
    let mut occupied = None;
    store
        .with_data(|data| {
            data.visit_entries(|key, row| {
                occupied = Some((
                    RawDataStoreKey::from_entity_and_primary_key_bytes(tag, &key.as_bytes()[8..]),
                    row.clone(),
                ));
                Ok::<_, InternalError>(StoreVisit::Stop)
            })
        })
        .unwrap();
    let (key, row) = occupied.unwrap();
    store.with_data_mut(|data| data.insert_raw_for_test(key, row));
    let physical = physical_state(&db);
    assert!(apply_schema(&db, &proposal(&target, &[("Quest", 1)], "occupied-row")).is_err());
    assert_eq!(
        schema_application_target(&db).unwrap().accepted_head(),
        target.accepted_head()
    );
    assert_eq!(physical_state(&db), physical);
}

#[test]
fn multiple_additions_allocate_in_canonical_source_order() {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = initialize(&root);
    let target = schema_application_target(&db).unwrap();
    let reversed = proposal(&target, &[("Quest", 1), ("Achievement", 1)], "reverse");
    let sorted = proposal(&target, &[("Achievement", 1), ("Quest", 1)], "sorted");
    let authorities = application_authorities(&db);
    let a = crate::db::schema::application::lower_application_candidates::<true>(
        &target,
        &reversed,
        &authorities,
    )
    .unwrap()
    .candidates;
    let b = crate::db::schema::application::lower_application_candidates::<true>(
        &target,
        &sorted,
        &authorities,
    )
    .unwrap()
    .candidates;
    assert_eq!(a[0].encoded_bundle(), b[0].encoded_bundle());
    apply_schema(&db, &reversed).unwrap();
    assert!(
        db.accepted_runtime_entity_for_path("Achievement")
            .unwrap()
            .entity_tag()
            < db.accepted_runtime_entity_for_path("Quest")
                .unwrap()
                .entity_tag()
    );
}

#[test]
fn occupied_new_index_domain_rejects_creation_before_publication() {
    use crate::{
        db::{
            index::{IndexEntryValue, IndexId, IndexKey, IndexKeyKind},
            key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        },
        types::EntityTag,
    };
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = initialize(&root);
    let target = schema_application_target(&db).unwrap();
    let tag = EntityTag::new(
        db.accepted_runtime_entity_for_path("Item")
            .unwrap()
            .entity_tag()
            .value()
            + 1,
    );
    let key = IndexKey::new_from_components_with_primary_key_value(
        &IndexId::new(tag, 0),
        IndexKeyKind::User,
        &[7_u64.to_be_bytes()],
        &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(7)),
    )
    .unwrap()
    .to_raw()
    .unwrap();
    db.store_handle(EVOLUTION_STORE_PATH)
        .unwrap()
        .with_index_mut(|index| index.insert(key, IndexEntryValue::presence()));
    let physical = physical_state(&db);
    assert!(apply_schema(&db, &proposal(&target, &[("Quest", 1)], "occupied-index")).is_err());
    assert_eq!(
        schema_application_target(&db).unwrap().accepted_head(),
        target.accepted_head()
    );
    assert_eq!(physical_state(&db), physical);
    assert!(db.accepted_runtime_entity_for_path("Quest").is_err());
}

#[cfg(feature = "migration")]
#[test]
fn creation_requires_existing_lineage_adoption() {
    use crate::db::schema::{
        EntitySourceLineageCatalogOp,
        application::load_entity_source_lineage_catalog,
        apply_entity_source_lineage_catalog_op,
        migration_lineage::{AcceptedEntitySourceLineage, AcceptedEntitySourceLineageCatalog},
    };
    use icydb_diagnostic_code::{DiagnosticDetail, SchemaMigrationCode};
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = initialize(&root);
    let target = schema_application_target(&db).unwrap();
    let tag = db
        .accepted_runtime_entity_for_path("Item")
        .unwrap()
        .entity_tag();
    let before = load_entity_source_lineage_catalog().unwrap().unwrap();
    let unadopted =
        AcceptedEntitySourceLineageCatalog::try_new(std::collections::BTreeMap::from([(
            (target.stores()[0].identity(), tag),
            AcceptedEntitySourceLineage::unadopted(target.accepted_head().clone()).unwrap(),
        )]))
        .unwrap();
    apply_entity_source_lineage_catalog_op(
        &EntitySourceLineageCatalogOp::replace(Some(&before), &unadopted).unwrap(),
    )
    .unwrap();
    let physical = physical_state(&db);
    let error = apply_schema(
        &db,
        &proposal(&target, &[("Quest", 1)], "unadopted-creation"),
    )
    .unwrap_err();
    assert_eq!(
        error.diagnostic().detail(),
        Some(&DiagnosticDetail::SchemaMigration {
            reason: SchemaMigrationCode::Unadopted
        })
    );
    assert_eq!(
        schema_application_target(&db).unwrap().accepted_head(),
        target.accepted_head()
    );
    assert_eq!(physical_state(&db), physical);
    assert_eq!(
        load_entity_source_lineage_catalog().unwrap(),
        Some(unadopted)
    );
}
