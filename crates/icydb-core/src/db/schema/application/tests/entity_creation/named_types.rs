//! New value definitions close over accepted identities and publish with added entities.

use super::*;
use crate::db::schema::{
    AcceptedFieldKind, AcceptedNamedTypeIdentity, AcceptedSchemaRevisionBundle,
    application_lowering::lower_existing_schema_proposal,
    composite_catalog::{AcceptedCompositeElement, AcceptedCompositeShape, CompositeTypeId},
};
use crate::value::{EnumTypeId, EnumVariantId};
use icydb_schema::{
    EnumTypeFragment, EnumVariantFragment, RecordFieldFragment, RecordTypeFragment,
};
use std::collections::BTreeMap;

fn named(value: &str) -> FieldType {
    FieldType::Named(TypeSourceKey::try_new(value).unwrap())
}

fn record_type(value: &str, fields: Vec<(&str, FieldType)>) -> NamedTypeFragment {
    NamedTypeFragment::Record(
        RecordTypeFragment::try_new(
            name(value),
            fields
                .into_iter()
                .map(|(name_value, kind)| RecordFieldFragment::new(name(name_value), kind, false))
                .collect(),
        )
        .unwrap(),
    )
}

fn old_types() -> Vec<NamedTypeFragment> {
    vec![
        NamedTypeFragment::Enum(
            EnumTypeFragment::try_new(
                name("Status"),
                vec![
                    EnumVariantFragment::new(name("Active")),
                    EnumVariantFragment::new(name("Disabled")),
                ],
            )
            .unwrap(),
        ),
        record_type(
            "Profile",
            vec![
                ("status", named("Status")),
                (
                    "label",
                    FieldType::Scalar(ScalarType::Text { max_len: Some(64) }),
                ),
            ],
        ),
    ]
}

fn added_types() -> Vec<NamedTypeFragment> {
    vec![
        record_type(
            "AddedNode",
            vec![
                ("children", FieldType::List(Box::new(named("AddedNode")))),
                ("outcome", named("AddedOutcome")),
                ("profile", named("Profile")),
            ],
        ),
        NamedTypeFragment::Enum(
            EnumTypeFragment::try_new(
                name("AddedOutcome"),
                vec![
                    EnumVariantFragment::new(name("Open")),
                    EnumVariantFragment::with_payload(
                        name("History"),
                        FieldType::List(Box::new(named("AddedNode"))),
                    ),
                ],
            )
            .unwrap(),
        ),
    ]
}

fn holder(value: &str, root: &str) -> EntityFragment {
    EntityFragment::try_new(
        name(value),
        version_one(),
        vec![
            scalar("id", ScalarType::Nat64),
            FieldFragment::new(
                name("root"),
                named(root),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
        ],
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
        Vec::new(),
        Vec::new(),
    )
    .unwrap()
}

fn compose(
    target: &SchemaApplicationTarget,
    entities: Vec<EntityFragment>,
    types: Vec<NamedTypeFragment>,
    key: &str,
) -> SchemaProposal {
    let assignments = entities
        .iter()
        .map(|entity| {
            EntityStoreAssignment::new(entity.source_key().clone(), target.stores()[0].identity())
        })
        .collect();
    SchemaProposal::try_compose(
        vec![SchemaCapability::EXACT_COMPOSITE_TYPES],
        target.database_identity(),
        SchemaSubmissionKey::try_new(key).unwrap(),
        target.accepted_head().clone(),
        vec![SchemaFragment::try_new(entities, types).unwrap()],
        assignments,
        Vec::new(),
        None,
    )
    .unwrap()
}

fn candidate(target: &SchemaApplicationTarget) -> SchemaProposal {
    let mut types = old_types();
    types.extend(added_types());
    compose(
        target,
        vec![
            declaration("Item", 1),
            holder("ProfileHolder", "Profile"),
            holder("NodeHolder", "AddedNode"),
            holder("OtherNodeHolder", "AddedNode"),
        ],
        types,
        "create-named-nodes",
    )
}

fn record(values: Vec<(&str, InputValue)>) -> InputValue {
    InputValue::map(
        values
            .into_iter()
            .map(|(name_value, value)| (InputValue::text(name_value.into()), value))
            .collect(),
    )
}

fn profile() -> InputValue {
    record(vec![
        ("status", InputValue::loose_enum("Active")),
        ("label", InputValue::text("retained".into())),
    ])
}

fn node(history: bool) -> InputValue {
    record(vec![
        ("children", InputValue::list(Vec::new())),
        ("profile", profile()),
        (
            "outcome",
            if history {
                InputValue::loose_enum("History")
                    .with_enum_payload(InputValue::list(vec![node(false)]))
                    .unwrap()
            } else {
                InputValue::loose_enum("Open")
            },
        ),
    ])
}

fn initialize_named(root: &RequestExecutionRoot) -> Db<EvolutionCanister> {
    let db = initialize(root);
    let target = schema_application_target(&db).unwrap();
    apply_schema(
        &db,
        &compose(
            &target,
            vec![declaration("Item", 1), holder("ProfileHolder", "Profile")],
            old_types(),
            "create-profile-holder",
        ),
    )
    .unwrap();
    let session = DbSession::new(&EVOLUTION_REGISTRY, root);
    insert(
        &session,
        "ProfileHolder",
        vec![("id", InputValue::nat64(7)), ("root", profile())],
    )
    .unwrap();
    db
}

fn bundle(db: &Db<EvolutionCanister>) -> AcceptedSchemaRevisionBundle {
    db.store_handle(EVOLUTION_STORE_PATH)
        .unwrap()
        .with_schema(SchemaStore::current_accepted_schema_bundle)
        .unwrap()
        .unwrap()
}

fn assert_preserved(before: &AcceptedSchemaRevisionBundle, after: &AcceptedSchemaRevisionBundle) {
    for (tag, snapshot) in before.entity_snapshots() {
        assert_eq!(after.entity_snapshots().get(tag), Some(snapshot));
    }
    for (source, identity) in before.source_bindings().named_types() {
        assert_eq!(after.source_bindings().named_type(source), Some(*identity));
        match identity {
            AcceptedNamedTypeIdentity::Enum(id) => assert_eq!(
                after.enum_catalog().enum_type(*id),
                before.enum_catalog().enum_type(*id)
            ),
            AcceptedNamedTypeIdentity::Composite(id) => assert_eq!(
                after.composite_catalog().composite_type(*id),
                before.composite_catalog().composite_type(*id)
            ),
        }
    }
    for (source, member) in [("Profile", "label"), ("Profile", "status")] {
        let AcceptedNamedTypeIdentity::Composite(id) = before
            .source_bindings()
            .named_type(&TypeSourceKey::try_new(source).unwrap())
            .unwrap()
        else {
            panic!("fixture record must be composite");
        };
        assert_eq!(
            after.source_bindings().composite_field(id, &field(member)),
            before.source_bindings().composite_field(id, &field(member))
        );
    }
    let status = before.enum_catalog().type_id("Status").unwrap();
    for variant in ["Active", "Disabled"] {
        let key = TypeSourceKey::try_new(variant).unwrap();
        assert_eq!(
            after.source_bindings().enum_variant(status, &key),
            before.source_bindings().enum_variant(status, &key)
        );
    }
}

fn assert_named_writes(root: &RequestExecutionRoot) {
    let session = DbSession::<EvolutionCanister>::new(&EVOLUTION_REGISTRY, root);
    for entity in ["NodeHolder", "OtherNodeHolder"] {
        let value = node(true);
        insert(
            &session,
            entity,
            vec![("id", InputValue::nat64(10)), ("root", value)],
        )
        .unwrap();
        let page = session
            .execute_trusted_live_page(
                &DynamicQuery::new(entity)
                    .select(["id", "root"])
                    .filter(FieldRef::new("id").eq(10_u64)),
                None,
            )
            .unwrap();
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0][0], OutputValue::nat64(10));
        let invalid = record(vec![
            ("children", InputValue::list(Vec::new())),
            ("profile", profile()),
            ("outcome", InputValue::loose_enum("Unknown")),
        ]);
        assert!(
            insert(
                &session,
                entity,
                vec![("id", InputValue::nat64(11)), ("root", invalid)]
            )
            .is_err()
        );
    }
    let page = session
        .execute_trusted_live_page(
            &DynamicQuery::new("ProfileHolder").select(["id", "root"]),
            None,
        )
        .unwrap();
    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0][0], OutputValue::nat64(7));
}

fn assert_creation(generated: bool) {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = initialize_named(&root);
    let before = bundle(&db);
    let physical = physical_state(&db);
    let target = schema_application_target(&db).unwrap();
    let proposal = candidate(&target);
    let receipt = if generated {
        apply_generated_schema(&db, &proposal)
    } else {
        apply_schema(&db, &proposal)
    }
    .unwrap();
    assert_eq!(apply_schema(&db, &proposal).unwrap(), receipt);
    assert_eq!(physical_state(&db), physical);
    let after = bundle(&db);
    assert_preserved(&before, &after);
    assert_eq!(after.composite_catalog().id_by_path().len(), 2);
    assert_eq!(after.enum_catalog().type_ids().count(), 2);
    assert_eq!(
        after.enum_catalog().type_id("AddedOutcome").unwrap().get(),
        2
    );
    assert_eq!(
        after
            .composite_catalog()
            .type_id("AddedNode")
            .unwrap()
            .get(),
        2
    );
    assert_named_writes(&root);
}

#[test]
fn recursive_shared_named_types_preserve_populated_catalogs() {
    assert_creation(false);
}

#[test]
fn generated_creation_accepts_recursive_shared_named_types() {
    assert_creation(true);
}

#[test]
fn missing_or_orphan_named_definitions_reject_atomically() {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = initialize_named(&root);
    let target = schema_application_target(&db).unwrap();
    let base = candidate(&target);
    let before = physical_state(&db);
    let mut missing = old_types();
    missing.push(added_types().remove(0));
    let mut orphan = base.fragments()[0].types().to_vec();
    orphan.push(record_type(
        "Orphan",
        vec![("value", FieldType::Scalar(ScalarType::Nat64))],
    ));
    for types in [missing, orphan] {
        let proposal = compose(
            &target,
            base.fragments()[0].entities().to_vec(),
            types,
            "invalid-named-closure",
        );
        assert!(apply_schema(&db, &proposal).is_err());
        assert_eq!(
            schema_application_target(&db).unwrap().accepted_head(),
            target.accepted_head()
        );
        assert_eq!(physical_state(&db), before);
    }
}

#[test]
fn existing_enum_or_record_edits_cannot_hide_in_entity_creation() {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = initialize_named(&root);
    let target = schema_application_target(&db).unwrap();
    let base = candidate(&target);
    let mut changed_enum = base.fragments()[0].types().to_vec();
    *changed_enum
        .iter_mut()
        .find(|definition| definition.source_key() == &TypeSourceKey::try_new("Status").unwrap())
        .unwrap() = NamedTypeFragment::Enum(
        EnumTypeFragment::try_new(
            name("Status"),
            vec![
                EnumVariantFragment::new(name("Active")),
                EnumVariantFragment::new(name("Disabled")),
                EnumVariantFragment::new(name("New")),
            ],
        )
        .unwrap(),
    );
    let mut changed_record = base.fragments()[0].types().to_vec();
    *changed_record
        .iter_mut()
        .find(|definition| definition.source_key() == &TypeSourceKey::try_new("Profile").unwrap())
        .unwrap() = record_type(
        "Profile",
        vec![
            ("label", FieldType::Scalar(ScalarType::Nat64)),
            ("status", named("Status")),
        ],
    );
    let before = physical_state(&db);
    for types in [changed_enum, changed_record] {
        let proposal = compose(
            &target,
            base.fragments()[0].entities().to_vec(),
            types,
            "edited-accepted-type",
        );
        assert!(apply_schema(&db, &proposal).is_err());
        assert_eq!(
            schema_application_target(&db).unwrap().accepted_head(),
            target.accepted_head()
        );
        assert_eq!(physical_state(&db), before);
    }
}

fn with_unbound_types(
    before: &AcceptedSchemaRevisionBundle,
    enum_id: u32,
    composite_id: u32,
) -> AcceptedSchemaRevisionBundle {
    let enums = before
        .enum_catalog()
        .clone()
        .with_added_definitions(BTreeMap::from([(
            EnumTypeId::new(enum_id).unwrap(),
            (
                "UnboundEnum".into(),
                BTreeMap::from([(EnumVariantId::new(1).unwrap(), ("Unit".into(), None))]),
            ),
        )]))
        .unwrap();
    let composites = before
        .composite_catalog()
        .clone()
        .with_added_definitions(
            BTreeMap::from([(
                CompositeTypeId::new(composite_id).unwrap(),
                (
                    "UnboundComposite".into(),
                    AcceptedCompositeShape::Newtype(AcceptedCompositeElement::new(
                        AcceptedFieldKind::Nat64,
                        false,
                    )),
                ),
            )]),
            &enums,
        )
        .unwrap();
    AcceptedSchemaRevisionBundle::new_with_source_bindings(
        before.revision(),
        before.store_path(),
        enums,
        composites,
        before.source_bindings().clone(),
        before.entity_snapshots().clone(),
    )
    .unwrap()
}

#[test]
fn allocation_preserves_unbound_accepted_ids_and_checks_exhaustion() {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = initialize_named(&root);
    let target = schema_application_target(&db).unwrap();
    let proposal = candidate(&target);
    let before = bundle(&db);
    let sparse = with_unbound_types(&before, 7, 11);
    let lower = |bundle: &AcceptedSchemaRevisionBundle| {
        lower_existing_schema_proposal(
            &proposal,
            &[ExistingProposalStore {
                path: EVOLUTION_STORE_PATH,
                identity: target.stores()[0].identity(),
                bundle,
            }],
        )
    };
    let created = lower(&sparse).unwrap();
    assert_eq!(
        created[0]
            .bundle()
            .enum_catalog()
            .type_id("AddedOutcome")
            .unwrap()
            .get(),
        8
    );
    assert_eq!(
        created[0]
            .bundle()
            .composite_catalog()
            .type_id("AddedNode")
            .unwrap()
            .get(),
        12
    );
    for (enum_id, composite_id) in [(u32::MAX, 11), (7, u32::MAX)] {
        let exhausted = with_unbound_types(&before, enum_id, composite_id);
        assert!(lower(&exhausted).is_err());
    }
}

#[test]
fn new_named_source_cannot_collide_with_an_unbound_accepted_path() {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = initialize_named(&root);
    let target = schema_application_target(&db).unwrap();
    let before = bundle(&db);
    let enums = before
        .enum_catalog()
        .clone()
        .with_added_definitions(BTreeMap::from([(
            EnumTypeId::new(7).unwrap(),
            (
                "AddedNode".into(),
                BTreeMap::from([(EnumVariantId::new(1).unwrap(), ("Unit".into(), None))]),
            ),
        )]))
        .unwrap();
    let occupied = AcceptedSchemaRevisionBundle::new_with_source_bindings(
        before.revision(),
        before.store_path(),
        enums,
        before.composite_catalog().clone(),
        before.source_bindings().clone(),
        before.entity_snapshots().clone(),
    )
    .unwrap();
    assert!(
        lower_existing_schema_proposal(
            &candidate(&target),
            &[ExistingProposalStore {
                path: EVOLUTION_STORE_PATH,
                identity: target.stores()[0].identity(),
                bundle: &occupied,
            }]
        )
        .is_err()
    );
}

fn initial_two_stores(
    database: TargetDatabaseIdentity,
    a: TargetStoreIdentity,
    b: TargetStoreIdentity,
) -> Vec<crate::db::schema::CandidateSchemaRevision> {
    use crate::db::schema::application_lowering::lower_initial_schema_proposal;
    let old_entities = vec![declaration("Item", 1), declaration("Resource", 1)];
    let initial = SchemaProposal::try_compose(
        vec![SchemaCapability::EXACT_COMPOSITE_TYPES],
        database,
        SchemaSubmissionKey::try_new("two-existing-stores").unwrap(),
        ExpectedAcceptedHead::Empty,
        vec![SchemaFragment::try_new(old_entities, Vec::new()).unwrap()],
        vec![
            EntityStoreAssignment::new(entity("Item"), a),
            EntityStoreAssignment::new(entity("Resource"), b),
        ],
        Vec::new(),
        None,
    )
    .unwrap();
    lower_initial_schema_proposal(
        &initial,
        &[
            ProposalStoreTarget {
                path: "test::A",
                identity: a,
            },
            ProposalStoreTarget {
                path: "test::B",
                identity: b,
            },
        ],
    )
    .unwrap()
}

#[test]
fn shared_new_definition_allocates_independently_in_existing_stores() {
    let database = TargetDatabaseIdentity::from_bytes([0x61; 32]);
    let a = TargetStoreIdentity::from_bytes([0x62; 32]);
    let b = TargetStoreIdentity::from_bytes([0x63; 32]);
    let installed = initial_two_stores(database, a, b);
    let before_a = with_unbound_types(installed[0].bundle(), 7, 11);
    let before_b = installed[1].bundle();
    let mut entities = vec![declaration("Item", 1), declaration("Resource", 1)];
    entities.extend([holder("AddedA", "Shared"), holder("AddedB", "Shared")]);
    let assignments = entities
        .iter()
        .map(|definition| {
            EntityStoreAssignment::new(
                definition.source_key().clone(),
                if matches!(definition.source_key().as_str(), "Item" | "AddedA") {
                    a
                } else {
                    b
                },
            )
        })
        .collect();
    let proposal = SchemaProposal::try_compose(
        vec![SchemaCapability::EXACT_COMPOSITE_TYPES],
        database,
        SchemaSubmissionKey::try_new("shared-new-type").unwrap(),
        ExpectedAcceptedHead::Exact {
            revision: 1,
            fingerprint: ExpectedSchemaFingerprint::from_bytes([0x64; 32]),
        },
        vec![
            SchemaFragment::try_new(
                entities,
                vec![record_type(
                    "Shared",
                    vec![("value", FieldType::Scalar(ScalarType::Nat64))],
                )],
            )
            .unwrap(),
        ],
        assignments,
        Vec::new(),
        None,
    )
    .unwrap();
    let created = lower_existing_schema_proposal(
        &proposal,
        &[
            ExistingProposalStore {
                path: "test::A",
                identity: a,
                bundle: &before_a,
            },
            ExistingProposalStore {
                path: "test::B",
                identity: b,
                bundle: before_b,
            },
        ],
    )
    .unwrap();
    assert_eq!(created.len(), 2);
    assert_eq!(
        created[0]
            .bundle()
            .composite_catalog()
            .type_id("Shared")
            .unwrap()
            .get(),
        12
    );
    assert_eq!(
        created[1]
            .bundle()
            .composite_catalog()
            .type_id("Shared")
            .unwrap()
            .get(),
        1
    );
    assert_eq!(
        created[0]
            .bundle()
            .composite_catalog()
            .composite_type(CompositeTypeId::new(11).unwrap()),
        before_a
            .composite_catalog()
            .composite_type(CompositeTypeId::new(11).unwrap())
    );
    for (before, after) in [
        (&before_a, created[0].bundle()),
        (before_b, created[1].bundle()),
    ] {
        for (tag, snapshot) in before.entity_snapshots() {
            assert_eq!(after.entity_snapshots().get(tag), Some(snapshot));
        }
    }
}

#[cfg(feature = "migration")]
fn assert_recovery(receipt_first: bool) {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = initialize_named(&root);
    let before = bundle(&db);
    let physical = physical_state(&db);
    let proposal = candidate(&schema_application_target(&db).unwrap());
    super::recovery::interrupt_publication(&db, &proposal, receipt_first);
    for _ in 0..2 {
        forget_recovered_domain_for_tests(&db).unwrap();
        drive_startup_recovery_to_completion(&db);
        assert!(matches!(
            apply_schema(&db, &proposal).unwrap().outcome(),
            SchemaChangeOutcome::Applied { .. }
        ));
        assert_eq!(physical_state(&db), physical);
        assert_preserved(&before, &bundle(&db));
    }
    assert_named_writes(&root);
}

#[cfg(feature = "migration")]
#[test]
fn named_catalogs_recover_with_entities_and_receipt() {
    assert_recovery(false);
}

#[cfg(feature = "migration")]
#[test]
fn named_catalogs_recover_after_receipt_publication() {
    assert_recovery(true);
}
