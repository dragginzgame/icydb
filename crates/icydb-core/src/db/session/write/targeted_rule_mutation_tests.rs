#[cfg(feature = "sql")]
mod value_traversal;
use super::{
    DbSession, DynamicMutation, DynamicStructuralPatch, DynamicTypedMutation, DynamicWriteCell,
    TypedEntityDescriptor, TypedFieldType,
};
use crate::{
    db::{
        TypedFieldDescriptor,
        data::{DataStore, encode_input_value_for_candidate_field_contract},
        index::IndexStore,
        registry::{StoreAllocationIdentities, StoreRegistry, StoreRuntimeStorageCapabilities},
        schema::{
            AcceptedCheckLiteralV1, AcceptedCompositeCatalog, AcceptedFieldDecodeContract,
            AcceptedFieldKind, AcceptedNamedTypeIdentity, AcceptedRuleOperation,
            AcceptedRuleTarget, AcceptedSchemaRevision, AcceptedSourceBindingCatalog,
            ConstraintOrigin, FieldId, FieldStorageDecode, FieldWriteManagement, LeafCodec,
            PersistedFieldSnapshot, PersistedNestedLeafSnapshot, PersistedSchemaSnapshot,
            ScalarCodec, SchemaFieldSlot, SchemaFieldWritePolicy, SchemaInsertDefault,
            SchemaRowLayout, SchemaStore, SchemaVersion,
            accepted_schema_candidate_with_catalogs_for_tests,
            build_record_newtype_composite_catalog_for_tests,
            empty_accepted_enum_catalog_for_tests, enum_catalog::ValueAdmissionBudget,
        },
    },
    error::InternalError,
    traits::{CanisterKind, Path},
    types::EntityTag,
    value::InputValue,
};
use icydb_schema::{
    ConstraintSourceKey, EntitySourceKey, FieldSourceKey, ScalarType, TypeSourceKey,
};
use std::{cell::RefCell, collections::BTreeMap};

const STORE_PATH: &str = "session::write::targeted_rule_mutation_tests::Store";
const ENTITY_SOURCE: &str = "session::write::targeted_rule_mutation_tests::Entity";
const ID_SOURCE: &str = "session::write::targeted_rule_mutation_tests::Entity::id";
const PROFILE_SOURCE: &str = "session::write::targeted_rule_mutation_tests::Entity::profile";
const UPDATED_AT_SOURCE: &str = "session::write::targeted_rule_mutation_tests::Entity::updated_at";
const PROFILE_TYPE_SOURCE: &str = "session::write::targeted_rule_mutation_tests::Profile";
const DEGREE_TYPE_SOURCE: &str = "session::write::targeted_rule_mutation_tests::Degree";
const DEGREE_MEMBER_SOURCE: &str = "session::write::targeted_rule_mutation_tests::Profile::degree";
const DEGREE_RULE_SOURCE: &str =
    "session::write::targeted_rule_mutation_tests::Profile::degree_multiple";
const TYPED_DESCRIPTOR: TypedEntityDescriptor = TypedEntityDescriptor::new(
    ENTITY_SOURCE,
    &[ID_SOURCE],
    &[
        TypedFieldDescriptor::new(ID_SOURCE, TypedFieldType::Scalar(ScalarType::Nat64), false),
        TypedFieldDescriptor::new(
            PROFILE_SOURCE,
            TypedFieldType::Named(PROFILE_TYPE_SOURCE),
            false,
        ),
        TypedFieldDescriptor::new(
            UPDATED_AT_SOURCE,
            TypedFieldType::Scalar(ScalarType::Timestamp),
            false,
        ),
    ],
);

struct TestCanister;

impl Path for TestCanister {
    const PATH: &'static str = "session::write::targeted_rule_mutation_tests::Canister";
}

impl CanisterKind for TestCanister {
    fn commit_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        Ok(43)
    }
    const COMMIT_STABLE_KEY: &'static str = "icydb.targeted_mutation_tests.commit.v1";
    fn startup_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        Ok(49)
    }
    const STARTUP_STABLE_KEY: &'static str = "icydb.targeted_mutation_tests.startup.control.v1";
    fn integrity_progress_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        Ok(44)
    }
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
        "icydb.targeted_mutation_tests.integrity.progress.v1";
}

thread_local! {
    static DATA_STORE: RefCell<DataStore> = const { RefCell::new(DataStore::init_heap()) };
    static INDEX_STORE: RefCell<IndexStore> = const { RefCell::new(IndexStore::init_heap()) };
    static SCHEMA_STORE: RefCell<SchemaStore> =
        const { RefCell::new(SchemaStore::init_heap()) };
    static STORE_REGISTRY: StoreRegistry = {
        let mut registry = StoreRegistry::new();
        registry.register_store(
            STORE_PATH,
            &DATA_STORE,
            &INDEX_STORE,
            &SCHEMA_STORE,
            StoreAllocationIdentities::absent(),
            StoreRuntimeStorageCapabilities::heap(),
        ).expect("targeted mutation test store should register");
        registry
    };
}

fn source<T, E: std::fmt::Debug>(raw: &str, parse: impl FnOnce(String) -> Result<T, E>) -> T {
    parse(raw.to_string()).expect("test source identity should admit")
}

fn profile_input(degree: u64) -> InputValue {
    InputValue::map(vec![(
        InputValue::from("degree"),
        InputValue::nat64(degree),
    )])
}

fn structural_patch(id: u64, degree: u64) -> DynamicStructuralPatch {
    DynamicStructuralPatch::new(vec![
        (
            "id".to_string(),
            DynamicWriteCell::Value(InputValue::nat64(id)),
        ),
        (
            "profile".to_string(),
            DynamicWriteCell::Value(profile_input(degree)),
        ),
    ])
}

fn encoded_value(
    enum_catalog: &crate::db::schema::AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
    name: &str,
    kind: &AcceptedFieldKind,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
    value: InputValue,
) -> Vec<u8> {
    let field = AcceptedFieldDecodeContract::new(name, kind, false, storage_decode, leaf_codec);
    encode_input_value_for_candidate_field_contract(
        enum_catalog,
        composite_catalog,
        field,
        value,
        &mut ValueAdmissionBudget::standard(),
    )
    .expect("test accepted value should encode")
}

fn nat64_literal(
    enum_catalog: &crate::db::schema::AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
    value: u64,
) -> AcceptedCheckLiteralV1 {
    let kind = AcceptedFieldKind::Nat64;
    AcceptedCheckLiteralV1::from_accepted_parts(
        kind.clone(),
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Nat64),
        encoded_value(
            enum_catalog,
            composite_catalog,
            "degree_bound",
            &kind,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
            InputValue::nat64(value),
        ),
    )
}

fn targeted_constraint_id(error: &InternalError) -> u32 {
    let facts = error.diagnostic_facts();
    assert!(facts.contains(&(
        icydb_diagnostic_code::DiagnosticFactTag::MutationOperation,
        icydb_diagnostic_code::DiagnosticMutationOperation::Insert.raw(),
    )));
    assert!(facts.contains(&(icydb_diagnostic_code::DiagnosticFactTag::BatchPosition, 0,)));
    assert!(facts.contains(&(
        icydb_diagnostic_code::DiagnosticFactTag::ConstraintKind,
        icydb_diagnostic_code::DiagnosticConstraintKind::TargetedRule.raw(),
    )));
    assert_eq!(
        facts
            .iter()
            .filter(|(tag, _)| matches!(
                tag,
                icydb_diagnostic_code::DiagnosticFactTag::RootField
                    | icydb_diagnostic_code::DiagnosticFactTag::RecordMember
            ))
            .copied()
            .collect::<Vec<_>>(),
        vec![
            (icydb_diagnostic_code::DiagnosticFactTag::RootField, 2),
            (
                icydb_diagnostic_code::DiagnosticFactTag::RecordMember,
                icydb_diagnostic_code::pack_u32_pair(1, 1),
            ),
        ]
    );
    let value = facts
        .iter()
        .find_map(|(tag, value)| {
            (*tag == icydb_diagnostic_code::DiagnosticFactTag::ConstraintId).then_some(*value)
        })
        .expect("targeted mutation should retain its accepted constraint ID");
    u32::try_from(value).expect("accepted constraint ID fits u32")
}

#[expect(
    clippy::too_many_lines,
    reason = "one end-to-end fixture proves every maintained write frontend converges on the same accepted targeted-rule schedule"
)]
#[test]
fn targeted_rules_converge_across_dynamic_typed_sql_default_timestamp_and_batch_writes() {
    DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
    INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());

    let entity_tag = EntityTag::new(93);
    let enum_catalog = empty_accepted_enum_catalog_for_tests();
    let (composite_catalog, profile_type, degree_type, degree_member) =
        build_record_newtype_composite_catalog_for_tests(
            "tests::TargetedProfile".to_string(),
            "degree".to_string(),
            "tests::TargetedDegree".to_string(),
            AcceptedFieldKind::Nat64,
            &enum_catalog,
        )
        .expect("targeted mutation composites should close");
    let profile_kind = AcceptedFieldKind::Composite {
        type_id: profile_type,
    };
    let profile_default = encoded_value(
        &enum_catalog,
        &composite_catalog,
        "profile",
        &profile_kind,
        FieldStorageDecode::CatalogValue,
        LeafCodec::Structural,
        profile_input(12),
    );
    let fields = vec![
        PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "id".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Nat64,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        ),
        PersistedFieldSnapshot::new_initial(
            FieldId::new(2),
            "profile".to_string(),
            SchemaFieldSlot::new(1),
            profile_kind,
            vec![PersistedNestedLeafSnapshot::new(
                vec!["degree".to_string()],
                AcceptedFieldKind::Composite {
                    type_id: degree_type,
                },
                false,
            )],
            false,
            SchemaInsertDefault::SlotPayload(profile_default),
            FieldStorageDecode::CatalogValue,
            LeafCodec::Structural,
        ),
        PersistedFieldSnapshot::new_initial_with_write_policy(
            FieldId::new(3),
            "updated_at".to_string(),
            SchemaFieldSlot::new(2),
            AcceptedFieldKind::Timestamp,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            SchemaFieldWritePolicy::from_model_policies(
                None,
                Some(FieldWriteManagement::UpdatedAt),
            ),
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Timestamp),
        ),
    ];
    let mut snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        ENTITY_SOURCE.to_string(),
        "TargetedMutation".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    );
    let constraint_catalog = snapshot
        .constraint_catalog()
        .clone()
        .with_added_targeted_rule(
            "profile_degree_multiple".to_string(),
            ConstraintOrigin::Generated,
            AcceptedRuleTarget::new(
                FieldId::new(2),
                AcceptedNamedTypeIdentity::Composite(degree_type),
            ),
            AcceptedRuleOperation::MultipleOf {
                divisor: nat64_literal(&enum_catalog, &composite_catalog, 5),
            },
        )
        .expect("targeted mutation rule should allocate");
    let targeted_rule_id = constraint_catalog
        .constraints()
        .last()
        .expect("targeted mutation rule should persist")
        .id();
    snapshot = snapshot.with_constraint_catalog(constraint_catalog);

    let entity_source = source(ENTITY_SOURCE, EntitySourceKey::try_new);
    let id_source = source(ID_SOURCE, FieldSourceKey::try_new);
    let profile_source = source(PROFILE_SOURCE, FieldSourceKey::try_new);
    let updated_at_source = source(UPDATED_AT_SOURCE, FieldSourceKey::try_new);
    let profile_type_source = source(PROFILE_TYPE_SOURCE, TypeSourceKey::try_new);
    let degree_type_source = source(DEGREE_TYPE_SOURCE, TypeSourceKey::try_new);
    let degree_member_source = source(DEGREE_MEMBER_SOURCE, FieldSourceKey::try_new);
    let degree_rule_source = source(DEGREE_RULE_SOURCE, ConstraintSourceKey::try_new);
    let source_bindings = AcceptedSourceBindingCatalog::initial_for_tests(
        BTreeMap::from([(entity_source, entity_tag)]),
        BTreeMap::from([
            ((entity_tag, id_source), FieldId::new(1)),
            ((entity_tag, profile_source), FieldId::new(2)),
            ((entity_tag, updated_at_source), FieldId::new(3)),
        ]),
        BTreeMap::from([((entity_tag, degree_rule_source), targeted_rule_id)]),
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .with_initial_named_types_for_tests(
        BTreeMap::from([
            (
                profile_type_source,
                AcceptedNamedTypeIdentity::Composite(profile_type),
            ),
            (
                degree_type_source,
                AcceptedNamedTypeIdentity::Composite(degree_type),
            ),
        ]),
        BTreeMap::new(),
        BTreeMap::from([((profile_type, degree_member_source), degree_member)]),
    );
    let candidate = accepted_schema_candidate_with_catalogs_for_tests(
        STORE_PATH,
        AcceptedSchemaRevision::INITIAL,
        enum_catalog,
        composite_catalog,
        source_bindings,
        BTreeMap::from([(entity_tag, snapshot)]),
    );

    let session = DbSession::<TestCanister>::new(
        &STORE_REGISTRY,
        &crate::db::RequestExecutionRoot::__new_runtime_root(),
    );
    session
        .db
        .drive_startup_recovery_page()
        .expect("targeted mutation test database should initialize");
    let store = session
        .db
        .store_handle(STORE_PATH)
        .expect("targeted mutation test store should resolve");
    crate::db::commit::publish_accepted_schema_candidate(
        STORE_PATH,
        store,
        AcceptedSchemaRevision::NONE,
        &candidate,
    )
    .expect("targeted mutation candidate should publish");

    let dynamic_error = session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
            entity: "TargetedMutation".to_string(),
            patch: structural_patch(1, 12),
        })
        .expect_err("dynamic write must enforce the targeted rule");
    assert_eq!(
        targeted_constraint_id(&dynamic_error),
        targeted_rule_id.get()
    );

    let binding = session
        .issue_typed_entity_binding(&TYPED_DESCRIPTOR)
        .expect("targeted typed binding should issue");
    let typed_patch = binding
        .bind_write_ordinals(vec![
            (0, DynamicWriteCell::Value(InputValue::nat64(2))),
            (1, DynamicWriteCell::Value(profile_input(12))),
        ])
        .expect("targeted typed patch should bind");
    let typed_error = session
        .execute_trusted_typed_mutation(
            &binding,
            DynamicTypedMutation::Insert { patch: typed_patch },
        )
        .expect_err("typed write must enforce the targeted rule");
    assert_eq!(targeted_constraint_id(&typed_error), targeted_rule_id.get());

    #[cfg(feature = "sql")]
    {
        let sql_error = session
            .execute_trusted_sql_mutation("INSERT INTO TargetedMutation (id) VALUES (3)")
            .expect_err("SQL default resolution must enforce the targeted rule");
        let crate::db::QueryError::Execute(execute) = sql_error else {
            panic!("targeted SQL write should fail at shared execution admission");
        };
        assert_eq!(
            targeted_constraint_id(execute.as_internal()),
            targeted_rule_id.get()
        );
    }

    session
        .execute_trusted_dynamic_mutation_batch(vec![
            DynamicMutation::Insert {
                entity: "TargetedMutation".to_string(),
                patch: structural_patch(4, 5),
            },
            DynamicMutation::Insert {
                entity: "TargetedMutation".to_string(),
                patch: structural_patch(5, 12),
            },
        ])
        .expect_err("one invalid targeted value must reject the whole batch");
    assert_eq!(
        DATA_STORE.with(|store| store.borrow().exact_entity_count(entity_tag)),
        Some(0),
        "no frontend or earlier valid batch row may escape targeted admission",
    );

    let admitted = session
        .execute_trusted_dynamic_mutation_batch(vec![
            DynamicMutation::Insert {
                entity: "TargetedMutation".to_string(),
                patch: structural_patch(6, 5),
            },
            DynamicMutation::Insert {
                entity: "TargetedMutation".to_string(),
                patch: structural_patch(7, 10),
            },
        ])
        .expect("compliant targeted values should share one accepted batch");
    let admitted_rows = admitted
        .iter()
        .flat_map(|result| result.rows.iter())
        .collect::<Vec<_>>();
    let [first, second] = admitted_rows.as_slice() else {
        panic!("the mixed targeted batch should return two rows");
    };
    let first_timestamp = first
        .get(2)
        .expect("the first mixed row should contain its managed timestamp");
    assert!(matches!(
        first_timestamp.as_public(),
        crate::value::PublicValue::Timestamp(_)
    ));
    assert_eq!(
        second.get(2),
        Some(first_timestamp),
        "one accepted mixed batch must materialize one managed timestamp",
    );
    assert_eq!(
        DATA_STORE.with(|store| store.borrow().exact_entity_count(entity_tag)),
        Some(2),
    );
}
