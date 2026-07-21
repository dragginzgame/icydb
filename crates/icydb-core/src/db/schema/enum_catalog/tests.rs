use super::*;
use super::{admission::*, equality_key::*};
#[cfg(feature = "sql")]
use crate::db::schema::{AcceptedFieldKind, sql_capabilities_with_enum_catalog};
use crate::{
    db::schema::{AcceptedFieldDecodeContract, AcceptedFieldPersistenceContract},
    model::{
        entity::EntityModel,
        field::{
            CompositeCodec, CompositeElementModel, CompositeFieldModel, CompositeShapeModel,
            EnumVariantModel, FieldModel, LeafCodec,
        },
        index::IndexModel,
    },
    testing::entity_model_from_static,
    value::{
        CanonicalEnumBody, CanonicalEnumValue, InputValue, InputValueEnum, OutputValue, Value,
        ValueTag,
    },
};

const UNIT_DECODE: FieldStorageDecode = FieldStorageDecode::ByKind;

fn accepted_field_contract<'a>(
    catalog: &'a AcceptedValueCatalogHandle,
    kind: &'a AcceptedFieldKind,
    nullable: bool,
    storage_decode: FieldStorageDecode,
) -> AcceptedFieldPersistenceContract<'a> {
    let field = AcceptedFieldDecodeContract::new(
        "value",
        kind,
        nullable,
        storage_decode,
        LeafCodec::Structural,
    );
    AcceptedFieldPersistenceContract::new_for_tests(catalog, field)
        .expect("accepted test field should match its catalog")
}

static ALPHA_VARIANTS: [EnumVariantModel; 2] = [
    EnumVariantModel::new("Zulu", None, UNIT_DECODE),
    EnumVariantModel::new("Alpha", None, UNIT_DECODE),
];
static ALPHA_KIND: FieldKind = FieldKind::Enum {
    path: "catalog::Alpha",
    variants: &ALPHA_VARIANTS,
};
static ZETA_VARIANTS: [EnumVariantModel; 1] = [EnumVariantModel::new("Ready", None, UNIT_DECODE)];
static ZETA_KIND: FieldKind = FieldKind::Enum {
    path: "catalog::Zeta",
    variants: &ZETA_VARIANTS,
};
static ALPHA_REORDERED_VARIANTS: [EnumVariantModel; 2] = [
    EnumVariantModel::new("Alpha", None, UNIT_DECODE),
    EnumVariantModel::new("Zulu", None, UNIT_DECODE),
];
static ALPHA_REORDERED_KIND: FieldKind = FieldKind::Enum {
    path: "catalog::Alpha",
    variants: &ALPHA_REORDERED_VARIANTS,
};
static ALPHA_ADDED_VARIANTS: [EnumVariantModel; 3] = [
    EnumVariantModel::new("Beta", None, UNIT_DECODE),
    EnumVariantModel::new("Zulu", None, UNIT_DECODE),
    EnumVariantModel::new("Alpha", None, UNIT_DECODE),
];
static ALPHA_ADDED_KIND: FieldKind = FieldKind::Enum {
    path: "catalog::Alpha",
    variants: &ALPHA_ADDED_VARIANTS,
};
static ALPHA_TAIL_ADDED_VARIANTS: [EnumVariantModel; 3] = [
    EnumVariantModel::new("Alpha", None, UNIT_DECODE),
    EnumVariantModel::new("Zulu", None, UNIT_DECODE),
    EnumVariantModel::new("Zzz", None, UNIT_DECODE),
];
static ALPHA_TAIL_ADDED_KIND: FieldKind = FieldKind::Enum {
    path: "catalog::Alpha",
    variants: &ALPHA_TAIL_ADDED_VARIANTS,
};
static ALPHA_REMOVED_VARIANTS: [EnumVariantModel; 1] =
    [EnumVariantModel::new("Alpha", None, UNIT_DECODE)];
static ALPHA_REMOVED_KIND: FieldKind = FieldKind::Enum {
    path: "catalog::Alpha",
    variants: &ALPHA_REMOVED_VARIANTS,
};
static AARDVARK_VARIANTS: [EnumVariantModel; 1] =
    [EnumVariantModel::new("Only", None, UNIT_DECODE)];
static AARDVARK_KIND: FieldKind = FieldKind::Enum {
    path: "catalog::Aardvark",
    variants: &AARDVARK_VARIANTS,
};
static DUPLICATE_VARIANTS: [EnumVariantModel; 2] = [
    EnumVariantModel::new("Same", None, UNIT_DECODE),
    EnumVariantModel::new("Same", None, UNIT_DECODE),
];
static DUPLICATE_KIND: FieldKind = FieldKind::Enum {
    path: "catalog::Duplicate",
    variants: &DUPLICATE_VARIANTS,
};
static PAYLOAD_KIND: FieldKind = FieldKind::Nat64;
static CONFLICTING_ALPHA_VARIANTS: [EnumVariantModel; 2] = [
    EnumVariantModel::new("Alpha", Some(&PAYLOAD_KIND), FieldStorageDecode::ByKind),
    EnumVariantModel::new("Zulu", None, UNIT_DECODE),
];
static CONFLICTING_ALPHA_KIND: FieldKind = FieldKind::Enum {
    path: "catalog::Alpha",
    variants: &CONFLICTING_ALPHA_VARIANTS,
};
static PAYLOAD_BY_KIND_VARIANTS: [EnumVariantModel; 1] = [EnumVariantModel::new(
    "Value",
    Some(&PAYLOAD_KIND),
    FieldStorageDecode::ByKind,
)];
static PAYLOAD_BY_KIND: FieldKind = FieldKind::Enum {
    path: "catalog::PayloadDecode",
    variants: &PAYLOAD_BY_KIND_VARIANTS,
};
static PAYLOAD_VALUE_VARIANTS: [EnumVariantModel; 1] = [EnumVariantModel::new(
    "Value",
    Some(&PAYLOAD_KIND),
    FieldStorageDecode::CatalogValue,
)];
static PAYLOAD_VALUE: FieldKind = FieldKind::Enum {
    path: "catalog::PayloadDecode",
    variants: &PAYLOAD_VALUE_VARIANTS,
};
static PAYLOAD_REF_VARIANTS: [EnumVariantModel; 1] = [EnumVariantModel::new(
    "Nested",
    Some(&ALPHA_KIND),
    FieldStorageDecode::ByKind,
)];
static PAYLOAD_REF_KIND: FieldKind = FieldKind::Enum {
    path: "catalog::Container",
    variants: &PAYLOAD_REF_VARIANTS,
};
static SAME_LABEL_LEFT_VARIANTS: [EnumVariantModel; 1] =
    [EnumVariantModel::new("Shared", None, UNIT_DECODE)];
static SAME_LABEL_LEFT_KIND: FieldKind = FieldKind::Enum {
    path: "catalog::Left",
    variants: &SAME_LABEL_LEFT_VARIANTS,
};
static SAME_LABEL_RIGHT_VARIANTS: [EnumVariantModel; 1] =
    [EnumVariantModel::new("Shared", None, UNIT_DECODE)];
static SAME_LABEL_RIGHT_KIND: FieldKind = FieldKind::Enum {
    path: "catalog::Right",
    variants: &SAME_LABEL_RIGHT_VARIANTS,
};
static RECURSIVE_VARIANTS: [EnumVariantModel; 1] = [EnumVariantModel::new(
    "Next",
    Some(&RECURSIVE_KIND),
    FieldStorageDecode::ByKind,
)];
static RECURSIVE_KIND: FieldKind = FieldKind::Enum {
    path: "catalog::Recursive",
    variants: &RECURSIVE_VARIANTS,
};
static MODEL_FIELDS: [FieldModel; 2] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated("status", ALPHA_KIND),
];
static MODEL_INDEXES: [&IndexModel; 0] = [];
static ENUM_MODEL: EntityModel = entity_model_from_static(
    "catalog::Entity",
    "Entity",
    &MODEL_FIELDS[0],
    0,
    &MODEL_FIELDS,
    &MODEL_INDEXES,
);
static PROFILE_FIELDS: [CompositeFieldModel; 2] = [
    CompositeFieldModel::generated("name", FieldKind::Text { max_len: Some(16) }, false),
    CompositeFieldModel::generated("score", FieldKind::Nat64, true),
];
static PROFILE_SHAPE: CompositeShapeModel = CompositeShapeModel::Record(&PROFILE_FIELDS);
static PROFILE_KIND: FieldKind = FieldKind::Composite {
    path: "catalog::Profile",
    codec: CompositeCodec::StructuralV1,
    shape: &PROFILE_SHAPE,
};
static PAIR_ELEMENTS: [CompositeElementModel; 2] = [
    CompositeElementModel::generated(FieldKind::Nat64, false),
    CompositeElementModel::generated(FieldKind::Bool, false),
];
static PAIR_SHAPE: CompositeShapeModel = CompositeShapeModel::Tuple(&PAIR_ELEMENTS);
static PAIR_KIND: FieldKind = FieldKind::Composite {
    path: "catalog::Pair",
    codec: CompositeCodec::StructuralV1,
    shape: &PAIR_SHAPE,
};
static IDENTIFIER_SHAPE: CompositeShapeModel =
    CompositeShapeModel::Newtype(CompositeElementModel::generated(FieldKind::Nat64, false));
static IDENTIFIER_KIND: FieldKind = FieldKind::Composite {
    path: "catalog::Identifier",
    codec: CompositeCodec::StructuralV1,
    shape: &IDENTIFIER_SHAPE,
};
static NESTED_FIELDS: [CompositeFieldModel; 1] =
    [CompositeFieldModel::generated("pair", PAIR_KIND, false)];
static NESTED_SHAPE: CompositeShapeModel = CompositeShapeModel::Record(&NESTED_FIELDS);
static NESTED_KIND: FieldKind = FieldKind::Composite {
    path: "catalog::Nested",
    codec: CompositeCodec::StructuralV1,
    shape: &NESTED_SHAPE,
};
static EMPTY_RECORD_FIELDS: [CompositeFieldModel; 0] = [];
static EMPTY_RECORD_SHAPE: CompositeShapeModel = CompositeShapeModel::Record(&EMPTY_RECORD_FIELDS);
static EMPTY_RECORD_KIND: FieldKind = FieldKind::Composite {
    path: "catalog::EmptyRecord",
    codec: CompositeCodec::StructuralV1,
    shape: &EMPTY_RECORD_SHAPE,
};
static EMPTY_TUPLE_ELEMENTS: [CompositeElementModel; 0] = [];
static EMPTY_TUPLE_SHAPE: CompositeShapeModel = CompositeShapeModel::Tuple(&EMPTY_TUPLE_ELEMENTS);
static EMPTY_TUPLE_KIND: FieldKind = FieldKind::Composite {
    path: "catalog::EmptyTuple",
    codec: CompositeCodec::StructuralV1,
    shape: &EMPTY_TUPLE_SHAPE,
};
static BENCH_RECORD_FIELDS: [CompositeFieldModel; 3] = [
    CompositeFieldModel::generated("alpha", FieldKind::Nat64, false),
    CompositeFieldModel::generated("beta", FieldKind::Nat64, false),
    CompositeFieldModel::generated("gamma", FieldKind::Nat64, false),
];
static BENCH_RECORD_SHAPE: CompositeShapeModel = CompositeShapeModel::Record(&BENCH_RECORD_FIELDS);
static BENCH_RECORD_KIND: FieldKind = FieldKind::Composite {
    path: "catalog::BenchRecord",
    codec: CompositeCodec::StructuralV1,
    shape: &BENCH_RECORD_SHAPE,
};
#[test]
fn enum_ids_are_non_zero_by_construction() {
    assert_eq!(EnumTypeId::new(0), None);
    assert_eq!(EnumVariantId::new(0), None);
    assert_eq!(EnumTypeId::new(1).map(EnumTypeId::get), Some(1));
    assert_eq!(EnumVariantId::new(1).map(EnumVariantId::get), Some(1));
}

#[test]
fn store_catalog_builder_collects_enum_fields_from_entity_models() {
    let catalog = build_initial_accepted_enum_catalog(&[&ENUM_MODEL])
        .expect("model enum catalog should build");

    assert_eq!(catalog.len(), 1);
    assert_eq!(
        catalog.type_id("catalog::Alpha").map(EnumTypeId::get),
        Some(1),
    );
}

fn enum_contract(type_id: EnumTypeId) -> AcceptedValueContract {
    AcceptedValueContract {
        kind: AcceptedFieldKind::Enum { type_id },
        storage_decode: FieldStorageDecode::ByKind,
    }
}

fn accepted_catalog_handle(
    catalog: &AcceptedEnumCatalog,
    revision: AcceptedSchemaRevision,
) -> AcceptedValueCatalogHandle {
    AcceptedValueCatalogHandle::new_for_tests(
        catalog.clone(),
        AcceptedCompositeCatalog::empty(),
        revision,
    )
}

fn admitted_unit_equality_key(
    catalog: &AcceptedEnumCatalog,
    enum_path: &str,
    variant_name: &str,
) -> [u8; 11] {
    let type_id = catalog.type_id(enum_path).expect("enum type should exist");
    let contract = enum_contract(type_id);
    let handle = accepted_catalog_handle(catalog, AcceptedSchemaRevision::INITIAL);
    let mut admission_budget = ValueAdmissionBudget::standard();
    let admitted = normalize_and_admit_value(
        &handle,
        &contract,
        InputValue::Enum(InputValueEnum::new(variant_name, Some(enum_path))),
        &mut admission_budget,
    )
    .expect("unit enum should admit through the accepted catalog");
    let mut key_budget = ValueAdmissionBudget::standard();
    let proof = validate_canonical_value(&handle, &contract, admitted.value(), &mut key_budget)
        .expect("admitted unit enum should prove its equality-key contract");

    encode_unit_enum_equality_key(&proof).expect("unit enum should have a stable equality key")
}

#[test]
fn accepted_value_admission_resolves_loose_unit_enum_to_catalog_ids() {
    let catalog = build_initial_accepted_enum_catalog_from_kinds(&[ALPHA_KIND])
        .expect("catalog should build");
    let type_id = catalog
        .type_id("catalog::Alpha")
        .expect("alpha type ID should exist");
    let variant_id = catalog
        .enum_type(type_id)
        .and_then(|definition| definition.variant_id("Alpha"))
        .expect("alpha variant ID should exist");
    let contract = enum_contract(type_id);
    let catalog_handle = accepted_catalog_handle(&catalog, AcceptedSchemaRevision::INITIAL);
    let mut budget = ValueAdmissionBudget::standard();

    let admitted = normalize_and_admit_value(
        &catalog_handle,
        &contract,
        InputValue::Enum(InputValueEnum::loose("Alpha")),
        &mut budget,
    )
    .expect("loose enum should resolve through its expected accepted contract");
    let CanonicalValue::Enum(value) = admitted.value() else {
        panic!("admitted enum input should produce one canonical enum value");
    };

    assert_eq!(admitted.revision(), AcceptedSchemaRevision::INITIAL);
    assert_eq!(value.type_id(), type_id);
    assert_eq!(value.variant_id(), variant_id);
    assert_eq!(value.body(), &CanonicalEnumBody::Unit);
    let selection = catalog
        .resolve_value(value.canonical())
        .expect("canonical enum IDs should resolve through the accepted catalog");
    assert_eq!(selection.type_id(), type_id);
    assert_eq!(selection.variant_id(), variant_id);
    assert_eq!(selection.path(), "catalog::Alpha");
    assert_eq!(selection.variant_name(), "Alpha");
    assert_eq!(selection.value_body(), &CanonicalEnumBody::Unit);

    let mut strict_budget = ValueAdmissionBudget::standard();
    let proof = validate_canonical_value(
        &catalog_handle,
        &contract,
        admitted.value(),
        &mut strict_budget,
    )
    .expect("freshly admitted enum should pass strict validation");
    assert_eq!(proof.revision(), AcceptedSchemaRevision::INITIAL);
    assert!(proof.authority().matches(admitted.authority()));
    assert_eq!(proof.catalog(), &catalog);
    assert_eq!(proof.contract(), &contract);
    assert_eq!(proof.value(), admitted.value());
    let OutputValue::Enum(output) = output_value_from_runtime(proof.catalog(), proof.value())
        .expect("accepted enum output should resolve")
    else {
        panic!("accepted enum should materialize as enum output");
    };
    assert_eq!(output.path(), Some("catalog::Alpha"));
    assert_eq!(output.variant(), "Alpha");
    assert_eq!(output.payload(), None);
    #[cfg(feature = "sql")]
    {
        let capabilities = sql_capabilities_with_enum_catalog(
            &AcceptedFieldKind::from_model_kind(ALPHA_KIND),
            &catalog,
        );
        assert_eq!(
            capabilities.enum_equality(),
            Some(EqualityCapability::CanonicalStableKey),
        );
        assert!(!capabilities.groupable());
    }
    assert_eq!(
        enum_equality_capability(&catalog, type_id),
        Ok(EqualityCapability::CanonicalStableKey),
    );
    assert_eq!(
        encode_unit_enum_equality_key(&proof),
        Ok([ValueTag::Enum.to_u8(), 1, 0, 0, 0, 1, 0, 0, 0, 1, 0]),
    );
}

#[test]
fn persisted_field_admission_resolves_enum_contract_and_nullable_null() {
    let catalog = build_initial_accepted_enum_catalog_from_kinds(&[ALPHA_KIND])
        .expect("catalog should build");
    let catalog_handle = accepted_catalog_handle(&catalog, AcceptedSchemaRevision::INITIAL);
    let enum_kind = AcceptedFieldKind::from_model_kind(ALPHA_KIND);
    let enum_contract = accepted_field_contract(
        &catalog_handle,
        &enum_kind,
        false,
        FieldStorageDecode::ByKind,
    );
    let mut enum_budget = ValueAdmissionBudget::standard();
    let admitted = enum_contract
        .admission_contract()
        .normalize_and_admit(
            InputValue::Enum(InputValueEnum::loose("Alpha")),
            &mut enum_budget,
        )
        .expect("persisted enum field should resolve through catalog authority");
    let CanonicalValue::Enum(value) = admitted.value() else {
        panic!("persisted enum field should admit to canonical IDs");
    };
    assert_eq!(
        value.type_id(),
        catalog
            .type_id("catalog::Alpha")
            .expect("alpha type ID should exist"),
    );

    let text_kind = AcceptedFieldKind::Text { max_len: Some(8) };
    let nullable_contract = accepted_field_contract(
        &catalog_handle,
        &text_kind,
        true,
        FieldStorageDecode::ByKind,
    );
    let mut nullable_budget = ValueAdmissionBudget::standard();
    nullable_contract
        .admission_contract()
        .normalize_and_admit(InputValue::Null, &mut nullable_budget)
        .expect("nullable accepted field should admit null");

    let required_contract = accepted_field_contract(
        &catalog_handle,
        &text_kind,
        false,
        FieldStorageDecode::ByKind,
    );
    let mut required_budget = ValueAdmissionBudget::standard();
    assert_eq!(
        required_contract
            .admission_contract()
            .normalize_and_admit(InputValue::Null, &mut required_budget),
        Err(ValueAdmissionError::TypeMismatch),
    );

    let invalid_kind = AcceptedFieldKind::Enum {
        type_id: EnumTypeId::new(999).expect("test enum type ID should be valid"),
    };
    let invalid_field = AcceptedFieldDecodeContract::new(
        "invalid",
        &invalid_kind,
        false,
        FieldStorageDecode::ByKind,
        LeafCodec::Structural,
    );
    assert!(
        AcceptedFieldPersistenceContract::new_for_tests(&catalog_handle, invalid_field).is_err(),
        "unknown enum IDs must not produce accepted field authority",
    );
}

#[test]
fn derived_collection_element_admission_retains_catalog_authority() {
    let catalog = build_initial_accepted_enum_catalog_from_kinds(&[ALPHA_KIND])
        .expect("catalog should build");
    let catalog_handle = accepted_catalog_handle(&catalog, AcceptedSchemaRevision::INITIAL);
    let type_id = catalog
        .type_id("catalog::Alpha")
        .expect("alpha type ID should exist");
    let list_kind = AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Enum { type_id }));
    let field_contract = accepted_field_contract(
        &catalog_handle,
        &list_kind,
        false,
        FieldStorageDecode::ByKind,
    );
    let element_contract = field_contract
        .admission_contract()
        .collection_element_contract()
        .expect("list fields should derive an element admission contract");

    assert!(std::ptr::eq(
        element_contract.catalogs().enum_catalog(),
        catalog_handle.enum_catalog(),
    ));
    assert_eq!(
        element_contract.kind(),
        &AcceptedFieldKind::Enum { type_id },
    );
    let admitted = element_contract
        .normalize_and_admit(
            InputValue::Enum(InputValueEnum::loose("Alpha")),
            &mut ValueAdmissionBudget::standard(),
        )
        .expect("derived enum element should normalize under the parent catalog");
    assert!(matches!(admitted.value(), CanonicalValue::Enum(_)));
}

#[test]
fn admitted_owned_value_requires_exact_store_revision_and_fingerprint_authority() {
    let catalog = build_initial_accepted_enum_catalog_from_kinds(&[ALPHA_KIND])
        .expect("catalog should build");
    let type_id = catalog
        .type_id("catalog::Alpha")
        .expect("alpha type ID should exist");
    let store_scope = AcceptedStoreCatalogScope::new();
    let authority = AcceptedValueCatalogHandle::new(
        catalog.clone(),
        AcceptedCompositeCatalog::empty(),
        store_scope.clone(),
        AcceptedSchemaRevision::INITIAL,
        AcceptedSchemaFingerprint::new([0x11; 32]),
    );
    let other_store = AcceptedValueCatalogHandle::new(
        catalog.clone(),
        AcceptedCompositeCatalog::empty(),
        AcceptedStoreCatalogScope::new(),
        AcceptedSchemaRevision::INITIAL,
        AcceptedSchemaFingerprint::new([0x11; 32]),
    );
    let other_revision = AcceptedValueCatalogHandle::new(
        catalog.clone(),
        AcceptedCompositeCatalog::empty(),
        store_scope.clone(),
        AcceptedSchemaRevision::new(2),
        AcceptedSchemaFingerprint::new([0x11; 32]),
    );
    let other_fingerprint = AcceptedValueCatalogHandle::new(
        catalog,
        AcceptedCompositeCatalog::empty(),
        store_scope,
        AcceptedSchemaRevision::INITIAL,
        AcceptedSchemaFingerprint::new([0x22; 32]),
    );
    let mut budget = ValueAdmissionBudget::standard();
    let admitted = normalize_and_admit_value(
        &authority,
        &enum_contract(type_id),
        InputValue::Enum(InputValueEnum::loose("Alpha")),
        &mut budget,
    )
    .expect("enum should admit under exact authority");

    assert!(admitted.authority().matches(authority.authority()));
    assert_eq!(
        admitted.authority().fingerprint(),
        AcceptedSchemaFingerprint::new([0x11; 32]),
    );
    assert!(!admitted.authority().matches(other_store.authority()));
    assert!(!admitted.authority().matches(other_revision.authority()));
    assert!(!admitted.authority().matches(other_fingerprint.authority()));
}

#[test]
fn catalog_value_resolution_rejects_unknown_type_and_variant_ids() {
    let catalog = build_initial_accepted_enum_catalog_from_kinds(&[ALPHA_KIND])
        .expect("catalog should build");
    let type_id = catalog
        .type_id("catalog::Alpha")
        .expect("alpha type ID should exist");
    let unknown_type = EnumTypeId::new(99).expect("test type ID should be non-zero");
    let unknown_variant = EnumVariantId::new(99).expect("test variant ID should be non-zero");
    let known_variant = catalog
        .enum_type(type_id)
        .and_then(|definition| definition.variant_id("Alpha"))
        .expect("alpha variant ID should exist");

    let missing_type = CanonicalEnumValue::<CanonicalValue>::new(
        unknown_type,
        known_variant,
        CanonicalEnumBody::Unit,
    );
    assert_eq!(
        catalog.resolve_value(&missing_type).err(),
        Some(EnumValueResolutionError::UnknownType),
    );

    let missing_variant = CanonicalEnumValue::<CanonicalValue>::new(
        type_id,
        unknown_variant,
        CanonicalEnumBody::Unit,
    );
    assert_eq!(
        catalog.resolve_value(&missing_variant).err(),
        Some(EnumValueResolutionError::UnknownVariant),
    );
}

#[test]
fn accepted_value_admission_recursively_resolves_payload_enum_ids() {
    let catalog = build_initial_accepted_enum_catalog_from_kinds(&[ALPHA_KIND, PAYLOAD_REF_KIND])
        .expect("nested enum catalog should build");
    let container_id = catalog
        .type_id("catalog::Container")
        .expect("container type ID should exist");
    let alpha_id = catalog
        .type_id("catalog::Alpha")
        .expect("alpha type ID should exist");
    let contract = enum_contract(container_id);
    let catalog_handle = accepted_catalog_handle(&catalog, AcceptedSchemaRevision::INITIAL);
    let input = InputValue::Enum(
        InputValueEnum::new("Nested", Some("catalog::Container"))
            .with_payload(InputValue::Enum(InputValueEnum::loose("Alpha"))),
    );
    let mut budget = ValueAdmissionBudget::standard();

    let admitted = normalize_and_admit_value(&catalog_handle, &contract, input, &mut budget)
        .expect("payload enum should recursively resolve through its accepted contract");
    let CanonicalValue::Enum(outer) = admitted.value() else {
        panic!("outer value should be canonical enum");
    };
    let CanonicalEnumBody::Payload(payload) = outer.body() else {
        panic!("outer enum should carry its canonical payload");
    };
    let CanonicalValue::Enum(inner) = payload.as_ref() else {
        panic!("nested payload should be canonical enum");
    };

    assert_eq!(outer.type_id(), container_id);
    assert_eq!(inner.type_id(), alpha_id);
    assert_eq!(inner.body(), &CanonicalEnumBody::Unit);

    let mut strict_budget = ValueAdmissionBudget::standard();
    validate_canonical_value(
        &catalog_handle,
        &contract,
        admitted.value(),
        &mut strict_budget,
    )
    .expect("recursive canonical payload should pass strict validation");

    assert_eq!(
        enum_equality_capability(&catalog, container_id),
        Ok(EqualityCapability::PairwiseOnly),
    );
    #[cfg(feature = "sql")]
    assert_eq!(
        sql_capabilities_with_enum_catalog(
            &AcceptedFieldKind::from_model_kind(PAYLOAD_REF_KIND),
            &catalog,
        )
        .enum_equality(),
        Some(EqualityCapability::PairwiseOnly),
    );
    let mut key_budget = ValueAdmissionBudget::standard();
    let proof = validate_canonical_value(
        &catalog_handle,
        &contract,
        admitted.value(),
        &mut key_budget,
    )
    .expect("payload enum remains valid for pairwise equality");
    assert_eq!(
        encode_unit_enum_equality_key(&proof),
        Err(EnumEqualityKeyError::PayloadEnumUnsupported),
    );
    let OutputValue::Enum(output) = output_value_from_runtime(proof.catalog(), proof.value())
        .expect("accepted payload enum output should resolve")
    else {
        panic!("accepted payload enum should materialize as enum output");
    };
    assert_eq!(output.path(), Some("catalog::Container"));
    assert_eq!(output.variant(), "Nested");
    let Some(OutputValue::Enum(payload)) = output.payload() else {
        panic!("accepted payload enum should retain nested enum output");
    };
    assert_eq!(payload.path(), Some("catalog::Alpha"));
    assert_eq!(payload.variant(), "Alpha");
    assert_eq!(payload.payload(), None);
}

#[test]
fn accepted_value_admission_rejects_path_variant_and_body_mismatches() {
    let catalog = build_initial_accepted_enum_catalog_from_kinds(&[ALPHA_KIND, ZETA_KIND])
        .expect("catalog should build");
    let alpha_id = catalog
        .type_id("catalog::Alpha")
        .expect("alpha type ID should exist");
    let zeta_id = catalog
        .type_id("catalog::Zeta")
        .expect("zeta type ID should exist");
    let contract = enum_contract(alpha_id);
    let catalog_handle = accepted_catalog_handle(&catalog, AcceptedSchemaRevision::INITIAL);

    let cases = [
        (
            InputValueEnum::new("Alpha", Some("catalog::Zeta")),
            ValueAdmissionError::EnumPathMismatch,
        ),
        (
            InputValueEnum::loose("Missing"),
            ValueAdmissionError::UnknownEnumVariant,
        ),
        (
            InputValueEnum::loose("Alpha").with_payload(InputValue::Nat64(1)),
            ValueAdmissionError::EnumBodyMismatch,
        ),
    ];
    for (input, expected) in cases {
        let mut budget = ValueAdmissionBudget::standard();
        assert_eq!(
            normalize_and_admit_value(
                &catalog_handle,
                &contract,
                InputValue::Enum(input),
                &mut budget,
            ),
            Err(expected),
        );
    }

    let mut budget = ValueAdmissionBudget::standard();
    let admitted = normalize_and_admit_value(
        &catalog_handle,
        &contract,
        InputValue::Enum(InputValueEnum::loose("Alpha")),
        &mut budget,
    )
    .expect("alpha should admit under alpha contract");
    let mut strict_budget = ValueAdmissionBudget::standard();
    assert!(matches!(
        validate_canonical_value(
            &catalog_handle,
            &enum_contract(zeta_id),
            admitted.value(),
            &mut strict_budget,
        ),
        Err(ValueAdmissionError::EnumTypeMismatch)
    ));
}

#[test]
fn accepted_value_admission_enforces_depth_and_size_budgets() {
    let catalog =
        build_initial_accepted_enum_catalog_from_kinds(&[]).expect("empty catalog should build");
    let nested_list = AcceptedValueContract {
        kind: AcceptedFieldKind::List(Box::new(AcceptedFieldKind::List(Box::new(
            AcceptedFieldKind::Unit,
        )))),
        storage_decode: FieldStorageDecode::ByKind,
    };
    let bounded_text = AcceptedValueContract {
        kind: AcceptedFieldKind::Text { max_len: None },
        storage_decode: FieldStorageDecode::ByKind,
    };
    let catalog_handle = accepted_catalog_handle(&catalog, AcceptedSchemaRevision::INITIAL);
    let nested = InputValue::List(vec![InputValue::List(vec![InputValue::Unit])]);
    let mut shallow_budget = ValueAdmissionBudget::with_limits(2, 1024);
    assert_eq!(
        normalize_and_admit_value(&catalog_handle, &nested_list, nested, &mut shallow_budget,),
        Err(ValueAdmissionError::DepthExceeded),
    );

    let mut small_budget = ValueAdmissionBudget::with_limits(64, 8);
    assert_eq!(
        normalize_and_admit_value(
            &catalog_handle,
            &bounded_text,
            InputValue::Text("larger than budget".to_string()),
            &mut small_budget,
        ),
        Err(ValueAdmissionError::SizeExceeded),
    );
}

fn nested_list_contract_and_input(depth: usize) -> (AcceptedValueContract, InputValue) {
    let mut kind = AcceptedFieldKind::Unit;
    let mut input = InputValue::Unit;
    for _ in 0..depth {
        kind = AcceptedFieldKind::List(Box::new(kind));
        input = InputValue::List(vec![input]);
    }
    (
        AcceptedValueContract {
            kind,
            storage_decode: FieldStorageDecode::ByKind,
        },
        input,
    )
}

#[test]
fn accepted_value_admission_uses_the_shared_recursive_depth_boundary() {
    let catalog =
        build_initial_accepted_enum_catalog_from_kinds(&[]).expect("empty catalog should build");
    let catalog_handle = accepted_catalog_handle(&catalog, AcceptedSchemaRevision::INITIAL);
    let (allowed_contract, allowed_input) =
        nested_list_contract_and_input(MAX_ACCEPTED_RECURSIVE_DEPTH - 1);
    let (excessive_contract, excessive_input) =
        nested_list_contract_and_input(MAX_ACCEPTED_RECURSIVE_DEPTH);

    normalize_and_admit_value(
        &catalog_handle,
        &allowed_contract,
        allowed_input,
        &mut ValueAdmissionBudget::standard(),
    )
    .expect("the maximum accepted recursive depth should admit");
    assert_eq!(
        normalize_and_admit_value(
            &catalog_handle,
            &excessive_contract,
            excessive_input,
            &mut ValueAdmissionBudget::standard(),
        ),
        Err(ValueAdmissionError::DepthExceeded),
    );
}

#[test]
fn accepted_value_admission_canonicalizes_sets_and_maps_and_requires_revision() {
    let catalog =
        build_initial_accepted_enum_catalog_from_kinds(&[]).expect("empty catalog should build");
    let set_contract = AcceptedValueContract {
        kind: AcceptedFieldKind::Set(Box::new(AcceptedFieldKind::Text { max_len: Some(8) })),
        storage_decode: FieldStorageDecode::ByKind,
    };
    let catalog_handle = accepted_catalog_handle(&catalog, AcceptedSchemaRevision::INITIAL);
    let mut budget = ValueAdmissionBudget::standard();
    let set = normalize_and_admit_value(
        &catalog_handle,
        &set_contract,
        InputValue::List(vec![InputValue::from("beta"), InputValue::from("alpha")]),
        &mut budget,
    )
    .expect("set input should canonicalize");
    assert_eq!(
        set.value(),
        &CanonicalValue::List(vec![
            CanonicalValue::Text("alpha".to_string()),
            CanonicalValue::Text("beta".to_string()),
        ]),
    );
    let mut strict_budget = ValueAdmissionBudget::standard();
    validate_canonical_value(
        &catalog_handle,
        &set_contract,
        set.value(),
        &mut strict_budget,
    )
    .expect("canonical set should pass strict validation");

    let map_contract = AcceptedValueContract {
        kind: AcceptedFieldKind::Map {
            key: Box::new(AcceptedFieldKind::Text { max_len: None }),
            value: Box::new(AcceptedFieldKind::Nat64),
        },
        storage_decode: FieldStorageDecode::ByKind,
    };
    let mut map_budget = ValueAdmissionBudget::standard();
    let map = normalize_and_admit_value(
        &catalog_handle,
        &map_contract,
        InputValue::Map(vec![
            (InputValue::from("zeta"), InputValue::Nat64(2)),
            (InputValue::from("alpha"), InputValue::Nat64(1)),
        ]),
        &mut map_budget,
    )
    .expect("map input should canonicalize by key");
    assert_eq!(
        map.value(),
        &CanonicalValue::Map(vec![
            (
                CanonicalValue::Text("alpha".to_string()),
                CanonicalValue::Nat64(1),
            ),
            (
                CanonicalValue::Text("zeta".to_string()),
                CanonicalValue::Nat64(2),
            ),
        ]),
    );

    let mut missing_revision_budget = ValueAdmissionBudget::standard();
    let missing_revision_catalog = accepted_catalog_handle(&catalog, AcceptedSchemaRevision::NONE);
    assert_eq!(
        normalize_and_admit_value(
            &missing_revision_catalog,
            &set_contract,
            InputValue::List(Vec::new()),
            &mut missing_revision_budget,
        ),
        Err(ValueAdmissionError::MissingSchemaRevision),
    );
}

#[test]
fn initial_catalog_ids_use_canonical_path_and_name_order() {
    let first = build_initial_accepted_enum_catalog_from_kinds(&[ZETA_KIND, ALPHA_KIND])
        .expect("catalog should build");
    let second = build_initial_accepted_enum_catalog_from_kinds(&[ALPHA_KIND, ZETA_KIND])
        .expect("catalog should be independent of proposal order");

    assert_eq!(first, second);
    assert_eq!(
        first.type_id("catalog::Alpha").map(EnumTypeId::get),
        Some(1)
    );
    assert_eq!(first.type_id("catalog::Zeta").map(EnumTypeId::get), Some(2));
    let alpha = first
        .enum_type(first.type_id("catalog::Alpha").expect("alpha id"))
        .expect("alpha definition");
    assert_eq!(alpha.variant_id("Alpha").map(EnumVariantId::get), Some(1));
    assert_eq!(alpha.variant_id("Zulu").map(EnumVariantId::get), Some(2));
    assert_eq!(alpha.ordering(), EnumOrderingPolicy::EqualityOnly);
}

#[test]
fn declaration_reorder_merges_as_canonical_no_op() {
    let catalog =
        build_initial_accepted_enum_catalog_from_kinds(&[ALPHA_KIND, ALPHA_REORDERED_KIND])
            .expect("declaration order should not affect canonical equality");

    assert_eq!(catalog.len(), 1);
}

#[test]
fn native_catalog_reconcile_keeps_current_tail_additions_dense() {
    let accepted = build_initial_accepted_enum_catalog_from_kinds(&[ALPHA_KIND])
        .expect("initial catalog should build");
    let alpha_id = accepted.type_id("catalog::Alpha").expect("alpha type ID");
    let alpha = accepted.enum_type(alpha_id).expect("alpha definition");
    let accepted_alpha_variant = alpha.variant_id("Alpha").expect("alpha variant ID");
    let accepted_zulu_variant = alpha.variant_id("Zulu").expect("zulu variant ID");

    let candidate =
        reconcile_accepted_enum_catalog_from_kinds(&accepted, &[ALPHA_TAIL_ADDED_KIND, ZETA_KIND])
            .expect("dense tail catalog additions should reconcile");
    let candidate_alpha_id = candidate.type_id("catalog::Alpha").expect("alpha type ID");
    let candidate_alpha = candidate
        .enum_type(candidate_alpha_id)
        .expect("alpha definition");

    assert_eq!(candidate_alpha_id, alpha_id);
    assert_eq!(
        candidate_alpha.variant_id("Alpha"),
        Some(accepted_alpha_variant)
    );
    assert_eq!(
        candidate_alpha.variant_id("Zulu"),
        Some(accepted_zulu_variant)
    );
    assert_eq!(
        candidate_alpha.variant_id("Zzz").map(EnumVariantId::get),
        Some(3)
    );
    assert_eq!(
        candidate.type_id("catalog::Zeta").map(EnumTypeId::get),
        Some(2),
    );
}

#[test]
fn existing_unit_enum_keys_survive_reorder_and_dense_tail_additions() {
    let accepted = build_initial_accepted_enum_catalog_from_kinds(&[ALPHA_KIND])
        .expect("initial catalog should build");
    let alpha_key = admitted_unit_equality_key(&accepted, "catalog::Alpha", "Alpha");
    let zulu_key = admitted_unit_equality_key(&accepted, "catalog::Alpha", "Zulu");

    let reordered = reconcile_accepted_enum_catalog_from_kinds(&accepted, &[ALPHA_REORDERED_KIND])
        .expect("declaration reorder should reconcile as a no-op");
    let extended =
        reconcile_accepted_enum_catalog_from_kinds(&accepted, &[ALPHA_TAIL_ADDED_KIND, ZETA_KIND])
            .expect("dense tail type and variant additions should reconcile");

    for catalog in [&reordered, &extended] {
        assert_eq!(
            admitted_unit_equality_key(catalog, "catalog::Alpha", "Alpha"),
            alpha_key,
        );
        assert_eq!(
            admitted_unit_equality_key(catalog, "catalog::Alpha", "Zulu"),
            zulu_key,
        );
    }
}

#[test]
fn native_catalog_reconcile_removes_unused_types_from_current_candidate() {
    let accepted = build_initial_accepted_enum_catalog_from_kinds(&[ALPHA_KIND, ZETA_KIND])
        .expect("initial catalog should build");
    let reordered = reconcile_accepted_enum_catalog_from_kinds(&accepted, &[ALPHA_REORDERED_KIND])
        .expect("declaration reorder should reconcile");
    let current = reconcile_accepted_enum_catalog_from_kinds(&accepted, &[ALPHA_KIND])
        .expect("unused accepted type should be removed");

    assert_eq!(reordered.len(), 1);
    assert_eq!(current.len(), 1);
    assert_eq!(
        current.type_id("catalog::Alpha").map(EnumTypeId::get),
        Some(1)
    );
    assert_eq!(current.type_id("catalog::Zeta"), None);
}

#[test]
fn native_catalog_reconcile_allows_tail_removal_and_rejects_contract_change() {
    let accepted = build_initial_accepted_enum_catalog_from_kinds(&[ALPHA_KIND])
        .expect("initial catalog should build");

    let removed = reconcile_accepted_enum_catalog_from_kinds(&accepted, &[ALPHA_REMOVED_KIND])
        .expect("tail variant removal should keep surviving IDs dense and stable");
    let alpha = removed
        .enum_type(removed.type_id("catalog::Alpha").expect("alpha ID"))
        .expect("alpha definition");
    assert_eq!(alpha.variant_id("Alpha").map(EnumVariantId::get), Some(1));
    assert_eq!(alpha.variant_id("Zulu"), None);
    assert_eq!(
        reconcile_accepted_enum_catalog_from_kinds(&accepted, &[CONFLICTING_ALPHA_KIND]),
        Err(EnumCatalogBuildError::ExistingVariantContractChanged {
            path: "catalog::Alpha".to_string(),
            name: "Alpha".to_string(),
        }),
    );
}

#[test]
fn native_catalog_reconcile_fails_when_dense_current_order_would_move_live_ids() {
    let accepted = build_initial_accepted_enum_catalog_from_kinds(&[ALPHA_KIND])
        .expect("initial catalog should build");

    assert_eq!(
        reconcile_accepted_enum_catalog_from_kinds(&accepted, &[AARDVARK_KIND, ALPHA_KIND]),
        Err(EnumCatalogBuildError::ExistingTypeIdentityChanged {
            path: "catalog::Alpha".to_string(),
        }),
    );
    assert_eq!(
        reconcile_accepted_enum_catalog_from_kinds(&accepted, &[ALPHA_ADDED_KIND]),
        Err(EnumCatalogBuildError::ExistingVariantIdentityChanged {
            path: "catalog::Alpha".to_string(),
            name: "Zulu".to_string(),
        }),
    );
}

#[test]
fn duplicate_variant_name_rejects_before_map_construction() {
    assert_eq!(
        build_initial_accepted_enum_catalog_from_kinds(&[DUPLICATE_KIND]),
        Err(EnumCatalogBuildError::DuplicateVariantName {
            path: "catalog::Duplicate".to_string(),
            name: "Same".to_string(),
        }),
    );
}

#[test]
fn conflicting_complete_definitions_reject() {
    assert_eq!(
        build_initial_accepted_enum_catalog_from_kinds(&[ALPHA_KIND, CONFLICTING_ALPHA_KIND,]),
        Err(EnumCatalogBuildError::ConflictingDefinition {
            path: "catalog::Alpha".to_string(),
        }),
    );
}

#[test]
fn payload_storage_decode_mismatch_rejects_complete_definition() {
    assert_eq!(
        build_initial_accepted_enum_catalog_from_kinds(&[PAYLOAD_BY_KIND, PAYLOAD_VALUE]),
        Err(EnumCatalogBuildError::ConflictingDefinition {
            path: "catalog::PayloadDecode".to_string(),
        }),
    );
}

#[test]
fn payload_contract_resolves_nested_enum_to_type_id() {
    let catalog = build_initial_accepted_enum_catalog_from_kinds(&[PAYLOAD_REF_KIND])
        .expect("nested enum proposal should resolve");
    let alpha_id = catalog.type_id("catalog::Alpha").expect("alpha id");
    let container = catalog
        .enum_type(catalog.type_id("catalog::Container").expect("container id"))
        .expect("container definition");
    let nested = container
        .variant(container.variant_id("Nested").expect("nested variant id"))
        .expect("nested variant");

    let AcceptedEnumVariantBody::Payload { contract } = nested.body() else {
        panic!("nested variant should carry a payload contract");
    };
    assert_eq!(contract.storage_decode(), FieldStorageDecode::ByKind);
    assert_eq!(
        contract.kind(),
        &AcceptedFieldKind::Enum { type_id: alpha_id }
    );
}

#[test]
fn same_variant_label_under_different_types_keeps_distinct_identity() {
    let catalog = build_initial_accepted_enum_catalog_from_kinds(&[
        SAME_LABEL_RIGHT_KIND,
        SAME_LABEL_LEFT_KIND,
    ])
    .expect("independent enum labels should coexist");
    let left_id = catalog.type_id("catalog::Left").expect("left id");
    let right_id = catalog.type_id("catalog::Right").expect("right id");
    let left_variant = catalog
        .enum_type(left_id)
        .and_then(|definition| definition.variant_id("Shared"))
        .expect("left variant");
    let right_variant = catalog
        .enum_type(right_id)
        .and_then(|definition| definition.variant_id("Shared"))
        .expect("right variant");

    assert_ne!(left_id, right_id);
    assert_eq!(left_variant.get(), right_variant.get());
    assert_ne!((left_id, left_variant), (right_id, right_variant));
}

#[test]
fn recursive_enum_contract_rejects_with_cycle() {
    assert_eq!(
        build_initial_accepted_enum_catalog_from_kinds(&[RECURSIVE_KIND]),
        Err(EnumCatalogBuildError::RecursiveEnumContract {
            cycle: vec![
                "catalog::Recursive".to_string(),
                "catalog::Recursive".to_string(),
            ],
        }),
    );
}

#[test]
fn checked_id_allocation_rejects_overflow_without_wrapping() {
    let max_type = EnumTypeId::new(u32::MAX).expect("u32 max is non-zero");
    let max_variant = EnumVariantId::new(u32::MAX).expect("u32 max is non-zero");

    assert_eq!(
        next_type_id(Some(max_type)),
        Err(EnumCatalogBuildError::EnumTypeIdExhausted),
    );
    assert_eq!(
        next_variant_id("catalog::Max", Some(max_variant)),
        Err(EnumCatalogBuildError::EnumVariantIdExhausted {
            path: "catalog::Max".to_string(),
        }),
    );
}

fn accepted_composite_contract(
    kind: FieldKind,
) -> (AcceptedValueCatalogHandle, AcceptedValueContract) {
    let (enum_catalog, composite_catalog) =
        crate::db::schema::build_initial_accepted_catalogs_from_kinds_for_tests(&[kind])
            .expect("composite catalogs should build");
    let accepted_kind =
        resolve_model_field_kind_with_composite_catalog(&enum_catalog, &composite_catalog, kind)
            .expect("composite kind should resolve");
    let handle = AcceptedValueCatalogHandle::new_for_tests(
        enum_catalog,
        composite_catalog,
        AcceptedSchemaRevision::INITIAL,
    );
    let contract = AcceptedValueContract::from_accepted_field(
        &handle,
        &accepted_kind,
        FieldStorageDecode::CatalogValue,
    )
    .expect("accepted composite contract should build");
    (handle, contract)
}

#[test]
fn exact_record_admission_canonicalizes_declared_fields_and_rejects_shape_drift() {
    let (catalog, contract) = accepted_composite_contract(PROFILE_KIND);
    let admitted = normalize_and_admit_value(
        &catalog,
        &contract,
        InputValue::Map(vec![
            (InputValue::Text("score".to_string()), InputValue::Null),
            (
                InputValue::Text("name".to_string()),
                InputValue::Text("Ada".to_string()),
            ),
        ]),
        &mut ValueAdmissionBudget::standard(),
    )
    .expect("declared record should admit");
    let Value::Map(entries) = admitted.value() else {
        panic!("record should retain the canonical map envelope");
    };
    assert!(matches!(&entries[0].0, Value::Text(name) if name == "name"));
    assert!(matches!(&entries[1].0, Value::Text(name) if name == "score"));

    for invalid in [
        InputValue::Map(vec![(
            InputValue::Text("name".to_string()),
            InputValue::Text("Ada".to_string()),
        )]),
        InputValue::Map(vec![
            (
                InputValue::Text("name".to_string()),
                InputValue::Text("Ada".to_string()),
            ),
            (InputValue::Text("score".to_string()), InputValue::Null),
            (
                InputValue::Text("extra".to_string()),
                InputValue::Bool(true),
            ),
        ]),
    ] {
        assert_eq!(
            normalize_and_admit_value(
                &catalog,
                &contract,
                invalid,
                &mut ValueAdmissionBudget::standard(),
            ),
            Err(ValueAdmissionError::CompositeShapeMismatch),
        );
    }
}

#[test]
fn exact_tuple_admission_rejects_wrong_arity_and_element_kind() {
    let (catalog, contract) = accepted_composite_contract(PAIR_KIND);

    assert!(
        normalize_and_admit_value(
            &catalog,
            &contract,
            InputValue::List(vec![InputValue::Nat64(7), InputValue::Bool(true)]),
            &mut ValueAdmissionBudget::standard(),
        )
        .is_ok()
    );
    assert_eq!(
        normalize_and_admit_value(
            &catalog,
            &contract,
            InputValue::List(vec![InputValue::Nat64(7)]),
            &mut ValueAdmissionBudget::standard(),
        ),
        Err(ValueAdmissionError::CompositeShapeMismatch),
    );
    assert_eq!(
        normalize_and_admit_value(
            &catalog,
            &contract,
            InputValue::List(vec![
                InputValue::Nat64(7),
                InputValue::Text("true".to_string())
            ]),
            &mut ValueAdmissionBudget::standard(),
        ),
        Err(ValueAdmissionError::TypeMismatch),
    );
}

#[test]
fn exact_newtype_nested_and_empty_composites_preserve_their_declared_shapes() {
    let (catalog, identifier) = accepted_composite_contract(IDENTIFIER_KIND);
    assert!(
        normalize_and_admit_value(
            &catalog,
            &identifier,
            InputValue::Nat64(7),
            &mut ValueAdmissionBudget::standard(),
        )
        .is_ok()
    );
    assert_eq!(
        normalize_and_admit_value(
            &catalog,
            &identifier,
            InputValue::Text("7".to_string()),
            &mut ValueAdmissionBudget::standard(),
        ),
        Err(ValueAdmissionError::TypeMismatch),
    );

    let (catalog, nested) = accepted_composite_contract(NESTED_KIND);
    let nested_value = |pair| InputValue::Map(vec![(InputValue::Text("pair".to_string()), pair)]);
    assert!(
        normalize_and_admit_value(
            &catalog,
            &nested,
            nested_value(InputValue::List(vec![
                InputValue::Nat64(7),
                InputValue::Bool(true),
            ])),
            &mut ValueAdmissionBudget::standard(),
        )
        .is_ok()
    );
    assert_eq!(
        normalize_and_admit_value(
            &catalog,
            &nested,
            nested_value(InputValue::List(vec![InputValue::Nat64(7)])),
            &mut ValueAdmissionBudget::standard(),
        ),
        Err(ValueAdmissionError::CompositeShapeMismatch),
    );

    let (catalog, empty_record) = accepted_composite_contract(EMPTY_RECORD_KIND);
    assert!(
        normalize_and_admit_value(
            &catalog,
            &empty_record,
            InputValue::Map(Vec::new()),
            &mut ValueAdmissionBudget::standard(),
        )
        .is_ok()
    );
    assert_eq!(
        normalize_and_admit_value(
            &catalog,
            &empty_record,
            InputValue::List(Vec::new()),
            &mut ValueAdmissionBudget::standard(),
        ),
        Err(ValueAdmissionError::CompositeShapeMismatch),
    );

    let (catalog, empty_tuple) = accepted_composite_contract(EMPTY_TUPLE_KIND);
    assert!(
        normalize_and_admit_value(
            &catalog,
            &empty_tuple,
            InputValue::List(Vec::new()),
            &mut ValueAdmissionBudget::standard(),
        )
        .is_ok()
    );
    assert_eq!(
        normalize_and_admit_value(
            &catalog,
            &empty_tuple,
            InputValue::Map(Vec::new()),
            &mut ValueAdmissionBudget::standard(),
        ),
        Err(ValueAdmissionError::CompositeShapeMismatch),
    );
}

#[test]
fn exact_composite_admission_enforces_shared_depth_and_byte_budgets() {
    let (catalog, nested) = accepted_composite_contract(NESTED_KIND);
    let input = InputValue::Map(vec![(
        InputValue::Text("pair".to_string()),
        InputValue::List(vec![InputValue::Nat64(7), InputValue::Bool(true)]),
    )]);

    assert_eq!(
        normalize_and_admit_value(
            &catalog,
            &nested,
            input.clone(),
            &mut ValueAdmissionBudget::with_limits(2, 1024),
        ),
        Err(ValueAdmissionError::DepthExceeded),
    );
    assert_eq!(
        normalize_and_admit_value(
            &catalog,
            &nested,
            input,
            &mut ValueAdmissionBudget::with_limits(64, 8),
        ),
        Err(ValueAdmissionError::SizeExceeded),
    );
}

#[test]
#[ignore = "native microbenchmark: run explicitly with --ignored --nocapture"]
fn exact_composite_admission_microbenchmark_report() {
    use std::{hint::black_box, time::Instant};

    const ITERATIONS: u32 = 20_000;
    let (catalog, composite_contract) = accepted_composite_contract(BENCH_RECORD_KIND);
    let map_kind = AcceptedFieldKind::Map {
        key: Box::new(AcceptedFieldKind::Text { max_len: Some(8) }),
        value: Box::new(AcceptedFieldKind::Nat64),
    };
    let map_contract = AcceptedValueContract::from_accepted_field(
        &catalog,
        &map_kind,
        FieldStorageDecode::CatalogValue,
    )
    .expect("comparison map contract should build");
    let input = InputValue::Map(vec![
        (InputValue::Text("gamma".to_string()), InputValue::Nat64(3)),
        (InputValue::Text("alpha".to_string()), InputValue::Nat64(1)),
        (InputValue::Text("beta".to_string()), InputValue::Nat64(2)),
    ]);

    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let admitted = normalize_and_admit_value(
            &catalog,
            &composite_contract,
            black_box(input.clone()),
            &mut ValueAdmissionBudget::standard(),
        )
        .expect("exact record should admit");
        black_box(admitted);
    }
    let composite_normalize = start.elapsed();

    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let admitted = normalize_and_admit_value(
            &catalog,
            &map_contract,
            black_box(input.clone()),
            &mut ValueAdmissionBudget::standard(),
        )
        .expect("typed map should admit");
        black_box(admitted);
    }
    let map_normalize = start.elapsed();

    let canonical = normalize_and_admit_value(
        &catalog,
        &composite_contract,
        input,
        &mut ValueAdmissionBudget::standard(),
    )
    .expect("exact record should admit for validation benchmark");
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        black_box(
            validate_canonical_value(
                &catalog,
                &composite_contract,
                canonical.value(),
                &mut ValueAdmissionBudget::standard(),
            )
            .expect("exact canonical record should validate"),
        );
    }
    let composite_validate = start.elapsed();

    let start = Instant::now();
    for _ in 0..ITERATIONS {
        black_box(
            validate_canonical_value(
                &catalog,
                &map_contract,
                canonical.value(),
                &mut ValueAdmissionBudget::standard(),
            )
            .expect("typed canonical map should validate"),
        );
    }
    let map_validate = start.elapsed();

    println!("Exact composite accepted-admission microbenchmark");
    println!("iterations={ITERATIONS}");
    println!(
        "record_normalize_ns_per_op={}",
        composite_normalize.as_nanos() / u128::from(ITERATIONS),
    );
    println!(
        "typed_map_normalize_ns_per_op={}",
        map_normalize.as_nanos() / u128::from(ITERATIONS),
    );
    println!(
        "record_validate_ns_per_op={}",
        composite_validate.as_nanos() / u128::from(ITERATIONS),
    );
    println!(
        "typed_map_validate_ns_per_op={}",
        map_validate.as_nanos() / u128::from(ITERATIONS),
    );
}
