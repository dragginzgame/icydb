use super::{
    ACCEPTED_SCHEMA_BUNDLE_CODEC_VERSION, ACCEPTED_SCHEMA_BUNDLE_MAGIC, ACCEPTED_SCHEMA_ROOT_BYTES,
    ACCEPTED_SCHEMA_ROOT_CODEC_VERSION, ACCEPTED_SCHEMA_ROOT_MAGIC, AcceptedSchemaBundleKey,
    AcceptedSchemaFingerprint, AcceptedSchemaPublicationError, AcceptedSchemaRevision,
    AcceptedSchemaRevisionBundle, AcceptedSchemaRoot, CandidateSchemaRevision,
    decode_accepted_schema_revision_bundle, encode_accepted_schema_revision_bundle,
    encode_accepted_schema_root, prepare_accepted_schema_root_publication,
    select_current_accepted_schema_root,
};
use crate::{
    db::{
        database_format::crc32c,
        schema::{
            AcceptedCompositeCatalog, AcceptedFieldKind, FieldId, PersistedFieldSnapshot,
            PersistedIndexFieldPathSnapshot, PersistedIndexKeySnapshot, PersistedIndexSnapshot,
            PersistedNestedLeafSnapshot, PersistedSchemaSnapshot, RowLayoutVersion,
            SchemaFieldSlot, SchemaHistoricalFill, SchemaInsertDefault, SchemaRowLayout,
            SchemaVersion, build_initial_accepted_catalogs_from_kinds_for_tests,
        },
    },
    model::field::{
        CompositeCodec, CompositeElementModel, CompositeFieldModel, CompositeShapeModel,
        EnumVariantModel, FieldKind, FieldStorageDecode, LeafCodec, ScalarCodec,
    },
    types::EntityTag,
    value::EnumTypeId,
};
use std::collections::BTreeMap;

static STATUS_VARIANTS: [EnumVariantModel; 1] = [EnumVariantModel::new(
    "Active",
    None,
    FieldStorageDecode::ByKind,
)];
static PAYLOAD_STATUS_VARIANTS: [EnumVariantModel; 1] = [EnumVariantModel::new(
    "Active",
    Some(&FieldKind::Nat64),
    FieldStorageDecode::ByKind,
)];
const STATUS_KIND: FieldKind = FieldKind::Enum {
    path: "test::Status",
    variants: &STATUS_VARIANTS,
};
const PAYLOAD_STATUS_KIND: FieldKind = FieldKind::Enum {
    path: "test::Status",
    variants: &PAYLOAD_STATUS_VARIANTS,
};
static PROFILE_FIELDS: [CompositeFieldModel; 1] = [CompositeFieldModel::generated(
    "name",
    FieldKind::Text { max_len: Some(32) },
    false,
)];
static PROFILE_SHAPE: CompositeShapeModel = CompositeShapeModel::Record(&PROFILE_FIELDS);
static PROFILE_KIND: FieldKind = FieldKind::Composite {
    path: "test::Profile",
    codec: CompositeCodec::StructuralV1,
    shape: &PROFILE_SHAPE,
};
static NONCANONICAL_PROFILE_FIELDS: [CompositeFieldModel; 2] = [
    CompositeFieldModel::generated("zeta", FieldKind::Nat64, false),
    CompositeFieldModel::generated("alpha", FieldKind::Bool, true),
];
static NONCANONICAL_PROFILE_SHAPE: CompositeShapeModel =
    CompositeShapeModel::Record(&NONCANONICAL_PROFILE_FIELDS);
static NONCANONICAL_PROFILE_KIND: FieldKind = FieldKind::Composite {
    path: "test::NoncanonicalProfile",
    codec: CompositeCodec::StructuralV1,
    shape: &NONCANONICAL_PROFILE_SHAPE,
};
static INVALID_COMPOSITE_RELATION_KIND: FieldKind = FieldKind::Relation {
    target_path: "test::Owner",
    target_entity_name: "Owner",
    target_entity_tag: EntityTag::new(8),
    target_store_path: "test::Store",
    key_kind: &PROFILE_KIND,
};
static INVALID_RELATION_RECORD_FIELDS: [CompositeFieldModel; 1] = [CompositeFieldModel::generated(
    "owner",
    INVALID_COMPOSITE_RELATION_KIND,
    false,
)];
static INVALID_RELATION_RECORD_SHAPE: CompositeShapeModel =
    CompositeShapeModel::Record(&INVALID_RELATION_RECORD_FIELDS);
static INVALID_RELATION_RECORD_KIND: FieldKind = FieldKind::Composite {
    path: "test::InvalidRelationRecord",
    codec: CompositeCodec::StructuralV1,
    shape: &INVALID_RELATION_RECORD_SHAPE,
};
static INVALID_RELATION_TUPLE_ELEMENTS: [CompositeElementModel; 1] =
    [CompositeElementModel::generated(
        INVALID_COMPOSITE_RELATION_KIND,
        false,
    )];
static INVALID_RELATION_TUPLE_SHAPE: CompositeShapeModel =
    CompositeShapeModel::Tuple(&INVALID_RELATION_TUPLE_ELEMENTS);
static INVALID_RELATION_TUPLE_KIND: FieldKind = FieldKind::Composite {
    path: "test::InvalidRelationTuple",
    codec: CompositeCodec::StructuralV1,
    shape: &INVALID_RELATION_TUPLE_SHAPE,
};
static INVALID_RELATION_NEWTYPE_SHAPE: CompositeShapeModel = CompositeShapeModel::Newtype(
    CompositeElementModel::generated(INVALID_COMPOSITE_RELATION_KIND, false),
);
static INVALID_RELATION_NEWTYPE_KIND: FieldKind = FieldKind::Composite {
    path: "test::InvalidRelationNewtype",
    codec: CompositeCodec::StructuralV1,
    shape: &INVALID_RELATION_NEWTYPE_SHAPE,
};
static INVALID_RELATION_ENUM_VARIANTS: [EnumVariantModel; 1] = [EnumVariantModel::new(
    "Owned",
    Some(&INVALID_COMPOSITE_RELATION_KIND),
    FieldStorageDecode::ByKind,
)];
static INVALID_RELATION_ENUM_KIND: FieldKind = FieldKind::Enum {
    path: "test::InvalidRelationEnum",
    variants: &INVALID_RELATION_ENUM_VARIANTS,
};
static NESTED_STATUS_FIELDS: [CompositeFieldModel; 1] =
    [CompositeFieldModel::generated("status", STATUS_KIND, false)];
static NESTED_STATUS_SHAPE: CompositeShapeModel =
    CompositeShapeModel::Record(&NESTED_STATUS_FIELDS);
static NESTED_STATUS_KIND: FieldKind = FieldKind::Composite {
    path: "test::NestedStatus",
    codec: CompositeCodec::StructuralV1,
    shape: &NESTED_STATUS_SHAPE,
};

fn empty_catalog() -> super::AcceptedEnumCatalog {
    super::AcceptedEnumCatalog {
        by_id: BTreeMap::new(),
        id_by_path: BTreeMap::new(),
    }
}

fn empty_composite_catalog() -> AcceptedCompositeCatalog {
    AcceptedCompositeCatalog::empty()
}

fn snapshot(entity_path: &str) -> PersistedSchemaSnapshot {
    snapshot_with_field(
        entity_path,
        AcceptedFieldKind::Ulid,
        Vec::new(),
        LeafCodec::Scalar(ScalarCodec::Ulid),
    )
}

fn snapshot_with_field(
    entity_path: &str,
    kind: AcceptedFieldKind,
    nested_leaves: Vec<PersistedNestedLeafSnapshot>,
    leaf_codec: LeafCodec,
) -> PersistedSchemaSnapshot {
    snapshot_with_field_and_default(
        entity_path,
        kind,
        nested_leaves,
        leaf_codec,
        SchemaInsertDefault::None,
    )
}

fn snapshot_with_field_and_default(
    entity_path: &str,
    kind: AcceptedFieldKind,
    nested_leaves: Vec<PersistedNestedLeafSnapshot>,
    leaf_codec: LeafCodec,
    default: SchemaInsertDefault,
) -> PersistedSchemaSnapshot {
    PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        entity_path.to_string(),
        "Item".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
        vec![PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "id".to_string(),
            SchemaFieldSlot::new(0),
            kind,
            nested_leaves,
            false,
            default,
            FieldStorageDecode::ByKind,
            leaf_codec,
        )],
    )
}

fn snapshot_with_indexed_enum(
    catalog: &super::super::AcceptedEnumCatalog,
    kind: FieldKind,
) -> PersistedSchemaSnapshot {
    let persisted_kind = super::super::resolve_model_field_kind(catalog, kind)
        .expect("test enum kind should resolve through its catalog");
    PersistedSchemaSnapshot::new_with_indexes(
        SchemaVersion::initial(),
        "test::Item".to_string(),
        "Item".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
        vec![PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "status".to_string(),
            SchemaFieldSlot::new(0),
            persisted_kind.clone(),
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Structural,
        )],
        vec![PersistedIndexSnapshot::new(
            1,
            "idx_item__status".to_string(),
            "test::Item::status".to_string(),
            false,
            PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                FieldId::new(1),
                SchemaFieldSlot::new(0),
                vec!["status".to_string()],
                persisted_kind,
                false,
            )]),
            None,
        )],
    )
}

fn status_catalog() -> super::super::AcceptedEnumCatalog {
    super::super::build_initial_accepted_enum_catalog_from_kinds(&[STATUS_KIND])
        .expect("status catalog should build")
}

fn snapshot_with_nested_status(
    composite_kind: AcceptedFieldKind,
    status_kind: AcceptedFieldKind,
) -> PersistedSchemaSnapshot {
    let nested = PersistedNestedLeafSnapshot::new(vec!["status".to_string()], status_kind, false);
    snapshot_with_non_key_field(
        "test::Item",
        "profile",
        composite_kind,
        vec![nested],
        FieldStorageDecode::CatalogValue,
        LeafCodec::Structural,
    )
}

fn snapshot_with_non_key_field(
    entity_path: &str,
    field_name: &str,
    kind: AcceptedFieldKind,
    nested_leaves: Vec<PersistedNestedLeafSnapshot>,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
) -> PersistedSchemaSnapshot {
    PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        entity_path.to_string(),
        "Item".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
        ]),
        vec![
            PersistedFieldSnapshot::new_initial(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(2),
                field_name.to_string(),
                SchemaFieldSlot::new(1),
                kind,
                nested_leaves,
                false,
                SchemaInsertDefault::None,
                storage_decode,
                leaf_codec,
            ),
        ],
    )
}

fn bundle(revision: u64) -> AcceptedSchemaRevisionBundle {
    AcceptedSchemaRevisionBundle::new(
        AcceptedSchemaRevision::new(revision),
        "test::Store",
        empty_catalog(),
        empty_composite_catalog(),
        BTreeMap::from([(EntityTag::new(7), snapshot("test::Item"))]),
    )
    .expect("accepted schema bundle should build")
}

fn candidate(revision: u64) -> CandidateSchemaRevision {
    CandidateSchemaRevision::new(bundle(revision)).expect("candidate should build")
}

#[test]
fn accepted_schema_bundle_requires_field_enum_type_to_exist() {
    let catalog = status_catalog();
    let status_type_id = catalog
        .type_id("test::Status")
        .expect("status type ID should exist");
    let exact = snapshot_with_field(
        "test::Item",
        AcceptedFieldKind::Enum {
            type_id: status_type_id,
        },
        Vec::new(),
        LeafCodec::Structural,
    );
    assert!(
        AcceptedSchemaRevisionBundle::new(
            AcceptedSchemaRevision::INITIAL,
            "test::Store",
            catalog.clone(),
            empty_composite_catalog(),
            BTreeMap::from([(EntityTag::new(7), exact)]),
        )
        .is_ok()
    );

    let mismatched = snapshot_with_field(
        "test::Item",
        AcceptedFieldKind::Enum {
            type_id: EnumTypeId::new(999).expect("test enum type ID should be valid"),
        },
        Vec::new(),
        LeafCodec::Structural,
    );
    assert!(
        AcceptedSchemaRevisionBundle::new(
            AcceptedSchemaRevision::INITIAL,
            "test::Store",
            catalog,
            empty_composite_catalog(),
            BTreeMap::from([(EntityTag::new(7), mismatched)]),
        )
        .is_err()
    );
}

#[test]
fn accepted_schema_bundle_requires_field_composite_type_to_exist() {
    let (enum_catalog, composite_catalog) =
        build_initial_accepted_catalogs_from_kinds_for_tests(&[PROFILE_KIND])
            .expect("profile catalogs should build");
    let profile_kind = AcceptedFieldKind::from_model_kind(PROFILE_KIND);
    let profile_leaf = PersistedNestedLeafSnapshot::new(
        vec!["name".to_string()],
        AcceptedFieldKind::Text { max_len: Some(32) },
        false,
    );
    let exact = snapshot_with_non_key_field(
        "test::Item",
        "profile",
        profile_kind.clone(),
        vec![profile_leaf.clone()],
        FieldStorageDecode::CatalogValue,
        LeafCodec::Structural,
    );
    assert!(
        AcceptedSchemaRevisionBundle::new(
            AcceptedSchemaRevision::INITIAL,
            "test::Store",
            enum_catalog.clone(),
            composite_catalog,
            BTreeMap::from([(EntityTag::new(7), exact)]),
        )
        .is_ok()
    );

    let missing = snapshot_with_non_key_field(
        "test::Item",
        "profile",
        profile_kind,
        vec![profile_leaf],
        FieldStorageDecode::CatalogValue,
        LeafCodec::Structural,
    );
    assert!(
        AcceptedSchemaRevisionBundle::new(
            AcceptedSchemaRevision::INITIAL,
            "test::Store",
            enum_catalog,
            empty_composite_catalog(),
            BTreeMap::from([(EntityTag::new(7), missing)]),
        )
        .is_err()
    );
}

#[test]
fn accepted_schema_bundle_requires_nested_leaves_to_match_composite_authority() {
    let (enum_catalog, composite_catalog) =
        build_initial_accepted_catalogs_from_kinds_for_tests(&[PROFILE_KIND])
            .expect("profile catalogs should build");
    let profile_kind = AcceptedFieldKind::from_model_kind(PROFILE_KIND);
    let snapshot_with_leaf = |leaf| {
        snapshot_with_non_key_field(
            "test::Item",
            "profile",
            profile_kind.clone(),
            vec![leaf],
            FieldStorageDecode::CatalogValue,
            LeafCodec::Structural,
        )
    };

    for mismatched_leaf in [
        PersistedNestedLeafSnapshot::new(
            vec!["other".to_string()],
            AcceptedFieldKind::Text { max_len: Some(32) },
            false,
        ),
        PersistedNestedLeafSnapshot::new(
            vec!["name".to_string()],
            AcceptedFieldKind::Blob { max_len: Some(32) },
            false,
        ),
        PersistedNestedLeafSnapshot::new(
            vec!["name".to_string()],
            AcceptedFieldKind::Text { max_len: Some(32) },
            true,
        ),
    ] {
        assert!(
            AcceptedSchemaRevisionBundle::new(
                AcceptedSchemaRevision::INITIAL,
                "test::Store",
                enum_catalog.clone(),
                composite_catalog.clone(),
                BTreeMap::from([(EntityTag::new(7), snapshot_with_leaf(mismatched_leaf))]),
            )
            .is_err(),
            "catalog-disconnected nested leaf metadata must reject",
        );
    }

    let missing_leaf = snapshot_with_non_key_field(
        "test::Item",
        "profile",
        profile_kind,
        Vec::new(),
        FieldStorageDecode::CatalogValue,
        LeafCodec::Structural,
    );
    assert!(
        AcceptedSchemaRevisionBundle::new(
            AcceptedSchemaRevision::INITIAL,
            "test::Store",
            enum_catalog,
            composite_catalog,
            BTreeMap::from([(EntityTag::new(7), missing_leaf)]),
        )
        .is_err(),
        "missing catalog-derived nested leaf metadata must reject",
    );
}

#[test]
fn accepted_schema_bundle_requires_canonical_nested_leaf_order() {
    let (enum_catalog, composite_catalog) =
        build_initial_accepted_catalogs_from_kinds_for_tests(&[NONCANONICAL_PROFILE_KIND])
            .expect("profile catalogs should build");
    let profile_kind = AcceptedFieldKind::from_model_kind(NONCANONICAL_PROFILE_KIND);
    let alpha =
        PersistedNestedLeafSnapshot::new(vec!["alpha".to_string()], AcceptedFieldKind::Bool, true);
    let zeta =
        PersistedNestedLeafSnapshot::new(vec!["zeta".to_string()], AcceptedFieldKind::Nat64, false);
    let snapshot_with_leaves = |nested_leaves| {
        snapshot_with_non_key_field(
            "test::Item",
            "profile",
            profile_kind.clone(),
            nested_leaves,
            FieldStorageDecode::CatalogValue,
            LeafCodec::Structural,
        )
    };

    assert!(
        AcceptedSchemaRevisionBundle::new(
            AcceptedSchemaRevision::INITIAL,
            "test::Store",
            enum_catalog.clone(),
            composite_catalog.clone(),
            BTreeMap::from([(
                EntityTag::new(7),
                snapshot_with_leaves(vec![alpha.clone(), zeta.clone()]),
            )]),
        )
        .is_ok(),
        "catalog-order nested leaf metadata should publish",
    );
    assert!(
        AcceptedSchemaRevisionBundle::new(
            AcceptedSchemaRevision::INITIAL,
            "test::Store",
            enum_catalog,
            composite_catalog,
            BTreeMap::from([(EntityTag::new(7), snapshot_with_leaves(vec![zeta, alpha]),)]),
        )
        .is_err(),
        "declaration-order nested leaf metadata must fail closed",
    );
}

#[test]
fn accepted_schema_bundle_rejects_nested_unsupported_relation_key_contracts() {
    for kind in [
        INVALID_RELATION_RECORD_KIND,
        INVALID_RELATION_TUPLE_KIND,
        INVALID_RELATION_NEWTYPE_KIND,
        INVALID_RELATION_ENUM_KIND,
    ] {
        let (enum_catalog, composite_catalog) =
            build_initial_accepted_catalogs_from_kinds_for_tests(&[kind])
                .expect("catalog candidate should build before publication-role validation");
        assert!(
            AcceptedSchemaRevisionBundle::new(
                AcceptedSchemaRevision::INITIAL,
                "test::Store",
                enum_catalog,
                composite_catalog,
                BTreeMap::from([(EntityTag::new(7), snapshot("test::Item"))]),
            )
            .is_err(),
            "unsupported relation keys nested in catalog definitions must fail publication",
        );
    }
}

#[test]
fn accepted_schema_bundle_rejects_composite_key_index_and_relation_contracts() {
    let (enum_catalog, composite_catalog) =
        build_initial_accepted_catalogs_from_kinds_for_tests(&[PROFILE_KIND])
            .expect("profile catalogs should build");
    let profile_kind = AcceptedFieldKind::from_model_kind(PROFILE_KIND);
    let profile_leaf = || {
        PersistedNestedLeafSnapshot::new(
            vec!["name".to_string()],
            AcceptedFieldKind::Text { max_len: Some(32) },
            false,
        )
    };
    let bundle_with = |snapshot| {
        AcceptedSchemaRevisionBundle::new(
            AcceptedSchemaRevision::INITIAL,
            "test::Store",
            enum_catalog.clone(),
            composite_catalog.clone(),
            BTreeMap::from([(EntityTag::new(7), snapshot)]),
        )
    };

    let composite_primary_key = snapshot_with_field(
        "test::Item",
        profile_kind.clone(),
        vec![profile_leaf()],
        LeafCodec::Structural,
    );
    assert!(
        bundle_with(composite_primary_key).is_err(),
        "whole composites must not become primary keys",
    );

    let indexed = snapshot_with_non_key_field(
        "test::Item",
        "profile",
        profile_kind.clone(),
        vec![profile_leaf()],
        FieldStorageDecode::CatalogValue,
        LeafCodec::Structural,
    );
    let composite_index = PersistedSchemaSnapshot::new_with_indexes(
        indexed.version(),
        indexed.entity_path().to_string(),
        indexed.entity_name().to_string(),
        indexed.primary_key_field_ids().to_vec(),
        indexed.row_layout().clone(),
        indexed.fields().to_vec(),
        vec![PersistedIndexSnapshot::new(
            1,
            "idx_item__profile".to_string(),
            "test::Item::profile".to_string(),
            false,
            PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                FieldId::new(2),
                SchemaFieldSlot::new(1),
                vec!["profile".to_string()],
                profile_kind.clone(),
                false,
            )]),
            None,
        )],
    );
    assert!(
        bundle_with(composite_index).is_err(),
        "whole composites must not become index leaves",
    );

    let composite_relation_key = snapshot_with_non_key_field(
        "test::Item",
        "owner",
        AcceptedFieldKind::Relation {
            target_path: "test::Owner".to_string(),
            target_entity_name: "Owner".to_string(),
            target_entity_tag: EntityTag::new(8),
            target_store_path: "test::Store".to_string(),
            key_kind: Box::new(profile_kind),
        },
        Vec::new(),
        FieldStorageDecode::ByKind,
        LeafCodec::Structural,
    );
    assert!(
        bundle_with(composite_relation_key).is_err(),
        "whole composites must not become relation keys",
    );
}

#[test]
fn accepted_schema_bundle_rejects_malformed_default_payload() {
    let snapshot = snapshot_with_field_and_default(
        "test::Item",
        AcceptedFieldKind::Ulid,
        Vec::new(),
        LeafCodec::Scalar(ScalarCodec::Ulid),
        SchemaInsertDefault::SlotPayload(vec![0xFE]),
    );

    assert!(
        AcceptedSchemaRevisionBundle::new(
            AcceptedSchemaRevision::INITIAL,
            "test::Store",
            empty_catalog(),
            empty_composite_catalog(),
            BTreeMap::from([(EntityTag::new(7), snapshot)]),
        )
        .is_err()
    );
}

#[test]
fn accepted_schema_bundle_rejects_malformed_historical_fill_payload() {
    let base = snapshot_with_non_key_field(
        "test::Item",
        "score",
        AcceptedFieldKind::Nat64,
        Vec::new(),
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Nat64),
    );
    let current = RowLayoutVersion::INITIAL
        .checked_next()
        .expect("temporal fixture layout should advance");
    let mut fields = base.fields().to_vec();
    let score = &base.fields()[1];
    fields[1] = PersistedFieldSnapshot::new_with_write_policy(
        score.id(),
        score.name().to_string(),
        score.slot(),
        score.kind().clone(),
        score.nested_leaves().to_vec(),
        score.nullable(),
        current,
        SchemaInsertDefault::None,
        SchemaHistoricalFill::SlotPayload(vec![0xFE]),
        score.write_policy(),
        score.storage_decode(),
        score.leaf_codec(),
    );
    let snapshot = PersistedSchemaSnapshot::new(
        base.version(),
        base.entity_path().to_string(),
        base.entity_name().to_string(),
        base.primary_key_field_ids().to_vec(),
        SchemaRowLayout::new(
            current,
            RowLayoutVersion::INITIAL,
            base.row_layout().field_to_slot().to_vec(),
        ),
        fields,
    );

    assert!(
        AcceptedSchemaRevisionBundle::new(
            AcceptedSchemaRevision::INITIAL,
            "test::Store",
            empty_catalog(),
            empty_composite_catalog(),
            BTreeMap::from([(EntityTag::new(7), snapshot)]),
        )
        .is_err()
    );
}

#[test]
fn accepted_schema_bundle_checks_nested_enum_type_ids() {
    let (catalog, composite_catalog) =
        build_initial_accepted_catalogs_from_kinds_for_tests(&[NESTED_STATUS_KIND])
            .expect("nested status catalogs should build");
    let composite_kind = super::super::resolve_model_field_kind_with_composite_catalog(
        &catalog,
        &composite_catalog,
        NESTED_STATUS_KIND,
    )
    .expect("nested status composite kind should resolve");
    let status_kind = AcceptedFieldKind::Enum {
        type_id: catalog
            .type_id("test::Status")
            .expect("status type ID should exist"),
    };
    assert!(
        AcceptedSchemaRevisionBundle::new(
            AcceptedSchemaRevision::INITIAL,
            "test::Store",
            catalog.clone(),
            composite_catalog.clone(),
            BTreeMap::from([(
                EntityTag::new(7),
                snapshot_with_nested_status(composite_kind.clone(), status_kind),
            )]),
        )
        .is_ok()
    );

    assert!(
        AcceptedSchemaRevisionBundle::new(
            AcceptedSchemaRevision::INITIAL,
            "test::Store",
            catalog,
            composite_catalog,
            BTreeMap::from([(
                EntityTag::new(7),
                snapshot_with_nested_status(
                    composite_kind,
                    AcceptedFieldKind::Enum {
                        type_id: EnumTypeId::new(999).expect("test enum type ID should be valid"),
                    },
                ),
            )]),
        )
        .is_err()
    );
}

#[test]
fn accepted_schema_bundle_admits_only_stable_key_enum_indexes() {
    let catalog = status_catalog();
    assert!(
        AcceptedSchemaRevisionBundle::new(
            AcceptedSchemaRevision::INITIAL,
            "test::Store",
            catalog.clone(),
            empty_composite_catalog(),
            BTreeMap::from([(
                EntityTag::new(7),
                snapshot_with_indexed_enum(&catalog, STATUS_KIND),
            )]),
        )
        .is_ok(),
        "unit enums have a canonical stable equality key",
    );

    let payload_catalog =
        super::super::build_initial_accepted_enum_catalog_from_kinds(&[PAYLOAD_STATUS_KIND])
            .expect("payload status catalog should build");
    assert!(
        AcceptedSchemaRevisionBundle::new(
            AcceptedSchemaRevision::INITIAL,
            "test::Store",
            payload_catalog.clone(),
            empty_composite_catalog(),
            BTreeMap::from([(
                EntityTag::new(7),
                snapshot_with_indexed_enum(&payload_catalog, PAYLOAD_STATUS_KIND),
            )]),
        )
        .is_err(),
        "payload enums must reject before accepted schema publication",
    );
}

#[test]
fn accepted_schema_bundle_round_trips_and_uses_current_header() {
    let expected = bundle(1);
    let candidate = CandidateSchemaRevision::new(expected.clone()).expect("candidate should build");
    let bytes = candidate.encoded_bundle();

    assert_eq!(&bytes[..8], ACCEPTED_SCHEMA_BUNDLE_MAGIC);
    assert_eq!(
        u16::from_be_bytes([bytes[8], bytes[9]]),
        ACCEPTED_SCHEMA_BUNDLE_CODEC_VERSION
    );
    assert_eq!(
        decode_accepted_schema_revision_bundle(bytes).expect("bundle should decode"),
        expected
    );
}

#[test]
fn accepted_schema_empty_bundle_wire_vector_is_frozen() {
    let bundle = AcceptedSchemaRevisionBundle::new(
        AcceptedSchemaRevision::INITIAL,
        "s",
        empty_catalog(),
        empty_composite_catalog(),
        BTreeMap::new(),
    )
    .expect("empty accepted schema bundle should build");

    assert_eq!(
        encode_accepted_schema_revision_bundle(&bundle).expect("bundle should encode"),
        vec![
            0x49, 0x43, 0x59, 0x44, 0x42, 0x41, 0x45, 0x42, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x73, 0x00, 0x00, 0x00, 0x0e, 0x49,
            0x43, 0x59, 0x44, 0x42, 0x45, 0x4e, 0x58, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x0e, 0x49, 0x43, 0x59, 0x44, 0x42, 0x43, 0x4d, 0x50, 0x00, 0x01, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ],
    );
}

#[test]
fn accepted_schema_semantic_fingerprint_excludes_revision() {
    let first = candidate(1);
    let second = candidate(2);

    assert_eq!(first.root.fingerprint, second.root.fingerprint);
    assert_ne!(first.root.bundle_hash, second.root.bundle_hash);
}

#[test]
fn accepted_schema_root_codec_is_fixed_width_and_checksummed() {
    let candidate = candidate(1);
    let bytes = candidate.encoded_root();

    assert_eq!(bytes.len(), ACCEPTED_SCHEMA_ROOT_BYTES);
    assert_eq!(&bytes[..8], ACCEPTED_SCHEMA_ROOT_MAGIC);
    assert_eq!(
        u16::from_be_bytes([bytes[8], bytes[9]]),
        ACCEPTED_SCHEMA_ROOT_CODEC_VERSION
    );

    let mut corrupt = bytes.to_vec();
    corrupt[24] ^= 0x80;
    assert!(select_current_accepted_schema_root([Some(&corrupt), None]).is_err());
}

#[test]
fn accepted_schema_root_wire_vector_is_frozen() {
    let root = AcceptedSchemaRoot {
        revision: AcceptedSchemaRevision::INITIAL,
        fingerprint: AcceptedSchemaFingerprint([0x11; 32]),
        bundle_key: AcceptedSchemaBundleKey(1),
        bundle_hash: [0x22; 32],
    };
    let mut expected = vec![
        0x49, 0x43, 0x59, 0x44, 0x42, 0x41, 0x45, 0x52, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x01,
    ];
    expected.extend_from_slice(&[0x11; 32]);
    expected.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]);
    expected.extend_from_slice(&[0x22; 32]);
    expected.extend_from_slice(&[0x22, 0x63, 0xad, 0x22]);

    assert_eq!(
        encode_accepted_schema_root(root).expect("root should encode"),
        expected
    );
}

#[test]
fn accepted_schema_root_selection_tolerates_one_torn_slot() {
    let first = candidate(1);
    let second = candidate(2);
    let mut torn = second.encoded_root().to_vec();
    torn.truncate(torn.len() - 3);

    let selected = select_current_accepted_schema_root([Some(first.encoded_root()), Some(&torn)])
        .expect("prior valid root should survive a torn inactive slot")
        .expect("one valid root should be selected");

    assert_eq!(selected.slot(), 0);
    assert_eq!(selected.root(), first.root());
}

#[test]
fn accepted_schema_root_selection_chooses_highest_revision() {
    let first = candidate(1);
    let second = candidate(2);

    let selected = select_current_accepted_schema_root([
        Some(first.encoded_root()),
        Some(second.encoded_root()),
    ])
    .expect("root slots should decode")
    .expect("one current root should be selected");

    assert_eq!(selected.slot(), 1);
    assert_eq!(selected.root(), second.root());
}

#[test]
fn accepted_schema_root_selection_rejects_unknown_version_before_fallback() {
    let first = candidate(1);
    let mut future = candidate(2).encoded_root().to_vec();
    future[8..10].copy_from_slice(&(ACCEPTED_SCHEMA_ROOT_CODEC_VERSION + 1).to_be_bytes());
    let checksum = crc32c(&future[..super::ACCEPTED_SCHEMA_ROOT_CHECKSUM_OFFSET]);
    future[super::ACCEPTED_SCHEMA_ROOT_CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_be_bytes());

    assert!(
        select_current_accepted_schema_root([Some(first.encoded_root()), Some(&future)]).is_err()
    );
}

#[test]
fn accepted_schema_root_selection_treats_checksum_invalid_version_as_torn() {
    let first = candidate(1);
    let mut torn = candidate(2).encoded_root().to_vec();
    torn[8..10].copy_from_slice(&(ACCEPTED_SCHEMA_ROOT_CODEC_VERSION + 1).to_be_bytes());

    let selected = select_current_accepted_schema_root([Some(first.encoded_root()), Some(&torn)])
        .expect("checksum-invalid inactive slot should be treated as torn")
        .expect("prior valid root should remain selected");

    assert_eq!(selected.root(), first.root());
}

#[test]
fn accepted_schema_root_publication_checks_revision_and_alternates_slots() {
    let first = candidate(1);
    let initial = prepare_accepted_schema_root_publication(
        [None, None],
        AcceptedSchemaRevision::NONE,
        &first,
    )
    .expect("initial root publication should prepare");
    assert_eq!(initial.target_slot(), 0);
    assert_eq!(initial.encoded_root(), first.encoded_root());

    let second = candidate(2);
    let next = prepare_accepted_schema_root_publication(
        [Some(first.encoded_root()), None],
        AcceptedSchemaRevision::INITIAL,
        &second,
    )
    .expect("next root publication should prepare");
    assert_eq!(next.target_slot(), 1);

    let stale = prepare_accepted_schema_root_publication(
        [Some(first.encoded_root()), None],
        AcceptedSchemaRevision::NONE,
        &second,
    )
    .expect_err("stale expected revision must reject");
    assert_eq!(
        stale,
        AcceptedSchemaPublicationError::StaleSchemaRevision {
            expected: AcceptedSchemaRevision::NONE,
            found: AcceptedSchemaRevision::INITIAL,
        }
    );
}

#[test]
fn accepted_schema_bundle_rejects_trailing_and_future_version_bytes() {
    let candidate = candidate(1);
    let mut trailing = candidate.encoded_bundle().to_vec();
    trailing.push(0);
    assert!(decode_accepted_schema_revision_bundle(&trailing).is_err());

    let mut future = candidate.encoded_bundle().to_vec();
    future[8..10].copy_from_slice(&(ACCEPTED_SCHEMA_BUNDLE_CODEC_VERSION + 1).to_be_bytes());
    assert!(decode_accepted_schema_revision_bundle(&future).is_err());
}

#[test]
fn accepted_schema_bundle_rejects_out_of_order_entity_tags() {
    let candidate = candidate(1);
    let mut bytes = candidate.encoded_bundle().to_vec();
    let entity_count_offset = bytes.len()
        - crate::db::schema::encode_persisted_schema_snapshot(&snapshot("test::Item"))
            .expect("snapshot should encode")
            .len()
        - size_of::<u32>()
        - size_of::<u64>()
        - size_of::<u32>();
    bytes[entity_count_offset..entity_count_offset + 4].copy_from_slice(&2_u32.to_be_bytes());
    let duplicate_entry = bytes[entity_count_offset + 4..].to_vec();
    bytes.extend_from_slice(&duplicate_entry);

    assert!(decode_accepted_schema_revision_bundle(&bytes).is_err());
}
