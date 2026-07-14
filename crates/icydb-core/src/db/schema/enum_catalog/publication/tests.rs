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
            AcceptedFieldKind, FieldId, PersistedFieldSnapshot, PersistedIndexFieldPathSnapshot,
            PersistedIndexKeySnapshot, PersistedIndexSnapshot, PersistedNestedLeafSnapshot,
            PersistedSchemaSnapshot, SchemaFieldDefault, SchemaFieldSlot, SchemaRowLayout,
            SchemaVersion,
        },
    },
    model::field::{EnumVariantModel, FieldKind, FieldStorageDecode, LeafCodec, ScalarCodec},
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

fn empty_catalog() -> super::AcceptedEnumCatalog {
    super::AcceptedEnumCatalog {
        by_id: BTreeMap::new(),
        id_by_path: BTreeMap::new(),
    }
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
        SchemaFieldDefault::None,
    )
}

fn snapshot_with_field_and_default(
    entity_path: &str,
    kind: AcceptedFieldKind,
    nested_leaves: Vec<PersistedNestedLeafSnapshot>,
    leaf_codec: LeafCodec,
    default: SchemaFieldDefault,
) -> PersistedSchemaSnapshot {
    PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        entity_path.to_string(),
        "Item".to_string(),
        FieldId::new(1),
        SchemaRowLayout::new(
            SchemaVersion::initial(),
            vec![(FieldId::new(1), SchemaFieldSlot::new(0))],
        ),
        vec![PersistedFieldSnapshot::new(
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
        SchemaRowLayout::new(
            SchemaVersion::initial(),
            vec![(FieldId::new(1), SchemaFieldSlot::new(0))],
        ),
        vec![PersistedFieldSnapshot::new(
            FieldId::new(1),
            "status".to_string(),
            SchemaFieldSlot::new(0),
            persisted_kind.clone(),
            Vec::new(),
            false,
            SchemaFieldDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::StructuralFallback,
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

fn snapshot_with_nested_status(kind: AcceptedFieldKind) -> PersistedSchemaSnapshot {
    let nested = PersistedNestedLeafSnapshot::new(
        vec!["status".to_string()],
        kind,
        false,
        FieldStorageDecode::ByKind,
        LeafCodec::StructuralFallback,
    );
    snapshot_with_field(
        "test::Item",
        AcceptedFieldKind::Structured { queryable: true },
        vec![nested],
        LeafCodec::StructuralFallback,
    )
}

fn bundle(revision: u64) -> AcceptedSchemaRevisionBundle {
    AcceptedSchemaRevisionBundle::new(
        AcceptedSchemaRevision::new(revision),
        "test::Store",
        empty_catalog(),
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
        LeafCodec::StructuralFallback,
    );
    assert!(
        AcceptedSchemaRevisionBundle::new(
            AcceptedSchemaRevision::INITIAL,
            "test::Store",
            catalog.clone(),
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
        LeafCodec::StructuralFallback,
    );
    assert!(
        AcceptedSchemaRevisionBundle::new(
            AcceptedSchemaRevision::INITIAL,
            "test::Store",
            catalog,
            BTreeMap::from([(EntityTag::new(7), mismatched)]),
        )
        .is_err()
    );
}

#[test]
fn accepted_schema_bundle_rejects_malformed_default_payload() {
    let snapshot = snapshot_with_field_and_default(
        "test::Item",
        AcceptedFieldKind::Ulid,
        Vec::new(),
        LeafCodec::Scalar(ScalarCodec::Ulid),
        SchemaFieldDefault::SlotPayload(vec![0xFE]),
    );

    assert!(
        AcceptedSchemaRevisionBundle::new(
            AcceptedSchemaRevision::INITIAL,
            "test::Store",
            empty_catalog(),
            BTreeMap::from([(EntityTag::new(7), snapshot)]),
        )
        .is_err()
    );
}

#[test]
fn accepted_schema_bundle_checks_nested_enum_type_ids() {
    let catalog = status_catalog();
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
            BTreeMap::from([(EntityTag::new(7), snapshot_with_nested_status(status_kind),)]),
        )
        .is_ok()
    );

    assert!(
        AcceptedSchemaRevisionBundle::new(
            AcceptedSchemaRevision::INITIAL,
            "test::Store",
            catalog,
            BTreeMap::from([(
                EntityTag::new(7),
                snapshot_with_nested_status(AcceptedFieldKind::Enum {
                    type_id: EnumTypeId::new(999).expect("test enum type ID should be valid"),
                }),
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
        BTreeMap::new(),
    )
    .expect("empty accepted schema bundle should build");

    assert_eq!(
        encode_accepted_schema_revision_bundle(&bundle).expect("bundle should encode"),
        vec![
            0x49, 0x43, 0x59, 0x44, 0x42, 0x41, 0x53, 0x42, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x73, 0x00, 0x00, 0x00, 0x0e, 0x49,
            0x43, 0x59, 0x44, 0x42, 0x45, 0x4e, 0x43, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00,
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
        0x49, 0x43, 0x59, 0x44, 0x42, 0x41, 0x53, 0x52, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x01,
    ];
    expected.extend_from_slice(&[0x11; 32]);
    expected.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]);
    expected.extend_from_slice(&[0x22; 32]);
    expected.extend_from_slice(&[0xaa, 0x2c, 0xa2, 0x69]);

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
fn accepted_schema_bundle_rejects_trailing_and_noncurrent_bytes() {
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
