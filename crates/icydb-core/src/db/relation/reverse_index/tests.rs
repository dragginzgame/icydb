use super::{
    AcceptedRelationInfo, AcceptedRelationLocalComponentSpec, AcceptedRelationLocalComponents,
    AcceptedRelationReverseIdentity, AcceptedRelationSource, AcceptedRelationTargetIdentity,
    MAX_NESTED_RELATION_IMAGE_RAW_REFERENCES, MAX_NESTED_RELATION_IMAGE_TRAVERSAL_WORK,
    MAX_RELATION_BATCH_RAW_REFERENCES, MAX_RELATION_BATCH_REVERSE_DELTAS,
    MAX_RELATION_BATCH_TRAVERSAL_WORK, MAX_RELATION_BATCH_UNIQUE_TARGET_LOOKUPS,
    RelationCommitBudget, RelationProjectionBudget, RelationTargetKeys, ReverseRelationSourceInfo,
    relation_scalar_slot_fast_path_key_kind_supported,
    reverse_index_key_bounds_for_target_primary_key_value,
    reverse_index_key_for_target_and_source_primary_key_value,
    validate_scalar_relation_target_primary_key_kind,
};
use crate::db::relation::AcceptedRelationCardinality;
use crate::db::schema::{FieldStorageDecode, LeafCodec, ScalarCodec};
use crate::db::{
    Db,
    data::{RawDataStoreKey, StructuralRowContract},
    index::{IndexEntryValue, IndexId},
    key_taxonomy::{
        CompositePrimaryKeyValue, EncodedIndexComponent, EncodedPrimaryKey, IndexStoreKeyKind,
        PrimaryKeyComponent, PrimaryKeyValue,
    },
    registry::StoreRegistry,
    schema::{
        AcceptedConstraintCatalog, AcceptedConstraintIdentity, AcceptedFieldDecodeContract,
        AcceptedFieldKind, AcceptedRowLayoutRuntimeContract, AcceptedSchemaRevision,
        AcceptedSchemaSnapshot, AcceptedValueCatalogHandle, ConstraintId, FieldId,
        PersistedFieldSnapshot, PersistedRelationEdgeSnapshot, PersistedSchemaSnapshot, RelationId,
        SchemaFieldSlot, SchemaInsertDefault, SchemaRowLayout, SchemaVersion,
        empty_accepted_enum_catalog_for_tests,
    },
};
use crate::traits::{CanisterKind, Path};
use crate::types::EntityTag;

struct RelationTestCanister;

impl Path for RelationTestCanister {
    const PATH: &'static str = "relation::tests::Canister";
}

impl CanisterKind for RelationTestCanister {
    const COMMIT_MEMORY_ID: u8 = 1;
    const COMMIT_STABLE_KEY: &'static str = "icydb.relation_tests.commit.v1";
    const STARTUP_MEMORY_ID: u8 = 3;
    const STARTUP_STABLE_KEY: &'static str = "icydb.relation_tests.startup.control.v1";
    const INTEGRITY_PROGRESS_MEMORY_ID: u8 = 2;
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
        "icydb.relation_tests.integrity.progress.v1";
}

thread_local! {
    static TEST_REGISTRY: StoreRegistry = StoreRegistry::new();
}

fn test_field_contract<'a>(
    name: &'a str,
    kind: &'a AcceptedFieldKind,
    leaf_codec: LeafCodec,
) -> AcceptedFieldDecodeContract<'a> {
    AcceptedFieldDecodeContract::new(name, kind, false, FieldStorageDecode::ByKind, leaf_codec)
}

fn relation(field_index: usize, key_kind: AcceptedFieldKind) -> AcceptedRelationInfo {
    let field_kind = AcceptedFieldKind::Relation {
        target_path: "Target".to_string(),
        target_entity_name: "Target".to_string(),
        target_entity_tag: EntityTag::new(77),
        target_store_path: "TargetStore".to_string(),
        key_kind: Box::new(key_kind.clone()),
    };

    AcceptedRelationInfo {
        constraint: AcceptedConstraintIdentity::new(
            ConstraintId::new(3).expect("test constraint identity should be non-zero"),
        ),
        reverse_identity: AcceptedRelationReverseIdentity::new(
            RelationId::new(1).expect("test relation identity should be non-zero"),
            0,
        ),
        relation_name: "target_id".to_string(),
        source_field_index: field_index,
        source: AcceptedRelationSource::Direct(
            AcceptedRelationLocalComponents::scalar(
                field_index,
                test_field_contract("target_id", &field_kind, LeafCodec::Structural),
            )
            .expect("test scalar relation component should build"),
        ),
        target: AcceptedRelationTargetIdentity::try_new(
            "Source",
            "target_id",
            "Target",
            "Target",
            EntityTag::new(77),
            "TargetStore",
            std::slice::from_ref(&key_kind),
        )
        .expect("target identity should build"),
        cardinality: AcceptedRelationCardinality::Single,
    }
}

#[test]
fn accepted_relation_target_identity_carries_ordered_primary_key_metadata() {
    let relation = relation(3, AcceptedFieldKind::Nat64);

    assert_eq!(
        relation.target().primary_key().component_kinds(),
        &[AcceptedFieldKind::Nat64],
        "current scalar relation metadata is represented as a one-component target primary key",
    );
}

#[test]
fn accepted_relation_target_identity_can_carry_ordered_composite_metadata() {
    let target = AcceptedRelationTargetIdentity::try_new(
        "Source",
        "target_id",
        "Target",
        "Target",
        EntityTag::new(77),
        "TargetStore",
        &[AcceptedFieldKind::Nat64, AcceptedFieldKind::Ulid],
    )
    .expect("target identity should build");

    assert_eq!(
        target.primary_key().component_kinds(),
        &[AcceptedFieldKind::Nat64, AcceptedFieldKind::Ulid],
    );
}

#[test]
fn accepted_relation_target_identity_rejects_empty_primary_key_metadata() {
    AcceptedRelationTargetIdentity::try_new(
        "Source",
        "target_id",
        "Target",
        "Target",
        EntityTag::new(77),
        "TargetStore",
        &[],
    )
    .expect_err("relation target identity must fail closed without PK metadata");
}

#[test]
fn relation_target_keys_make_none_one_and_many_explicit() {
    assert!(
        !RelationTargetKeys::none()
            .contains(&PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(1),))
    );

    let key = PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(7));
    let one = RelationTargetKeys::one(&key);
    assert!(one.contains(&PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(7))));
    assert_eq!(one.into_values().len(), 1);

    let many = RelationTargetKeys::from_scalar_components(vec![
        PrimaryKeyComponent::Nat64(7),
        PrimaryKeyComponent::Nat64(8),
    ]);
    assert!(many.contains(&PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(8))));
    assert_eq!(many.into_values().len(), 2);
}

#[test]
fn accepted_relation_info_carries_ordered_local_component_metadata() {
    let relation = relation(3, AcceptedFieldKind::Nat64);
    let [component] = relation
        .local_components()
        .expect("direct relation should expose local components")
        .components()
    else {
        panic!("scalar relation metadata should expose one local component");
    };

    assert_eq!(component.field_index(), 3);
    assert_eq!(component.field_name(), "target_id");
    std::assert_matches!(component.field_kind(), AcceptedFieldKind::Relation { .. });
}

#[test]
fn accepted_relation_violation_preserves_catalog_identity() {
    let relation = relation(3, AcceptedFieldKind::Nat64);
    let error = relation.write_violation(
        [0x44; 16],
        EntityTag::new(9),
        Some(crate::error::MutationDiagnosticContext::new(
            9,
            icydb_diagnostic_code::DiagnosticMutationOperation::Delete,
            6,
        )),
    );
    let facts = error.diagnostic_facts();
    assert!(facts.contains(&(icydb_diagnostic_code::DiagnosticFactTag::ConstraintId, 3,)));
    assert!(facts.contains(&(
        icydb_diagnostic_code::DiagnosticFactTag::ConstraintKind,
        icydb_diagnostic_code::DiagnosticConstraintKind::Relation.raw(),
    )));
    assert!(facts.contains(&(icydb_diagnostic_code::DiagnosticFactTag::EntityTag, 9,)));
    assert!(facts.contains(&(
        icydb_diagnostic_code::DiagnosticFactTag::MutationOperation,
        icydb_diagnostic_code::DiagnosticMutationOperation::Delete.raw(),
    )));
    assert!(facts.contains(&(icydb_diagnostic_code::DiagnosticFactTag::BatchPosition, 6,)));
}

#[test]
fn accepted_relations_require_accepted_target_authority() {
    let relation_kind = AcceptedFieldKind::Relation {
        target_path: "Target".to_string(),
        target_entity_name: "Target".to_string(),
        target_entity_tag: EntityTag::new(77),
        target_store_path: "TargetStore".to_string(),
        key_kind: Box::new(AcceptedFieldKind::Ulid),
    };
    let relation = PersistedRelationEdgeSnapshot::new_direct(
        RelationId::new(1).expect("test relation identity should be non-zero"),
        "target".to_string(),
        "Target".to_string(),
        vec![FieldId::new(2)],
    );
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "Source".to_string(),
        "Source".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(4)),
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
                "target_id".to_string(),
                SchemaFieldSlot::new(4),
                relation_kind,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Structural,
            ),
        ],
    );
    let constraint_catalog = AcceptedConstraintCatalog::initial(
        snapshot.fields(),
        snapshot.indexes(),
        std::slice::from_ref(&relation),
    )
    .expect("test relation constraint should close");
    let accepted = AcceptedSchemaSnapshot::new(
        snapshot
            .with_relations(vec![relation])
            .with_constraint_catalog(constraint_catalog),
    );
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
        .expect("accepted relation runtime contract should build");
    let catalog = empty_accepted_enum_catalog_for_tests();
    let catalog = AcceptedValueCatalogHandle::new_for_tests(
        catalog,
        crate::db::schema::AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    );
    let row_contract = StructuralRowContract::from_accepted_decode_contract(
        "Source",
        descriptor.row_decode_contract(catalog),
    );

    let db: Db<RelationTestCanister> = Db::new(
        &TEST_REGISTRY,
        crate::db::RequestExecutionRoot::__new_runtime_root().scope(),
    );
    super::accepted_relations_for_row_contract(&db, "Source", &row_contract, None)
        .expect_err("accepted relation targets must exist in the current accepted catalog");
}

#[test]
fn accepted_relation_local_components_can_carry_ordered_tuple_metadata() {
    let tenant_kind = AcceptedFieldKind::Nat64;
    let local_kind = AcceptedFieldKind::Ulid;

    let components = AcceptedRelationLocalComponents::try_from_component_specs(&[
        AcceptedRelationLocalComponentSpec {
            index: 2,
            field: test_field_contract(
                "tenant_id",
                &tenant_kind,
                LeafCodec::Scalar(ScalarCodec::Nat64),
            ),
        },
        AcceptedRelationLocalComponentSpec {
            index: 4,
            field: test_field_contract(
                "local_id",
                &local_kind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
        },
    ])
    .expect("ordered local component tuple should build");

    let [tenant, local] = components.components() else {
        panic!("tuple relation metadata should expose both local components");
    };
    assert_eq!(tenant.field_index(), 2);
    assert_eq!(tenant.field_name(), "tenant_id");
    assert_eq!(tenant.field_kind(), &AcceptedFieldKind::Nat64);
    assert_eq!(local.field_index(), 4);
    assert_eq!(local.field_name(), "local_id");
    assert_eq!(local.field_kind(), &AcceptedFieldKind::Ulid);
}

#[test]
fn accepted_relation_local_components_reject_empty_metadata() {
    AcceptedRelationLocalComponents::try_from_component_specs(&[])
        .expect_err("relation local component metadata must fail closed when empty");
}

#[test]
fn relation_validation_rejects_local_target_component_arity_mismatch() {
    let field_kind = AcceptedFieldKind::Relation {
        target_path: "Target".to_string(),
        target_entity_name: "Target".to_string(),
        target_entity_tag: EntityTag::new(77),
        target_store_path: "TargetStore".to_string(),
        key_kind: Box::new(AcceptedFieldKind::Nat64),
    };
    let relation = AcceptedRelationInfo {
        constraint: AcceptedConstraintIdentity::new(
            ConstraintId::new(3).expect("test constraint identity should be non-zero"),
        ),
        reverse_identity: AcceptedRelationReverseIdentity::new(
            RelationId::new(1).expect("test relation identity should be non-zero"),
            0,
        ),
        relation_name: "target_id".to_string(),
        source_field_index: 3,
        source: AcceptedRelationSource::Direct(
            AcceptedRelationLocalComponents::scalar(
                3,
                test_field_contract("target_id", &field_kind, LeafCodec::Structural),
            )
            .expect("test scalar relation component should build"),
        ),
        target: AcceptedRelationTargetIdentity::try_new(
            "Source",
            "target_id",
            "Target",
            "Target",
            EntityTag::new(77),
            "TargetStore",
            &[AcceptedFieldKind::Nat64, AcceptedFieldKind::Ulid],
        )
        .expect("target identity should build"),
        cardinality: AcceptedRelationCardinality::Single,
    };

    validate_scalar_relation_target_primary_key_kind(&relation)
        .expect_err("single local field must not validate against composite target metadata");
}

#[test]
fn scalar_relation_target_key_kind_validation_accepts_128_bit_lanes() {
    for key_kind in [AcceptedFieldKind::Int128, AcceptedFieldKind::Nat128] {
        let relation = relation(3, key_kind);

        validate_scalar_relation_target_primary_key_kind(&relation)
            .expect("128-bit scalar relation target key kinds should validate");
    }
}

#[test]
fn relation_scalar_slot_fast_path_excludes_structural_128_bit_lanes() {
    for key_kind in [
        AcceptedFieldKind::Int64,
        AcceptedFieldKind::Nat64,
        AcceptedFieldKind::Ulid,
    ] {
        let relation = relation(3, key_kind);
        assert!(
            relation_scalar_slot_fast_path_key_kind_supported(
                relation
                    .scalar_relation_field_kind()
                    .expect("scalar relation kind"),
            ),
            "scalar-slot relation key kinds should stay on the fast path",
        );
    }

    for key_kind in [AcceptedFieldKind::Int128, AcceptedFieldKind::Nat128] {
        let relation = relation(3, key_kind);
        assert!(
            !relation_scalar_slot_fast_path_key_kind_supported(
                relation
                    .scalar_relation_field_kind()
                    .expect("scalar relation kind"),
            ),
            "128-bit relation key kinds use structural field-bytes decoding",
        );
    }
}

#[test]
fn reverse_relation_keys_accept_128_bit_target_primary_key_components() {
    let source = ReverseRelationSourceInfo {
        path: "Source".into(),
        entity_tag: EntityTag::new(9),
    };
    let source_primary_key = PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(44));

    for (ordinal, key_kind, target_component) in [
        (
            3,
            AcceptedFieldKind::Int128,
            PrimaryKeyComponent::Int128(i128::MIN + 91),
        ),
        (
            4,
            AcceptedFieldKind::Nat128,
            PrimaryKeyComponent::Nat128(u128::MAX - 91),
        ),
    ] {
        let relation = relation(ordinal, key_kind);
        let target_key = PrimaryKeyValue::Scalar(target_component);
        let raw = reverse_index_key_for_target_and_source_primary_key_value(
            &source,
            &relation,
            &target_key,
            &source_primary_key,
        )
        .expect("reverse key should build")
        .expect("128-bit target component should be index encodable");
        let decoded = raw.decode().expect("reverse key should decode");
        let expected_component = EncodedIndexComponent::from_canonical_bytes(
            EncodedPrimaryKey::encode(target_key)
                .expect("target primary key should encode")
                .as_bytes()
                .to_vec(),
        );
        let expected_relation =
            EncodedIndexComponent::from_canonical_bytes(1_u32.to_be_bytes().to_vec());

        assert_eq!(
            decoded.key_kind(),
            IndexStoreKeyKind::System,
            "reverse indexes use system key kind",
        );
        assert_eq!(
            decoded.index_id(),
            IndexId::new(EntityTag::new(9), u16::MAX)
        );
        assert_eq!(
            decoded.components(),
            &[expected_relation, expected_component]
        );
        assert_eq!(
            decoded.primary_key().decode().expect("source key decodes"),
            source_primary_key,
        );

        let bounds =
            reverse_index_key_bounds_for_target_primary_key_value(&source, &relation, &target_key)
                .expect("reverse bounds should build");
        assert!(
            bounds.is_some(),
            "128-bit target component should produce reverse index bounds",
        );
    }
}

#[test]
fn reverse_relation_keys_encode_full_composite_target_primary_key_identity() {
    let source = ReverseRelationSourceInfo {
        path: "Source".into(),
        entity_tag: EntityTag::new(9),
    };
    let relation = relation(5, AcceptedFieldKind::Nat64);
    let source_primary_key = PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(44));
    let target_key = PrimaryKeyValue::Composite(
        CompositePrimaryKeyValue::try_from_components(&[
            PrimaryKeyComponent::Nat64(7),
            PrimaryKeyComponent::Ulid(crate::types::Ulid::from_bytes([9; 16])),
        ])
        .expect("composite target key should build"),
    );

    let raw = reverse_index_key_for_target_and_source_primary_key_value(
        &source,
        &relation,
        &target_key,
        &source_primary_key,
    )
    .expect("reverse key should build")
    .expect("composite target identity should be index encodable");
    let decoded = raw.decode().expect("reverse key should decode");
    let expected_component = EncodedIndexComponent::from_canonical_bytes(
        EncodedPrimaryKey::encode(target_key)
            .expect("target primary key should encode")
            .as_bytes()
            .to_vec(),
    );
    let expected_relation =
        EncodedIndexComponent::from_canonical_bytes(1_u32.to_be_bytes().to_vec());

    assert_eq!(
        decoded.components(),
        &[expected_relation, expected_component]
    );
    assert_eq!(
        decoded.primary_key().decode().expect("source key decodes"),
        source_primary_key,
    );

    let bounds =
        reverse_index_key_bounds_for_target_primary_key_value(&source, &relation, &target_key)
            .expect("reverse bounds should build")
            .expect("composite target identity should produce reverse index bounds");

    assert!(
        raw.as_bytes() >= bounds.0.as_bytes() && raw.as_bytes() < bounds.1.as_bytes(),
        "reverse bounds should cover the full composite target identity"
    );
}

#[test]
fn reverse_relation_key_size_evidence_is_linear_in_source_and_target_identity() {
    let source = ReverseRelationSourceInfo {
        path: "Source".into(),
        entity_tag: EntityTag::new(9),
    };
    let relation = relation(5, AcceptedFieldKind::Nat64);
    let scalar_target = PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(7));
    let scalar_source = PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(44));
    let composite_target = PrimaryKeyValue::Composite(
        CompositePrimaryKeyValue::try_from_components(&[
            PrimaryKeyComponent::Nat64(7),
            PrimaryKeyComponent::Nat64(8),
        ])
        .expect("composite target key should build"),
    );
    let composite_source = PrimaryKeyValue::Composite(
        CompositePrimaryKeyValue::try_from_components(&[
            PrimaryKeyComponent::Nat64(44),
            PrimaryKeyComponent::Nat64(45),
        ])
        .expect("composite source key should build"),
    );
    let int128_target = PrimaryKeyValue::Scalar(PrimaryKeyComponent::Int128(i128::MIN + 91));

    let raw_len = |target: &PrimaryKeyValue, source_key: &PrimaryKeyValue| {
        reverse_index_key_for_target_and_source_primary_key_value(
            &source, &relation, target, source_key,
        )
        .expect("reverse key should build")
        .expect("relation target key should encode")
        .as_bytes()
        .len()
    };

    assert_eq!(
        raw_len(&scalar_target, &scalar_source),
        48,
        "scalar reverse keys include exact relation identity and physical generation"
    );
    assert_eq!(
        raw_len(&composite_target, &scalar_source),
        59,
        "composite target overhead should equal its encoded PK width"
    );
    assert_eq!(
        raw_len(&scalar_target, &composite_source),
        59,
        "composite source overhead should equal its encoded PK suffix width"
    );
    assert_eq!(
        raw_len(&composite_target, &composite_source),
        70,
        "composite target/source overhead should remain additive"
    );
    assert_eq!(
        raw_len(&int128_target, &scalar_source),
        56,
        "fixed 128-bit target lanes should add their fixed encoded width"
    );
    assert_eq!(
        IndexEntryValue::presence().len(),
        1,
        "reverse-index entry values remain presence witnesses; row identity stays key-owned"
    );
}

#[test]
fn reverse_relation_domains_are_owned_by_exact_relation_id_not_source_slot() {
    let source = ReverseRelationSourceInfo {
        path: "Source".into(),
        entity_tag: EntityTag::new(9),
    };
    let first = relation(5, AcceptedFieldKind::Nat64);
    let mut second = relation(5, AcceptedFieldKind::Nat64);
    second.reverse_identity = AcceptedRelationReverseIdentity::new(
        RelationId::new(2).expect("test relation identity should be non-zero"),
        second.physical_generation(),
    );
    let target_key = PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(7));
    let source_key = PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(44));

    let first_key = reverse_index_key_for_target_and_source_primary_key_value(
        &source,
        &first,
        &target_key,
        &source_key,
    )
    .expect("first reverse key should build")
    .expect("first reverse key should encode");
    let second_key = reverse_index_key_for_target_and_source_primary_key_value(
        &source,
        &second,
        &target_key,
        &source_key,
    )
    .expect("second reverse key should build")
    .expect("second reverse key should encode");

    assert_ne!(first_key, second_key);
    assert_eq!(
        first_key
            .decode()
            .expect("first key should decode")
            .index_id(),
        second_key
            .decode()
            .expect("second key should decode")
            .index_id(),
        "relations share one reserved physical system domain",
    );
    assert_eq!(
        first_key
            .decode()
            .expect("first key should decode")
            .components()[0]
            .as_bytes(),
        1_u32.to_be_bytes(),
    );
    assert_eq!(
        second_key
            .decode()
            .expect("second key should decode")
            .components()[0]
            .as_bytes(),
        2_u32.to_be_bytes(),
    );
}

#[test]
fn relation_projection_and_batch_counters_accept_exact_limits_and_reject_next_unit() {
    let mut projection = RelationProjectionBudget::default();
    let mut batch = RelationCommitBudget::default();
    projection
        .charge_traversal(
            &mut batch,
            usize::try_from(MAX_NESTED_RELATION_IMAGE_TRAVERSAL_WORK)
                .expect("traversal limit fits usize"),
        )
        .expect("exact image and batch traversal limit should pass");
    assert_eq!(batch.traversal_work, MAX_RELATION_BATCH_TRAVERSAL_WORK);
    assert!(projection.charge_traversal(&mut batch, 1).is_err());

    let mut projection = RelationProjectionBudget::default();
    let mut batch = RelationCommitBudget::default();
    projection
        .charge_nested_references(
            &mut batch,
            usize::try_from(MAX_NESTED_RELATION_IMAGE_RAW_REFERENCES)
                .expect("reference limit fits usize"),
        )
        .expect("exact image and batch reference limit should pass");
    assert_eq!(batch.raw_references, MAX_RELATION_BATCH_RAW_REFERENCES);
    assert!(projection.charge_nested_references(&mut batch, 1).is_err());
}

#[test]
fn relation_batch_bounds_unique_target_lookups_and_coalesced_reverse_deltas() {
    let mut budget = RelationCommitBudget::default();
    let missing = RawDataStoreKey::from_persisted_bytes(77_u64.to_be_bytes().to_vec());
    assert_eq!(
        budget
            .validate_target_once(missing.clone(), |_| Ok(false))
            .expect("a missing target should remain reportable"),
        Some(missing),
    );
    assert!(budget.validated_target_keys.is_empty());

    for value in 0..MAX_RELATION_BATCH_UNIQUE_TARGET_LOOKUPS {
        let key = RawDataStoreKey::from_persisted_bytes(value.to_be_bytes().to_vec());
        assert!(
            budget
                .validate_target_once(key, |_| Ok(true))
                .expect("each distinct lookup through the exact limit should pass")
                .is_none()
        );
    }
    let duplicate = RawDataStoreKey::from_persisted_bytes(
        (MAX_RELATION_BATCH_UNIQUE_TARGET_LOOKUPS - 1)
            .to_be_bytes()
            .to_vec(),
    );
    assert!(
        budget
            .validate_target_once(duplicate, |_| Ok(true))
            .expect("a previously validated target should not consume another lookup")
            .is_none()
    );
    assert_eq!(
        budget.validated_target_keys.len(),
        usize::try_from(MAX_RELATION_BATCH_UNIQUE_TARGET_LOOKUPS).expect("lookup limit fits usize")
    );
    let over = RawDataStoreKey::from_persisted_bytes(
        MAX_RELATION_BATCH_UNIQUE_TARGET_LOOKUPS
            .to_be_bytes()
            .to_vec(),
    );
    assert!(budget.validate_target_once(over, |_| Ok(true)).is_err());

    let mut budget = RelationCommitBudget::default();
    for _ in 0..MAX_RELATION_BATCH_REVERSE_DELTAS {
        budget
            .charge_reverse_delta()
            .expect("each distinct reverse delta through the exact limit should pass");
    }
    assert!(budget.charge_reverse_delta().is_err());
}
