use super::{
    AcceptedRelationInfo, AcceptedRelationLocalComponentSpec, AcceptedRelationLocalComponents,
    AcceptedRelationTargetIdentity, RelationTargetKeys, ReverseRelationSourceInfo,
    SchemaRelationStageBudget, relation_scalar_slot_fast_path_key_kind_supported,
    reverse_index_key_bounds_for_target_primary_key_value,
    reverse_index_key_for_target_and_source_primary_key_value,
    validate_scalar_relation_target_primary_key_kind,
};
use crate::db::relation::AcceptedRelationCardinality;
use crate::db::{
    Db,
    data::StructuralRowContract,
    index::{IndexEntryValue, IndexId},
    key_taxonomy::{
        CompositePrimaryKeyValue, EncodedIndexComponent, EncodedPrimaryKey, IndexStoreKeyKind,
        PrimaryKeyComponent, PrimaryKeyValue,
    },
    registry::StoreRegistry,
    schema::{
        AcceptedFieldDecodeContract, AcceptedFieldKind, AcceptedRowLayoutRuntimeContract,
        AcceptedSchemaRevision, AcceptedSchemaSnapshot, AcceptedValueCatalogHandle, FieldId,
        PersistedFieldSnapshot, PersistedRelationEdgeSnapshot, PersistedSchemaSnapshot, RelationId,
        SchemaFieldSlot, SchemaInsertDefault, SchemaRowLayout, SchemaVersion,
        enum_catalog::build_initial_accepted_enum_catalog,
    },
};
use crate::error::{ErrorDetail, SchemaTransitionBudgetResource, StoreError};
use crate::model::field::{FieldStorageDecode, LeafCodec, ScalarCodec};
use crate::traits::{CanisterKind, Path};
use crate::types::EntityTag;

#[test]
fn schema_relation_stage_capacity_rejection_preserves_typed_resource() {
    let mut budget = SchemaRelationStageBudget {
        effects: crate::db::schema::MAX_SCHEMA_PROJECTION_ENTRIES,
        projection_work_units: 0,
        staged_raw_bytes: 0,
    };

    let error = budget
        .consume_effect(0, 0)
        .expect_err("one relation effect beyond the exact cap must reject");

    assert!(matches!(
        error.detail(),
        Some(ErrorDetail::Store(
            StoreError::SchemaTransitionBudgetExceeded {
                resource: SchemaTransitionBudgetResource::ProjectionEntries,
            }
        )),
    ));
}

struct RelationTestCanister;

impl Path for RelationTestCanister {
    const PATH: &'static str = "relation::tests::Canister";
}

impl CanisterKind for RelationTestCanister {
    const COMMIT_MEMORY_ID: u8 = 1;
    const COMMIT_STABLE_KEY: &'static str = "icydb.relation_tests.commit.v1";
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
        relation_name: "target_id".to_string(),
        relation_ordinal: field_index,
        physical_generation: 0,
        local_components: AcceptedRelationLocalComponents::scalar(
            field_index,
            test_field_contract("target_id", &field_kind, LeafCodec::Structural),
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
    let [component] = relation.local_components().components() else {
        panic!("scalar relation metadata should expose one local component");
    };

    assert_eq!(component.field_index(), 3);
    assert_eq!(component.field_name(), "target_id");
    std::assert_matches!(component.field_kind(), AcceptedFieldKind::Relation { .. });
}

#[test]
fn accepted_relations_require_registered_target_authority() {
    let relation_kind = AcceptedFieldKind::Relation {
        target_path: "Target".to_string(),
        target_entity_name: "Target".to_string(),
        target_entity_tag: EntityTag::new(77),
        target_store_path: "TargetStore".to_string(),
        key_kind: Box::new(AcceptedFieldKind::Ulid),
    };
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
    )
    .with_relations(vec![PersistedRelationEdgeSnapshot::new(
        RelationId::new(1).expect("test relation identity should be non-zero"),
        "target".to_string(),
        "Target".to_string(),
        vec![FieldId::new(2)],
    )]);
    let accepted = AcceptedSchemaSnapshot::new(snapshot);
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
        .expect("accepted relation runtime contract should build");
    let catalog =
        build_initial_accepted_enum_catalog(&[]).expect("empty accepted enum catalog should build");
    let catalog = AcceptedValueCatalogHandle::new_for_tests(
        catalog,
        crate::db::schema::AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    );
    let row_contract = StructuralRowContract::from_accepted_decode_contract(
        "Source",
        descriptor.row_decode_contract(catalog),
    );

    let db: Db<RelationTestCanister> = Db::new_with_hooks(&TEST_REGISTRY, &[]);
    super::accepted_relations_for_row_contract(&db, "Source", &row_contract, None)
        .expect_err("accepted relation targets must have registered runtime authority");
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
        relation_name: "target_id".to_string(),
        relation_ordinal: 3,
        physical_generation: 0,
        local_components: AcceptedRelationLocalComponents::scalar(
            3,
            test_field_contract("target_id", &field_kind, LeafCodec::Structural),
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
        path: "Source",
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
            source,
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

        assert_eq!(
            decoded.key_kind(),
            IndexStoreKeyKind::System,
            "reverse indexes use system key kind",
        );
        assert_eq!(
            decoded.index_id(),
            IndexId::new(
                EntityTag::new(9),
                u16::try_from(ordinal).expect("test ordinal fits u16"),
            )
        );
        assert_eq!(decoded.components(), &[expected_component]);
        assert_eq!(
            decoded.primary_key().decode().expect("source key decodes"),
            source_primary_key,
        );

        let bounds =
            reverse_index_key_bounds_for_target_primary_key_value(source, &relation, &target_key)
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
        path: "Source",
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
        source,
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

    assert_eq!(decoded.components(), &[expected_component]);
    assert_eq!(
        decoded.primary_key().decode().expect("source key decodes"),
        source_primary_key,
    );

    let bounds =
        reverse_index_key_bounds_for_target_primary_key_value(source, &relation, &target_key)
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
        path: "Source",
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
            source, &relation, target, source_key,
        )
        .expect("reverse key should build")
        .expect("relation target key should encode")
        .as_bytes()
        .len()
    };

    assert_eq!(
        raw_len(&scalar_target, &scalar_source),
        42,
        "scalar reverse keys include the isolated physical generation"
    );
    assert_eq!(
        raw_len(&composite_target, &scalar_source),
        53,
        "composite target overhead should equal its encoded PK width"
    );
    assert_eq!(
        raw_len(&scalar_target, &composite_source),
        53,
        "composite source overhead should equal its encoded PK suffix width"
    );
    assert_eq!(
        raw_len(&composite_target, &composite_source),
        64,
        "composite target/source overhead should remain additive"
    );
    assert_eq!(
        raw_len(&int128_target, &scalar_source),
        50,
        "fixed 128-bit target lanes should add their fixed encoded width"
    );
    assert_eq!(
        IndexEntryValue::presence().len(),
        1,
        "reverse-index entry values remain presence witnesses; row identity stays key-owned"
    );
}
