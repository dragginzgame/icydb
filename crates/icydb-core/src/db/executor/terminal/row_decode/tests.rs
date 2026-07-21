use super::*;
use crate::{
    db::{
        codec::serialize_row_payload,
        data::{
            CanonicalRow, RawRow, SlotReader, encode_structural_value_storage_bytes,
            with_structural_read_metrics,
        },
        schema::{
            AcceptedEnumCatalogHandle, AcceptedFieldKind, AcceptedRowLayoutRuntimeContract,
            AcceptedSchemaRevision, AcceptedSchemaSnapshot, FieldId, PersistedFieldSnapshot,
            PersistedSchemaSnapshot, SchemaFieldSlot, SchemaRowLayout, SchemaVersion,
            compiled_schema_proposal_for_model,
        },
    },
    entity::EntityDeclaration,
    error::{ErrorClass, ErrorOrigin, InternalError},
    model::{
        entity::EntityModel,
        field::{FieldKind, FieldStorageDecode, LeafCodec, ScalarCodec},
    },
    types::{Blob, Text},
    value::Value,
};
use icydb_derive::{FieldProjection, PersistedRow};
use serde::Deserialize;

crate::test_canister! {
    ident = RowDecodeCanister,
    commit_memory_id = crate::testing::test_commit_memory_id(),
}

crate::test_store! {
    ident = RowDecodeStore,
    canister = RowDecodeCanister,
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct RowDecodeEntity {
    id: Ulid,
    title: Text,
    tags: Vec<Text>,
    portrait: Blob,
}

crate::test_entity! {
    ident = RowDecodeEntity,
    entity_name = "RowDecodeEntity",
    tag = crate::testing::PROBE_ENTITY_TAG,
    store = RowDecodeStore,
    canister = RowDecodeCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { title: Text => FieldKind::Text { max_len: None } },
        crate::test_field! { tags: Vec<Text> => FieldKind::List(&FieldKind::Text { max_len: None }) },
        crate::test_field! { portrait: Blob => FieldKind::Blob { max_len: None } },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(id),
}

fn decode_test_row(entity: &RowDecodeEntity) -> KernelRow {
    let key = crate::db::data::DecodedDataStoreKey::try_new::<RowDecodeEntity>(entity.id)
        .expect("test key construction should succeed");
    let row = CanonicalRow::from_entity_with_model_proposal_for_test(entity)
        .expect("test row serialization should succeed")
        .into_raw_row();

    RowDecoder::structural()
        .decode(
            &RowLayout::from_model_proposal_for_test(RowDecodeEntity::MODEL),
            (key, row),
        )
        .expect("structural row decode should succeed")
}

fn decode_test_row_with_metrics(
    entity: &RowDecodeEntity,
) -> (KernelRow, crate::db::data::StructuralReadMetrics) {
    let key = crate::db::data::DecodedDataStoreKey::try_new::<RowDecodeEntity>(entity.id)
        .expect("test key construction should succeed");
    let row = CanonicalRow::from_entity_with_model_proposal_for_test(entity)
        .expect("test row serialization should succeed")
        .into_raw_row();
    let layout = RowLayout::from_model_proposal_for_test(RowDecodeEntity::MODEL);

    with_structural_read_metrics(|| {
        RowDecoder::structural()
            .decode(&layout, (key, row))
            .expect("structural row decode should succeed")
    })
}

fn decode_required_test_slots_with_metrics(
    entity: &RowDecodeEntity,
    required_slots: &[usize],
) -> (Vec<Option<Value>>, crate::db::data::StructuralReadMetrics) {
    let key = crate::db::data::DecodedDataStoreKey::try_new::<RowDecodeEntity>(entity.id)
        .expect("test key construction should succeed");
    let row = CanonicalRow::from_entity_with_model_proposal_for_test(entity)
        .expect("test row serialization should succeed")
        .into_raw_row();

    with_structural_read_metrics(|| {
        RowDecoder::structural()
            .decode_slots(
                &RowLayout::from_model_proposal_for_test(RowDecodeEntity::MODEL),
                &key.primary_key_value(),
                &row,
                Some(required_slots),
            )
            .expect("selective slot decode should succeed")
    })
}

fn row_decode_schema_snapshot() -> PersistedSchemaSnapshot {
    let proposal = compiled_schema_proposal_for_model(RowDecodeEntity::MODEL);

    proposal.initial_persisted_schema_snapshot()
}

fn accepted_row_decode_schema() -> AcceptedSchemaSnapshot {
    AcceptedSchemaSnapshot::new(row_decode_schema_snapshot())
}

fn accepted_enum_catalog_handle(models: &[&'static EntityModel]) -> AcceptedEnumCatalogHandle {
    let (catalog, composite_catalog) =
        crate::db::schema::build_initial_accepted_catalogs_for_tests(models)
            .expect("accepted catalogs fixture should build");

    AcceptedEnumCatalogHandle::new_for_tests(
        catalog,
        composite_catalog,
        AcceptedSchemaRevision::INITIAL,
    )
}

fn accepted_row_decode_layout(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
) -> Result<RowLayout, InternalError> {
    accepted_row_decode_layout_for_model(RowDecodeEntity::MODEL, descriptor)
}

fn accepted_row_decode_layout_for_model(
    model: &'static EntityModel,
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
) -> Result<RowLayout, InternalError> {
    let row_proof = descriptor.generated_row_compatibility_proof_for_model(model)?;

    Ok(
        RowLayout::from_generated_compatible_accepted_decode_contract(
            model.path(),
            row_proof,
            descriptor.row_decode_contract(accepted_enum_catalog_handle(&[model])),
        ),
    )
}

// Build one canonical raw row from already encoded slot payloads. The
// value-storage scalar guard uses this to construct a deliberately mixed row
// whose generated Rust field type would otherwise encode through its normal
// by-kind lane.
fn raw_row_from_encoded_slot_payloads(slot_payloads: &[Vec<u8>]) -> RawRow {
    let field_count = u16::try_from(slot_payloads.len())
        .expect("row decode test slot count should fit in row table");
    let mut slot_table = Vec::with_capacity(slot_payloads.len());
    let mut payload_bytes = Vec::new();

    for payload in slot_payloads {
        let start = u32::try_from(payload_bytes.len())
            .expect("row decode test payload start should fit row table");
        let len = u32::try_from(payload.len())
            .expect("row decode test payload length should fit row table");
        payload_bytes.extend_from_slice(payload);
        slot_table.push((start, len));
    }

    let mut row_payload = Vec::with_capacity(2 + slot_payloads.len() * 8 + payload_bytes.len());
    row_payload.extend_from_slice(&field_count.to_be_bytes());
    for (start, len) in slot_table {
        row_payload.extend_from_slice(&start.to_be_bytes());
        row_payload.extend_from_slice(&len.to_be_bytes());
    }
    row_payload.extend_from_slice(&payload_bytes);

    RawRow::from_untrusted_bytes(
        serialize_row_payload(row_payload).expect("row decode test payload should serialize"),
    )
    .expect("row decode test raw row should be bounded")
}

fn composite_row_decode_layout() -> (RowLayout, crate::types::EntityTag) {
    let entity_tag = crate::types::EntityTag::new(91_777);
    let accepted = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "row_decode::tests::CompositeKeyEntity".to_string(),
        "CompositeKeyEntity".to_string(),
        vec![FieldId::new(1), FieldId::new(2)],
        SchemaRowLayout::new(
            SchemaVersion::initial(),
            vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(1)),
                (FieldId::new(3), SchemaFieldSlot::new(2)),
            ],
        ),
        vec![
            PersistedFieldSnapshot::new(
                FieldId::new(1),
                "tenant_id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Nat64,
                Vec::new(),
                false,
                crate::db::schema::SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Nat64),
            ),
            PersistedFieldSnapshot::new(
                FieldId::new(2),
                "local_id".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Nat64,
                Vec::new(),
                false,
                crate::db::schema::SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Nat64),
            ),
            PersistedFieldSnapshot::new(
                FieldId::new(3),
                "label".to_string(),
                SchemaFieldSlot::new(2),
                AcceptedFieldKind::Text { max_len: Some(64) },
                Vec::new(),
                false,
                crate::db::schema::SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Text),
            ),
        ],
    ));
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
        .expect("accepted composite row-decode schema should build");
    let contract = StructuralRowContract::from_accepted_decode_contract(
        "row_decode::tests::CompositeKeyEntity",
        descriptor.row_decode_contract(accepted_enum_catalog_handle(&[])),
    );

    (RowLayout { contract }, entity_tag)
}

fn composite_data_key(
    entity_tag: crate::types::EntityTag,
    tenant_id: u64,
    local_id: u64,
) -> crate::db::data::DecodedDataStoreKey {
    use crate::db::key_taxonomy::{CompositePrimaryKeyValue, PrimaryKeyComponent, PrimaryKeyValue};

    let key = CompositePrimaryKeyValue::try_from_components(&[
        PrimaryKeyComponent::Nat64(tenant_id),
        PrimaryKeyComponent::Nat64(local_id),
    ])
    .expect("test composite primary key should be valid");

    crate::db::data::DecodedDataStoreKey::new_primary_key_value(
        entity_tag,
        &PrimaryKeyValue::Composite(key),
    )
}

#[test]
fn row_layout_can_be_frozen_from_accepted_row_decode_contract() {
    let accepted = accepted_row_decode_schema();
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
        .expect("accepted row decode schema should project into runtime contract");
    let layout = accepted_row_decode_layout(&descriptor)
        .expect("exact accepted row layout should be generated-compatible");

    assert_eq!(layout.field_count(), RowDecodeEntity::MODEL.fields().len());
    assert_eq!(
        layout.contract().primary_key_slot(),
        RowDecodeEntity::MODEL.primary_key_slot()
    );
}

#[test]
fn row_layout_decodes_required_slot_through_composite_data_key() {
    let (layout, entity_tag) = composite_row_decode_layout();
    let raw_row = raw_row_from_encoded_slot_payloads(&[
        crate::db::encode_persisted_scalar_slot_payload(&7u64, "tenant_id")
            .expect("tenant_id slot payload should encode"),
        crate::db::encode_persisted_scalar_slot_payload(&9u64, "local_id")
            .expect("local_id slot payload should encode"),
        crate::db::encode_persisted_scalar_slot_payload(&"composite-row".to_string(), "label")
            .expect("label slot payload should encode"),
    ]);
    let matching_key = composite_data_key(entity_tag, 7, 9);
    let mismatched_key = composite_data_key(entity_tag, 7, 10);

    let decoded = layout
        .decode_required_value_from_data_key(&raw_row, &matching_key, 2)
        .expect("composite data-key required-slot decode should succeed");

    assert_eq!(decoded, Some(Value::Text("composite-row".to_string())));
    assert!(
        layout
            .decode_required_value_from_data_key(&raw_row, &mismatched_key, 2)
            .is_err(),
        "composite data-key row decode must validate the full primary-key identity",
    );
}

#[test]
fn accepted_row_layout_decode_matches_model_proposal_projection_for_full_and_sparse_rows() {
    let entity = RowDecodeEntity {
        id: Ulid::from_u128(41),
        title: "accepted-parity".to_string(),
        tags: vec!["nested".to_string(), "list".to_string()],
        portrait: Blob::from(vec![0xA1, 0xB2, 0xC3, 0xD4]),
    };
    let key = crate::db::data::DecodedDataStoreKey::try_new::<RowDecodeEntity>(entity.id)
        .expect("test key construction should succeed");
    let primary_key = key.primary_key_value();
    let raw_row = CanonicalRow::from_entity_with_model_proposal_for_test(&entity)
        .expect("test row serialization should succeed")
        .into_raw_row();
    let accepted = accepted_row_decode_schema();
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
        .expect("accepted row decode schema should project into runtime contract");
    let projected_layout = RowLayout::from_model_proposal_for_test(RowDecodeEntity::MODEL);
    let accepted_layout = accepted_row_decode_layout(&descriptor)
        .expect("exact accepted row layout should be generated-compatible");

    // Phase 1: full-row decode must produce identical logical values whether
    // the accepted contract is projected from the model or supplied directly.
    let projected_full = RowDecoder::structural()
        .decode(&projected_layout, (key.clone(), raw_row.clone()))
        .expect("model-projected full-row decode should succeed");
    let accepted_full = RowDecoder::structural()
        .decode(&accepted_layout, (key, raw_row.clone()))
        .expect("accepted full-row decode should succeed");
    for slot in 0..projected_layout.field_count() {
        assert_eq!(
            accepted_full.slot(slot),
            projected_full.slot(slot),
            "slot {slot} should decode identically through explicit and model-projected accepted layouts",
        );
    }

    // Phase 2: sparse decode is the hotter direct-row path, so compare the
    // selected non-primary slots that now use accepted field contracts.
    let selected_slots = [1, 2, 3];
    let projected_sparse = RowDecoder::structural()
        .decode_slots(
            &projected_layout,
            &primary_key,
            &raw_row,
            Some(&selected_slots),
        )
        .expect("model-projected sparse-row decode should succeed");
    let accepted_sparse = RowDecoder::structural()
        .decode_slots(
            &accepted_layout,
            &primary_key,
            &raw_row,
            Some(&selected_slots),
        )
        .expect("accepted sparse-row decode should succeed");
    assert_eq!(accepted_sparse, projected_sparse);

    // Phase 3: retained/lazy row readers must also stay aligned because they
    // now validate and materialize touched slots through accepted contracts.
    let mut projected_reader = projected_layout
        .open_raw_row_with_contract(&raw_row)
        .expect("model-projected lazy row reader should open");
    let mut accepted_reader = accepted_layout
        .open_raw_row_with_contract(&raw_row)
        .expect("accepted lazy row reader should open");
    for slot in selected_slots {
        assert_eq!(
            accepted_reader
                .get_value(slot)
                .expect("accepted lazy slot decode should succeed"),
            projected_reader
                .get_value(slot)
                .expect("model-projected lazy slot decode should succeed"),
            "lazy slot {slot} should decode identically through explicit and model-projected accepted layouts",
        );
    }
}

#[test]
fn row_layout_rejects_accepted_slot_reorder_at_generated_compatibility_proof() {
    let snapshot = row_decode_schema_snapshot();
    let changed = PersistedSchemaSnapshot::new(
        snapshot.version(),
        snapshot.entity_path().to_string(),
        snapshot.entity_name().to_string(),
        snapshot.first_primary_key_field_id(),
        SchemaRowLayout::new(
            SchemaVersion::initial(),
            vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(2)),
                (FieldId::new(3), SchemaFieldSlot::new(1)),
                (FieldId::new(4), SchemaFieldSlot::new(3)),
            ],
        ),
        snapshot.fields().to_vec(),
    );
    let accepted = AcceptedSchemaSnapshot::new(changed);
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
        .expect("slot-reordered test schema should still form a descriptor");

    let err = accepted_row_decode_layout(&descriptor)
        .expect_err("slot reorder must stay rejected at generated-compatible bridge");
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn row_layout_rejects_accepted_payload_contract_drift_at_generated_compatibility_proof() {
    let snapshot = row_decode_schema_snapshot();
    let mut fields = snapshot.fields().to_vec();
    let title = fields
        .get_mut(1)
        .expect("row decode test schema should include title");
    *title = PersistedFieldSnapshot::new(
        title.id(),
        title.name().to_string(),
        title.slot(),
        AcceptedFieldKind::Text { max_len: None },
        title.nested_leaves().to_vec(),
        title.nullable(),
        title.default().clone(),
        FieldStorageDecode::CatalogValue,
        LeafCodec::Structural,
    );
    let changed = PersistedSchemaSnapshot::new(
        snapshot.version(),
        snapshot.entity_path().to_string(),
        snapshot.entity_name().to_string(),
        snapshot.first_primary_key_field_id(),
        snapshot.row_layout().clone(),
        fields,
    );
    let accepted = AcceptedSchemaSnapshot::new(changed);
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
        .expect("storage-decode-drift test schema should still form a descriptor");

    let err = accepted_row_decode_layout(&descriptor)
        .expect_err("payload contract drift must stay rejected at generated-compatible bridge");
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Store);
    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreInvariantViolation,
        "storage-decode compatibility drift should stay store-invariant classified",
    );
}

#[test]
fn accepted_row_layout_decoder_rejects_malformed_raw_row() {
    let accepted = accepted_row_decode_schema();
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
        .expect("accepted row decode schema should project into runtime contract");
    let layout = accepted_row_decode_layout(&descriptor)
        .expect("exact accepted row layout should be generated-compatible");
    let key = crate::db::data::DecodedDataStoreKey::try_new::<RowDecodeEntity>(Ulid::from_u128(31))
        .expect("test key construction should succeed");
    let malformed = RawRow::from_untrusted_bytes(vec![0xFF])
        .expect("malformed test row should still be bounded");
    let Err(err) = RowDecoder::structural().decode(&layout, (key, malformed)) else {
        panic!("malformed row bytes must fail closed through accepted layout")
    };

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Serialize);
    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeCorruption,
        "malformed-row decode diagnostic drifted: {err:?}",
    );
}

#[test]
fn structural_row_decoder_materializes_slot_values_without_entity_decode() {
    let entity = RowDecodeEntity {
        id: Ulid::from_u128(7),
        title: "alpha".to_string(),
        tags: vec!["one".to_string(), "two".to_string()],
        portrait: Blob::from(vec![0x10, 0x20, 0x30]),
    };
    let row = decode_test_row(&entity);

    assert_eq!(row.slot(0), Some(Value::Ulid(entity.id)));
    assert_eq!(row.slot(1), Some(Value::Text(entity.title)));
    assert_eq!(
        row.slot(2),
        Some(Value::List(vec![
            Value::Text("one".to_string()),
            Value::Text("two".to_string()),
        ])),
    );
    assert_eq!(row.slot(3), Some(Value::Blob(vec![0x10, 0x20, 0x30])));
}

#[test]
fn structural_row_decoder_metrics_report_full_non_scalar_materialization() {
    let entity = RowDecodeEntity {
        id: Ulid::from_u128(17),
        title: "alpha".to_string(),
        tags: vec!["one".to_string(), "two".to_string()],
        portrait: Blob::from(vec![0x10, 0x20, 0x30]),
    };
    let (_row, metrics) = decode_test_row_with_metrics(&entity);

    assert_eq!(metrics.rows_opened, 0);
    assert_eq!(metrics.declared_slots_validated, 0);
    assert_eq!(
        metrics.validated_non_scalar_slots, 0,
        "dense full-row decode now bypasses the sparse reader metrics path",
    );
    assert_eq!(
        metrics.materialized_non_scalar_slots, 0,
        "dense full-row decode now bypasses the sparse reader metrics path",
    );
    assert_eq!(
        metrics.rows_without_lazy_non_scalar_materializations, 0,
        "dense full-row decode no longer routes through reader-probe aggregation",
    );
}

#[test]
fn selective_slot_decode_can_skip_unused_non_scalar_materialization() {
    let entity = RowDecodeEntity {
        id: Ulid::from_u128(23),
        title: "alpha".to_string(),
        tags: vec!["one".to_string(), "two".to_string()],
        portrait: Blob::from(vec![0x10, 0x20, 0x30]),
    };
    let (slots, metrics) = decode_required_test_slots_with_metrics(&entity, &[0, 1]);

    assert_eq!(slots[0], Some(Value::Ulid(entity.id)));
    assert_eq!(slots[1], Some(Value::Text(entity.title)));
    assert_eq!(slots[2], None);
    assert_eq!(slots[3], None);
    assert_eq!(metrics.rows_opened, 1);
    assert_eq!(metrics.declared_slots_validated, 2);
    assert_eq!(
        metrics.validated_non_scalar_slots, 0,
        "selective slot decode should not validate untouched non-scalar fields",
    );
    assert_eq!(
        metrics.materialized_non_scalar_slots, 0,
        "selective slot decode should leave untouched non-scalar fields lazy",
    );
    assert_eq!(metrics.rows_without_lazy_non_scalar_materializations, 1);
}

#[test]
fn retained_slot_decode_can_materialize_scalar_octet_lengths_without_blob_values() {
    let entity = RowDecodeEntity {
        id: Ulid::from_u128(29),
        title: "alpha".to_string(),
        tags: vec!["one".to_string(), "two".to_string()],
        portrait: Blob::from(vec![0x10, 0x20, 0x30, 0x40]),
    };
    let key = crate::db::data::DecodedDataStoreKey::try_new::<RowDecodeEntity>(entity.id)
        .expect("test key construction should succeed");
    let row = CanonicalRow::from_entity_with_model_proposal_for_test(&entity)
        .expect("test row serialization should succeed")
        .into_raw_row();
    let layout = RetainedSlotLayout::compile_with_value_modes(
        RowDecodeEntity::MODEL.fields().len(),
        vec![0, 1, 3],
        vec![
            RetainedSlotValueMode::Normal,
            RetainedSlotValueMode::ScalarOctetLength,
            RetainedSlotValueMode::ScalarOctetLength,
        ],
    );
    let accepted = accepted_row_decode_schema();
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
        .expect("accepted retained-slot row decode schema should form descriptor");
    let row_layout = accepted_row_decode_layout(&descriptor)
        .expect("accepted retained-slot row layout should be generated-compatible");

    let (values, value_read_metrics) = with_structural_read_metrics(|| {
        RowDecoder::decode_indexed_slot_values(&row_layout, &key.primary_key_value(), &row, &layout)
            .expect("retained scalar length decode should succeed")
    });

    assert_eq!(
        values,
        vec![
            Some(Value::Ulid(entity.id)),
            Some(Value::Nat64(5)),
            Some(Value::Nat64(4)),
        ],
        "retained scalar byte-length slots should mix normal values with lengths instead of text/blob payloads",
    );
    assert_eq!(
        value_read_metrics.rows_opened, 1,
        "mixed retained value-mode decode should open the row once",
    );

    let (retained_row, retained_read_metrics) = with_structural_read_metrics(|| {
        RowDecoder::decode_retained_slots_from_data_key(&row_layout, &key, &row, &layout)
            .expect("retained scalar length row decode should succeed")
    });

    assert_eq!(retained_row.slot_ref(0), Some(&Value::Ulid(entity.id)));
    assert_eq!(retained_row.slot_ref(1), Some(&Value::Nat64(5)));
    assert_eq!(retained_row.slot_ref(3), Some(&Value::Nat64(4)));
    assert_eq!(
        retained_read_metrics.rows_opened, 1,
        "retained row decode with byte-length slots should open the row once",
    );
}

#[test]
fn structural_row_decoder_rejects_primary_key_mismatch() {
    let entity = RowDecodeEntity {
        id: Ulid::from_u128(9),
        title: "alpha".to_string(),
        tags: vec![],
        portrait: Blob::default(),
    };
    let wrong_key =
        crate::db::data::DecodedDataStoreKey::try_new::<RowDecodeEntity>(Ulid::from_u128(10))
            .expect("wrong test key construction should succeed");
    let row = CanonicalRow::from_entity_with_model_proposal_for_test(&entity)
        .expect("test row serialization should succeed")
        .into_raw_row();
    let Err(err) = RowDecoder::structural().decode(
        &RowLayout::from_model_proposal_for_test(RowDecodeEntity::MODEL),
        (wrong_key, row),
    ) else {
        panic!("key mismatch must fail closed")
    };

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct RowDecodeValueTextEntity {
    id: Ulid,
    label: Text,
}

crate::test_entity! {
    ident = RowDecodeValueTextEntity,
    entity_name = "RowDecodeValueTextEntity",
    tag = crate::testing::PROBE_ENTITY_TAG,
    store = RowDecodeStore,
    canister = RowDecodeCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! {
            label: Text => FieldKind::Text { max_len: None },
            options = crate::testing::TestFieldModelOptions::DEFAULT
                .with_storage_decode(crate::model::field::FieldStorageDecode::CatalogValue),
        },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(id),
}

#[test]
fn accepted_row_layout_direct_projection_value_storage_scalar_matches_generated_layout() {
    let entity = RowDecodeValueTextEntity {
        id: Ulid::from_u128(79),
        label: "stored-as-value".to_string(),
    };
    let raw_row = raw_row_from_encoded_slot_payloads(&[
        crate::db::encode_persisted_scalar_slot_payload(&entity.id, "id")
            .expect("value-storage scalar test id should encode"),
        encode_structural_value_storage_bytes(&Value::Text(entity.label.clone()))
            .expect("value-storage scalar test label should encode"),
    ]);
    let accepted = AcceptedSchemaSnapshot::new(
        compiled_schema_proposal_for_model(RowDecodeValueTextEntity::MODEL)
            .initial_persisted_schema_snapshot(),
    );
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
        .expect("accepted value-storage scalar row decode schema should form descriptor");
    let generated_layout = RowLayout::from_model_proposal_for_test(RowDecodeValueTextEntity::MODEL);
    let accepted_layout =
        accepted_row_decode_layout_for_model(RowDecodeValueTextEntity::MODEL, &descriptor)
            .expect("value-storage scalar accepted layout should be generated-compatible");
    let generated_reader = generated_layout
        .open_raw_row_with_contract(&raw_row)
        .expect("generated value-storage scalar lazy row reader should open");
    let accepted_reader = accepted_layout
        .open_raw_row_with_contract(&raw_row)
        .expect("accepted value-storage scalar lazy row reader should open");

    assert_eq!(
        accepted_reader
            .required_direct_projection_value(1)
            .expect("accepted value-storage scalar projection should decode"),
        generated_reader
            .required_direct_projection_value(1)
            .expect("generated value-storage scalar projection should decode"),
    );
}

#[test]
fn accepted_row_layout_direct_projection_rejects_malformed_value_storage_scalar() {
    let id = Ulid::from_u128(80);
    let raw_row = raw_row_from_encoded_slot_payloads(&[
        crate::db::encode_persisted_scalar_slot_payload(&id, "id")
            .expect("value-storage scalar malformed test id should encode"),
        vec![0xFF],
    ]);
    let accepted = AcceptedSchemaSnapshot::new(
        compiled_schema_proposal_for_model(RowDecodeValueTextEntity::MODEL)
            .initial_persisted_schema_snapshot(),
    );
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
        .expect("accepted value-storage scalar row decode schema should form descriptor");
    let accepted_layout =
        accepted_row_decode_layout_for_model(RowDecodeValueTextEntity::MODEL, &descriptor)
            .expect("value-storage scalar accepted layout should be generated-compatible");
    let accepted_reader = accepted_layout
        .open_raw_row_with_contract(&raw_row)
        .expect("accepted malformed value-storage scalar lazy row reader should open");
    let err = accepted_reader
        .required_direct_projection_value(1)
        .expect_err("accepted value-storage scalar projection should reject malformed payload");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Serialize);
    assert!(err.detail().is_none());
    let diagnostic = err.diagnostic();
    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeCorruption
    );
    assert_eq!(
        diagnostic.origin(),
        icydb_diagnostic_code::ErrorOrigin::Serialize
    );
    assert_eq!(diagnostic.detail(), None);
}
