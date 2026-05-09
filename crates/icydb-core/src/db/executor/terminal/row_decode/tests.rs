use super::*;
use crate::{
    db::{
        codec::serialize_row_payload,
        data::{
            CanonicalRow, RawRow, SlotReader, decode_structural_field_by_kind_bytes,
            encode_structural_field_by_kind_bytes, encode_structural_value_storage_bytes,
            with_structural_read_metrics,
        },
        schema::{
            AcceptedRowLayoutRuntimeDescriptor, AcceptedSchemaSnapshot, FieldId,
            PersistedFieldKind, PersistedFieldSnapshot, PersistedSchemaSnapshot, SchemaFieldSlot,
            SchemaRowLayout, SchemaVersion, compiled_schema_proposal_for_model,
        },
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    model::{
        entity::EntityModel,
        field::{FieldKind, FieldStorageDecode, LeafCodec},
    },
    traits::{
        EntitySchema, FieldTypeMeta, PersistedFieldSlotCodec, RuntimeValueDecode,
        RuntimeValueEncode,
    },
    types::{Blob, Text},
    value::{Value, ValueEnum},
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

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct RowDecodeEntity {
    id: Ulid,
    title: Text,
    tags: Vec<Text>,
    portrait: Blob,
}

crate::test_entity_schema! {
    ident = RowDecodeEntity,
    id = Ulid,
    id_field = id,
    entity_name = "RowDecodeEntity",
    entity_tag = crate::testing::PROBE_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("title", FieldKind::Text { max_len: None }),
        ("tags", FieldKind::List(&FieldKind::Text { max_len: None })),
        ("portrait", FieldKind::Blob { max_len: None }),
    ],
    indexes = [],
    store = RowDecodeStore,
    canister = RowDecodeCanister,
}

fn decode_test_row(entity: &RowDecodeEntity) -> KernelRow {
    let key = crate::db::data::DataKey::try_new::<RowDecodeEntity>(entity.id)
        .expect("test key construction should succeed");
    let row = CanonicalRow::from_generated_entity_for_test(entity)
        .expect("test row serialization should succeed")
        .into_raw_row();

    RowDecoder::structural()
        .decode(
            &RowLayout::from_generated_model_for_test(RowDecodeEntity::MODEL),
            (key, row),
        )
        .expect("structural row decode should succeed")
}

fn decode_test_row_with_metrics(
    entity: &RowDecodeEntity,
) -> (KernelRow, crate::db::data::StructuralReadMetrics) {
    with_structural_read_metrics(|| decode_test_row(entity))
}

fn decode_required_test_slots_with_metrics(
    entity: &RowDecodeEntity,
    required_slots: &[usize],
) -> (Vec<Option<Value>>, crate::db::data::StructuralReadMetrics) {
    let key = crate::db::data::DataKey::try_new::<RowDecodeEntity>(entity.id)
        .expect("test key construction should succeed");
    let row = CanonicalRow::from_generated_entity_for_test(entity)
        .expect("test row serialization should succeed")
        .into_raw_row();

    with_structural_read_metrics(|| {
        RowDecoder::structural()
            .decode_slots(
                &RowLayout::from_generated_model_for_test(RowDecodeEntity::MODEL),
                key.storage_key(),
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

fn accepted_row_decode_layout(
    descriptor: &AcceptedRowLayoutRuntimeDescriptor<'_>,
) -> Result<RowLayout, InternalError> {
    accepted_row_decode_layout_for_model(RowDecodeEntity::MODEL, descriptor)
}

fn accepted_row_decode_layout_for_model(
    model: &'static EntityModel,
    descriptor: &AcceptedRowLayoutRuntimeDescriptor<'_>,
) -> Result<RowLayout, InternalError> {
    let row_shape = descriptor.generated_compatible_row_shape_for_model(model)?;

    Ok(
        RowLayout::from_generated_compatible_accepted_decode_contract(
            model.path(),
            row_shape,
            descriptor.row_decode_contract(),
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

#[test]
fn row_layout_can_be_frozen_from_accepted_row_decode_contract() {
    let accepted = accepted_row_decode_schema();
    let descriptor = AcceptedRowLayoutRuntimeDescriptor::from_accepted_schema(&accepted)
        .expect("accepted row decode schema should project into runtime descriptor");
    let layout = accepted_row_decode_layout(&descriptor)
        .expect("exact accepted row layout should be generated-compatible");

    assert_eq!(layout.field_count(), RowDecodeEntity::MODEL.fields().len());
    assert_eq!(
        layout.contract().primary_key_slot(),
        RowDecodeEntity::MODEL.primary_key_slot()
    );
}

#[test]
fn accepted_row_layout_decode_matches_generated_layout_for_full_and_sparse_rows() {
    let entity = RowDecodeEntity {
        id: Ulid::from_u128(41),
        title: "accepted-parity".to_string(),
        tags: vec!["nested".to_string(), "list".to_string()],
        portrait: Blob::from(vec![0xA1, 0xB2, 0xC3, 0xD4]),
    };
    let key = crate::db::data::DataKey::try_new::<RowDecodeEntity>(entity.id)
        .expect("test key construction should succeed");
    let storage_key = key.storage_key();
    let raw_row = CanonicalRow::from_generated_entity_for_test(&entity)
        .expect("test row serialization should succeed")
        .into_raw_row();
    let accepted = accepted_row_decode_schema();
    let descriptor = AcceptedRowLayoutRuntimeDescriptor::from_accepted_schema(&accepted)
        .expect("accepted row decode schema should project into runtime descriptor");
    let generated_layout = RowLayout::from_generated_model_for_test(RowDecodeEntity::MODEL);
    let accepted_layout = accepted_row_decode_layout(&descriptor)
        .expect("exact accepted row layout should be generated-compatible");

    // Phase 1: full-row decode must produce identical logical values whether
    // non-primary slots are decoded through accepted contracts or the generated
    // compatibility bridge.
    let generated_full = RowDecoder::structural()
        .decode(&generated_layout, (key.clone(), raw_row.clone()))
        .expect("generated full-row decode should succeed");
    let accepted_full = RowDecoder::structural()
        .decode(&accepted_layout, (key, raw_row.clone()))
        .expect("accepted full-row decode should succeed");
    for slot in 0..generated_layout.field_count() {
        assert_eq!(
            accepted_full.slot(slot),
            generated_full.slot(slot),
            "slot {slot} should decode identically through accepted and generated layouts",
        );
    }

    // Phase 2: sparse decode is the hotter direct-row path, so compare the
    // selected non-primary slots that now use accepted field contracts.
    let selected_slots = [1, 2, 3];
    let generated_sparse = RowDecoder::structural()
        .decode_slots(
            &generated_layout,
            storage_key,
            &raw_row,
            Some(&selected_slots),
        )
        .expect("generated sparse-row decode should succeed");
    let accepted_sparse = RowDecoder::structural()
        .decode_slots(
            &accepted_layout,
            storage_key,
            &raw_row,
            Some(&selected_slots),
        )
        .expect("accepted sparse-row decode should succeed");
    assert_eq!(accepted_sparse, generated_sparse);

    // Phase 3: retained/lazy row readers must also stay aligned because they
    // now validate and materialize touched slots through accepted contracts.
    let mut generated_reader = generated_layout
        .open_raw_row_with_contract(&raw_row)
        .expect("generated lazy row reader should open");
    let mut accepted_reader = accepted_layout
        .open_raw_row_with_contract(&raw_row)
        .expect("accepted lazy row reader should open");
    for slot in selected_slots {
        assert_eq!(
            accepted_reader
                .get_value(slot)
                .expect("accepted lazy slot decode should succeed"),
            generated_reader
                .get_value(slot)
                .expect("generated lazy slot decode should succeed"),
            "lazy slot {slot} should decode identically through accepted and generated layouts",
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
        snapshot.primary_key_field_id(),
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
    let descriptor = AcceptedRowLayoutRuntimeDescriptor::from_accepted_schema(&accepted)
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
        PersistedFieldKind::Text { max_len: None },
        title.nested_leaves().to_vec(),
        title.nullable(),
        title.default().clone(),
        FieldStorageDecode::Value,
        LeafCodec::StructuralFallback,
    );
    let changed = PersistedSchemaSnapshot::new(
        snapshot.version(),
        snapshot.entity_path().to_string(),
        snapshot.entity_name().to_string(),
        snapshot.primary_key_field_id(),
        snapshot.row_layout().clone(),
        fields,
    );
    let accepted = AcceptedSchemaSnapshot::new(changed);
    let descriptor = AcceptedRowLayoutRuntimeDescriptor::from_accepted_schema(&accepted)
        .expect("storage-decode-drift test schema should still form a descriptor");

    let err = accepted_row_decode_layout(&descriptor)
        .expect_err("payload contract drift must stay rejected at generated-compatible bridge");
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Store);
    assert!(
        err.message
            .contains("accepted row layout storage decode is not generated-compatible"),
        "unexpected compatibility error: {}",
        err.message,
    );
}

#[test]
fn accepted_row_layout_decoder_rejects_malformed_raw_row() {
    let accepted = accepted_row_decode_schema();
    let descriptor = AcceptedRowLayoutRuntimeDescriptor::from_accepted_schema(&accepted)
        .expect("accepted row decode schema should project into runtime descriptor");
    let layout = accepted_row_decode_layout(&descriptor)
        .expect("exact accepted row layout should be generated-compatible");
    let key = crate::db::data::DataKey::try_new::<RowDecodeEntity>(Ulid::from_u128(31))
        .expect("test key construction should succeed");
    let malformed = RawRow::from_untrusted_bytes(vec![0xFF])
        .expect("malformed test row should still be bounded");
    let Err(err) = RowDecoder::structural().decode(&layout, (key, malformed)) else {
        panic!("malformed row bytes must fail closed through accepted layout")
    };

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Serialize);
    assert!(
        err.message.contains("row decode"),
        "unexpected malformed-row decode error: {err:?}",
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
    let key = crate::db::data::DataKey::try_new::<RowDecodeEntity>(entity.id)
        .expect("test key construction should succeed");
    let row = CanonicalRow::from_generated_entity_for_test(&entity)
        .expect("test row serialization should succeed")
        .into_raw_row();
    let layout = RetainedSlotLayout::compile_with_value_modes(
        RowDecodeEntity::MODEL.fields().len(),
        vec![1, 3],
        vec![
            RetainedSlotValueMode::ScalarOctetLength,
            RetainedSlotValueMode::ScalarOctetLength,
        ],
    );
    let accepted = accepted_row_decode_schema();
    let descriptor = AcceptedRowLayoutRuntimeDescriptor::from_accepted_schema(&accepted)
        .expect("accepted retained-slot row decode schema should form descriptor");
    let row_layout = accepted_row_decode_layout(&descriptor)
        .expect("accepted retained-slot row layout should be generated-compatible");

    let values =
        RowDecoder::decode_indexed_slot_values(&row_layout, key.storage_key(), &row, &layout)
            .expect("retained scalar length decode should succeed");

    assert_eq!(
        values,
        vec![Some(Value::Uint(5)), Some(Value::Uint(4))],
        "retained scalar byte-length slots should store lengths instead of text/blob payloads",
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
    let wrong_key = crate::db::data::DataKey::try_new::<RowDecodeEntity>(Ulid::from_u128(10))
        .expect("wrong test key construction should succeed");
    let row = CanonicalRow::from_generated_entity_for_test(&entity)
        .expect("test row serialization should succeed")
        .into_raw_row();
    let Err(err) = RowDecoder::structural().decode(
        &RowLayout::from_generated_model_for_test(RowDecodeEntity::MODEL),
        (wrong_key, row),
    ) else {
        panic!("key mismatch must fail closed")
    };

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn structural_row_decoder_preserves_enum_payload_shape_best_effort() {
    static ENUM_VARIANTS: &[EnumVariantModel] = &[EnumVariantModel::new(
        "Loaded",
        Some(&FieldKind::Uint),
        FieldStorageDecode::ByKind,
    )];
    let bytes = encode_structural_field_by_kind_bytes(
        FieldKind::Enum {
            path: "tests::State",
            variants: ENUM_VARIANTS,
        },
        &Value::Enum(ValueEnum::new("Loaded", Some("tests::State")).with_payload(Value::Uint(7))),
        "status",
    )
    .expect("enum payload bytes should encode");

    let decoded = decode_structural_field_by_kind_bytes(
        &bytes,
        FieldKind::Enum {
            path: "tests::State",
            variants: ENUM_VARIANTS,
        },
    )
    .expect("enum payload decode should succeed");

    assert_eq!(
        decoded,
        Value::Enum(ValueEnum::new("Loaded", Some("tests::State")).with_payload(Value::Uint(7)),),
    );
}

///
/// RowDecodeStatus
///
/// RowDecodeStatus is the typed persisted enum wrapper for structural row
/// decode tests.
/// The decoder still materializes runtime `Value` outputs, but persistence
/// enters through this static field contract instead of `Value` itself.
///

#[derive(Clone, Debug, Deserialize, PartialEq)]
struct RowDecodeStatus(ValueEnum);

impl FieldTypeMeta for RowDecodeStatus {
    const KIND: FieldKind = FieldKind::Enum {
        path: "tests::Status",
        variants: &[],
    };
    const STORAGE_DECODE: FieldStorageDecode = FieldStorageDecode::Value;
}

impl RuntimeValueEncode for RowDecodeStatus {
    fn to_value(&self) -> Value {
        Value::Enum(self.0.clone())
    }
}

impl RuntimeValueDecode for RowDecodeStatus {
    fn from_value(value: &Value) -> Option<Self> {
        let Value::Enum(value) = value else {
            return None;
        };

        Some(Self(value.clone()))
    }
}

impl PersistedFieldSlotCodec for RowDecodeStatus {
    fn encode_persisted_slot(
        &self,
        field_name: &'static str,
    ) -> Result<Vec<u8>, crate::error::InternalError> {
        crate::db::encode_persisted_slot_payload_by_meta(self, field_name)
    }

    fn decode_persisted_slot(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, crate::error::InternalError> {
        crate::db::decode_persisted_slot_payload_by_meta(bytes, field_name)
    }

    fn encode_persisted_option_slot(
        value: &Option<Self>,
        field_name: &'static str,
    ) -> Result<Vec<u8>, crate::error::InternalError> {
        crate::db::encode_persisted_option_slot_payload_by_meta(value, field_name)
    }

    fn decode_persisted_option_slot(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Option<Self>, crate::error::InternalError> {
        crate::db::decode_persisted_option_slot_payload_by_meta(bytes, field_name)
    }
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct RowDecodeValueEntity {
    id: Ulid,
    status: RowDecodeStatus,
}

impl Default for RowDecodeValueEntity {
    fn default() -> Self {
        Self {
            id: Ulid::from_u128(0),
            status: RowDecodeStatus(ValueEnum::new("Pending", Some("tests::Status"))),
        }
    }
}

crate::test_entity_schema! {
    ident = RowDecodeValueEntity,
    id = Ulid,
    id_field = id,
    entity_name = "RowDecodeValueEntity",
    entity_tag = crate::testing::PROBE_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        (
            "status",
            FieldKind::Enum {
                path: "tests::Status",
                variants: &[],
            },
            crate::model::field::FieldStorageDecode::Value
        ),
    ],
    indexes = [],
    store = RowDecodeStore,
    canister = RowDecodeCanister,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct RowDecodeValueTextEntity {
    id: Ulid,
    label: Text,
}

crate::test_entity_schema! {
    ident = RowDecodeValueTextEntity,
    id = Ulid,
    id_field = id,
    entity_name = "RowDecodeValueTextEntity",
    entity_tag = crate::testing::PROBE_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        (
            "label",
            FieldKind::Text { max_len: None },
            crate::model::field::FieldStorageDecode::Value
        ),
    ],
    indexes = [],
    store = RowDecodeStore,
    canister = RowDecodeCanister,
}

#[test]
fn structural_row_decoder_respects_value_storage_decode_contract() {
    let entity = RowDecodeValueEntity {
        id: Ulid::from_u128(77),
        status: RowDecodeStatus(
            ValueEnum::new("Paid", Some("tests::Status")).with_payload(Value::Uint(7)),
        ),
    };
    let key = crate::db::data::DataKey::try_new::<RowDecodeValueEntity>(entity.id)
        .expect("test key construction should succeed");
    let row = CanonicalRow::from_generated_entity_for_test(&entity)
        .expect("test row serialization should succeed")
        .into_raw_row();
    let decoded = RowDecoder::structural()
        .decode(
            &RowLayout::from_generated_model_for_test(RowDecodeValueEntity::MODEL),
            (key, row),
        )
        .expect("structural row decode should succeed");

    assert_eq!(decoded.slot(1), Some(entity.status.to_value()));
}

#[test]
fn accepted_row_layout_decode_matches_generated_layout_for_value_storage_field() {
    let entity = RowDecodeValueEntity {
        id: Ulid::from_u128(78),
        status: RowDecodeStatus(
            ValueEnum::new("Settled", Some("tests::Status")).with_payload(Value::Uint(11)),
        ),
    };
    let key = crate::db::data::DataKey::try_new::<RowDecodeValueEntity>(entity.id)
        .expect("test key construction should succeed");
    let storage_key = key.storage_key();
    let raw_row = CanonicalRow::from_generated_entity_for_test(&entity)
        .expect("test row serialization should succeed")
        .into_raw_row();
    let accepted = AcceptedSchemaSnapshot::new(
        compiled_schema_proposal_for_model(RowDecodeValueEntity::MODEL)
            .initial_persisted_schema_snapshot(),
    );
    let descriptor = AcceptedRowLayoutRuntimeDescriptor::from_accepted_schema(&accepted)
        .expect("accepted value-storage row decode schema should project into runtime descriptor");
    let generated_layout = RowLayout::from_generated_model_for_test(RowDecodeValueEntity::MODEL);
    let accepted_layout =
        accepted_row_decode_layout_for_model(RowDecodeValueEntity::MODEL, &descriptor)
            .expect("value-storage accepted layout should be generated-compatible");

    // Phase 1: full-row decode must keep `FieldStorageDecode::Value` logical
    // values identical after accepted contracts take over the direct path.
    let generated_full = RowDecoder::structural()
        .decode(&generated_layout, (key.clone(), raw_row.clone()))
        .expect("generated value-storage full-row decode should succeed");
    let accepted_full = RowDecoder::structural()
        .decode(&accepted_layout, (key, raw_row.clone()))
        .expect("accepted value-storage full-row decode should succeed");
    assert_eq!(accepted_full.slot(1), generated_full.slot(1));

    // Phase 2: sparse slot decode is the accepted-contract branch that
    // validates and materializes structural-fallback payloads directly.
    let selected_slots = [1];
    let generated_sparse = RowDecoder::structural()
        .decode_slots(
            &generated_layout,
            storage_key,
            &raw_row,
            Some(&selected_slots),
        )
        .expect("generated value-storage sparse decode should succeed");
    let accepted_sparse = RowDecoder::structural()
        .decode_slots(
            &accepted_layout,
            storage_key,
            &raw_row,
            Some(&selected_slots),
        )
        .expect("accepted value-storage sparse decode should succeed");
    assert_eq!(accepted_sparse, generated_sparse);

    // Phase 3: the narrow single-slot direct path must use the same accepted
    // contract as the sparse slot-vector path so grouped/projection fast paths
    // cannot fall back to generated-only value-storage semantics.
    let generated_required =
        RowDecoder::decode_required_slot_value(&generated_layout, storage_key, &raw_row, 1)
            .expect("generated value-storage required-slot decode should succeed");
    let accepted_required =
        RowDecoder::decode_required_slot_value(&accepted_layout, storage_key, &raw_row, 1)
            .expect("accepted value-storage required-slot decode should succeed");
    assert_eq!(accepted_required, generated_required);

    let mut generated_reader = generated_layout
        .open_raw_row_with_contract(&raw_row)
        .expect("generated value-storage lazy row reader should open");
    let mut accepted_reader = accepted_layout
        .open_raw_row_with_contract(&raw_row)
        .expect("accepted value-storage lazy row reader should open");
    assert_eq!(
        accepted_reader
            .get_value(1)
            .expect("accepted value-storage lazy slot decode should succeed"),
        generated_reader
            .get_value(1)
            .expect("generated value-storage lazy slot decode should succeed"),
    );
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
    let descriptor = AcceptedRowLayoutRuntimeDescriptor::from_accepted_schema(&accepted)
        .expect("accepted value-storage scalar row decode schema should form descriptor");
    let generated_layout =
        RowLayout::from_generated_model_for_test(RowDecodeValueTextEntity::MODEL);
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
    let descriptor = AcceptedRowLayoutRuntimeDescriptor::from_accepted_schema(&accepted)
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
    assert!(
        err.message.contains("structural binary") || err.message.contains("field kind"),
        "unexpected malformed value-storage scalar error: {err:?}",
    );
}
