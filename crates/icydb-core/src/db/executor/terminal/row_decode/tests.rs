use super::*;
use crate::{
    db::{
        data::{
            CanonicalRow, RawRow, decode_structural_field_by_kind_bytes,
            encode_structural_field_by_kind_bytes, with_structural_read_metrics,
        },
        schema::{
            AcceptedRowLayoutRuntimeDescriptor, AcceptedSchemaSnapshot, FieldId,
            PersistedFieldKind, PersistedFieldSnapshot, PersistedSchemaSnapshot, SchemaFieldSlot,
            SchemaRowLayout, SchemaVersion, compiled_schema_proposal_for_model,
        },
    },
    error::{ErrorClass, ErrorOrigin},
    model::field::{FieldKind, FieldStorageDecode, LeafCodec},
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
    let row = CanonicalRow::from_entity(entity)
        .expect("test row serialization should succeed")
        .into_raw_row();

    RowDecoder::structural()
        .decode(&RowLayout::from_model(RowDecodeEntity::MODEL), (key, row))
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
    let row = CanonicalRow::from_entity(entity)
        .expect("test row serialization should succeed")
        .into_raw_row();

    with_structural_read_metrics(|| {
        RowDecoder::structural()
            .decode_slots(
                &RowLayout::from_model(RowDecodeEntity::MODEL),
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

#[test]
fn row_layout_can_be_frozen_from_generated_compatible_accepted_schema() {
    let accepted = accepted_row_decode_schema();
    let descriptor = AcceptedRowLayoutRuntimeDescriptor::from_accepted_schema(&accepted)
        .expect("accepted row decode schema should project into runtime descriptor");
    let layout = RowLayout::from_generated_compatible_accepted_descriptor(
        RowDecodeEntity::MODEL,
        &descriptor,
    )
    .expect("exact accepted row layout should be generated-compatible");

    assert_eq!(layout.field_count(), RowDecodeEntity::MODEL.fields().len());
    assert_eq!(
        layout.primary_key_slot(),
        RowDecodeEntity::MODEL.primary_key_slot()
    );
}

#[test]
fn row_layout_rejects_accepted_slot_reorder_until_decoder_consumes_accepted_fields() {
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

    let err = RowLayout::from_generated_compatible_accepted_descriptor(
        RowDecodeEntity::MODEL,
        &descriptor,
    )
    .expect_err("slot reorder must stay rejected at generated-compatible bridge");
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn row_layout_rejects_accepted_payload_contract_drift_until_decoder_consumes_accepted_fields() {
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
        title.default(),
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

    let err = RowLayout::from_generated_compatible_accepted_descriptor(
        RowDecodeEntity::MODEL,
        &descriptor,
    )
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
    let layout = RowLayout::from_generated_compatible_accepted_descriptor(
        RowDecodeEntity::MODEL,
        &descriptor,
    )
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
    let row = CanonicalRow::from_entity(&entity)
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
    let row_layout = RowLayout::from_generated_compatible_accepted_descriptor(
        RowDecodeEntity::MODEL,
        &descriptor,
    )
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
    let row = CanonicalRow::from_entity(&entity)
        .expect("test row serialization should succeed")
        .into_raw_row();
    let Err(err) = RowDecoder::structural().decode(
        &RowLayout::from_model(RowDecodeEntity::MODEL),
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
    let row = CanonicalRow::from_entity(&entity)
        .expect("test row serialization should succeed")
        .into_raw_row();
    let decoded = RowDecoder::structural()
        .decode(
            &RowLayout::from_model(RowDecodeValueEntity::MODEL),
            (key, row),
        )
        .expect("structural row decode should succeed");

    assert_eq!(decoded.slot(1), Some(entity.status.to_value()));
}
