use super::*;
use crate::{
    db::data::{
        CanonicalRow, decode_structural_field_by_kind_bytes, encode_structural_field_by_kind_bytes,
        with_structural_read_metrics,
    },
    error::{ErrorClass, ErrorOrigin},
    model::field::{FieldKind, FieldStorageDecode},
    traits::EntitySchema,
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
        ("portrait", FieldKind::Blob),
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

    let values = RowDecoder::decode_indexed_slot_values(
        &RowLayout::from_model(RowDecodeEntity::MODEL),
        key.storage_key(),
        &row,
        &layout,
    )
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

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct RowDecodeValueEntity {
    id: Ulid,
    #[icydb(meta)]
    status: Value,
}

impl Default for RowDecodeValueEntity {
    fn default() -> Self {
        Self {
            id: Ulid::from_u128(0),
            status: Value::Null,
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
        status: Value::Enum(
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

    assert_eq!(decoded.slot(1), Some(entity.status));
}
