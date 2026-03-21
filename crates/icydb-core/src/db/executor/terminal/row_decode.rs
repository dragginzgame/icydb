//! Module: executor::terminal::row_decode
//! Responsibility: structural scalar row decode from persisted bytes into kernel rows.
//! Does not own: typed response reconstruction or access-path iteration policy.
//! Boundary: scalar runtime row production consumes this structural decode contract.

#[cfg(test)]
use crate::model::field::EnumVariantModel;
#[cfg(test)]
use crate::types::Ulid;
use crate::{
    db::{
        data::decode_structural_field_bytes,
        data::{DataRow, RawRow, ScalarSlotValueRef, SlotReader, StorageKey, StructuralSlotReader},
        executor::terminal::page::KernelRow,
    },
    error::InternalError,
    model::entity::{EntityModel, resolve_primary_key_slot},
    model::field::{FieldKind, FieldStorageDecode, LeafCodec},
    value::Value,
};
///
/// RowFieldLayout
///
/// RowFieldLayout is one structural field-decode descriptor for scalar row
/// production.
/// It precomputes the stable persisted field key and runtime field kind once
/// so hot row decode can stay slot-driven and non-generic.
///

#[derive(Clone, Debug)]
struct RowFieldLayout {
    name: &'static str,
    kind: FieldKind,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
}

///
/// RowLayout
///
/// RowLayout is the structural scalar row-decode plan built once at the typed
/// boundary.
/// It captures stable field ordering, persisted field-name lookup keys, and
/// primary-key slot metadata so row production no longer needs typed entity
/// materialization.
///

#[derive(Clone, Debug)]
pub(in crate::db::executor) struct RowLayout {
    model: &'static EntityModel,
    fields: Vec<RowFieldLayout>,
    primary_key_slot: Option<usize>,
}

impl RowLayout {
    /// Build one structural row layout from model metadata.
    #[must_use]
    pub(in crate::db::executor) fn from_model(model: &'static EntityModel) -> Self {
        let fields = model
            .fields()
            .iter()
            .map(|field| RowFieldLayout {
                name: field.name(),
                kind: field.kind(),
                storage_decode: field.storage_decode(),
                leaf_codec: field.leaf_codec(),
            })
            .collect::<Vec<_>>();

        Self {
            model,
            fields,
            primary_key_slot: resolve_primary_key_slot(model),
        }
    }
}

///
/// RowDecoder
///
/// RowDecoder is the named structural decode contract for scalar row
/// production.
/// The scalar runtime owns this decoder and feeds it raw persisted rows plus a
/// precomputed `RowLayout`, keeping typed entity reconstruction out of the hot
/// execution loop.
///

#[derive(Clone, Copy, Debug)]
pub(in crate::db::executor) struct RowDecoder {
    decode: fn(&RowLayout, DataRow) -> Result<KernelRow, InternalError>,
}

impl RowDecoder {
    /// Build the canonical structural row decoder used by scalar execution.
    #[must_use]
    pub(in crate::db::executor) const fn structural() -> Self {
        Self {
            decode: decode_kernel_row_structural,
        }
    }

    /// Decode one persisted row into one structural kernel row.
    pub(in crate::db::executor) fn decode(
        self,
        layout: &RowLayout,
        data_row: DataRow,
    ) -> Result<KernelRow, InternalError> {
        (self.decode)(layout, data_row)
    }
}

// Decode one persisted data row into one structural kernel row using the
// precomputed slot layout and structural field decoders only.
fn decode_kernel_row_structural(
    layout: &RowLayout,
    data_row: DataRow,
) -> Result<KernelRow, InternalError> {
    let row_fields = decode_row_fields(&data_row.1, layout.model)?;
    let mut slots = Vec::with_capacity(layout.fields.len());

    // Phase 1: decode declared slots through the canonical structural slot reader.
    for (slot, field) in layout.fields.iter().enumerate() {
        slots.push(decode_row_field(&row_fields, slot, field)?);
    }

    // Phase 2: verify the decoded primary-key value still matches storage identity.
    validate_primary_key_slot(layout, data_row.0.storage_key(), slots.as_slice())?;

    Ok(KernelRow::new(data_row, slots))
}

// Decode the persisted row envelope into slot-aligned encoded field payload spans.
fn decode_row_fields<'a>(
    row: &'a RawRow,
    model: &'static EntityModel,
) -> Result<StructuralSlotReader<'a>, InternalError> {
    StructuralSlotReader::from_raw_row(row, model)
}

// Decode one declared field from the persisted row field bytes.
fn decode_row_field(
    row_fields: &StructuralSlotReader<'_>,
    slot: usize,
    field: &RowFieldLayout,
) -> Result<Option<Value>, InternalError> {
    // Phase 1: keep scalar slots on the borrowed fast path when possible.
    if matches!(field.leaf_codec, LeafCodec::Scalar(_))
        && let Some(value) = row_fields.get_scalar(slot)?
    {
        return Ok(Some(match value {
            ScalarSlotValueRef::Null => Value::Null,
            ScalarSlotValueRef::Value(value) => value.into_value(),
        }));
    }

    // Phase 2: fall back to the declared field decode contract for complex payloads.
    let Some(bytes) = row_fields.get_bytes(slot) else {
        return Err(InternalError::serialize_corruption(format!(
            "row decode failed: missing declared field `{}`",
            field.name,
        )));
    };
    let value =
        decode_structural_field_bytes(bytes, field.kind, field.storage_decode).map_err(|err| {
            InternalError::serialize_corruption(format!(
                "row decode failed for field '{}' kind={:?}: {err}",
                field.name, field.kind,
            ))
        })?;

    Ok(Some(value))
}

// Validate the decoded primary-key slot against the authoritative data-key suffix.
fn validate_primary_key_slot(
    layout: &RowLayout,
    expected_key: StorageKey,
    slots: &[Option<Value>],
) -> Result<(), InternalError> {
    let Some(primary_key_slot) = layout.primary_key_slot else {
        return Err(crate::db::error::query_executor_invariant(
            "row layout missing primary-key slot",
        ));
    };
    let Some(Some(primary_key_value)) = slots.get(primary_key_slot) else {
        return Err(InternalError::serialize_corruption(
            "row decode failed: missing primary-key slot value",
        ));
    };
    let decoded_key = StorageKey::try_from_value(primary_key_value).map_err(|err| {
        InternalError::serialize_corruption(format!(
            "row decode failed: primary-key value is not storage-key encodable: {err}",
        ))
    })?;

    if decoded_key != expected_key {
        return Err(InternalError::store_corruption(format!(
            "row key mismatch: expected {expected_key}, found {decoded_key}",
        )));
    }

    Ok(())
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        error::{ErrorClass, ErrorOrigin},
        model::field::{FieldKind, FieldStorageDecode},
        traits::EntitySchema,
        types::{Blob, Text},
        value::{Value, ValueEnum},
    };
    use icydb_derive::{FieldProjection, PersistedRow};
    use serde::{Deserialize, Serialize};
    use std::collections::BTreeMap;

    crate::test_canister! {
        ident = RowDecodeCanister,
        commit_memory_id = crate::testing::test_commit_memory_id(),
    }

    crate::test_store! {
        ident = RowDecodeStore,
        canister = RowDecodeCanister,
    }

    #[derive(
        Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
    )]
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
        primary_key = "id",
        pk_index = 0,
        fields = [
            ("id", FieldKind::Ulid),
            ("title", FieldKind::Text),
            ("tags", FieldKind::List(&FieldKind::Text)),
            ("portrait", FieldKind::Blob),
        ],
        indexes = [],
        store = RowDecodeStore,
        canister = RowDecodeCanister,
    }

    fn decode_test_row(entity: &RowDecodeEntity) -> KernelRow {
        let key = crate::db::data::DataKey::try_new::<RowDecodeEntity>(entity.id)
            .expect("test key construction should succeed");
        let row = RawRow::from_entity(entity).expect("test row serialization should succeed");

        RowDecoder::structural()
            .decode(&RowLayout::from_model(RowDecodeEntity::MODEL), (key, row))
            .expect("structural row decode should succeed")
    }

    fn to_cbor_bytes<T: Serialize>(value: &T) -> Vec<u8> {
        serde_cbor::to_vec(value).expect("test fixture should serialize into CBOR bytes")
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
    fn structural_row_decoder_rejects_primary_key_mismatch() {
        let entity = RowDecodeEntity {
            id: Ulid::from_u128(9),
            title: "alpha".to_string(),
            tags: vec![],
            portrait: Blob::default(),
        };
        let wrong_key = crate::db::data::DataKey::try_new::<RowDecodeEntity>(Ulid::from_u128(10))
            .expect("wrong test key construction should succeed");
        let row = RawRow::from_entity(&entity).expect("test row serialization should succeed");
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
    fn structural_row_decoder_returns_null_for_structured_field_kind() {
        let decoded = decode_structural_field_bytes(
            &to_cbor_bytes(&vec!["x".to_string(), "y".to_string()]),
            FieldKind::Structured { queryable: false },
            FieldStorageDecode::ByKind,
        )
        .expect("structured field decode should succeed");

        assert_eq!(decoded, Value::Null);
    }

    #[test]
    fn structural_row_decoder_preserves_enum_payload_shape_best_effort() {
        static ENUM_VARIANTS: &[EnumVariantModel] = &[EnumVariantModel::new(
            "Loaded",
            Some(&FieldKind::Uint),
            FieldStorageDecode::ByKind,
        )];

        let decoded = decode_structural_field_bytes(
            &to_cbor_bytes(&serde_cbor::Value::Map(BTreeMap::from([(
                serde_cbor::Value::Text("Loaded".to_string()),
                serde_cbor::Value::Integer(7),
            )]))),
            FieldKind::Enum {
                path: "tests::State",
                variants: ENUM_VARIANTS,
            },
            FieldStorageDecode::ByKind,
        )
        .expect("enum payload decode should succeed");

        assert_eq!(
            decoded,
            Value::Enum(
                ValueEnum::new("Loaded", Some("tests::State")).with_payload(Value::Uint(7)),
            ),
        );
    }

    #[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize)]
    struct RowDecodeValueEntity {
        id: Ulid,
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
        primary_key = "id",
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
        let row = RawRow::from_entity(&entity).expect("test row serialization should succeed");
        let decoded = RowDecoder::structural()
            .decode(
                &RowLayout::from_model(RowDecodeValueEntity::MODEL),
                (key, row),
            )
            .expect("structural row decode should succeed");

        assert_eq!(decoded.slot(1), Some(entity.status));
    }
}
