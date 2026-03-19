//! Module: executor::terminal::row_decode
//! Responsibility: structural scalar row decode from persisted bytes into kernel rows.
//! Does not own: typed response reconstruction or access-path iteration policy.
//! Boundary: scalar runtime row production consumes this structural decode contract.

use crate::{
    db::{
        codec::deserialize_row,
        data::{DataRow, RawRow, StorageKey},
        executor::terminal::page::KernelRow,
    },
    error::InternalError,
    model::{
        entity::{EntityModel, resolve_primary_key_slot},
        field::FieldKind,
    },
    types::{
        Account, Date, Decimal, Duration, Float32, Float64, Int, Int128, Nat, Nat128, Principal,
        Subaccount, Timestamp, Ulid,
    },
    value::{Value, ValueEnum},
};
use serde_cbor::{Value as CborValue, value::from_value as cbor_from_value};
use std::collections::BTreeMap;

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
    lookup_key: CborValue,
    kind: FieldKind,
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
                lookup_key: CborValue::Text(field.name().to_string()),
                kind: field.kind(),
            })
            .collect::<Vec<_>>();

        Self {
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
    let row_map = decode_row_map(&data_row.1)?;
    let slots = layout
        .fields
        .iter()
        .map(|field| decode_row_field(&row_map, field))
        .collect::<Result<Vec<_>, _>>()?;
    validate_primary_key_slot(layout, data_row.0.storage_key(), slots.as_slice())?;

    Ok(KernelRow::new(data_row, slots))
}

// Decode the persisted row envelope into one structural CBOR object map.
fn decode_row_map(row: &RawRow) -> Result<BTreeMap<CborValue, CborValue>, InternalError> {
    let decoded = deserialize_row::<CborValue>(row.as_bytes())?;
    let CborValue::Map(map) = unwrap_cbor_tags(decoded) else {
        return Err(InternalError::serialize_corruption(
            "row decode failed: expected top-level CBOR map",
        ));
    };

    Ok(map)
}

// Decode one declared field from the persisted row object.
fn decode_row_field(
    row_map: &BTreeMap<CborValue, CborValue>,
    field: &RowFieldLayout,
) -> Result<Option<Value>, InternalError> {
    let Some(raw_value) = row_map.get(&field.lookup_key) else {
        return Err(InternalError::serialize_corruption(format!(
            "row decode failed: missing declared field `{}`",
            field.name,
        )));
    };
    let value = decode_field_value(raw_value, field.kind).map_err(|err| {
        InternalError::serialize_corruption(format!(
            "row decode failed for field `{}` kind={:?}: {}",
            field.name, field.kind, err.message
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
/// RowDecodeFailure
///
/// RowDecodeFailure is one structured local decode failure used while
/// converting persisted CBOR trees into runtime `Value` payloads.
/// The structural decoder maps these failures into taxonomy-correct
/// `InternalError` values at the row boundary.
///

#[derive(Clone, Debug)]
struct RowDecodeFailure {
    message: String,
}

impl RowDecodeFailure {
    // Build one field-decode failure message.
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

// Decode one runtime field value using only structural field metadata.
fn decode_field_value(raw_value: &CborValue, kind: FieldKind) -> Result<Value, RowDecodeFailure> {
    let raw_value = unwrap_cbor_tags(raw_value.clone());
    if matches!(raw_value, CborValue::Null) && !matches!(kind, FieldKind::Unit) {
        return Ok(Value::Null);
    }

    match kind {
        FieldKind::Account => decode_account_value(raw_value),
        FieldKind::Blob => decode_blob_value(raw_value),
        FieldKind::Bool => decode_bool_value(raw_value),
        FieldKind::Date => decode_date_value(raw_value),
        FieldKind::Decimal { .. } => decode_decimal_value(raw_value),
        FieldKind::Duration => decode_duration_value(raw_value),
        FieldKind::Enum { path } => decode_enum_value(raw_value, path),
        FieldKind::Float32 => decode_float32_value(raw_value),
        FieldKind::Float64 => decode_float64_value(raw_value),
        FieldKind::Int => decode_int64_value(raw_value),
        FieldKind::Int128 => decode_int128_value(raw_value),
        FieldKind::IntBig => decode_int_big_value(raw_value),
        FieldKind::Principal => decode_principal_value(raw_value),
        FieldKind::Subaccount => decode_subaccount_value(raw_value),
        FieldKind::Text => decode_text_value(raw_value),
        FieldKind::Timestamp => decode_timestamp_value(raw_value),
        FieldKind::Uint => decode_uint64_value(raw_value),
        FieldKind::Uint128 => decode_uint128_value(raw_value),
        FieldKind::UintBig => decode_uint_big_value(raw_value),
        FieldKind::Ulid => decode_ulid_value(raw_value),
        FieldKind::Unit => decode_unit_value(raw_value),
        FieldKind::Relation { key_kind, .. } => decode_field_value(&raw_value, *key_kind),
        FieldKind::List(inner) | FieldKind::Set(inner) => decode_list_value(raw_value, *inner),
        FieldKind::Map { key, value } => decode_map_value(raw_value, *key, *value),
        FieldKind::Structured { .. } => Ok(Value::Null),
    }
}

macro_rules! decode_typed_cbor_value {
    ($raw_value:expr, $ty:ty, $into_value:expr) => {
        cbor_from_value::<$ty>($raw_value)
            .map($into_value)
            .map_err(|err| RowDecodeFailure::new(format!("typed CBOR decode failed: {err}")))
    };
}

// Decode one account field using the persisted CBOR account codec.
fn decode_account_value(raw_value: CborValue) -> Result<Value, RowDecodeFailure> {
    decode_typed_cbor_value!(raw_value, Account, Value::Account)
}

// Decode one blob field using the persisted CBOR byte codec.
fn decode_blob_value(raw_value: CborValue) -> Result<Value, RowDecodeFailure> {
    match raw_value {
        CborValue::Bytes(value) => Ok(Value::Blob(value)),
        other => Err(RowDecodeFailure::new(format!(
            "typed CBOR decode failed: invalid type: {other:?}, expected a byte string",
        ))),
    }
}

// Decode one boolean field using the persisted CBOR bool codec.
fn decode_bool_value(raw_value: CborValue) -> Result<Value, RowDecodeFailure> {
    decode_typed_cbor_value!(raw_value, bool, Value::Bool)
}

// Decode one date field using the persisted CBOR date codec.
fn decode_date_value(raw_value: CborValue) -> Result<Value, RowDecodeFailure> {
    decode_typed_cbor_value!(raw_value, Date, Value::Date)
}

// Decode one decimal field using the persisted CBOR decimal codec.
fn decode_decimal_value(raw_value: CborValue) -> Result<Value, RowDecodeFailure> {
    decode_typed_cbor_value!(raw_value, Decimal, Value::Decimal)
}

// Decode one duration field using the persisted CBOR duration codec.
fn decode_duration_value(raw_value: CborValue) -> Result<Value, RowDecodeFailure> {
    decode_typed_cbor_value!(raw_value, Duration, Value::Duration)
}

// Decode one float32 field using the persisted CBOR float32 codec.
fn decode_float32_value(raw_value: CborValue) -> Result<Value, RowDecodeFailure> {
    decode_typed_cbor_value!(raw_value, Float32, Value::Float32)
}

// Decode one float64 field using the persisted CBOR float64 codec.
fn decode_float64_value(raw_value: CborValue) -> Result<Value, RowDecodeFailure> {
    decode_typed_cbor_value!(raw_value, Float64, Value::Float64)
}

// Decode one signed 64-bit integer field using the persisted CBOR integer codec.
fn decode_int64_value(raw_value: CborValue) -> Result<Value, RowDecodeFailure> {
    decode_typed_cbor_value!(raw_value, i64, Value::Int)
}

// Decode one signed 128-bit integer field using the persisted CBOR integer codec.
fn decode_int128_value(raw_value: CborValue) -> Result<Value, RowDecodeFailure> {
    decode_typed_cbor_value!(raw_value, Int128, Value::Int128)
}

// Decode one arbitrary-precision signed integer field using the persisted CBOR integer codec.
fn decode_int_big_value(raw_value: CborValue) -> Result<Value, RowDecodeFailure> {
    decode_typed_cbor_value!(raw_value, Int, Value::IntBig)
}

// Decode one principal field using the persisted CBOR principal codec.
fn decode_principal_value(raw_value: CborValue) -> Result<Value, RowDecodeFailure> {
    decode_typed_cbor_value!(raw_value, Principal, Value::Principal)
}

// Decode one subaccount field using the persisted CBOR subaccount codec.
fn decode_subaccount_value(raw_value: CborValue) -> Result<Value, RowDecodeFailure> {
    decode_typed_cbor_value!(raw_value, Subaccount, Value::Subaccount)
}

// Decode one text field using the persisted CBOR string codec.
fn decode_text_value(raw_value: CborValue) -> Result<Value, RowDecodeFailure> {
    decode_typed_cbor_value!(raw_value, String, Value::Text)
}

// Decode one timestamp field using the persisted CBOR timestamp codec.
fn decode_timestamp_value(raw_value: CborValue) -> Result<Value, RowDecodeFailure> {
    decode_typed_cbor_value!(raw_value, Timestamp, Value::Timestamp)
}

// Decode one unsigned 64-bit integer field using the persisted CBOR integer codec.
fn decode_uint64_value(raw_value: CborValue) -> Result<Value, RowDecodeFailure> {
    decode_typed_cbor_value!(raw_value, u64, Value::Uint)
}

// Decode one unsigned 128-bit integer field using the persisted CBOR integer codec.
fn decode_uint128_value(raw_value: CborValue) -> Result<Value, RowDecodeFailure> {
    decode_typed_cbor_value!(raw_value, Nat128, Value::Uint128)
}

// Decode one arbitrary-precision unsigned integer field using the persisted CBOR integer codec.
fn decode_uint_big_value(raw_value: CborValue) -> Result<Value, RowDecodeFailure> {
    decode_typed_cbor_value!(raw_value, Nat, Value::UintBig)
}

// Decode one ULID field using the persisted CBOR ULID codec.
fn decode_ulid_value(raw_value: CborValue) -> Result<Value, RowDecodeFailure> {
    decode_typed_cbor_value!(raw_value, Ulid, Value::Ulid)
}

// Decode one unit field while preserving the runtime `Value::Unit` contract.
fn decode_unit_value(raw_value: CborValue) -> Result<Value, RowDecodeFailure> {
    decode_typed_cbor_value!(raw_value, (), |()| Value::Unit)
}

// Decode one collection field into the canonical runtime list representation.
fn decode_list_value(raw_value: CborValue, inner: FieldKind) -> Result<Value, RowDecodeFailure> {
    let CborValue::Array(items) = raw_value else {
        return Err(RowDecodeFailure::new(
            "expected CBOR array for list/set field",
        ));
    };
    let items = items
        .iter()
        .map(|item| decode_field_value(item, inner))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Value::List(items))
}

// Decode one persisted map field while preserving current `FieldValue::to_value`
// semantics: canonicalize when possible, but keep the raw decoded entry order
// when validation rejects the decoded entry shapes.
fn decode_map_value(
    raw_value: CborValue,
    key_kind: FieldKind,
    value_kind: FieldKind,
) -> Result<Value, RowDecodeFailure> {
    let CborValue::Map(entries) = raw_value else {
        return Err(RowDecodeFailure::new("expected CBOR map for map field"));
    };
    let entries = entries
        .into_iter()
        .map(|(entry_key, entry_value)| {
            Ok((
                decode_field_value(&entry_key, key_kind)?,
                decode_field_value(&entry_value, value_kind)?,
            ))
        })
        .collect::<Result<Vec<_>, RowDecodeFailure>>()?;

    Ok(match Value::normalize_map_entries(entries.clone()) {
        Ok(normalized) => Value::Map(normalized),
        Err(_) => Value::Map(entries),
    })
}

// Decode one enum field using the schema path plus the persisted CBOR enum
// shape. Unit variants arrive as text; data-carrying variants arrive as the
// canonical externally-tagged one-entry map.
fn decode_enum_value(raw_value: CborValue, path: &'static str) -> Result<Value, RowDecodeFailure> {
    match raw_value {
        CborValue::Text(variant) => Ok(Value::Enum(ValueEnum::new(&variant, Some(path)))),
        CborValue::Map(entries) => {
            if entries.len() != 1 {
                return Err(RowDecodeFailure::new(
                    "expected one-entry CBOR map for enum payload variant",
                ));
            }
            let Some((variant, payload)) = entries.into_iter().next() else {
                return Err(RowDecodeFailure::new(
                    "expected one-entry CBOR map for enum payload variant",
                ));
            };
            let CborValue::Text(variant) = unwrap_cbor_tags(variant) else {
                return Err(RowDecodeFailure::new(
                    "expected text variant tag for enum payload variant",
                ));
            };
            let payload = decode_untyped_cbor_value(unwrap_cbor_tags(payload))?;

            Ok(Value::Enum(
                ValueEnum::new(&variant, Some(path)).with_payload(payload),
            ))
        }
        other => Err(RowDecodeFailure::new(format!(
            "unsupported CBOR enum shape: {other:?}",
        ))),
    }
}

// Convert one untyped CBOR value into a structural runtime `Value`.
// This is used for enum payloads, where field metadata does not retain payload
// schema and the runtime must preserve a best-effort structural representation.
fn decode_untyped_cbor_value(raw_value: CborValue) -> Result<Value, RowDecodeFailure> {
    match unwrap_cbor_tags(raw_value) {
        CborValue::Null => Ok(Value::Null),
        CborValue::Bool(value) => Ok(Value::Bool(value)),
        CborValue::Integer(value) => decode_untyped_integer_value(value),
        CborValue::Float(value) => Float64::try_new(value)
            .map(Value::Float64)
            .ok_or_else(|| RowDecodeFailure::new("non-finite CBOR float payload")),
        CborValue::Bytes(value) => Ok(Value::Blob(value)),
        CborValue::Text(value) => Ok(Value::Text(value)),
        CborValue::Array(items) => Ok(Value::List(
            items
                .into_iter()
                .map(decode_untyped_cbor_value)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        CborValue::Map(entries) => {
            let entries = entries
                .into_iter()
                .map(|(entry_key, entry_value)| {
                    Ok((
                        decode_untyped_cbor_value(entry_key)?,
                        decode_untyped_cbor_value(entry_value)?,
                    ))
                })
                .collect::<Result<Vec<_>, RowDecodeFailure>>()?;

            Ok(match Value::normalize_map_entries(entries.clone()) {
                Ok(normalized) => Value::Map(normalized),
                Err(_) => Value::Map(entries),
            })
        }
        CborValue::Tag(_, inner) => decode_untyped_cbor_value(*inner),
        other => Err(RowDecodeFailure::new(format!(
            "unsupported CBOR payload shape: {other:?}",
        ))),
    }
}

// Decode one untyped integer into the narrowest structural numeric value that
// preserves the persisted magnitude without consulting typed schema metadata.
fn decode_untyped_integer_value(value: i128) -> Result<Value, RowDecodeFailure> {
    if value.is_negative() {
        if let Ok(value) = i64::try_from(value) {
            return Ok(Value::Int(value));
        }

        return Ok(Value::Int128(Int128::from(value)));
    }
    if let Ok(value) = u64::try_from(value) {
        return Ok(Value::Uint(value));
    }

    u128::try_from(value)
        .map(Nat128::from)
        .map(Value::Uint128)
        .map_err(|_| RowDecodeFailure::new("CBOR integer exceeds supported structural range"))
}

// Strip transparent CBOR tags before structural field decode. Persisted row
// payloads are not expected to depend on tag semantics inside runtime fields.
fn unwrap_cbor_tags(mut value: CborValue) -> CborValue {
    while let CborValue::Tag(_, inner) = value {
        value = *inner;
    }

    value
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        error::{ErrorClass, ErrorOrigin},
        model::field::FieldKind,
        traits::EntitySchema,
        types::{Blob, Text},
        value::{Value, ValueEnum},
    };
    use icydb_derive::FieldProjection;
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

    #[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
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

    fn to_cbor_value<T: Serialize>(value: &T) -> CborValue {
        let bytes =
            serde_cbor::to_vec(value).expect("test fixture should serialize into CBOR bytes");
        serde_cbor::from_slice(&bytes).expect("test fixture should decode into CBOR tree")
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
        let decoded = decode_field_value(
            &to_cbor_value(&vec!["x".to_string(), "y".to_string()]),
            FieldKind::Structured { queryable: false },
        )
        .expect("structured field decode should succeed");

        assert_eq!(decoded, Value::Null);
    }

    #[test]
    fn structural_row_decoder_preserves_enum_payload_shape_best_effort() {
        let decoded = decode_field_value(
            &to_cbor_value(&serde_cbor::Value::Map(BTreeMap::from([(
                serde_cbor::Value::Text("Loaded".to_string()),
                serde_cbor::Value::Integer(7),
            )]))),
            FieldKind::Enum {
                path: "tests::State",
            },
        )
        .expect("enum payload decode should succeed");

        assert_eq!(
            decoded,
            Value::Enum(
                ValueEnum::new("Loaded", Some("tests::State")).with_payload(Value::Uint(7)),
            ),
        );
    }
}
