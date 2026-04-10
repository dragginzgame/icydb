use crate::{
    db::{
        codec::serialize_row_payload,
        data::{
            CanonicalRow, RawRow, decode_structural_field_by_kind_bytes,
            decode_structural_value_storage_bytes, validate_structural_field_by_kind_bytes,
            validate_structural_value_storage_bytes,
        },
        scalar_expr::compile_scalar_literal_expr_value,
        schema::{field_type_from_model_kind, literal_matches_type},
    },
    error::InternalError,
    model::{
        entity::EntityModel,
        field::{FieldKind, FieldModel, FieldStorageDecode, LeafCodec},
    },
    serialize::serialize,
    value::{StorageKey, Value, ValueEnum},
};
use serde_cbor::{Value as CborValue, value::to_value as to_cbor_value};
use std::{borrow::Cow, cmp::Ordering, collections::BTreeMap};

use crate::db::data::persisted_row::{
    codec::{
        ScalarSlotValueRef, ScalarValueRef, decode_scalar_slot_value, encode_scalar_slot_value,
    },
    types::SerializedUpdatePatch,
};

/// Decode one slot value through the declared field contract without routing
/// through `SlotReader::get_value`.
#[cfg(test)]
pub(in crate::db::data::persisted_row) fn decode_slot_value_by_contract(
    slots: &dyn crate::db::data::persisted_row::SlotReader,
    slot: usize,
) -> Result<Option<Value>, InternalError> {
    let Some(raw_value) = slots.get_bytes(slot) else {
        return Ok(None);
    };

    decode_slot_value_from_bytes(slots.model(), slot, raw_value).map(Some)
}

/// Decode one structural slot payload using the owning model field contract.
///
/// This is the canonical field-level decode boundary for persisted-row bytes.
/// Higher-level row readers may still cache decoded values, but they should not
/// rebuild scalar-vs-CBOR field dispatch themselves.
pub(in crate::db) fn decode_slot_value_from_bytes(
    model: &'static EntityModel,
    slot: usize,
    raw_value: &[u8],
) -> Result<Value, InternalError> {
    let field = field_model_for_slot(model, slot)?;

    decode_slot_value_for_field(field, raw_value)
}

// Decode one structural slot payload once the owning field contract has
// already been resolved.
pub(in crate::db::data::persisted_row) fn decode_slot_value_for_field(
    field: &FieldModel,
    raw_value: &[u8],
) -> Result<Value, InternalError> {
    match field.leaf_codec() {
        LeafCodec::Scalar(codec) => match decode_scalar_slot_value(raw_value, codec, field.name())?
        {
            ScalarSlotValueRef::Null => Ok(Value::Null),
            ScalarSlotValueRef::Value(value) => Ok(value.into_value()),
        },
        LeafCodec::CborFallback => {
            if field.nullable() && is_canonical_nullable_cbor_null_payload(raw_value) {
                return Ok(Value::Null);
            }

            decode_non_scalar_slot_value(raw_value, field)
        }
    }
}

/// Encode one structural slot value using the owning model field contract.
///
/// This is the initial `0.64` write-side field-codec boundary. It currently
/// covers:
/// - scalar leaf slots
/// - `FieldStorageDecode::Value` slots
///
/// Composite `ByKind` field encoding remains a follow-up slice so the runtime
/// can add one structural encoder owner instead of quietly rebuilding typed
/// per-field branches.
pub(in crate::db::data::persisted_row) fn encode_slot_value_from_value(
    model: &'static EntityModel,
    slot: usize,
    value: &Value,
) -> Result<Vec<u8>, InternalError> {
    let field = field_model_for_slot(model, slot)?;
    ensure_slot_value_matches_field_contract(field, value)?;

    match field.storage_decode() {
        FieldStorageDecode::Value => serialize(value)
            .map_err(|err| InternalError::persisted_row_field_encode_failed(field.name(), err)),
        FieldStorageDecode::ByKind => match field.leaf_codec() {
            LeafCodec::Scalar(_) => {
                let scalar = compile_scalar_literal_expr_value(value).ok_or_else(|| {
                    InternalError::persisted_row_field_encode_failed(
                        field.name(),
                        format!(
                            "field kind {:?} requires a scalar runtime value, found {value:?}",
                            field.kind()
                        ),
                    )
                })?;

                Ok(encode_scalar_slot_value(scalar.as_slot_value_ref()))
            }
            LeafCodec::CborFallback => {
                encode_structural_field_bytes_by_kind(field.kind(), value, field.name())
            }
        },
    }
}

// Decode one slot payload and immediately re-encode it through the current
// field contract so every row-emission path normalizes bytes at the boundary.
fn canonicalize_slot_payload(
    model: &'static EntityModel,
    slot: usize,
    raw_value: &[u8],
) -> Result<Vec<u8>, InternalError> {
    let value = decode_slot_value_from_bytes(model, slot, raw_value)?;

    encode_slot_value_from_value(model, slot, &value)
}

// Build one dense canonical slot image from any slot-addressable payload source.
// Callers keep ownership of missing-slot policy while this helper centralizes
// the slot-by-slot canonicalization loop.
pub(in crate::db::data::persisted_row) fn dense_canonical_slot_image_from_payload_source<'a, F>(
    model: &'static EntityModel,
    mut payload_for_slot: F,
) -> Result<Vec<Vec<u8>>, InternalError>
where
    F: FnMut(usize) -> Result<&'a [u8], InternalError>,
{
    let mut slot_payloads = Vec::with_capacity(model.fields().len());

    for slot in 0..model.fields().len() {
        let payload = payload_for_slot(slot)?;
        slot_payloads.push(canonicalize_slot_payload(model, slot, payload)?);
    }

    Ok(slot_payloads)
}

// Build one dense canonical slot image from already-decoded runtime values.
// This keeps row-emission paths from re-decoding raw slot bytes when a caller
// already owns the validated structural value cache.
pub(in crate::db::data::persisted_row) fn dense_canonical_slot_image_from_value_source<'a, F>(
    model: &'static EntityModel,
    mut value_for_slot: F,
) -> Result<Vec<Vec<u8>>, InternalError>
where
    F: FnMut(usize) -> Result<Cow<'a, Value>, InternalError>,
{
    let mut slot_payloads = Vec::with_capacity(model.fields().len());

    for slot in 0..model.fields().len() {
        let value = value_for_slot(slot)?;
        slot_payloads.push(encode_slot_value_from_value(model, slot, value.as_ref())?);
    }

    Ok(slot_payloads)
}

// Encode one fixed-width slot table plus concatenated slot payload bytes into
// the canonical row payload container.
pub(in crate::db::data::persisted_row) fn encode_slot_payload_from_parts(
    slot_count: usize,
    slot_table: &[(u32, u32)],
    payload_bytes: &[u8],
) -> Result<Vec<u8>, InternalError> {
    let field_count = u16::try_from(slot_count).map_err(|_| {
        InternalError::persisted_row_encode_failed(format!(
            "field count {slot_count} exceeds u16 slot table capacity",
        ))
    })?;
    let mut encoded = Vec::with_capacity(
        usize::from(field_count) * (u32::BITS as usize / 4) + 2 + payload_bytes.len(),
    );
    encoded.extend_from_slice(&field_count.to_be_bytes());
    for (start, len) in slot_table {
        encoded.extend_from_slice(&start.to_be_bytes());
        encoded.extend_from_slice(&len.to_be_bytes());
    }
    encoded.extend_from_slice(payload_bytes);

    Ok(encoded)
}

// Emit one raw row from a dense canonical slot image.
pub(in crate::db::data::persisted_row) fn emit_raw_row_from_slot_payloads(
    model: &'static EntityModel,
    slot_payloads: &[Vec<u8>],
) -> Result<CanonicalRow, InternalError> {
    if slot_payloads.len() != model.fields().len() {
        return Err(InternalError::persisted_row_encode_failed(format!(
            "canonical slot image expected {} slots for entity '{}', found {}",
            model.fields().len(),
            model.path(),
            slot_payloads.len()
        )));
    }

    let payload_capacity = slot_payloads
        .iter()
        .try_fold(0usize, |len, payload| len.checked_add(payload.len()))
        .ok_or_else(|| {
            InternalError::persisted_row_encode_failed(
                "canonical slot image payload length overflow",
            )
        })?;
    let mut payload_bytes = Vec::with_capacity(payload_capacity);
    let mut slot_table = Vec::with_capacity(slot_payloads.len());

    // Phase 1: flatten the already canonicalized dense slot image directly so
    // row re-emission does not clone each slot payload back through the
    // mutable slot-writer staging buffer first.
    for (slot, payload) in slot_payloads.iter().enumerate() {
        let start = u32::try_from(payload_bytes.len()).map_err(|_| {
            InternalError::persisted_row_encode_failed(format!(
                "canonical slot payload start exceeds u32 range: slot={slot}",
            ))
        })?;
        let len = u32::try_from(payload.len()).map_err(|_| {
            InternalError::persisted_row_encode_failed(format!(
                "canonical slot payload length exceeds u32 range: slot={slot}",
            ))
        })?;
        payload_bytes.extend_from_slice(payload.as_slice());
        slot_table.push((start, len));
    }

    // Phase 2: wrap the canonical slot container in the shared row envelope.
    let row_payload =
        encode_slot_payload_from_parts(slot_payloads.len(), slot_table.as_slice(), &payload_bytes)?;
    let encoded = serialize_row_payload(row_payload)?;
    let raw_row = RawRow::from_untrusted_bytes(encoded).map_err(InternalError::from)?;

    Ok(CanonicalRow::from_canonical_raw_row(raw_row))
}

// Decode one non-scalar slot through the exact persisted contract declared by
// the field model.
fn decode_non_scalar_slot_value(
    raw_value: &[u8],
    field: &FieldModel,
) -> Result<Value, InternalError> {
    let decoded = match field.storage_decode() {
        crate::model::field::FieldStorageDecode::ByKind => {
            decode_structural_field_by_kind_bytes(raw_value, field.kind())
        }
        crate::model::field::FieldStorageDecode::Value => {
            decode_structural_value_storage_bytes(raw_value)
        }
    };

    decoded.map_err(|err| {
        InternalError::persisted_row_field_kind_decode_failed(field.name(), field.kind(), err)
    })
}

// Validate one non-scalar slot through the exact persisted contract declared
// by the field model without eagerly building the final runtime `Value`.
pub(in crate::db::data::persisted_row) fn validate_non_scalar_slot_value(
    raw_value: &[u8],
    field: &FieldModel,
) -> Result<(), InternalError> {
    if field.nullable() && is_canonical_nullable_cbor_null_payload(raw_value) {
        return Ok(());
    }

    let validated = match field.storage_decode() {
        crate::model::field::FieldStorageDecode::ByKind => {
            validate_structural_field_by_kind_bytes(raw_value, field.kind())
        }
        crate::model::field::FieldStorageDecode::Value => {
            validate_structural_value_storage_bytes(raw_value)
        }
    };

    validated.map_err(|err| {
        InternalError::persisted_row_field_kind_decode_failed(field.name(), field.kind(), err)
    })
}

// Detect the canonical explicit-null payload used for nullable CBOR-backed
// fields before field-kind-specific structural decode requires a concrete shape.
fn is_canonical_nullable_cbor_null_payload(raw_value: &[u8]) -> bool {
    raw_value == [0xF6]
}

// Validate one runtime value against the persisted field contract before field-
// level structural encoding writes bytes into a row slot.
fn ensure_slot_value_matches_field_contract(
    field: &FieldModel,
    value: &Value,
) -> Result<(), InternalError> {
    if matches!(value, Value::Null) {
        if field.nullable() {
            return Ok(());
        }

        return Err(InternalError::persisted_row_field_encode_failed(
            field.name(),
            "required field cannot store null",
        ));
    }

    // `FieldStorageDecode::Value` fields persist the generic `Value` envelope
    // directly, so storage-side validation must accept structured leaves nested
    // under collection contracts instead of reusing the predicate literal gate.
    if matches!(field.storage_decode(), FieldStorageDecode::Value) {
        if !storage_value_matches_field_kind(field.kind(), value) {
            return Err(InternalError::persisted_row_field_encode_failed(
                field.name(),
                format!(
                    "field kind {:?} does not accept runtime value {value:?}",
                    field.kind()
                ),
            ));
        }

        ensure_decimal_scale_matches(field.name(), field.kind(), value)?;

        return ensure_value_is_deterministic_for_storage(field.name(), field.kind(), value);
    }

    let field_type = field_type_from_model_kind(&field.kind());
    if !literal_matches_type(value, &field_type) {
        return Err(InternalError::persisted_row_field_encode_failed(
            field.name(),
            format!(
                "field kind {:?} does not accept runtime value {value:?}",
                field.kind()
            ),
        ));
    }

    ensure_decimal_scale_matches(field.name(), field.kind(), value)?;
    ensure_value_is_deterministic_for_storage(field.name(), field.kind(), value)
}

// Match one runtime value against the semantic field kind used by value-backed
// storage. Unlike predicate literals, non-queryable structured leaves are valid
// persisted payloads when they arrive as canonical `Value::List` / `Value::Map`
// shapes.
fn storage_value_matches_field_kind(kind: FieldKind, value: &Value) -> bool {
    match (kind, value) {
        (FieldKind::Account, Value::Account(_))
        | (FieldKind::Blob, Value::Blob(_))
        | (FieldKind::Bool, Value::Bool(_))
        | (FieldKind::Date, Value::Date(_))
        | (FieldKind::Decimal { .. }, Value::Decimal(_))
        | (FieldKind::Duration, Value::Duration(_))
        | (FieldKind::Enum { .. }, Value::Enum(_))
        | (FieldKind::Float32, Value::Float32(_))
        | (FieldKind::Float64, Value::Float64(_))
        | (FieldKind::Int, Value::Int(_))
        | (FieldKind::Int128, Value::Int128(_))
        | (FieldKind::IntBig, Value::IntBig(_))
        | (FieldKind::Principal, Value::Principal(_))
        | (FieldKind::Subaccount, Value::Subaccount(_))
        | (FieldKind::Text, Value::Text(_))
        | (FieldKind::Timestamp, Value::Timestamp(_))
        | (FieldKind::Uint, Value::Uint(_))
        | (FieldKind::Uint128, Value::Uint128(_))
        | (FieldKind::UintBig, Value::UintBig(_))
        | (FieldKind::Ulid, Value::Ulid(_))
        | (FieldKind::Unit, Value::Unit)
        | (FieldKind::Structured { .. }, Value::List(_) | Value::Map(_)) => true,
        (FieldKind::Relation { key_kind, .. }, value) => {
            storage_value_matches_field_kind(*key_kind, value)
        }
        (FieldKind::List(inner) | FieldKind::Set(inner), Value::List(items)) => items
            .iter()
            .all(|item| storage_value_matches_field_kind(*inner, item)),
        (FieldKind::Map { key, value }, Value::Map(entries)) => {
            if Value::validate_map_entries(entries.as_slice()).is_err() {
                return false;
            }

            entries.iter().all(|(entry_key, entry_value)| {
                storage_value_matches_field_kind(*key, entry_key)
                    && storage_value_matches_field_kind(*value, entry_value)
            })
        }
        _ => false,
    }
}

// Enforce fixed decimal scales through nested collection/map shapes before a
// field-level patch value is persisted.
fn ensure_decimal_scale_matches(
    field_name: &str,
    kind: FieldKind,
    value: &Value,
) -> Result<(), InternalError> {
    if matches!(value, Value::Null) {
        return Ok(());
    }

    match (kind, value) {
        (FieldKind::Decimal { scale }, Value::Decimal(decimal)) => {
            if decimal.scale() != scale {
                return Err(InternalError::persisted_row_field_encode_failed(
                    field_name,
                    format!(
                        "decimal scale mismatch: expected {scale}, found {}",
                        decimal.scale()
                    ),
                ));
            }

            Ok(())
        }
        (FieldKind::Relation { key_kind, .. }, value) => {
            ensure_decimal_scale_matches(field_name, *key_kind, value)
        }
        (FieldKind::List(inner) | FieldKind::Set(inner), Value::List(items)) => {
            for item in items {
                ensure_decimal_scale_matches(field_name, *inner, item)?;
            }

            Ok(())
        }
        (
            FieldKind::Map {
                key,
                value: map_value,
            },
            Value::Map(entries),
        ) => {
            for (entry_key, entry_value) in entries {
                ensure_decimal_scale_matches(field_name, *key, entry_key)?;
                ensure_decimal_scale_matches(field_name, *map_value, entry_value)?;
            }

            Ok(())
        }
        _ => Ok(()),
    }
}

// Enforce the canonical persisted ordering rules for set/map shapes before one
// field-level patch value becomes row bytes.
fn ensure_value_is_deterministic_for_storage(
    field_name: &str,
    kind: FieldKind,
    value: &Value,
) -> Result<(), InternalError> {
    match (kind, value) {
        (FieldKind::Set(_), Value::List(items)) => {
            for pair in items.windows(2) {
                let [left, right] = pair else {
                    continue;
                };
                if Value::canonical_cmp(left, right) != Ordering::Less {
                    return Err(InternalError::persisted_row_field_encode_failed(
                        field_name,
                        "set payload must already be canonical and deduplicated",
                    ));
                }
            }

            Ok(())
        }
        (FieldKind::Map { .. }, Value::Map(entries)) => {
            Value::validate_map_entries(entries.as_slice())
                .map_err(|err| InternalError::persisted_row_field_encode_failed(field_name, err))?;

            if !Value::map_entries_are_strictly_canonical(entries.as_slice()) {
                return Err(InternalError::persisted_row_field_encode_failed(
                    field_name,
                    "map payload must already be canonical and deduplicated",
                ));
            }

            Ok(())
        }
        _ => Ok(()),
    }
}

// Materialize the last-write-wins serialized patch view indexed by stable slot.
pub(in crate::db::data::persisted_row) fn serialized_patch_payload_by_slot<'a>(
    model: &'static EntityModel,
    patch: &'a SerializedUpdatePatch,
) -> Result<Vec<Option<&'a [u8]>>, InternalError> {
    let mut payloads = vec![None; model.fields().len()];

    for entry in patch.entries() {
        let slot = entry.slot().index();
        field_model_for_slot(model, slot)?;
        payloads[slot] = Some(entry.payload());
    }

    Ok(payloads)
}

// Encode one `ByKind` field payload into the raw CBOR shape expected by the
// structural field decoder.
fn encode_structural_field_bytes_by_kind(
    kind: FieldKind,
    value: &Value,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    let cbor_value = encode_structural_field_cbor_by_kind(kind, value, field_name)?;

    serialize(&cbor_value)
        .map_err(|err| InternalError::persisted_row_field_encode_failed(field_name, err))
}

// Encode one `ByKind` field payload into its raw CBOR value form.
fn encode_structural_field_cbor_by_kind(
    kind: FieldKind,
    value: &Value,
    field_name: &str,
) -> Result<CborValue, InternalError> {
    match (kind, value) {
        (_, Value::Null) => Ok(CborValue::Null),
        (FieldKind::Blob, Value::Blob(value)) => Ok(CborValue::Bytes(value.clone())),
        (FieldKind::Bool, Value::Bool(value)) => Ok(CborValue::Bool(*value)),
        (FieldKind::Text, Value::Text(value)) => Ok(CborValue::Text(value.clone())),
        (FieldKind::Int, Value::Int(value)) => Ok(CborValue::Integer(i128::from(*value))),
        (FieldKind::Uint, Value::Uint(value)) => Ok(CborValue::Integer(i128::from(*value))),
        (FieldKind::Float32, Value::Float32(value)) => to_cbor_value(value)
            .map_err(|err| InternalError::persisted_row_field_encode_failed(field_name, err)),
        (FieldKind::Float64, Value::Float64(value)) => to_cbor_value(value)
            .map_err(|err| InternalError::persisted_row_field_encode_failed(field_name, err)),
        (FieldKind::Int128, Value::Int128(value)) => to_cbor_value(value)
            .map_err(|err| InternalError::persisted_row_field_encode_failed(field_name, err)),
        (FieldKind::Uint128, Value::Uint128(value)) => to_cbor_value(value)
            .map_err(|err| InternalError::persisted_row_field_encode_failed(field_name, err)),
        (FieldKind::Ulid, Value::Ulid(value)) => Ok(CborValue::Text(value.to_string())),
        (FieldKind::Account, Value::Account(value)) => encode_leaf_cbor_value(value, field_name),
        (FieldKind::Date, Value::Date(value)) => encode_leaf_cbor_value(value, field_name),
        (FieldKind::Decimal { .. }, Value::Decimal(value)) => {
            encode_leaf_cbor_value(value, field_name)
        }
        (FieldKind::Duration, Value::Duration(value)) => encode_leaf_cbor_value(value, field_name),
        (FieldKind::IntBig, Value::IntBig(value)) => encode_leaf_cbor_value(value, field_name),
        (FieldKind::Principal, Value::Principal(value)) => {
            encode_leaf_cbor_value(value, field_name)
        }
        (FieldKind::Subaccount, Value::Subaccount(value)) => {
            encode_leaf_cbor_value(value, field_name)
        }
        (FieldKind::Timestamp, Value::Timestamp(value)) => {
            encode_leaf_cbor_value(value, field_name)
        }
        (FieldKind::UintBig, Value::UintBig(value)) => encode_leaf_cbor_value(value, field_name),
        (FieldKind::Unit, Value::Unit) => encode_leaf_cbor_value(&(), field_name),
        (FieldKind::Relation { key_kind, .. }, value) => {
            encode_structural_field_cbor_by_kind(*key_kind, value, field_name)
        }
        (FieldKind::List(inner) | FieldKind::Set(inner), Value::List(items)) => {
            Ok(CborValue::Array(
                items
                    .iter()
                    .map(|item| encode_structural_field_cbor_by_kind(*inner, item, field_name))
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        }
        (FieldKind::Map { key, value }, Value::Map(entries)) => {
            let mut encoded = BTreeMap::new();
            for (entry_key, entry_value) in entries {
                encoded.insert(
                    encode_structural_field_cbor_by_kind(*key, entry_key, field_name)?,
                    encode_structural_field_cbor_by_kind(*value, entry_value, field_name)?,
                );
            }

            Ok(CborValue::Map(encoded))
        }
        (FieldKind::Enum { path, variants }, Value::Enum(value)) => {
            encode_enum_cbor_value(path, variants, value, field_name)
        }
        (FieldKind::Structured { .. }, _) => Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            "structured ByKind field encoding is unsupported",
        )),
        _ => Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("field kind {kind:?} does not accept runtime value {value:?}"),
        )),
    }
}

// Encode one typed leaf wrapper into its raw CBOR value form.
fn encode_leaf_cbor_value<T>(value: &T, field_name: &str) -> Result<CborValue, InternalError>
where
    T: serde::Serialize,
{
    to_cbor_value(value)
        .map_err(|err| InternalError::persisted_row_field_encode_failed(field_name, err))
}

// Encode one enum field using the same unit-vs-one-entry-map envelope expected
// by structural enum decode.
fn encode_enum_cbor_value(
    path: &'static str,
    variants: &'static [crate::model::field::EnumVariantModel],
    value: &ValueEnum,
    field_name: &str,
) -> Result<CborValue, InternalError> {
    if let Some(actual_path) = value.path()
        && actual_path != path
    {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("enum path mismatch: expected '{path}', found '{actual_path}'"),
        ));
    }

    let Some(payload) = value.payload() else {
        return Ok(CborValue::Text(value.variant().to_string()));
    };

    let Some(variant_model) = variants.iter().find(|item| item.ident() == value.variant()) else {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!(
                "unknown enum variant '{}' for path '{path}'",
                value.variant()
            ),
        ));
    };
    let Some(payload_kind) = variant_model.payload_kind() else {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!(
                "enum variant '{}' does not accept a payload",
                value.variant()
            ),
        ));
    };

    let payload_value = match variant_model.payload_storage_decode() {
        FieldStorageDecode::ByKind => {
            encode_structural_field_cbor_by_kind(*payload_kind, payload, field_name)?
        }
        FieldStorageDecode::Value => to_cbor_value(payload)
            .map_err(|err| InternalError::persisted_row_field_encode_failed(field_name, err))?,
    };

    let mut encoded = BTreeMap::new();
    encoded.insert(CborValue::Text(value.variant().to_string()), payload_value);

    Ok(CborValue::Map(encoded))
}

// Resolve one field model entry by stable slot index.
pub(in crate::db::data::persisted_row) fn field_model_for_slot(
    model: &'static EntityModel,
    slot: usize,
) -> Result<&'static FieldModel, InternalError> {
    model
        .fields()
        .get(slot)
        .ok_or_else(|| InternalError::persisted_row_slot_lookup_out_of_bounds(model.path(), slot))
}

// Convert one scalar slot fast-path value into its storage-key form when the
// field kind is storage-key-compatible.
pub(in crate::db::data::persisted_row) const fn storage_key_from_scalar_ref(
    value: ScalarValueRef<'_>,
) -> Option<StorageKey> {
    match value {
        ScalarValueRef::Int(value) => Some(StorageKey::Int(value)),
        ScalarValueRef::Principal(value) => Some(StorageKey::Principal(value)),
        ScalarValueRef::Subaccount(value) => Some(StorageKey::Subaccount(value)),
        ScalarValueRef::Timestamp(value) => Some(StorageKey::Timestamp(value)),
        ScalarValueRef::Uint(value) => Some(StorageKey::Uint(value)),
        ScalarValueRef::Ulid(value) => Some(StorageKey::Ulid(value)),
        ScalarValueRef::Unit => Some(StorageKey::Unit),
        _ => None,
    }
}
