//! Module: data::structural_field
//! Responsibility: canonical persisted-field structural decode helpers.
//! Does not own: row layout planning, typed entity reconstruction, or query semantics.
//! Boundary: runtime paths use this module when they need one persisted field decoded without `E`.

use crate::{
    model::field::{EnumVariantModel, FieldKind, FieldStorageDecode},
    traits::NumFromPrimitive,
    types::{
        Account, Date, Decimal, Duration, Float32, Float64, Int, Int128, Nat, Nat128, Timestamp,
        Ulid,
    },
    value::{StorageKey, Value, ValueEnum},
};
use candid::{Int as WrappedInt, Nat as WrappedNat};
use num_bigint::{BigInt, BigUint, Sign as BigIntSign};
use std::str;
use thiserror::Error as ThisError;

///
/// StructuralFieldDecodeError
///
/// StructuralFieldDecodeError captures one persisted-field structural decode
/// failure.
/// It keeps structural decode diagnostics local to the field boundary so row
/// and relation callers can map them into taxonomy-correct higher-level errors.
///

#[derive(Clone, Debug, ThisError)]
#[error("{message}")]
pub(in crate::db) struct StructuralFieldDecodeError {
    message: String,
}

impl StructuralFieldDecodeError {
    // Build one structural field-decode failure message.
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Decode one encoded persisted field payload using the runtime storage-decode contract.
///
/// This wrapper only exists for structural-field tests. Production decode paths
/// should dispatch directly into the by-kind or value-storage entrypoint so
/// `ByKind` recursion does not retain the generic branch.
#[cfg(test)]
pub(in crate::db) fn decode_structural_field_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
    storage_decode: FieldStorageDecode,
) -> Result<Value, StructuralFieldDecodeError> {
    match storage_decode {
        FieldStorageDecode::ByKind => decode_structural_field_by_kind_bytes(raw_bytes, kind),
        FieldStorageDecode::Value => decode_structural_value_storage_bytes(raw_bytes),
    }
}

/// Decode one encoded persisted field payload strictly by semantic field kind.
pub(in crate::db) fn decode_structural_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Value, StructuralFieldDecodeError> {
    // Keep byte-backed `ByKind` leaves off the generic `ValueWire` bridge
    // whenever their persisted shape is fixed or already owned by the leaf
    // type.
    if let Some(value) = decode_scalar_fast_path_bytes(raw_bytes, kind)? {
        return Ok(value);
    }

    match kind {
        FieldKind::Account => decode_account_value_bytes(raw_bytes),
        FieldKind::Blob
        | FieldKind::Bool
        | FieldKind::Float32
        | FieldKind::Float64
        | FieldKind::Int
        | FieldKind::Int128
        | FieldKind::Text
        | FieldKind::Uint
        | FieldKind::Uint128
        | FieldKind::Ulid => Err(StructuralFieldDecodeError::new(
            "scalar field unexpectedly bypassed byte-level fast path",
        )),
        FieldKind::Date => decode_date_value_bytes(raw_bytes),
        FieldKind::Decimal { .. } => decode_decimal_value_bytes(raw_bytes),
        FieldKind::Duration => decode_duration_value_bytes(raw_bytes),
        FieldKind::Enum { path, variants } => decode_enum_bytes(raw_bytes, path, variants),
        FieldKind::IntBig => decode_int_big_value_bytes(raw_bytes),
        FieldKind::List(inner) | FieldKind::Set(inner) => decode_list_bytes(raw_bytes, *inner),
        FieldKind::Map { key, value } => decode_map_bytes(raw_bytes, *key, *value),
        FieldKind::Principal => decode_principal_value_bytes(raw_bytes),
        FieldKind::Relation { key_kind, .. } => {
            decode_structural_field_by_kind_bytes(raw_bytes, *key_kind)
        }
        FieldKind::Structured { .. } => Ok(Value::Null),
        FieldKind::Subaccount => decode_subaccount_value_bytes(raw_bytes),
        FieldKind::Timestamp => decode_timestamp_value_bytes(raw_bytes),
        FieldKind::UintBig => decode_uint_big_value_bytes(raw_bytes),
        FieldKind::Unit => decode_unit_value_bytes(raw_bytes),
    }
}

/// Decode one strong-relation field payload directly into target storage keys.
///
/// This keeps delete validation and reverse-index maintenance on structural
/// key forms without first rebuilding a runtime `Value` or `Value::List`.
pub(in crate::db) fn decode_relation_target_storage_keys_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Vec<StorageKey>, StructuralFieldDecodeError> {
    match kind {
        FieldKind::Relation { key_kind, .. } => Ok(decode_optional_relation_storage_key_bytes(
            raw_bytes, *key_kind,
        )?
        .into_iter()
        .collect()),
        FieldKind::List(FieldKind::Relation { key_kind, .. })
        | FieldKind::Set(FieldKind::Relation { key_kind, .. }) => {
            decode_relation_storage_key_list_bytes(raw_bytes, **key_kind)
        }
        other => Err(StructuralFieldDecodeError::new(format!(
            "invalid strong relation field kind during structural key decode: {other:?}"
        ))),
    }
}

/// Decode one storage-key-compatible field payload directly into its canonical
/// `StorageKey` form.
pub(in crate::db) fn decode_storage_key_field_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<StorageKey, StructuralFieldDecodeError> {
    match kind {
        FieldKind::Account => decode_account_storage_key_bytes(raw_bytes),
        FieldKind::Int => decode_int_storage_key_bytes(raw_bytes),
        FieldKind::Principal => decode_principal_storage_key_bytes(raw_bytes),
        FieldKind::Relation { key_kind, .. } => {
            decode_storage_key_field_bytes(raw_bytes, *key_kind)
        }
        FieldKind::Subaccount => decode_subaccount_storage_key_bytes(raw_bytes),
        FieldKind::Timestamp => decode_timestamp_storage_key_bytes(raw_bytes),
        FieldKind::Uint => decode_uint_storage_key_bytes(raw_bytes),
        FieldKind::Ulid => decode_ulid_storage_key_bytes(raw_bytes),
        FieldKind::Unit => decode_unit_storage_key_bytes(raw_bytes),
        other => Err(StructuralFieldDecodeError::new(format!(
            "unsupported storage-key field kind during structural key decode: {other:?}"
        ))),
    }
}

// Decode one singular relation payload, treating explicit null as "no target".
fn decode_optional_relation_storage_key_bytes(
    raw_bytes: &[u8],
    key_kind: FieldKind,
) -> Result<Option<StorageKey>, StructuralFieldDecodeError> {
    let Some((major, argument, _payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated CBOR value",
        ));
    };
    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after relation field",
        ));
    }
    if major == 7 && argument == 22 {
        return Ok(None);
    }

    decode_relation_storage_key_scalar_bytes(raw_bytes, key_kind).map(Some)
}

// Decode one list/set relation payload into canonical storage keys while
// preserving current null-item semantics.
fn decode_relation_storage_key_list_bytes(
    raw_bytes: &[u8],
    key_kind: FieldKind,
) -> Result<Vec<StorageKey>, StructuralFieldDecodeError> {
    let Some((major, argument, _cursor)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated CBOR value",
        ));
    };
    if major == 7 && argument == 22 {
        return Ok(Vec::new());
    }
    if major != 4 {
        return Err(StructuralFieldDecodeError::new(
            "expected CBOR array for list/set field",
        ));
    }

    let mut state = (Vec::new(), key_kind);
    walk_cbor_array_items(
        raw_bytes,
        "expected CBOR array for list/set field",
        "typed CBOR decode failed: trailing bytes after list/set field",
        (&raw mut state).cast(),
        push_relation_storage_key_item,
    )?;

    Ok(state.0)
}

// Decode one relation-compatible scalar field payload into its storage-key form.
fn decode_relation_storage_key_scalar_bytes(
    raw_bytes: &[u8],
    key_kind: FieldKind,
) -> Result<StorageKey, StructuralFieldDecodeError> {
    decode_storage_key_field_bytes(raw_bytes, key_kind)
}

// Parse one bounded CBOR container length into a host `usize`.
fn bounded_cbor_len(
    argument: u64,
    label: &'static str,
) -> Result<usize, StructuralFieldDecodeError> {
    usize::try_from(argument).map_err(|_| StructuralFieldDecodeError::new(label))
}

type RelationKeyDecodeState = (Vec<StorageKey>, FieldKind);
type ValueArrayDecodeState = Vec<Value>;
type KindArrayDecodeState = (Vec<Value>, FieldKind);
type KindMapDecodeState = (Vec<(Value, Value)>, FieldKind, FieldKind);
type UntypedMapDecodeState = Vec<(Value, Value)>;
type ArrayItemDecodeFn = unsafe fn(&[u8], *mut ()) -> Result<(), StructuralFieldDecodeError>;
type MapEntryDecodeFn = unsafe fn(&[u8], &[u8], *mut ()) -> Result<(), StructuralFieldDecodeError>;

#[derive(Clone, Copy)]
enum ValueVariantTag {
    Account,
    Date,
    Decimal,
    Duration,
    Enum,
    IntBig,
    List,
    Map,
    Null,
    Principal,
    Scalar(FieldKind),
    Subaccount,
    Timestamp,
    UintBig,
    Unit,
}

// Walk one CBOR array and yield each raw item slice to the caller.
//
// Safety:
// `context` must point at the state type expected by `on_item` for the full
// duration of this call.
fn walk_cbor_array_items(
    raw_bytes: &[u8],
    shape_label: &'static str,
    trailing_label: &'static str,
    context: *mut (),
    on_item: ArrayItemDecodeFn,
) -> Result<(), StructuralFieldDecodeError> {
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated CBOR value",
        ));
    };
    if major != 4 {
        return Err(StructuralFieldDecodeError::new(shape_label));
    }

    let item_count = bounded_cbor_len(argument, "expected bounded CBOR array length")?;
    for _ in 0..item_count {
        let item_start = cursor;
        cursor = skip_cbor_value(raw_bytes, cursor)?;
        // Safety: the caller pairs `context` with the matching callback, so the
        // callback sees the concrete state type it expects.
        unsafe { on_item(&raw_bytes[item_start..cursor], context)? };
    }
    if cursor != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(trailing_label));
    }

    Ok(())
}

// Resolve one tagged `Value` variant label into its decode contract.
fn parse_value_variant_tag(variant: &str) -> Result<ValueVariantTag, StructuralFieldDecodeError> {
    let tag = match variant {
        "Account" => ValueVariantTag::Account,
        "Blob" => ValueVariantTag::Scalar(FieldKind::Blob),
        "Bool" => ValueVariantTag::Scalar(FieldKind::Bool),
        "Date" => ValueVariantTag::Date,
        "Decimal" => ValueVariantTag::Decimal,
        "Duration" => ValueVariantTag::Duration,
        "Enum" => ValueVariantTag::Enum,
        "Float32" => ValueVariantTag::Scalar(FieldKind::Float32),
        "Float64" => ValueVariantTag::Scalar(FieldKind::Float64),
        "Int" => ValueVariantTag::Scalar(FieldKind::Int),
        "Int128" => ValueVariantTag::Scalar(FieldKind::Int128),
        "IntBig" => ValueVariantTag::IntBig,
        "List" => ValueVariantTag::List,
        "Map" => ValueVariantTag::Map,
        "Null" => ValueVariantTag::Null,
        "Principal" => ValueVariantTag::Principal,
        "Subaccount" => ValueVariantTag::Subaccount,
        "Text" => ValueVariantTag::Scalar(FieldKind::Text),
        "Timestamp" => ValueVariantTag::Timestamp,
        "Uint" => ValueVariantTag::Scalar(FieldKind::Uint),
        "Uint128" => ValueVariantTag::Scalar(FieldKind::Uint128),
        "UintBig" => ValueVariantTag::UintBig,
        "Ulid" => ValueVariantTag::Scalar(FieldKind::Ulid),
        "Unit" => ValueVariantTag::Unit,
        other => {
            return Err(StructuralFieldDecodeError::new(format!(
                "unsupported value variant '{other}'"
            )));
        }
    };

    Ok(tag)
}

// Walk one CBOR map and yield each raw key/value slice pair to the caller.
//
// Safety:
// `context` must point at the state type expected by `on_entry` for the full
// duration of this call.
fn walk_cbor_map_entries(
    raw_bytes: &[u8],
    shape_label: &'static str,
    trailing_label: &'static str,
    context: *mut (),
    on_entry: MapEntryDecodeFn,
) -> Result<(), StructuralFieldDecodeError> {
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated CBOR value",
        ));
    };
    if major != 5 {
        return Err(StructuralFieldDecodeError::new(shape_label));
    }

    let entry_count = bounded_cbor_len(argument, "expected bounded CBOR map length")?;
    for _ in 0..entry_count {
        let key_start = cursor;
        cursor = skip_cbor_value(raw_bytes, cursor)?;
        let value_start = cursor;
        cursor = skip_cbor_value(raw_bytes, cursor)?;
        // Safety: the caller pairs `context` with the matching callback, so the
        // callback sees the concrete state type it expects.
        unsafe {
            on_entry(
                &raw_bytes[key_start..value_start],
                &raw_bytes[value_start..cursor],
                context,
            )?;
        };
    }
    if cursor != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(trailing_label));
    }

    Ok(())
}

// Push one relation-key list item into the decoded target-key buffer.
//
// Safety:
// `context` must be a valid `RelationKeyDecodeState`.
fn push_relation_storage_key_item(
    item_bytes: &[u8],
    context: *mut (),
) -> Result<(), StructuralFieldDecodeError> {
    let state = unsafe { &mut *context.cast::<RelationKeyDecodeState>() };
    if let Some(value) = decode_optional_relation_storage_key_bytes(item_bytes, state.1)? {
        state.0.push(value);
    }

    Ok(())
}

// Push one by-kind list item into the decoded runtime value buffer.
//
// Safety:
// `context` must be a valid `KindArrayDecodeState`.
fn push_kind_array_item(
    item_bytes: &[u8],
    context: *mut (),
) -> Result<(), StructuralFieldDecodeError> {
    let state = unsafe { &mut *context.cast::<KindArrayDecodeState>() };
    state
        .0
        .push(decode_structural_field_by_kind_bytes(item_bytes, state.1)?);

    Ok(())
}

// Push one recursively tagged `Value` list item into the decoded buffer.
//
// Safety:
// `context` must be a valid `ValueArrayDecodeState`.
fn push_value_array_item(
    item_bytes: &[u8],
    context: *mut (),
) -> Result<(), StructuralFieldDecodeError> {
    let items = unsafe { &mut *context.cast::<ValueArrayDecodeState>() };
    items.push(decode_structural_value_storage_bytes(item_bytes)?);

    Ok(())
}

// Push one shallow fallback list item into the decoded buffer.
//
// Safety:
// `context` must be a valid `ValueArrayDecodeState`.
fn push_untyped_array_item(
    item_bytes: &[u8],
    context: *mut (),
) -> Result<(), StructuralFieldDecodeError> {
    let items = unsafe { &mut *context.cast::<ValueArrayDecodeState>() };
    items.push(decode_untyped_shallow_bytes(item_bytes)?);

    Ok(())
}

// Push one decoded `Value::Map` entry into the runtime entry buffer.
//
// Safety:
// `context` must be a valid `Vec<(Value, Value)>`.
fn push_value_storage_map_entry_item(
    item_bytes: &[u8],
    context: *mut (),
) -> Result<(), StructuralFieldDecodeError> {
    let entries = unsafe { &mut *context.cast::<Vec<(Value, Value)>>() };
    entries.push(decode_value_storage_map_entry_bytes(item_bytes)?);

    Ok(())
}

// Push one by-kind map entry into the decoded runtime entry buffer.
//
// Safety:
// `context` must be a valid `KindMapDecodeState`.
fn push_kind_map_entry(
    key_bytes: &[u8],
    value_bytes: &[u8],
    context: *mut (),
) -> Result<(), StructuralFieldDecodeError> {
    let state = unsafe { &mut *context.cast::<KindMapDecodeState>() };
    state.0.push((
        decode_structural_field_by_kind_bytes(key_bytes, state.1)?,
        decode_structural_field_by_kind_bytes(value_bytes, state.2)?,
    ));

    Ok(())
}

// Push one shallow fallback map entry into the decoded runtime entry buffer.
//
// Safety:
// `context` must be a valid `UntypedMapDecodeState`.
fn push_untyped_map_entry(
    key_bytes: &[u8],
    value_bytes: &[u8],
    context: *mut (),
) -> Result<(), StructuralFieldDecodeError> {
    let entries = unsafe { &mut *context.cast::<UntypedMapDecodeState>() };
    entries.push((
        decode_untyped_shallow_bytes(key_bytes)?,
        decode_untyped_shallow_bytes(value_bytes)?,
    ));

    Ok(())
}

// Decode one list/set field directly from CBOR bytes and recurse only through
// the declared item contract.
fn decode_list_bytes(
    raw_bytes: &[u8],
    inner: FieldKind,
) -> Result<Value, StructuralFieldDecodeError> {
    let mut state = (Vec::new(), inner);
    walk_cbor_array_items(
        raw_bytes,
        "expected CBOR array for list/set field",
        "typed CBOR decode failed: trailing bytes after list/set field",
        (&raw mut state).cast(),
        push_kind_array_item,
    )?;

    Ok(Value::List(state.0))
}

// Decode one map field directly from CBOR bytes and recurse only through the
// declared key/value contracts.
fn decode_map_bytes(
    raw_bytes: &[u8],
    key_kind: FieldKind,
    value_kind: FieldKind,
) -> Result<Value, StructuralFieldDecodeError> {
    let mut state = (Vec::new(), key_kind, value_kind);
    walk_cbor_map_entries(
        raw_bytes,
        "expected CBOR map for map field",
        "typed CBOR decode failed: trailing bytes after map field",
        (&raw mut state).cast(),
        push_kind_map_entry,
    )?;

    Ok(normalize_map_entries_or_preserve(state.0))
}

// Decode one enum field directly from CBOR bytes using the schema-declared
// variant payload contract when available.
fn decode_enum_bytes(
    raw_bytes: &[u8],
    path: &'static str,
    variants: &'static [EnumVariantModel],
) -> Result<Value, StructuralFieldDecodeError> {
    let (variant, payload_bytes) = parse_tagged_variant_payload_bytes(
        raw_bytes,
        "typed CBOR decode failed: truncated CBOR value",
        "expected text or one-entry CBOR map for enum field",
        "expected one-entry CBOR map for enum payload variant",
        "typed CBOR decode failed: trailing bytes after enum field",
    )?;

    if let Some(payload_bytes) = payload_bytes {
        let payload =
            if let Some(variant_model) = variants.iter().find(|item| item.ident() == variant) {
                if let Some(payload_kind) = variant_model.payload_kind() {
                    match variant_model.payload_storage_decode() {
                        FieldStorageDecode::ByKind => {
                            decode_structural_field_by_kind_bytes(payload_bytes, *payload_kind)?
                        }
                        FieldStorageDecode::Value => {
                            decode_structural_value_storage_bytes(payload_bytes)?
                        }
                    }
                } else {
                    decode_untyped_enum_payload_bytes(payload_bytes)?
                }
            } else {
                decode_untyped_enum_payload_bytes(payload_bytes)?
            };

        Ok(Value::Enum(
            ValueEnum::new(variant, Some(path)).with_payload(payload),
        ))
    } else {
        Ok(Value::Enum(ValueEnum::new(variant, Some(path))))
    }
}

// Decode one conservative enum payload directly from bytes.
//
// This keeps the fallback shallow: scalar payloads decode directly, and
// composite payloads decode only one structural level before degrading nested
// composites to `Null`.
fn decode_untyped_enum_payload_bytes(
    raw_bytes: &[u8],
) -> Result<Value, StructuralFieldDecodeError> {
    let Some((major, argument, payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated CBOR value",
        ));
    };

    match major {
        0 | 1 | 2 | 3 | 7 => decode_untyped_scalar_bytes(raw_bytes, major, argument, payload_start),
        4 => decode_untyped_list_bytes(raw_bytes, argument, payload_start),
        5 => decode_untyped_map_bytes(raw_bytes, argument, payload_start),
        _ => Err(StructuralFieldDecodeError::new(
            "unsupported enum payload CBOR shape",
        )),
    }
}

// Parse one tagged CBOR head into `(major, argument, payload_start)`.
fn parse_tagged_cbor_head(
    bytes: &[u8],
    mut cursor: usize,
) -> Result<Option<(u8, u64, usize)>, StructuralFieldDecodeError> {
    let Some((mut major, mut argument, mut next_cursor)) = parse_cbor_head(bytes, cursor)? else {
        return Ok(None);
    };

    while major == 6 {
        cursor = next_cursor;
        let Some((inner_major, inner_argument, inner_next_cursor)) =
            parse_cbor_head(bytes, cursor)?
        else {
            return Err(StructuralFieldDecodeError::new(
                "typed CBOR decode failed: truncated tagged CBOR value",
            ));
        };
        major = inner_major;
        argument = inner_argument;
        next_cursor = inner_next_cursor;
    }

    Ok(Some((major, argument, next_cursor)))
}

// Parse one definite-length CBOR head.
fn parse_cbor_head(
    bytes: &[u8],
    cursor: usize,
) -> Result<Option<(u8, u64, usize)>, StructuralFieldDecodeError> {
    let Some(&first) = bytes.get(cursor) else {
        return Ok(None);
    };
    let major = first >> 5;
    let additional = first & 0x1f;
    let mut next_cursor = cursor + 1;

    let argument = match additional {
        value @ 0..=23 => u64::from(value),
        24 => {
            let value = *bytes.get(next_cursor).ok_or_else(|| {
                StructuralFieldDecodeError::new("typed CBOR decode failed: truncated CBOR head")
            })?;
            next_cursor += 1;

            u64::from(value)
        }
        25 => {
            let payload = bytes.get(next_cursor..next_cursor + 2).ok_or_else(|| {
                StructuralFieldDecodeError::new("typed CBOR decode failed: truncated CBOR head")
            })?;
            next_cursor += 2;

            u64::from(u16::from_be_bytes([payload[0], payload[1]]))
        }
        26 => {
            let payload = bytes.get(next_cursor..next_cursor + 4).ok_or_else(|| {
                StructuralFieldDecodeError::new("typed CBOR decode failed: truncated CBOR head")
            })?;
            next_cursor += 4;

            u64::from(u32::from_be_bytes([
                payload[0], payload[1], payload[2], payload[3],
            ]))
        }
        27 => {
            let payload = bytes.get(next_cursor..next_cursor + 8).ok_or_else(|| {
                StructuralFieldDecodeError::new("typed CBOR decode failed: truncated CBOR head")
            })?;
            next_cursor += 8;

            u64::from_be_bytes([
                payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
                payload[7],
            ])
        }
        31 => {
            return Err(StructuralFieldDecodeError::new(
                "typed CBOR decode failed: indefinite-length CBOR is unsupported",
            ));
        }
        _ => {
            return Err(StructuralFieldDecodeError::new(
                "typed CBOR decode failed: invalid CBOR additional info",
            ));
        }
    };

    Ok(Some((major, argument, next_cursor)))
}

// Skip one tagged CBOR value without rebuilding a `CborValue`.
fn skip_cbor_value(bytes: &[u8], cursor: usize) -> Result<usize, StructuralFieldDecodeError> {
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(bytes, cursor)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated CBOR value",
        ));
    };

    match major {
        0 | 1 | 7 => Ok(cursor),
        2 | 3 => {
            let len = usize::try_from(argument).map_err(|_| {
                StructuralFieldDecodeError::new("typed CBOR decode failed: CBOR scalar too large")
            })?;
            cursor = cursor.checked_add(len).ok_or_else(|| {
                StructuralFieldDecodeError::new(
                    "typed CBOR decode failed: CBOR scalar length overflow",
                )
            })?;
            if cursor > bytes.len() {
                return Err(StructuralFieldDecodeError::new(
                    "typed CBOR decode failed: truncated CBOR scalar payload",
                ));
            }

            Ok(cursor)
        }
        4 => {
            let len = usize::try_from(argument).map_err(|_| {
                StructuralFieldDecodeError::new("typed CBOR decode failed: CBOR array too large")
            })?;
            for _ in 0..len {
                cursor = skip_cbor_value(bytes, cursor)?;
            }

            Ok(cursor)
        }
        5 => {
            let len = usize::try_from(argument).map_err(|_| {
                StructuralFieldDecodeError::new("typed CBOR decode failed: CBOR map too large")
            })?;
            for _ in 0..len {
                cursor = skip_cbor_value(bytes, cursor)?;
                cursor = skip_cbor_value(bytes, cursor)?;
            }

            Ok(cursor)
        }
        _ => Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: unsupported CBOR major type",
        )),
    }
}

// Parse one tagged CBOR text scalar in place.
fn parse_text_scalar_at(
    bytes: &[u8],
    cursor: usize,
) -> Result<(&str, usize), StructuralFieldDecodeError> {
    let Some((major, argument, payload_start)) = parse_tagged_cbor_head(bytes, cursor)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: missing text scalar",
        ));
    };
    if major != 3 {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: expected a text string",
        ));
    }

    let text = decode_text_scalar_bytes(bytes, argument, payload_start)?;
    let text_len = usize::try_from(argument)
        .map_err(|_| StructuralFieldDecodeError::new("typed CBOR decode failed: text too large"))?;
    let next_cursor = payload_start.checked_add(text_len).ok_or_else(|| {
        StructuralFieldDecodeError::new("typed CBOR decode failed: text length overflow")
    })?;

    Ok((text, next_cursor))
}

// Parse one externally tagged variant envelope as either a unit variant name
// or a single payload-bearing variant entry.
fn parse_tagged_variant_payload_bytes<'a>(
    raw_bytes: &'a [u8],
    truncated_label: &'static str,
    unit_or_payload_label: &'static str,
    one_entry_map_label: &'static str,
    trailing_label: &'static str,
) -> Result<(&'a str, Option<&'a [u8]>), StructuralFieldDecodeError> {
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(truncated_label));
    };

    match major {
        3 => {
            let variant = decode_text_scalar_bytes(raw_bytes, argument, cursor)?;
            let text_len = usize::try_from(argument).map_err(|_| {
                StructuralFieldDecodeError::new("typed CBOR decode failed: text too large")
            })?;
            cursor = cursor.checked_add(text_len).ok_or_else(|| {
                StructuralFieldDecodeError::new("typed CBOR decode failed: text length overflow")
            })?;
            if cursor != raw_bytes.len() {
                return Err(StructuralFieldDecodeError::new(trailing_label));
            }

            Ok((variant, None))
        }
        5 => {
            if argument != 1 {
                return Err(StructuralFieldDecodeError::new(one_entry_map_label));
            }

            let (variant, next_cursor) = parse_text_scalar_at(raw_bytes, cursor)?;
            cursor = next_cursor;
            let payload_start = cursor;
            cursor = skip_cbor_value(raw_bytes, cursor)?;
            if cursor != raw_bytes.len() {
                return Err(StructuralFieldDecodeError::new(trailing_label));
            }

            Ok((variant, Some(&raw_bytes[payload_start..cursor])))
        }
        _ => Err(StructuralFieldDecodeError::new(unit_or_payload_label)),
    }
}

// Decode one definite-length CBOR text payload from the enclosing field bytes.
fn decode_text_scalar_bytes(
    bytes: &[u8],
    argument: u64,
    payload_start: usize,
) -> Result<&str, StructuralFieldDecodeError> {
    let text_len = usize::try_from(argument)
        .map_err(|_| StructuralFieldDecodeError::new("typed CBOR decode failed: text too large"))?;
    let payload_end = payload_start.checked_add(text_len).ok_or_else(|| {
        StructuralFieldDecodeError::new("typed CBOR decode failed: text length overflow")
    })?;
    let payload = bytes.get(payload_start..payload_end).ok_or_else(|| {
        StructuralFieldDecodeError::new("typed CBOR decode failed: truncated text payload")
    })?;

    str::from_utf8(payload).map_err(|_| {
        StructuralFieldDecodeError::new("typed CBOR decode failed: non-utf8 text string")
    })
}

// Decode one date payload from its persisted CBOR text form.
fn decode_date_value_bytes(raw_bytes: &[u8]) -> Result<Value, StructuralFieldDecodeError> {
    let text = decode_required_text_payload(raw_bytes, "date")?;

    Date::parse(text).map(Value::Date).ok_or_else(|| {
        StructuralFieldDecodeError::new(format!("typed CBOR decode failed: invalid date: {text}"))
    })
}

// Decode one account payload from its persisted CBOR struct form.
fn decode_account_value_bytes(raw_bytes: &[u8]) -> Result<Value, StructuralFieldDecodeError> {
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated account payload",
        ));
    };
    if major != 5 {
        return Err(StructuralFieldDecodeError::new(
            "expected CBOR map for account payload",
        ));
    }

    let entry_count = usize::try_from(argument)
        .map_err(|_| StructuralFieldDecodeError::new("expected bounded CBOR map length"))?;
    let mut owner = None;
    let mut subaccount = None;

    for _ in 0..entry_count {
        let (field_name, next_cursor) = parse_text_scalar_at(raw_bytes, cursor)?;
        cursor = next_cursor;

        let field_value_start = cursor;
        cursor = skip_cbor_value(raw_bytes, cursor)?;
        let field_value = &raw_bytes[field_value_start..cursor];

        match field_name {
            "owner" => match decode_principal_value_bytes(field_value)? {
                Value::Principal(value) => owner = Some(value),
                _ => {
                    return Err(StructuralFieldDecodeError::new(
                        "typed CBOR decode failed: invalid account owner payload",
                    ));
                }
            },
            "subaccount" => subaccount = decode_optional_subaccount_value(field_value)?,
            _ => {}
        }
    }

    if cursor != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after account payload",
        ));
    }

    let owner = owner.ok_or_else(|| {
        StructuralFieldDecodeError::new("typed CBOR decode failed: missing account owner field")
    })?;

    Ok(Value::Account(Account::from_parts(owner, subaccount)))
}

// Decode one account relation-key payload without routing through typed serde.
fn decode_account_storage_key_bytes(
    raw_bytes: &[u8],
) -> Result<StorageKey, StructuralFieldDecodeError> {
    match decode_account_value_bytes(raw_bytes)? {
        Value::Account(value) => Ok(StorageKey::Account(value)),
        _ => Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: invalid account storage key payload",
        )),
    }
}

// Decode one decimal payload from its persisted binary-or-text CBOR form.
fn decode_decimal_value_bytes(raw_bytes: &[u8]) -> Result<Value, StructuralFieldDecodeError> {
    let Some((major, _, _)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated decimal payload",
        ));
    };
    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after decimal payload",
        ));
    }

    let value = match major {
        3 => decode_required_text_payload(raw_bytes, "decimal")?
            .parse::<Decimal>()
            .map_err(|err| {
                StructuralFieldDecodeError::new(format!("typed CBOR decode failed: {err}"))
            })?,
        4 => decode_decimal_binary_payload(raw_bytes)?,
        _ => {
            return Err(StructuralFieldDecodeError::new(
                "typed CBOR decode failed: expected decimal text or binary tuple",
            ));
        }
    };

    Ok(Value::Decimal(value))
}

// Decode one duration payload from its persisted integer-or-string CBOR form.
fn decode_duration_value_bytes(raw_bytes: &[u8]) -> Result<Value, StructuralFieldDecodeError> {
    let Some((major, argument, payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated duration payload",
        ));
    };
    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after duration payload",
        ));
    }

    let value = match major {
        0 => Duration::from_millis(argument),
        3 => Duration::parse_flexible(decode_text_scalar_bytes(
            raw_bytes,
            argument,
            payload_start,
        )?)
        .map_err(|err| {
            StructuralFieldDecodeError::new(format!("typed CBOR decode failed: {err}"))
        })?,
        _ => {
            return Err(StructuralFieldDecodeError::new(
                "typed CBOR decode failed: expected duration millis or string",
            ));
        }
    };

    Ok(Value::Duration(value))
}

// Decode one timestamp payload from its persisted integer-or-string CBOR form.
fn decode_timestamp_value_bytes(raw_bytes: &[u8]) -> Result<Value, StructuralFieldDecodeError> {
    let Some((major, argument, payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated timestamp payload",
        ));
    };
    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after timestamp payload",
        ));
    }

    let value = match major {
        0 | 1 => {
            let millis = i64::try_from(decode_cbor_integer(major, argument)?).map_err(|_| {
                StructuralFieldDecodeError::new(
                    "typed CBOR decode failed: timestamp out of i64 range",
                )
            })?;
            Timestamp::from_millis(millis)
        }
        3 => Timestamp::parse_flexible(decode_text_scalar_bytes(
            raw_bytes,
            argument,
            payload_start,
        )?)
        .map_err(|err| {
            StructuralFieldDecodeError::new(format!("typed CBOR decode failed: {err}"))
        })?,
        _ => {
            return Err(StructuralFieldDecodeError::new(
                "typed CBOR decode failed: expected unix millis or RFC3339 string",
            ));
        }
    };

    Ok(Value::Timestamp(value))
}

// Decode one arbitrary-precision signed integer payload from its persisted
// CBOR `(sign, limbs)` tuple.
fn decode_int_big_value_bytes(raw_bytes: &[u8]) -> Result<Value, StructuralFieldDecodeError> {
    let (sign, magnitude) = decode_bigint_tuple_payload(raw_bytes)?;
    let wrapped = WrappedInt::from(BigInt::from_biguint(sign, magnitude));

    Ok(Value::IntBig(Int::from(wrapped)))
}

// Decode one arbitrary-precision unsigned integer payload from its persisted
// CBOR limb sequence.
fn decode_uint_big_value_bytes(raw_bytes: &[u8]) -> Result<Value, StructuralFieldDecodeError> {
    let wrapped = WrappedNat::from(decode_biguint_payload(raw_bytes)?);

    Ok(Value::UintBig(Nat::from(wrapped)))
}

// Decode one principal payload from its persisted CBOR byte-string form.
fn decode_principal_value_bytes(raw_bytes: &[u8]) -> Result<Value, StructuralFieldDecodeError> {
    let bytes = decode_required_bytes_payload(raw_bytes, "principal")?;
    let principal = crate::types::Principal::try_from_bytes(bytes).map_err(|err| {
        StructuralFieldDecodeError::new(format!("typed CBOR decode failed: {err}"))
    })?;

    Ok(Value::Principal(principal))
}

// Decode one subaccount payload from its persisted CBOR sequence or byte-string
// form.
fn decode_subaccount_value_bytes(raw_bytes: &[u8]) -> Result<Value, StructuralFieldDecodeError> {
    let bytes = decode_subaccount_payload_bytes(raw_bytes)?;

    Ok(Value::Subaccount(crate::types::Subaccount::from_array(
        bytes,
    )))
}

// Decode one optional subaccount field, treating explicit null as absence.
fn decode_optional_subaccount_value(
    raw_bytes: &[u8],
) -> Result<Option<crate::types::Subaccount>, StructuralFieldDecodeError> {
    let Some((major, argument, _)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated subaccount payload",
        ));
    };
    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after subaccount payload",
        ));
    }
    if major == 7 && argument == 22 {
        return Ok(None);
    }

    match decode_subaccount_value_bytes(raw_bytes)? {
        Value::Subaccount(value) => Ok(Some(value)),
        _ => Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: invalid subaccount payload",
        )),
    }
}

// Decode one decimal binary payload tuple `(mantissa_bytes, scale)`.
fn decode_decimal_binary_payload(raw_bytes: &[u8]) -> Result<Decimal, StructuralFieldDecodeError> {
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated decimal payload",
        ));
    };
    if major != 4 || argument != 2 {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: expected decimal binary tuple",
        ));
    }

    let mantissa_start = cursor;
    cursor = skip_cbor_value(raw_bytes, cursor)?;
    let scale_start = cursor;
    cursor = skip_cbor_value(raw_bytes, cursor)?;
    if cursor != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after decimal payload",
        ));
    }

    let mantissa_bytes: [u8; 16] =
        decode_required_bytes_payload(&raw_bytes[mantissa_start..scale_start], "decimal mantissa")?
            .try_into()
            .map_err(|_| {
                StructuralFieldDecodeError::new(
                    "typed CBOR decode failed: invalid decimal mantissa length: 16 bytes expected",
                )
            })?;
    let scale = decode_required_u32_payload(&raw_bytes[scale_start..cursor], "decimal scale")?;

    decode_decimal_mantissa_scale(i128::from_be_bytes(mantissa_bytes), scale)
}

// Decode one `(sign, magnitude)` tuple into a `BigInt` construction pair.
fn decode_bigint_tuple_payload(
    raw_bytes: &[u8],
) -> Result<(BigIntSign, BigUint), StructuralFieldDecodeError> {
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated bigint payload",
        ));
    };
    if major != 4 || argument != 2 {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: expected bigint sign/magnitude tuple",
        ));
    }

    let sign_start = cursor;
    cursor = skip_cbor_value(raw_bytes, cursor)?;
    let magnitude_start = cursor;
    cursor = skip_cbor_value(raw_bytes, cursor)?;
    if cursor != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after bigint payload",
        ));
    }

    let sign = decode_bigint_sign_payload(&raw_bytes[sign_start..magnitude_start])?;
    let magnitude = decode_biguint_payload(&raw_bytes[magnitude_start..cursor])?;

    Ok((sign, magnitude))
}

// Decode one bigint sign payload serialized as -1, 0, or 1.
fn decode_bigint_sign_payload(raw_bytes: &[u8]) -> Result<BigIntSign, StructuralFieldDecodeError> {
    let Some((major, argument, _)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated bigint sign payload",
        ));
    };
    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after bigint sign payload",
        ));
    }

    match decode_cbor_integer(major, argument)? {
        -1 => Ok(BigIntSign::Minus),
        0 => Ok(BigIntSign::NoSign),
        1 => Ok(BigIntSign::Plus),
        other => Err(StructuralFieldDecodeError::new(format!(
            "typed CBOR decode failed: invalid bigint sign {other}"
        ))),
    }
}

// Decode one biguint payload serialized as a sequence of base-2^32 limbs.
fn decode_biguint_payload(raw_bytes: &[u8]) -> Result<BigUint, StructuralFieldDecodeError> {
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated biguint payload",
        ));
    };
    if major != 4 {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: expected biguint limb sequence",
        ));
    }

    let limb_count = usize::try_from(argument)
        .map_err(|_| StructuralFieldDecodeError::new("expected bounded CBOR array length"))?;
    let mut limbs = Vec::with_capacity(limb_count);

    for _ in 0..limb_count {
        let limb_start = cursor;
        cursor = skip_cbor_value(raw_bytes, cursor)?;
        limbs.push(decode_required_u32_payload(
            &raw_bytes[limb_start..cursor],
            "biguint limb",
        )?);
    }

    if cursor != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after biguint payload",
        ));
    }

    Ok(BigUint::new(limbs))
}

// Decode one unit payload from its persisted CBOR null form.
fn decode_unit_value_bytes(raw_bytes: &[u8]) -> Result<Value, StructuralFieldDecodeError> {
    decode_unit_storage_key_bytes(raw_bytes)?;

    Ok(Value::Unit)
}

// Decode one null payload from its persisted CBOR null form.
fn decode_null_value_bytes(raw_bytes: &[u8]) -> Result<Value, StructuralFieldDecodeError> {
    decode_unit_storage_key_bytes(raw_bytes)?;

    Ok(Value::Null)
}

// Decode one timestamp relation-key payload without routing through typed serde.
fn decode_timestamp_storage_key_bytes(
    raw_bytes: &[u8],
) -> Result<StorageKey, StructuralFieldDecodeError> {
    match decode_timestamp_value_bytes(raw_bytes)? {
        Value::Timestamp(value) => Ok(StorageKey::Timestamp(value)),
        _ => Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: invalid timestamp storage key payload",
        )),
    }
}

// Decode one principal relation-key payload without routing through typed serde.
fn decode_principal_storage_key_bytes(
    raw_bytes: &[u8],
) -> Result<StorageKey, StructuralFieldDecodeError> {
    match decode_principal_value_bytes(raw_bytes)? {
        Value::Principal(value) => Ok(StorageKey::Principal(value)),
        _ => Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: invalid principal storage key payload",
        )),
    }
}

// Decode one subaccount relation-key payload without routing through typed serde.
fn decode_subaccount_storage_key_bytes(
    raw_bytes: &[u8],
) -> Result<StorageKey, StructuralFieldDecodeError> {
    match decode_subaccount_value_bytes(raw_bytes)? {
        Value::Subaccount(value) => Ok(StorageKey::Subaccount(value)),
        _ => Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: invalid subaccount storage key payload",
        )),
    }
}

// Decode one ULID relation-key payload directly from its persisted CBOR text form.
fn decode_ulid_storage_key_bytes(
    raw_bytes: &[u8],
) -> Result<StorageKey, StructuralFieldDecodeError> {
    let Some((major, argument, payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated ulid payload",
        ));
    };
    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after ulid payload",
        ));
    }
    if major != 3 {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: invalid type, expected a text string",
        ));
    }

    Ulid::from_str(decode_text_scalar_bytes(
        raw_bytes,
        argument,
        payload_start,
    )?)
    .map(StorageKey::Ulid)
    .map_err(|_| StructuralFieldDecodeError::new("typed CBOR decode failed: invalid ulid string"))
}

// Decode one unit relation-key payload without routing through typed serde.
fn decode_unit_storage_key_bytes(
    raw_bytes: &[u8],
) -> Result<StorageKey, StructuralFieldDecodeError> {
    let Some((major, argument, _payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated unit payload",
        ));
    };
    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after unit payload",
        ));
    }
    if major != 7 || argument != 22 {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: expected null for unit payload",
        ));
    }

    Ok(StorageKey::Unit)
}

// Decode one required top-level text payload and enforce full-byte consumption.
fn decode_required_text_payload<'a>(
    raw_bytes: &'a [u8],
    label: &'static str,
) -> Result<&'a str, StructuralFieldDecodeError> {
    let Some((major, argument, payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(format!(
            "typed CBOR decode failed: truncated {label} payload"
        )));
    };
    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(format!(
            "typed CBOR decode failed: trailing bytes after {label} payload"
        )));
    }
    if major != 3 {
        return Err(StructuralFieldDecodeError::new(format!(
            "typed CBOR decode failed: expected a text string for {label}"
        )));
    }

    decode_text_scalar_bytes(raw_bytes, argument, payload_start)
}

// Decode one required top-level byte-string payload and enforce full-byte
// consumption.
fn decode_required_bytes_payload<'a>(
    raw_bytes: &'a [u8],
    label: &'static str,
) -> Result<&'a [u8], StructuralFieldDecodeError> {
    let Some((major, argument, payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(format!(
            "typed CBOR decode failed: truncated {label} payload"
        )));
    };
    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(format!(
            "typed CBOR decode failed: trailing bytes after {label} payload"
        )));
    }
    if major != 2 {
        return Err(StructuralFieldDecodeError::new(format!(
            "typed CBOR decode failed: expected a byte string for {label}"
        )));
    }

    payload_bytes(raw_bytes, argument, payload_start, "byte string")
}

// Decode one required top-level unsigned-32 payload and enforce full-byte
// consumption.
fn decode_required_u32_payload(
    raw_bytes: &[u8],
    label: &'static str,
) -> Result<u32, StructuralFieldDecodeError> {
    let Some((major, argument, _)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(format!(
            "typed CBOR decode failed: truncated {label} payload"
        )));
    };
    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(format!(
            "typed CBOR decode failed: trailing bytes after {label} payload"
        )));
    }
    if major != 0 {
        return Err(StructuralFieldDecodeError::new(format!(
            "typed CBOR decode failed: expected unsigned integer for {label}"
        )));
    }

    u32::try_from(argument).map_err(|_| {
        StructuralFieldDecodeError::new(format!(
            "typed CBOR decode failed: {label} out of u32 range"
        ))
    })
}

// Apply Decimal's binary mantissa/scale validation without routing through
// serde.
fn decode_decimal_mantissa_scale(
    mantissa: i128,
    scale: u32,
) -> Result<Decimal, StructuralFieldDecodeError> {
    if scale <= Decimal::max_supported_scale() {
        return Ok(Decimal::from_i128_with_scale(mantissa, scale));
    }

    let mut value = mantissa;
    let mut normalized_scale = scale;
    while normalized_scale > Decimal::max_supported_scale() {
        if value == 0 {
            return Ok(Decimal::from_i128_with_scale(
                0,
                Decimal::max_supported_scale(),
            ));
        }
        if value % 10 != 0 {
            return Err(StructuralFieldDecodeError::new(
                "typed CBOR decode failed: invalid decimal binary payload",
            ));
        }
        value /= 10;
        normalized_scale -= 1;
    }

    Ok(Decimal::from_i128_with_scale(value, normalized_scale))
}

// Decode one subaccount payload as either the derived 32-item byte array shape
// or an equivalent raw byte string.
fn decode_subaccount_payload_bytes(
    raw_bytes: &[u8],
) -> Result<[u8; 32], StructuralFieldDecodeError> {
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated subaccount payload",
        ));
    };

    match major {
        2 => decode_required_bytes_payload(raw_bytes, "subaccount")?
            .try_into()
            .map_err(|_| {
                StructuralFieldDecodeError::new(
                    "typed CBOR decode failed: expected 32 bytes for subaccount payload",
                )
            }),
        4 => {
            if argument != 32 {
                return Err(StructuralFieldDecodeError::new(
                    "typed CBOR decode failed: expected 32-byte array for subaccount payload",
                ));
            }

            let mut bytes = [0u8; 32];
            for byte in &mut bytes {
                let item_start = cursor;
                cursor = skip_cbor_value(raw_bytes, cursor)?;
                let Some((item_major, item_argument, _)) =
                    parse_tagged_cbor_head(&raw_bytes[item_start..cursor], 0)?
                else {
                    return Err(StructuralFieldDecodeError::new(
                        "typed CBOR decode failed: truncated subaccount item",
                    ));
                };
                if item_major != 0 {
                    return Err(StructuralFieldDecodeError::new(
                        "typed CBOR decode failed: expected unsigned byte in subaccount payload",
                    ));
                }
                *byte = u8::try_from(item_argument).map_err(|_| {
                    StructuralFieldDecodeError::new(
                        "typed CBOR decode failed: subaccount byte out of range",
                    )
                })?;
            }

            if cursor != raw_bytes.len() {
                return Err(StructuralFieldDecodeError::new(
                    "typed CBOR decode failed: trailing bytes after subaccount payload",
                ));
            }

            Ok(bytes)
        }
        _ => Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: expected byte string or byte array for subaccount payload",
        )),
    }
}

// Decode one signed storage-key-compatible integer payload directly from CBOR.
fn decode_int_storage_key_bytes(
    raw_bytes: &[u8],
) -> Result<StorageKey, StructuralFieldDecodeError> {
    let Some((major, argument, _payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated CBOR value",
        ));
    };
    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after relation field",
        ));
    }

    let value = match major {
        0 => i64::try_from(argument).map_err(|_| {
            StructuralFieldDecodeError::new(format!(
                "typed CBOR decode failed: integer {argument} out of range for i64",
            ))
        })?,
        1 => {
            let signed = i64::try_from(argument).map_err(|_| {
                StructuralFieldDecodeError::new(format!(
                    "typed CBOR decode failed: integer -{} out of range for i64",
                    argument.saturating_add(1),
                ))
            })?;
            signed
                .checked_neg()
                .and_then(|value| value.checked_sub(1))
                .ok_or_else(|| {
                    StructuralFieldDecodeError::new(format!(
                        "typed CBOR decode failed: integer -{} out of range for i64",
                        argument.saturating_add(1),
                    ))
                })?
        }
        _ => {
            return Err(StructuralFieldDecodeError::new(
                "typed CBOR decode failed: invalid type, expected an integer",
            ));
        }
    };

    Ok(StorageKey::Int(value))
}

// Decode one unsigned storage-key-compatible integer payload directly from CBOR.
fn decode_uint_storage_key_bytes(
    raw_bytes: &[u8],
) -> Result<StorageKey, StructuralFieldDecodeError> {
    let Some((major, argument, _payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated CBOR value",
        ));
    };
    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after relation field",
        ));
    }
    if major != 0 {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: invalid type, expected an integer",
        ));
    }

    Ok(StorageKey::Uint(argument))
}

// Keep one narrow list/map fast-path whitelist so composite decode only skips
// the generic field dispatcher for truly direct scalar cases.
const fn supports_scalar_fast_path(kind: FieldKind) -> bool {
    matches!(
        kind,
        FieldKind::Blob
            | FieldKind::Bool
            | FieldKind::Float32
            | FieldKind::Float64
            | FieldKind::Int
            | FieldKind::Int128
            | FieldKind::Text
            | FieldKind::Uint
            | FieldKind::Uint128
            | FieldKind::Ulid
    )
}

// Decode one `FieldStorageDecode::Value` payload directly from the externally
// tagged `Value` wire shape without routing through serde's recursive enum
// visitor graph.
pub(in crate::db) fn decode_structural_value_storage_bytes(
    raw_bytes: &[u8],
) -> Result<Value, StructuralFieldDecodeError> {
    let (variant, payload_bytes) = parse_tagged_variant_payload_bytes(
        raw_bytes,
        "typed CBOR decode failed: truncated value payload",
        "expected text or one-entry CBOR map for value payload",
        "expected one-entry CBOR map for value payload",
        "typed CBOR decode failed: trailing bytes after value payload",
    )?;
    let variant = parse_value_variant_tag(variant)?;

    if let Some(payload_bytes) = payload_bytes {
        decode_value_variant_payload(variant, payload_bytes)
    } else {
        decode_unit_value_variant(variant)
    }
}

// Decode one unit `Value` variant from the externally tagged wire shape.
fn decode_unit_value_variant(
    variant: ValueVariantTag,
) -> Result<Value, StructuralFieldDecodeError> {
    match variant {
        ValueVariantTag::Null => Ok(Value::Null),
        ValueVariantTag::Unit => Ok(Value::Unit),
        _ => Err(StructuralFieldDecodeError::new(
            "unsupported unit value variant",
        )),
    }
}

// Decode one non-unit `Value` payload variant using the variant's declared
// runtime contract.
fn decode_value_variant_payload(
    variant: ValueVariantTag,
    payload_bytes: &[u8],
) -> Result<Value, StructuralFieldDecodeError> {
    match variant {
        ValueVariantTag::Account => decode_account_value_bytes(payload_bytes),
        ValueVariantTag::Date => decode_date_value_bytes(payload_bytes),
        ValueVariantTag::Decimal => decode_decimal_value_bytes(payload_bytes),
        ValueVariantTag::Duration => decode_duration_value_bytes(payload_bytes),
        ValueVariantTag::Enum => decode_value_enum_payload_bytes(payload_bytes),
        ValueVariantTag::IntBig => decode_int_big_value_bytes(payload_bytes),
        ValueVariantTag::List => decode_value_storage_list_bytes(payload_bytes),
        ValueVariantTag::Map => decode_value_storage_map_bytes(payload_bytes),
        ValueVariantTag::Null => decode_null_value_bytes(payload_bytes),
        ValueVariantTag::Principal => decode_principal_value_bytes(payload_bytes),
        ValueVariantTag::Scalar(kind) => decode_structural_field_by_kind_bytes(payload_bytes, kind),
        ValueVariantTag::Subaccount => decode_subaccount_value_bytes(payload_bytes),
        ValueVariantTag::Timestamp => decode_timestamp_value_bytes(payload_bytes),
        ValueVariantTag::UintBig => decode_uint_big_value_bytes(payload_bytes),
        ValueVariantTag::Unit => decode_unit_value_bytes(payload_bytes),
    }
}

// Decode one persisted `Value::List` payload recursively from raw element bytes.
fn decode_value_storage_list_bytes(raw_bytes: &[u8]) -> Result<Value, StructuralFieldDecodeError> {
    let mut items = Vec::new();
    walk_cbor_array_items(
        raw_bytes,
        "expected CBOR array for value list payload",
        "typed CBOR decode failed: trailing bytes after value list payload",
        (&raw mut items).cast(),
        push_value_array_item,
    )?;

    Ok(Value::List(items))
}

// Decode one persisted `Value::Map` payload recursively while preserving
// runtime map invariants.
fn decode_value_storage_map_bytes(raw_bytes: &[u8]) -> Result<Value, StructuralFieldDecodeError> {
    let mut entries = Vec::new();
    walk_cbor_array_items(
        raw_bytes,
        "expected CBOR array for value map payload",
        "typed CBOR decode failed: trailing bytes after value map payload",
        (&raw mut entries).cast(),
        push_value_storage_map_entry_item,
    )?;

    Value::from_map(entries)
        .map_err(|err| StructuralFieldDecodeError::new(format!("typed CBOR decode failed: {err}")))
}

// Decode one persisted `Value::Map` entry tuple from raw CBOR bytes.
fn decode_value_storage_map_entry_bytes(
    raw_bytes: &[u8],
) -> Result<(Value, Value), StructuralFieldDecodeError> {
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated value map entry",
        ));
    };
    if major != 4 || argument != 2 {
        return Err(StructuralFieldDecodeError::new(
            "expected two-item CBOR array for value map entry",
        ));
    }

    let key_start = cursor;
    cursor = skip_cbor_value(raw_bytes, cursor)?;
    let value_start = cursor;
    cursor = skip_cbor_value(raw_bytes, cursor)?;
    if cursor != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after value map entry",
        ));
    }

    Ok((
        decode_structural_value_storage_bytes(&raw_bytes[key_start..value_start])?,
        decode_structural_value_storage_bytes(&raw_bytes[value_start..cursor])?,
    ))
}

// Decode one persisted `Value::Enum` payload struct without routing through the
// generic `Value` deserializer.
fn decode_value_enum_payload_bytes(raw_bytes: &[u8]) -> Result<Value, StructuralFieldDecodeError> {
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated value enum payload",
        ));
    };
    if major != 5 {
        return Err(StructuralFieldDecodeError::new(
            "expected CBOR map for value enum payload",
        ));
    }

    let entry_count = usize::try_from(argument)
        .map_err(|_| StructuralFieldDecodeError::new("expected bounded CBOR map length"))?;
    let mut variant = None;
    let mut path = None;
    let mut payload = None;

    // Phase 1: collect the struct fields while preserving serde's tolerant
    // unknown-field behavior.
    for _ in 0..entry_count {
        let (field_name, next_cursor) = parse_text_scalar_at(raw_bytes, cursor)?;
        cursor = next_cursor;

        let field_value_start = cursor;
        cursor = skip_cbor_value(raw_bytes, cursor)?;
        let field_value = &raw_bytes[field_value_start..cursor];

        match field_name {
            "variant" => variant = Some(decode_required_text_value_field(field_value)?),
            "path" => path = decode_optional_text_value_field(field_value)?,
            "payload" => payload = decode_optional_nested_value_field(field_value)?,
            _ => {}
        }
    }

    if cursor != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after value enum payload",
        ));
    }

    let variant = variant.ok_or_else(|| {
        StructuralFieldDecodeError::new("typed CBOR decode failed: missing enum variant field")
    })?;
    let mut value = ValueEnum::new(variant, path);
    if let Some(payload) = payload {
        value = value.with_payload(payload);
    }

    Ok(Value::Enum(value))
}

// Decode one required text field from the `ValueEnum` payload struct.
fn decode_required_text_value_field(raw_bytes: &[u8]) -> Result<&str, StructuralFieldDecodeError> {
    let Some((major, argument, payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: missing text field",
        ));
    };
    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after text field",
        ));
    }
    if major != 3 {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: expected a text string",
        ));
    }

    decode_text_scalar_bytes(raw_bytes, argument, payload_start)
}

// Decode one optional text field from the `ValueEnum` payload struct.
fn decode_optional_text_value_field(
    raw_bytes: &[u8],
) -> Result<Option<&str>, StructuralFieldDecodeError> {
    let Some((major, argument, payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: missing optional text field",
        ));
    };
    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after optional text field",
        ));
    }
    if major == 7 && argument == 22 {
        return Ok(None);
    }
    if major != 3 {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: expected a text string",
        ));
    }

    Ok(Some(decode_text_scalar_bytes(
        raw_bytes,
        argument,
        payload_start,
    )?))
}

// Decode one optional nested `Value` field from the `ValueEnum` payload struct.
fn decode_optional_nested_value_field(
    raw_bytes: &[u8],
) -> Result<Option<Value>, StructuralFieldDecodeError> {
    let Some((major, argument, _payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: missing nested value field",
        ));
    };
    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after nested value field",
        ));
    }
    if major == 7 && argument == 22 {
        return Ok(None);
    }

    decode_structural_value_storage_bytes(raw_bytes).map(Some)
}

// Decode one scalar field directly from persisted CBOR bytes without
// rebuilding an intermediate `CborValue`.
fn decode_scalar_fast_path_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<Value>, StructuralFieldDecodeError> {
    if !supports_scalar_fast_path(kind) {
        return Ok(None);
    }

    // Phase 1: parse one bounded scalar payload and preserve explicit nulls.
    let Some((major, argument, payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated CBOR value",
        ));
    };
    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after scalar payload",
        ));
    }
    if major == 7 && argument == 22 {
        return Ok(Some(Value::Null));
    }

    // Phase 2: decode the declared scalar kind directly from the payload bytes.
    decode_scalar_fast_path_value(raw_bytes, kind, major, argument, payload_start)
}

// Decode one non-null scalar fast-path payload by scalar family.
fn decode_scalar_fast_path_value(
    raw_bytes: &[u8],
    kind: FieldKind,
    major: u8,
    argument: u64,
    payload_start: usize,
) -> Result<Option<Value>, StructuralFieldDecodeError> {
    let value = match kind {
        FieldKind::Blob | FieldKind::Int128 | FieldKind::Uint128 => {
            decode_scalar_fast_path_bytes_kind(raw_bytes, kind, major, argument, payload_start)?
        }
        FieldKind::Text | FieldKind::Ulid => {
            decode_scalar_fast_path_text_kind(raw_bytes, kind, major, argument, payload_start)?
        }
        FieldKind::Bool
        | FieldKind::Float32
        | FieldKind::Float64
        | FieldKind::Int
        | FieldKind::Uint => {
            decode_scalar_fast_path_numeric_kind(raw_bytes, kind, major, argument, payload_start)?
        }
        _ => return Ok(None),
    };

    Ok(Some(value))
}

// Decode one scalar fast-path payload whose persisted shape is bytes.
fn decode_scalar_fast_path_bytes_kind(
    raw_bytes: &[u8],
    kind: FieldKind,
    major: u8,
    argument: u64,
    payload_start: usize,
) -> Result<Value, StructuralFieldDecodeError> {
    if major != 2 {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: invalid type, expected a byte string",
        ));
    }

    match kind {
        FieldKind::Blob => Ok(Value::Blob(
            payload_bytes(raw_bytes, argument, payload_start, "byte string")?.to_vec(),
        )),
        FieldKind::Int128 => {
            let bytes: [u8; 16] = payload_bytes(raw_bytes, argument, payload_start, "byte string")?
                .try_into()
                .map_err(|_| {
                    StructuralFieldDecodeError::new("typed CBOR decode failed: expected 16 bytes")
                })?;

            Ok(Value::Int128(Int128::from(i128::from_be_bytes(bytes))))
        }
        FieldKind::Uint128 => {
            let bytes: [u8; 16] = payload_bytes(raw_bytes, argument, payload_start, "byte string")?
                .try_into()
                .map_err(|_| {
                    StructuralFieldDecodeError::new("typed CBOR decode failed: expected 16 bytes")
                })?;

            Ok(Value::Uint128(Nat128::from(u128::from_be_bytes(bytes))))
        }
        _ => Err(StructuralFieldDecodeError::new(
            "scalar field unexpectedly routed to byte fast-path helper",
        )),
    }
}

// Decode one scalar fast-path payload whose persisted shape is text.
fn decode_scalar_fast_path_text_kind(
    raw_bytes: &[u8],
    kind: FieldKind,
    major: u8,
    argument: u64,
    payload_start: usize,
) -> Result<Value, StructuralFieldDecodeError> {
    if major != 3 {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: invalid type, expected a text string",
        ));
    }

    let text = decode_text_scalar_bytes(raw_bytes, argument, payload_start)?;
    match kind {
        FieldKind::Text => Ok(Value::Text(text.to_string())),
        FieldKind::Ulid => Ok(Value::Ulid(Ulid::from_str(text).map_err(|_| {
            StructuralFieldDecodeError::new("typed CBOR decode failed: invalid ulid string")
        })?)),
        _ => Err(StructuralFieldDecodeError::new(
            "scalar field unexpectedly routed to text fast-path helper",
        )),
    }
}

// Decode one scalar fast-path payload whose persisted shape is numeric or bool.
fn decode_scalar_fast_path_numeric_kind(
    raw_bytes: &[u8],
    kind: FieldKind,
    major: u8,
    argument: u64,
    payload_start: usize,
) -> Result<Value, StructuralFieldDecodeError> {
    match kind {
        FieldKind::Bool => match (major, argument) {
            (7, 20) => Ok(Value::Bool(false)),
            (7, 21) => Ok(Value::Bool(true)),
            _ => Err(StructuralFieldDecodeError::new(
                "typed CBOR decode failed: invalid type, expected a bool",
            )),
        },
        FieldKind::Float32 => {
            decode_scalar_fast_path_float32(raw_bytes, major, argument, payload_start)
        }
        FieldKind::Float64 => {
            decode_scalar_fast_path_float64(raw_bytes, major, argument, payload_start)
        }
        FieldKind::Int => {
            let integer = decode_cbor_integer(major, argument)?;
            Ok(Value::Int(i64::try_from(integer).map_err(|_| {
                StructuralFieldDecodeError::new(format!(
                    "typed CBOR decode failed: integer {integer} out of range for i64",
                ))
            })?))
        }
        FieldKind::Uint => {
            let integer = decode_cbor_integer(major, argument)?;
            Ok(Value::Uint(u64::try_from(integer).map_err(|_| {
                StructuralFieldDecodeError::new(format!(
                    "typed CBOR decode failed: integer {integer} out of range for u64",
                ))
            })?))
        }
        _ => Err(StructuralFieldDecodeError::new(
            "scalar field unexpectedly routed to numeric fast-path helper",
        )),
    }
}

// Decode one float32 scalar fast-path payload.
fn decode_scalar_fast_path_float32(
    raw_bytes: &[u8],
    major: u8,
    argument: u64,
    payload_start: usize,
) -> Result<Value, StructuralFieldDecodeError> {
    if major != 7 {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: invalid type, expected a float",
        ));
    }

    let value = decode_cbor_float(raw_bytes, argument, payload_start)?;
    if value < f64::from(f32::MIN) || value > f64::from(f32::MAX) {
        return Err(StructuralFieldDecodeError::new(
            "CBOR float payload out of range for float32",
        ));
    }

    Ok(Value::Float32(Float32::from_f64(value).ok_or_else(
        || StructuralFieldDecodeError::new("non-finite CBOR float payload"),
    )?))
}

// Decode one float64 scalar fast-path payload.
fn decode_scalar_fast_path_float64(
    raw_bytes: &[u8],
    major: u8,
    argument: u64,
    payload_start: usize,
) -> Result<Value, StructuralFieldDecodeError> {
    if major != 7 {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: invalid type, expected a float",
        ));
    }

    Ok(Value::Float64(
        Float64::try_new(decode_cbor_float(raw_bytes, argument, payload_start)?)
            .ok_or_else(|| StructuralFieldDecodeError::new("non-finite CBOR float payload"))?,
    ))
}

// Decode one untyped scalar payload directly from bytes.
fn decode_untyped_scalar_bytes(
    raw_bytes: &[u8],
    major: u8,
    argument: u64,
    payload_start: usize,
) -> Result<Value, StructuralFieldDecodeError> {
    let value = match major {
        0 | 1 => decode_untyped_integer(decode_cbor_integer(major, argument)?),
        2 => {
            Value::Blob(payload_bytes(raw_bytes, argument, payload_start, "byte string")?.to_vec())
        }
        3 => Value::Text(decode_text_scalar_bytes(raw_bytes, argument, payload_start)?.to_string()),
        7 => match argument {
            20 => Value::Bool(false),
            21 => Value::Bool(true),
            22 => Value::Null,
            26 | 27 => Value::Float64(
                Float64::try_new(decode_cbor_float(raw_bytes, argument, payload_start)?)
                    .ok_or_else(|| {
                        StructuralFieldDecodeError::new("non-finite CBOR float payload")
                    })?,
            ),
            _ => {
                return Err(StructuralFieldDecodeError::new(
                    "unsupported enum payload CBOR shape",
                ));
            }
        },
        _ => {
            return Err(StructuralFieldDecodeError::new(
                "unsupported enum payload CBOR shape",
            ));
        }
    };

    Ok(value)
}

// Decode one untyped list payload one level deep directly from bytes.
fn decode_untyped_list_bytes(
    raw_bytes: &[u8],
    _argument: u64,
    _cursor: usize,
) -> Result<Value, StructuralFieldDecodeError> {
    let mut values = Vec::new();
    walk_cbor_array_items(
        raw_bytes,
        "expected CBOR array for enum payload array",
        "typed CBOR decode failed: trailing bytes after enum payload array",
        (&raw mut values).cast(),
        push_untyped_array_item,
    )?;

    Ok(Value::List(values))
}

// Decode one untyped map payload one level deep directly from bytes.
fn decode_untyped_map_bytes(
    raw_bytes: &[u8],
    _argument: u64,
    _cursor: usize,
) -> Result<Value, StructuralFieldDecodeError> {
    let mut values = Vec::new();
    walk_cbor_map_entries(
        raw_bytes,
        "expected CBOR map for enum payload map",
        "typed CBOR decode failed: trailing bytes after enum payload map",
        (&raw mut values).cast(),
        push_untyped_map_entry,
    )?;

    Ok(normalize_map_entries_or_preserve(values))
}

// Decode one fallback payload item without rebuilding nested composites.
fn decode_untyped_shallow_bytes(raw_bytes: &[u8]) -> Result<Value, StructuralFieldDecodeError> {
    let Some((major, argument, payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: truncated CBOR value",
        ));
    };

    match major {
        0 | 1 | 2 | 3 | 7 => decode_untyped_scalar_bytes(raw_bytes, major, argument, payload_start),
        4 | 5 => Ok(Value::Null),
        _ => Err(StructuralFieldDecodeError::new(
            "unsupported enum payload CBOR shape",
        )),
    }
}

// Normalize decoded map entries in place when they satisfy the runtime map
// invariants, but preserve the original decoded order when validation rejects
// the shape. This keeps current semantics without cloning the whole entry list.
fn normalize_map_entries_or_preserve(mut entries: Vec<(Value, Value)>) -> Value {
    if Value::validate_map_entries(&entries).is_err() {
        return Value::Map(entries);
    }

    Value::sort_map_entries_in_place(entries.as_mut_slice());

    for i in 1..entries.len() {
        let (left_key, _) = &entries[i - 1];
        let (right_key, _) = &entries[i];
        if Value::canonical_cmp_key(left_key, right_key) == std::cmp::Ordering::Equal {
            return Value::Map(entries);
        }
    }

    Value::Map(entries)
}

// Decode one untyped CBOR integer into the narrowest deterministic runtime value.
fn decode_untyped_integer(value: i128) -> Value {
    if let Ok(value) = u64::try_from(value) {
        return Value::Uint(value);
    }
    if let Ok(value) = i64::try_from(value) {
        return Value::Int(value);
    }

    if value.is_negative() {
        Value::IntBig(Int::from(WrappedInt::from(value)))
    } else {
        Value::UintBig(Nat::from(WrappedNat::from(value.cast_unsigned())))
    }
}

// Borrow one definite-length payload slice from the original CBOR bytes.
fn payload_bytes<'a>(
    bytes: &'a [u8],
    argument: u64,
    payload_start: usize,
    expected: &'static str,
) -> Result<&'a [u8], StructuralFieldDecodeError> {
    let payload_len = usize::try_from(argument).map_err(|_| {
        StructuralFieldDecodeError::new(format!("typed CBOR decode failed: {expected} too large"))
    })?;
    let payload_end = payload_start.checked_add(payload_len).ok_or_else(|| {
        StructuralFieldDecodeError::new(format!(
            "typed CBOR decode failed: {expected} length overflow"
        ))
    })?;
    let payload = bytes.get(payload_start..payload_end).ok_or_else(|| {
        StructuralFieldDecodeError::new(format!(
            "typed CBOR decode failed: truncated {expected} payload"
        ))
    })?;
    if payload_end != bytes.len() {
        return Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after scalar payload",
        ));
    }

    Ok(payload)
}

// Decode one CBOR integer head into the shared signed authority.
fn decode_cbor_integer(major: u8, argument: u64) -> Result<i128, StructuralFieldDecodeError> {
    match major {
        0 => Ok(i128::from(argument)),
        1 => Ok(-1 - i128::from(argument)),
        _ => Err(StructuralFieldDecodeError::new(
            "typed CBOR decode failed: expected an integer",
        )),
    }
}

// Decode one CBOR float payload into the shared `f64` authority.
fn decode_cbor_float(
    bytes: &[u8],
    argument: u64,
    payload_start: usize,
) -> Result<f64, StructuralFieldDecodeError> {
    let value = match argument {
        26 => {
            let payload = payload_bytes(bytes, 4, payload_start, "float")?;

            f64::from(f32::from_bits(u32::from_be_bytes([
                payload[0], payload[1], payload[2], payload[3],
            ])))
        }
        27 => {
            let payload = payload_bytes(bytes, 8, payload_start, "float")?;

            f64::from_bits(u64::from_be_bytes([
                payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
                payload[7],
            ]))
        }
        _ => {
            return Err(StructuralFieldDecodeError::new(
                "typed CBOR decode failed: expected a float",
            ));
        }
    };
    if !value.is_finite() {
        return Err(StructuralFieldDecodeError::new(
            "non-finite CBOR float payload",
        ));
    }

    Ok(value)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        decode_relation_target_storage_keys_bytes, decode_structural_field_bytes,
        decode_structural_value_storage_bytes,
    };
    use crate::{
        model::field::{FieldKind, RelationStrength},
        types::{Account, Decimal, EntityTag, Principal, Subaccount, Ulid},
        value::{StorageKey, Value, ValueEnum},
    };
    use std::collections::BTreeMap;

    static RELATION_ULID_KEY_KIND: FieldKind = FieldKind::Ulid;
    static STRONG_RELATION_KIND: FieldKind = FieldKind::Relation {
        target_path: "RelationTargetEntity",
        target_entity_name: "RelationTargetEntity",
        target_entity_tag: EntityTag::new(7),
        target_store_path: "RelationTargetStore",
        key_kind: &RELATION_ULID_KEY_KIND,
        strength: RelationStrength::Strong,
    };
    static STRONG_RELATION_LIST_KIND: FieldKind = FieldKind::List(&STRONG_RELATION_KIND);

    #[test]
    fn relation_target_storage_key_decode_handles_single_ulid_and_null() {
        let target = Ulid::from_u128(7);
        let target_bytes = serde_cbor::to_vec(&target).expect("ulid relation bytes should encode");
        let null_bytes =
            serde_cbor::to_vec(&Option::<Ulid>::None).expect("null relation bytes should encode");

        let decoded =
            decode_relation_target_storage_keys_bytes(&target_bytes, STRONG_RELATION_KIND)
                .expect("single relation should decode");
        let decoded_null =
            decode_relation_target_storage_keys_bytes(&null_bytes, STRONG_RELATION_KIND)
                .expect("null relation should decode");

        assert_eq!(decoded, vec![StorageKey::Ulid(target)]);
        assert!(
            decoded_null.is_empty(),
            "null relation should yield no targets"
        );
    }

    #[test]
    fn relation_target_storage_key_decode_handles_list_and_skips_null_items() {
        let left = Ulid::from_u128(8);
        let right = Ulid::from_u128(9);
        let bytes = serde_cbor::to_vec(&vec![Some(left), None, Some(right)])
            .expect("relation list bytes should encode");

        let decoded = decode_relation_target_storage_keys_bytes(&bytes, STRONG_RELATION_LIST_KIND)
            .expect("relation list should decode");

        assert_eq!(
            decoded,
            vec![StorageKey::Ulid(left), StorageKey::Ulid(right)],
        );
    }

    #[test]
    fn structural_field_decode_list_bytes_preserves_scalar_items() {
        let bytes = serde_cbor::to_vec(&vec!["left".to_string(), "right".to_string()])
            .expect("list bytes should encode");

        let decoded = decode_structural_field_bytes(
            &bytes,
            FieldKind::List(&FieldKind::Text),
            crate::model::field::FieldStorageDecode::ByKind,
        )
        .expect("scalar list field should decode");

        assert_eq!(
            decoded,
            Value::List(vec![
                Value::Text("left".to_string()),
                Value::Text("right".to_string()),
            ]),
        );
    }

    #[test]
    fn structural_field_decode_map_bytes_preserves_scalar_entries() {
        let bytes = serde_cbor::to_vec(&BTreeMap::from([
            ("alpha".to_string(), 1_u64),
            ("beta".to_string(), 2_u64),
        ]))
        .expect("map bytes should encode");

        let decoded = decode_structural_field_bytes(
            &bytes,
            FieldKind::Map {
                key: &FieldKind::Text,
                value: &FieldKind::Uint,
            },
            crate::model::field::FieldStorageDecode::ByKind,
        )
        .expect("scalar map field should decode");

        assert_eq!(
            decoded,
            Value::Map(vec![
                (Value::Text("alpha".to_string()), Value::Uint(1)),
                (Value::Text("beta".to_string()), Value::Uint(2)),
            ]),
        );
    }

    #[test]
    fn structural_value_storage_decode_preserves_list_and_map_variants() {
        let map = Value::from_map(vec![(Value::Text("k".to_string()), Value::Uint(7))])
            .expect("value map should satisfy invariants");
        let value = Value::List(vec![Value::Text("left".to_string()), map]);
        let bytes = serde_cbor::to_vec(&value).expect("value storage bytes should encode");

        let decoded =
            decode_structural_value_storage_bytes(&bytes).expect("value storage should decode");

        assert_eq!(decoded, value);
    }

    #[test]
    fn structural_value_storage_decode_preserves_enum_payload_variant() {
        let value =
            Value::Enum(ValueEnum::new("Some", Some("test::Enum")).with_payload(Value::Uint(9)));
        let bytes = serde_cbor::to_vec(&value).expect("value enum bytes should encode");

        let decoded =
            decode_structural_value_storage_bytes(&bytes).expect("value enum should decode");

        assert_eq!(decoded, value);
    }

    #[test]
    fn structural_field_decode_preserves_principal_and_subaccount_wrappers() {
        let principal = Principal::from_slice(&[1, 2, 3]);
        let subaccount = Subaccount::from_array([7; 32]);
        let principal_bytes =
            serde_cbor::to_vec(&principal).expect("principal bytes should encode");
        let subaccount_bytes =
            serde_cbor::to_vec(&subaccount).expect("subaccount bytes should encode");

        let decoded_principal = decode_structural_field_bytes(
            &principal_bytes,
            FieldKind::Principal,
            crate::model::field::FieldStorageDecode::ByKind,
        )
        .expect("principal field should decode");
        let decoded_subaccount = decode_structural_field_bytes(
            &subaccount_bytes,
            FieldKind::Subaccount,
            crate::model::field::FieldStorageDecode::ByKind,
        )
        .expect("subaccount field should decode");

        assert_eq!(decoded_principal, Value::Principal(principal));
        assert_eq!(decoded_subaccount, Value::Subaccount(subaccount));
    }

    #[test]
    fn structural_value_storage_decode_preserves_principal_variant() {
        let value = Value::Principal(Principal::from_slice(&[9, 8, 7]));
        let bytes = serde_cbor::to_vec(&value).expect("principal value bytes should encode");

        let decoded =
            decode_structural_value_storage_bytes(&bytes).expect("principal value should decode");

        assert_eq!(decoded, value);
    }

    #[test]
    fn structural_field_decode_preserves_account_wrapper() {
        let account = Account::from_parts(
            Principal::from_slice(&[1, 2, 3]),
            Some(Subaccount::from_array([5; 32])),
        );
        let bytes = serde_cbor::to_vec(&account).expect("account bytes should encode");

        let decoded = decode_structural_field_bytes(
            &bytes,
            FieldKind::Account,
            crate::model::field::FieldStorageDecode::ByKind,
        )
        .expect("account field should decode");

        assert_eq!(decoded, Value::Account(account));
    }

    #[test]
    fn structural_value_storage_decode_preserves_account_variant() {
        let value = Value::Account(Account::from_parts(
            Principal::from_slice(&[4, 5]),
            Some(Subaccount::from_array([6; 32])),
        ));
        let bytes = serde_cbor::to_vec(&value).expect("account value bytes should encode");

        let decoded =
            decode_structural_value_storage_bytes(&bytes).expect("account value should decode");

        assert_eq!(decoded, value);
    }

    #[test]
    fn structural_field_decode_preserves_decimal_and_bigint_wrappers() {
        let decimal = Decimal::from_i128_with_scale(12_340, 3);
        let int_big = crate::types::Int::from(candid::Int::from(-123_456_i64));
        let uint_big = crate::types::Nat::from(candid::Nat::from(654_321_u64));

        let decimal_bytes = serde_cbor::to_vec(&decimal).expect("decimal bytes should encode");
        let int_big_bytes = serde_cbor::to_vec(&int_big).expect("int-big bytes should encode");
        let uint_big_bytes = serde_cbor::to_vec(&uint_big).expect("uint-big bytes should encode");

        let decoded_decimal = decode_structural_field_bytes(
            &decimal_bytes,
            FieldKind::Decimal { scale: 3 },
            crate::model::field::FieldStorageDecode::ByKind,
        )
        .expect("decimal field should decode");
        let decoded_int_big = decode_structural_field_bytes(
            &int_big_bytes,
            FieldKind::IntBig,
            crate::model::field::FieldStorageDecode::ByKind,
        )
        .expect("int-big field should decode");
        let decoded_nat_big = decode_structural_field_bytes(
            &uint_big_bytes,
            FieldKind::UintBig,
            crate::model::field::FieldStorageDecode::ByKind,
        )
        .expect("uint-big field should decode");

        assert_eq!(decoded_decimal, Value::Decimal(decimal));
        assert_eq!(decoded_int_big, Value::IntBig(int_big));
        assert_eq!(decoded_nat_big, Value::UintBig(uint_big));
    }

    #[test]
    fn structural_value_storage_decode_preserves_decimal_and_bigint_variants() {
        let decimal = Value::Decimal(Decimal::from_i128_with_scale(55_000, 4));
        let int_big = Value::IntBig(crate::types::Int::from(candid::Int::from(-42_i64)));
        let uint_big = Value::UintBig(crate::types::Nat::from(candid::Nat::from(99_u64)));

        for value in [decimal, int_big, uint_big] {
            let bytes = serde_cbor::to_vec(&value).expect("value bytes should encode");
            let decoded =
                decode_structural_value_storage_bytes(&bytes).expect("value should decode");
            assert_eq!(decoded, value);
        }
    }
}
