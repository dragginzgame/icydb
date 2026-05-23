//! Module: data::structural_field::accepted
//! Responsibility: accepted-schema structural field encode, decode, and validation.
//! Does not own: generated model fallback, row layout selection, or schema mutation authority.
//! Boundary: consumes accepted field-kind metadata directly while preserving the structural payload grammar.

use crate::{
    db::{
        data::structural_field::{
            FieldDecodeError,
            binary::{
                push_binary_list_len, push_binary_map_len, push_binary_variant_payload,
                push_binary_variant_unit, split_binary_variant_payload, walk_binary_list_items,
                walk_binary_map_entries,
            },
            decode_structural_field_by_kind_bytes, decode_structural_value_storage_bytes,
            encode_structural_field_by_kind_bytes, encode_structural_value_storage_bytes,
            validate_structural_field_by_kind_bytes, validate_structural_value_storage_bytes,
        },
        schema::{PersistedEnumVariant, PersistedFieldKind},
    },
    error::InternalError,
    model::field::{FieldKind, FieldStorageDecode},
    value::{Value, ValueEnum},
};
use std::str;

// Decode one accepted-schema by-kind field payload. Simple non-recursive kinds
// still reuse the existing generated-compatible decoder because their runtime
// shape has no borrowed nested metadata. Recursive kinds stay on accepted
// `PersistedFieldKind` references throughout the traversal.
pub(in crate::db) fn decode_structural_field_by_accepted_kind_bytes(
    raw_bytes: &[u8],
    kind: &PersistedFieldKind,
) -> Result<Value, FieldDecodeError> {
    if let Some(runtime_kind) = generated_compatible_simple_kind_from_accepted_kind(kind) {
        return decode_structural_field_by_kind_bytes(raw_bytes, runtime_kind);
    }

    match kind {
        PersistedFieldKind::Enum { path, variants } => {
            decode_accepted_enum_bytes(raw_bytes, path, variants.as_slice())
        }
        PersistedFieldKind::List(inner) | PersistedFieldKind::Set(inner) => {
            decode_accepted_list_bytes(raw_bytes, inner.as_ref())
        }
        PersistedFieldKind::Map { key, value } => {
            decode_accepted_map_bytes(raw_bytes, key.as_ref(), value.as_ref())
        }
        PersistedFieldKind::Relation { key_kind, .. } => {
            decode_structural_field_by_accepted_kind_bytes(raw_bytes, key_kind.as_ref())
        }
        PersistedFieldKind::Account
        | PersistedFieldKind::Blob { .. }
        | PersistedFieldKind::Bool
        | PersistedFieldKind::Date
        | PersistedFieldKind::Decimal { .. }
        | PersistedFieldKind::Duration
        | PersistedFieldKind::Float32
        | PersistedFieldKind::Float64
        | PersistedFieldKind::Int
        | PersistedFieldKind::Int128
        | PersistedFieldKind::IntBig
        | PersistedFieldKind::Principal
        | PersistedFieldKind::Structured { .. }
        | PersistedFieldKind::Subaccount
        | PersistedFieldKind::Text { .. }
        | PersistedFieldKind::Timestamp
        | PersistedFieldKind::Nat
        | PersistedFieldKind::Nat128
        | PersistedFieldKind::NatBig
        | PersistedFieldKind::Ulid
        | PersistedFieldKind::Unit => unreachable!("simple accepted kinds are decoded above"),
    }
}

// Encode one accepted-schema by-kind field payload. Simple non-recursive kinds
// reuse the generated-compatible structural encoder after the accepted
// `PersistedFieldKind` has selected the kind. Recursive shapes stay on
// accepted metadata throughout traversal.
pub(in crate::db) fn encode_structural_field_by_accepted_kind_bytes(
    kind: &PersistedFieldKind,
    value: &Value,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    if let Some(runtime_kind) = generated_compatible_simple_kind_from_accepted_kind(kind) {
        return encode_structural_field_by_kind_bytes(runtime_kind, value, field_name);
    }

    let mut encoded = Vec::new();
    encode_accepted_binary_field_into(&mut encoded, kind, value, field_name)?;

    Ok(encoded)
}

// Validate one accepted-schema by-kind field payload. This mirrors the decode
// entrypoint so accepted row readers have a fail-closed validation seam before
// deciding whether to materialize the final runtime `Value`.
pub(in crate::db) fn validate_structural_field_by_accepted_kind_bytes(
    raw_bytes: &[u8],
    kind: &PersistedFieldKind,
) -> Result<(), FieldDecodeError> {
    if let Some(runtime_kind) = generated_compatible_simple_kind_from_accepted_kind(kind) {
        return validate_structural_field_by_kind_bytes(raw_bytes, runtime_kind);
    }

    match kind {
        PersistedFieldKind::Enum { variants, .. } => {
            validate_accepted_enum_bytes(raw_bytes, variants.as_slice())
        }
        PersistedFieldKind::List(inner) | PersistedFieldKind::Set(inner) => {
            validate_accepted_list_bytes(raw_bytes, inner.as_ref())
        }
        PersistedFieldKind::Map { key, value } => {
            validate_accepted_map_bytes(raw_bytes, key.as_ref(), value.as_ref())
        }
        PersistedFieldKind::Relation { key_kind, .. } => {
            validate_structural_field_by_accepted_kind_bytes(raw_bytes, key_kind.as_ref())
        }
        PersistedFieldKind::Account
        | PersistedFieldKind::Blob { .. }
        | PersistedFieldKind::Bool
        | PersistedFieldKind::Date
        | PersistedFieldKind::Decimal { .. }
        | PersistedFieldKind::Duration
        | PersistedFieldKind::Float32
        | PersistedFieldKind::Float64
        | PersistedFieldKind::Int
        | PersistedFieldKind::Int128
        | PersistedFieldKind::IntBig
        | PersistedFieldKind::Principal
        | PersistedFieldKind::Structured { .. }
        | PersistedFieldKind::Subaccount
        | PersistedFieldKind::Text { .. }
        | PersistedFieldKind::Timestamp
        | PersistedFieldKind::Nat
        | PersistedFieldKind::Nat128
        | PersistedFieldKind::NatBig
        | PersistedFieldKind::Ulid
        | PersistedFieldKind::Unit => unreachable!("simple accepted kinds are validated above"),
    }
}

// Return whether one accepted persisted kind uses the storage-key binary lane.
// This mirrors the generated-kind lane so nullable structural-null detection
// can avoid treating storage-key nulls as value-storage null sentinels.
pub(in crate::db) fn accepted_kind_supports_storage_key_binary(kind: &PersistedFieldKind) -> bool {
    match kind {
        PersistedFieldKind::Account
        | PersistedFieldKind::Int
        | PersistedFieldKind::Principal
        | PersistedFieldKind::Subaccount
        | PersistedFieldKind::Timestamp
        | PersistedFieldKind::Nat
        | PersistedFieldKind::Ulid
        | PersistedFieldKind::Unit => true,
        PersistedFieldKind::Relation { key_kind, .. } => {
            accepted_kind_supports_storage_key_binary(key_kind)
        }
        PersistedFieldKind::List(inner) | PersistedFieldKind::Set(inner) => {
            matches!(inner.as_ref(), PersistedFieldKind::Relation { .. })
                && accepted_kind_supports_storage_key_binary(inner)
        }
        _ => false,
    }
}

// Adapt accepted field kinds that carry no borrowed nested metadata into the
// existing generated-compatible field-codec shape. The accepted
// `PersistedFieldKind` remains the authority; this is only a leaf-codec reuse
// seam, not Rust-type inference. Recursive collections, relations, and enums
// stay in accepted-kind form throughout traversal.
const fn generated_compatible_simple_kind_from_accepted_kind(
    kind: &PersistedFieldKind,
) -> Option<FieldKind> {
    match kind {
        PersistedFieldKind::Account => Some(FieldKind::Account),
        PersistedFieldKind::Blob { max_len } => Some(FieldKind::Blob { max_len: *max_len }),
        PersistedFieldKind::Bool => Some(FieldKind::Bool),
        PersistedFieldKind::Date => Some(FieldKind::Date),
        PersistedFieldKind::Decimal { scale } => Some(FieldKind::Decimal { scale: *scale }),
        PersistedFieldKind::Duration => Some(FieldKind::Duration),
        PersistedFieldKind::Float32 => Some(FieldKind::Float32),
        PersistedFieldKind::Float64 => Some(FieldKind::Float64),
        PersistedFieldKind::Int => Some(FieldKind::Int),
        PersistedFieldKind::Int128 => Some(FieldKind::Int128),
        PersistedFieldKind::IntBig => Some(FieldKind::IntBig),
        PersistedFieldKind::Principal => Some(FieldKind::Principal),
        PersistedFieldKind::Structured { queryable } => Some(FieldKind::Structured {
            queryable: *queryable,
        }),
        PersistedFieldKind::Subaccount => Some(FieldKind::Subaccount),
        PersistedFieldKind::Text { max_len } => Some(FieldKind::Text { max_len: *max_len }),
        PersistedFieldKind::Timestamp => Some(FieldKind::Timestamp),
        PersistedFieldKind::Nat => Some(FieldKind::Nat),
        PersistedFieldKind::Nat128 => Some(FieldKind::Nat128),
        PersistedFieldKind::NatBig => Some(FieldKind::NatBig),
        PersistedFieldKind::Ulid => Some(FieldKind::Ulid),
        PersistedFieldKind::Unit => Some(FieldKind::Unit),
        PersistedFieldKind::Enum { .. }
        | PersistedFieldKind::List(_)
        | PersistedFieldKind::Map { .. }
        | PersistedFieldKind::Relation { .. }
        | PersistedFieldKind::Set(_) => None,
    }
}

// Encode one accepted recursive field into Structural Binary v1 bytes.
fn encode_accepted_binary_field_into(
    out: &mut Vec<u8>,
    kind: &PersistedFieldKind,
    value: &Value,
    field_name: &str,
) -> Result<(), InternalError> {
    if let Some(runtime_kind) = generated_compatible_simple_kind_from_accepted_kind(kind) {
        let bytes = encode_structural_field_by_kind_bytes(runtime_kind, value, field_name)?;
        out.extend_from_slice(bytes.as_slice());
        return Ok(());
    }

    match kind {
        PersistedFieldKind::Enum { path, variants } => {
            encode_accepted_enum_bytes(out, path, variants.as_slice(), value, field_name)
        }
        PersistedFieldKind::List(inner) | PersistedFieldKind::Set(inner) => {
            encode_accepted_list_bytes(out, inner.as_ref(), value, field_name)
        }
        PersistedFieldKind::Map { key, value: item } => {
            encode_accepted_map_bytes(out, key.as_ref(), item.as_ref(), value, field_name)
        }
        PersistedFieldKind::Relation { key_kind, .. } => {
            encode_accepted_binary_field_into(out, key_kind.as_ref(), value, field_name)
        }
        PersistedFieldKind::Account
        | PersistedFieldKind::Blob { .. }
        | PersistedFieldKind::Bool
        | PersistedFieldKind::Date
        | PersistedFieldKind::Decimal { .. }
        | PersistedFieldKind::Duration
        | PersistedFieldKind::Float32
        | PersistedFieldKind::Float64
        | PersistedFieldKind::Int
        | PersistedFieldKind::Int128
        | PersistedFieldKind::IntBig
        | PersistedFieldKind::Principal
        | PersistedFieldKind::Structured { .. }
        | PersistedFieldKind::Subaccount
        | PersistedFieldKind::Text { .. }
        | PersistedFieldKind::Timestamp
        | PersistedFieldKind::Nat
        | PersistedFieldKind::Nat128
        | PersistedFieldKind::NatBig
        | PersistedFieldKind::Ulid
        | PersistedFieldKind::Unit => unreachable!("simple accepted kinds are encoded above"),
    }
}

// Decode-state for accepted-schema list traversal.
struct AcceptedListDecodeState<'a> {
    inner: &'a PersistedFieldKind,
    items: Vec<Value>,
}

// Validate-state for accepted-schema list traversal.
struct AcceptedListValidateState<'a> {
    inner: &'a PersistedFieldKind,
}

// Decode-state for accepted-schema map traversal.
struct AcceptedMapDecodeState<'a> {
    key_kind: &'a PersistedFieldKind,
    value_kind: &'a PersistedFieldKind,
    entries: Vec<(Value, Value)>,
}

// Validate-state for accepted-schema map traversal.
struct AcceptedMapValidateState<'a> {
    key_kind: &'a PersistedFieldKind,
    value_kind: &'a PersistedFieldKind,
}

// Push one accepted-schema list item into the current decode state.
unsafe fn push_accepted_list_item(
    item_bytes: &[u8],
    context: *mut (),
) -> Result<(), FieldDecodeError> {
    let state = unsafe { &mut *context.cast::<AcceptedListDecodeState<'_>>() };
    let item = decode_structural_field_by_accepted_kind_bytes(item_bytes, state.inner)?;
    if matches!(state.inner, PersistedFieldKind::Relation { .. }) && matches!(item, Value::Null) {
        return Ok(());
    }
    state.items.push(item);

    Ok(())
}

// Validate one accepted-schema list item against the current traversal state.
unsafe fn validate_accepted_list_item(
    item_bytes: &[u8],
    context: *mut (),
) -> Result<(), FieldDecodeError> {
    let state = unsafe { &mut *context.cast::<AcceptedListValidateState<'_>>() };

    validate_structural_field_by_accepted_kind_bytes(item_bytes, state.inner)
}

// Push one accepted-schema map entry into the current decode state.
unsafe fn push_accepted_map_entry(
    key_bytes: &[u8],
    value_bytes: &[u8],
    context: *mut (),
) -> Result<(), FieldDecodeError> {
    let state = unsafe { &mut *context.cast::<AcceptedMapDecodeState<'_>>() };
    state.entries.push((
        decode_structural_field_by_accepted_kind_bytes(key_bytes, state.key_kind)?,
        decode_structural_field_by_accepted_kind_bytes(value_bytes, state.value_kind)?,
    ));

    Ok(())
}

// Validate one accepted-schema map entry against the current traversal state.
unsafe fn validate_accepted_map_entry(
    key_bytes: &[u8],
    value_bytes: &[u8],
    context: *mut (),
) -> Result<(), FieldDecodeError> {
    let state = unsafe { &mut *context.cast::<AcceptedMapValidateState<'_>>() };
    validate_structural_field_by_accepted_kind_bytes(key_bytes, state.key_kind)?;
    validate_structural_field_by_accepted_kind_bytes(value_bytes, state.value_kind)
}

// Decode one accepted list or set by recursively decoding each item slice.
fn decode_accepted_list_bytes(
    raw_bytes: &[u8],
    inner: &PersistedFieldKind,
) -> Result<Value, FieldDecodeError> {
    let mut state = AcceptedListDecodeState {
        inner,
        items: Vec::new(),
    };
    walk_binary_list_items(
        raw_bytes,
        "expected Structural Binary list for list/set field",
        "structural binary: trailing bytes after list/set field",
        (&raw mut state).cast(),
        push_accepted_list_item,
    )?;

    Ok(Value::List(state.items))
}

// Encode one accepted list or set by recursively encoding each item. Accepted
// relation collections preserve generated-compatible relation-list behavior by
// skipping explicit null items.
fn encode_accepted_list_bytes(
    out: &mut Vec<u8>,
    inner: &PersistedFieldKind,
    value: &Value,
    field_name: &str,
) -> Result<(), InternalError> {
    let Value::List(items) = value else {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("accepted field kind {inner:?} list does not accept runtime value {value:?}"),
        ));
    };
    let skip_null_items = matches!(inner, PersistedFieldKind::Relation { .. });
    let encoded_len = if skip_null_items {
        items
            .iter()
            .filter(|item| !matches!(item, Value::Null))
            .count()
    } else {
        items.len()
    };

    push_binary_list_len(out, encoded_len);
    for item in items {
        if skip_null_items && matches!(item, Value::Null) {
            continue;
        }
        encode_accepted_binary_field_into(out, inner, item, field_name)?;
    }

    Ok(())
}

// Validate one accepted list or set by recursively validating each item slice.
fn validate_accepted_list_bytes(
    raw_bytes: &[u8],
    inner: &PersistedFieldKind,
) -> Result<(), FieldDecodeError> {
    let mut state = AcceptedListValidateState { inner };
    walk_binary_list_items(
        raw_bytes,
        "expected Structural Binary list for list/set field",
        "structural binary: trailing bytes after list/set field",
        (&raw mut state).cast(),
        validate_accepted_list_item,
    )
}

// Encode one accepted map by recursively encoding each key/value pair.
fn encode_accepted_map_bytes(
    out: &mut Vec<u8>,
    key_kind: &PersistedFieldKind,
    value_kind: &PersistedFieldKind,
    value: &Value,
    field_name: &str,
) -> Result<(), InternalError> {
    let Value::Map(entries) = value else {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("accepted map field does not accept runtime value {value:?}"),
        ));
    };

    push_binary_map_len(out, entries.len());
    for (entry_key, entry_value) in entries {
        encode_accepted_binary_field_into(out, key_kind, entry_key, field_name)?;
        encode_accepted_binary_field_into(out, value_kind, entry_value, field_name)?;
    }

    Ok(())
}

// Decode one accepted map by recursively decoding each key/value slice pair.
fn decode_accepted_map_bytes(
    raw_bytes: &[u8],
    key_kind: &PersistedFieldKind,
    value_kind: &PersistedFieldKind,
) -> Result<Value, FieldDecodeError> {
    let mut state = AcceptedMapDecodeState {
        key_kind,
        value_kind,
        entries: Vec::new(),
    };
    walk_binary_map_entries(
        raw_bytes,
        "expected Structural Binary map for map field",
        "structural binary: trailing bytes after map field",
        (&raw mut state).cast(),
        push_accepted_map_entry,
    )?;

    Ok(Value::Map(state.entries))
}

// Validate one accepted map by recursively validating each key/value slice
// pair.
fn validate_accepted_map_bytes(
    raw_bytes: &[u8],
    key_kind: &PersistedFieldKind,
    value_kind: &PersistedFieldKind,
) -> Result<(), FieldDecodeError> {
    let mut state = AcceptedMapValidateState {
        key_kind,
        value_kind,
    };
    walk_binary_map_entries(
        raw_bytes,
        "expected Structural Binary map for map field",
        "structural binary: trailing bytes after map field",
        (&raw mut state).cast(),
        validate_accepted_map_entry,
    )
}

// Encode one accepted enum payload using persisted variant metadata rather
// than generated static enum descriptors.
fn encode_accepted_enum_bytes(
    out: &mut Vec<u8>,
    path: &str,
    variants: &[PersistedEnumVariant],
    value: &Value,
    field_name: &str,
) -> Result<(), InternalError> {
    let Value::Enum(value) = value else {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("enum field '{path}' does not accept runtime value {value:?}"),
        ));
    };

    if let Some(actual_path) = value.path()
        && actual_path != path
    {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("enum path mismatch: expected '{path}', found '{actual_path}'"),
        ));
    }

    let Some(payload) = value.payload() else {
        push_binary_variant_unit(out, value.variant());
        return Ok(());
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
    let payload_bytes = match variant_model.payload_storage_decode() {
        FieldStorageDecode::ByKind => {
            encode_structural_field_by_accepted_kind_bytes(payload_kind, payload, field_name)?
        }
        FieldStorageDecode::Value => encode_structural_value_storage_bytes(payload)?,
    };
    push_binary_variant_payload(out, value.variant(), payload_bytes.as_slice());

    Ok(())
}

// Decode one accepted enum payload using persisted variant metadata rather
// than generated static enum descriptors.
fn decode_accepted_enum_bytes(
    raw_bytes: &[u8],
    path: &str,
    variants: &[PersistedEnumVariant],
) -> Result<Value, FieldDecodeError> {
    let (variant_bytes, payload_bytes) = split_binary_variant_payload(
        raw_bytes,
        "structural binary: truncated enum field",
        "expected Structural Binary variant for enum field",
        "structural binary: trailing bytes after enum field",
    )?;
    let variant = str::from_utf8(variant_bytes)
        .map_err(|_| FieldDecodeError::new("structural binary: enum label must be UTF-8"))?;

    let Some(payload_bytes) = payload_bytes else {
        return Ok(Value::Enum(ValueEnum::new(variant, Some(path))));
    };
    let Some(variant_model) = variants.iter().find(|item| item.ident() == variant) else {
        return Err(FieldDecodeError::new(
            "structural binary untyped enum payload is unsupported",
        ));
    };
    let Some(payload_kind) = variant_model.payload_kind() else {
        return Err(FieldDecodeError::new(
            "structural binary untyped enum payload is unsupported",
        ));
    };
    let payload = match variant_model.payload_storage_decode() {
        FieldStorageDecode::ByKind => {
            decode_structural_field_by_accepted_kind_bytes(payload_bytes, payload_kind)?
        }
        FieldStorageDecode::Value => decode_structural_value_storage_bytes(payload_bytes)?,
    };

    Ok(Value::Enum(
        ValueEnum::new(variant, Some(path)).with_payload(payload),
    ))
}

// Validate one accepted enum payload using persisted variant metadata rather
// than generated static enum descriptors.
fn validate_accepted_enum_bytes(
    raw_bytes: &[u8],
    variants: &[PersistedEnumVariant],
) -> Result<(), FieldDecodeError> {
    let (variant_bytes, payload_bytes) = split_binary_variant_payload(
        raw_bytes,
        "structural binary: truncated enum field",
        "expected Structural Binary variant for enum field",
        "structural binary: trailing bytes after enum field",
    )?;
    let variant = str::from_utf8(variant_bytes)
        .map_err(|_| FieldDecodeError::new("structural binary: enum label must be UTF-8"))?;
    let Some(payload_bytes) = payload_bytes else {
        return Ok(());
    };
    if let Some(variant_model) = variants.iter().find(|item| item.ident() == variant)
        && let Some(payload_kind) = variant_model.payload_kind()
    {
        return match variant_model.payload_storage_decode() {
            FieldStorageDecode::ByKind => {
                validate_structural_field_by_accepted_kind_bytes(payload_bytes, payload_kind)
            }
            FieldStorageDecode::Value => validate_structural_value_storage_bytes(payload_bytes),
        };
    }

    Err(FieldDecodeError::new(
        "structural binary untyped enum payload is unsupported",
    ))
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            data::{
                decode_structural_field_by_accepted_kind_bytes,
                decode_structural_field_by_kind_bytes, encode_structural_field_by_kind_bytes,
                validate_structural_field_by_accepted_kind_bytes,
                validate_structural_field_by_kind_bytes,
            },
            schema::{PersistedEnumVariant, PersistedFieldKind},
        },
        model::field::{FieldKind, FieldStorageDecode},
        value::{Value, ValueEnum},
    };

    fn assert_generated_and_accepted_decode_match(
        generated_kind: FieldKind,
        accepted_kind: &PersistedFieldKind,
        value: &Value,
        field_name: &str,
    ) {
        let encoded = encode_structural_field_by_kind_bytes(generated_kind, value, field_name)
            .expect("generated-compatible test payload should encode");
        let generated = decode_structural_field_by_kind_bytes(&encoded, generated_kind)
            .expect("generated decoder should decode test payload");
        let accepted = decode_structural_field_by_accepted_kind_bytes(&encoded, accepted_kind)
            .expect("accepted decoder should decode generated-compatible payload");

        validate_structural_field_by_accepted_kind_bytes(&encoded, accepted_kind)
            .expect("accepted kind should validate generated-compatible payload");

        assert_eq!(generated, *value);
        assert_eq!(accepted, generated);
    }

    fn assert_generated_and_accepted_reject_match(
        generated_kind: FieldKind,
        accepted_kind: &PersistedFieldKind,
        raw_bytes: &[u8],
    ) {
        assert!(decode_structural_field_by_kind_bytes(raw_bytes, generated_kind).is_err());
        assert!(decode_structural_field_by_accepted_kind_bytes(raw_bytes, accepted_kind).is_err());
        assert!(validate_structural_field_by_kind_bytes(raw_bytes, generated_kind).is_err());
        assert!(
            validate_structural_field_by_accepted_kind_bytes(raw_bytes, accepted_kind).is_err()
        );
    }

    #[test]
    fn accepted_kind_decoder_matches_generated_nested_collection_payloads() {
        let generated_kind = FieldKind::Map {
            key: &FieldKind::Text { max_len: None },
            value: &FieldKind::List(&FieldKind::Nat),
        };
        let accepted_kind = PersistedFieldKind::Map {
            key: Box::new(PersistedFieldKind::Text { max_len: None }),
            value: Box::new(PersistedFieldKind::List(Box::new(PersistedFieldKind::Nat))),
        };
        let value = Value::Map(vec![
            (
                Value::Text("alpha".to_string()),
                Value::List(vec![Value::Nat(1), Value::Nat(2)]),
            ),
            (
                Value::Text("beta".to_string()),
                Value::List(vec![Value::Nat(3)]),
            ),
        ]);

        assert_generated_and_accepted_decode_match(
            generated_kind,
            &accepted_kind,
            &value,
            "payload",
        );
    }

    #[test]
    fn accepted_kind_decoder_rejects_malformed_nested_lists_like_generated_decoder() {
        let generated_kind = FieldKind::List(&FieldKind::Nat);
        let accepted_kind = PersistedFieldKind::List(Box::new(PersistedFieldKind::Nat));
        let value = Value::List(vec![Value::Nat(1), Value::Nat(2)]);
        let mut malformed =
            encode_structural_field_by_kind_bytes(generated_kind, &value, "numbers")
                .expect("generated-compatible list payload should encode");
        malformed.pop();

        assert_generated_and_accepted_reject_match(
            generated_kind,
            &accepted_kind,
            malformed.as_slice(),
        );
    }

    #[test]
    fn accepted_kind_decoder_rejects_malformed_nested_maps_like_generated_decoder() {
        let generated_kind = FieldKind::Map {
            key: &FieldKind::Text { max_len: None },
            value: &FieldKind::Nat,
        };
        let accepted_kind = PersistedFieldKind::Map {
            key: Box::new(PersistedFieldKind::Text { max_len: None }),
            value: Box::new(PersistedFieldKind::Nat),
        };
        let value = Value::Map(vec![(Value::Text("alpha".to_string()), Value::Nat(1))]);
        let mut malformed =
            encode_structural_field_by_kind_bytes(generated_kind, &value, "entries")
                .expect("generated-compatible map payload should encode");
        malformed.pop();

        assert_generated_and_accepted_reject_match(
            generated_kind,
            &accepted_kind,
            malformed.as_slice(),
        );
    }

    #[test]
    fn accepted_kind_decoder_matches_generated_enum_payload_contracts() {
        static GENERATED_VARIANTS: &[crate::model::field::EnumVariantModel] =
            &[crate::model::field::EnumVariantModel::new(
                "Loaded",
                Some(&FieldKind::Nat),
                FieldStorageDecode::ByKind,
            )];
        let generated_kind = FieldKind::Enum {
            path: "tests::State",
            variants: GENERATED_VARIANTS,
        };
        let accepted_kind = PersistedFieldKind::Enum {
            path: "tests::State".to_string(),
            variants: vec![PersistedEnumVariant::new(
                "Loaded".to_string(),
                Some(Box::new(PersistedFieldKind::Nat)),
                FieldStorageDecode::ByKind,
            )],
        };
        let value =
            Value::Enum(ValueEnum::new("Loaded", Some("tests::State")).with_payload(Value::Nat(9)));

        assert_generated_and_accepted_decode_match(generated_kind, &accepted_kind, &value, "state");
    }

    #[test]
    fn accepted_kind_decoder_matches_generated_relation_list_payloads() {
        const RELATION_KEY_KIND: FieldKind = FieldKind::Ulid;
        let generated_kind = FieldKind::List(&FieldKind::Relation {
            target_path: "tests::Target",
            target_entity_name: "Target",
            target_entity_tag: crate::testing::PROBE_ENTITY_TAG,
            target_store_path: "tests::TargetStore",
            key_kind: &RELATION_KEY_KIND,
            strength: crate::model::field::RelationStrength::Strong,
        });
        let accepted_kind = PersistedFieldKind::List(Box::new(PersistedFieldKind::Relation {
            target_path: "tests::Target".to_string(),
            target_entity_name: "Target".to_string(),
            target_entity_tag: crate::testing::PROBE_ENTITY_TAG,
            target_store_path: "tests::TargetStore".to_string(),
            key_kind: Box::new(PersistedFieldKind::Ulid),
            strength: crate::db::schema::PersistedRelationStrength::Strong,
        }));
        let value = Value::List(vec![
            Value::Ulid(crate::types::Ulid::from_u128(11)),
            Value::Ulid(crate::types::Ulid::from_u128(12)),
        ]);

        assert_generated_and_accepted_decode_match(
            generated_kind,
            &accepted_kind,
            &value,
            "targets",
        );
    }
}
