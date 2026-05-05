//! Accepted-schema structural field decoding.
//!
//! This module is the first row-decode bridge that consumes accepted schema
//! field kind metadata directly instead of first projecting back into generated
//! `FieldKind` values. It intentionally starts at the field boundary; row
//! layout selection can move onto it later without changing the payload grammar.

use crate::{
    db::{
        data::structural_field::{
            FieldDecodeError,
            binary::{
                TAG_LIST, TAG_MAP, parse_binary_head, skip_binary_value,
                split_binary_variant_payload,
            },
            decode_structural_field_by_kind_bytes, decode_structural_value_storage_bytes,
            validate_structural_field_by_kind_bytes, validate_structural_value_storage_bytes,
        },
        schema::{PersistedEnumVariant, PersistedFieldKind},
    },
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
        | PersistedFieldKind::Uint
        | PersistedFieldKind::Uint128
        | PersistedFieldKind::UintBig
        | PersistedFieldKind::Ulid
        | PersistedFieldKind::Unit => unreachable!("simple accepted kinds are decoded above"),
    }
}

// Validate one accepted-schema by-kind field payload. This mirrors the decode
// entrypoint so the future accepted row reader has a fail-closed validation
// seam before it decides whether to materialize the final runtime `Value`.
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
        | PersistedFieldKind::Uint
        | PersistedFieldKind::Uint128
        | PersistedFieldKind::UintBig
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
        | PersistedFieldKind::Uint
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
        PersistedFieldKind::Uint => Some(FieldKind::Uint),
        PersistedFieldKind::Uint128 => Some(FieldKind::Uint128),
        PersistedFieldKind::UintBig => Some(FieldKind::UintBig),
        PersistedFieldKind::Ulid => Some(FieldKind::Ulid),
        PersistedFieldKind::Unit => Some(FieldKind::Unit),
        PersistedFieldKind::Enum { .. }
        | PersistedFieldKind::List(_)
        | PersistedFieldKind::Map { .. }
        | PersistedFieldKind::Relation { .. }
        | PersistedFieldKind::Set(_) => None,
    }
}

// Decode one accepted list or set by recursively decoding each item slice.
fn decode_accepted_list_bytes(
    raw_bytes: &[u8],
    inner: &PersistedFieldKind,
) -> Result<Value, FieldDecodeError> {
    let mut items = Vec::new();
    walk_accepted_list_items(raw_bytes, |item_bytes| {
        let item = decode_structural_field_by_accepted_kind_bytes(item_bytes, inner)?;
        if matches!(inner, PersistedFieldKind::Relation { .. }) && matches!(item, Value::Null) {
            return Ok(());
        }
        items.push(item);

        Ok(())
    })?;

    Ok(Value::List(items))
}

// Validate one accepted list or set by recursively validating each item slice.
fn validate_accepted_list_bytes(
    raw_bytes: &[u8],
    inner: &PersistedFieldKind,
) -> Result<(), FieldDecodeError> {
    walk_accepted_list_items(raw_bytes, |item_bytes| {
        validate_structural_field_by_accepted_kind_bytes(item_bytes, inner)
    })
}

// Decode one accepted map by recursively decoding each key/value slice pair.
fn decode_accepted_map_bytes(
    raw_bytes: &[u8],
    key_kind: &PersistedFieldKind,
    value_kind: &PersistedFieldKind,
) -> Result<Value, FieldDecodeError> {
    let mut entries = Vec::new();
    walk_accepted_map_entries(raw_bytes, |key_bytes, value_bytes| {
        entries.push((
            decode_structural_field_by_accepted_kind_bytes(key_bytes, key_kind)?,
            decode_structural_field_by_accepted_kind_bytes(value_bytes, value_kind)?,
        ));

        Ok(())
    })?;

    Ok(Value::Map(entries))
}

// Validate one accepted map by recursively validating each key/value slice
// pair.
fn validate_accepted_map_bytes(
    raw_bytes: &[u8],
    key_kind: &PersistedFieldKind,
    value_kind: &PersistedFieldKind,
) -> Result<(), FieldDecodeError> {
    walk_accepted_map_entries(raw_bytes, |key_bytes, value_bytes| {
        validate_structural_field_by_accepted_kind_bytes(key_bytes, key_kind)?;
        validate_structural_field_by_accepted_kind_bytes(value_bytes, value_kind)
    })
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

// Walk one accepted list/set payload and yield each raw item slice to the
// caller. This avoids the raw-pointer callback shape used by the lower binary
// walker so accepted field lifetimes remain normal Rust borrows.
fn walk_accepted_list_items(
    raw_bytes: &[u8],
    mut on_item: impl FnMut(&[u8]) -> Result<(), FieldDecodeError>,
) -> Result<(), FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated binary value",
        ));
    };
    if tag != TAG_LIST {
        return Err(FieldDecodeError::new(
            "expected Structural Binary list for list/set field",
        ));
    }

    let mut cursor = payload_start;
    for _ in 0..len {
        let item_start = cursor;
        cursor = skip_binary_value(raw_bytes, cursor)?;
        on_item(&raw_bytes[item_start..cursor])?;
    }
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after list/set field",
        ));
    }

    Ok(())
}

// Walk one accepted map payload and yield each raw key/value slice pair to the
// caller. The accepted decoder owns the semantic recursion; the binary helper
// only proves the payload frame is bounded and complete.
fn walk_accepted_map_entries(
    raw_bytes: &[u8],
    mut on_entry: impl FnMut(&[u8], &[u8]) -> Result<(), FieldDecodeError>,
) -> Result<(), FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated binary value",
        ));
    };
    if tag != TAG_MAP {
        return Err(FieldDecodeError::new(
            "expected Structural Binary map for map field",
        ));
    }

    let mut cursor = payload_start;
    for _ in 0..len {
        let key_start = cursor;
        cursor = skip_binary_value(raw_bytes, cursor)?;
        let value_start = cursor;
        cursor = skip_binary_value(raw_bytes, cursor)?;
        on_entry(
            &raw_bytes[key_start..value_start],
            &raw_bytes[value_start..cursor],
        )?;
    }
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after map field",
        ));
    }

    Ok(())
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
        let generated_decode = decode_structural_field_by_kind_bytes(raw_bytes, generated_kind)
            .expect_err("generated decoder should reject malformed payload");
        let accepted_decode =
            decode_structural_field_by_accepted_kind_bytes(raw_bytes, accepted_kind)
                .expect_err("accepted decoder should reject malformed payload");
        assert_eq!(
            accepted_decode.to_string(),
            generated_decode.to_string(),
            "accepted decode should preserve generated-compatible malformed-payload taxonomy",
        );

        let generated_validate = validate_structural_field_by_kind_bytes(raw_bytes, generated_kind)
            .expect_err("generated validator should reject malformed payload");
        let accepted_validate =
            validate_structural_field_by_accepted_kind_bytes(raw_bytes, accepted_kind)
                .expect_err("accepted validator should reject malformed payload");
        assert_eq!(
            accepted_validate.to_string(),
            generated_validate.to_string(),
            "accepted validation should preserve generated-compatible malformed-payload taxonomy",
        );
    }

    #[test]
    fn accepted_kind_decoder_matches_generated_nested_collection_payloads() {
        let generated_kind = FieldKind::Map {
            key: &FieldKind::Text { max_len: None },
            value: &FieldKind::List(&FieldKind::Uint),
        };
        let accepted_kind = PersistedFieldKind::Map {
            key: Box::new(PersistedFieldKind::Text { max_len: None }),
            value: Box::new(PersistedFieldKind::List(Box::new(PersistedFieldKind::Uint))),
        };
        let value = Value::Map(vec![
            (
                Value::Text("alpha".to_string()),
                Value::List(vec![Value::Uint(1), Value::Uint(2)]),
            ),
            (
                Value::Text("beta".to_string()),
                Value::List(vec![Value::Uint(3)]),
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
        let generated_kind = FieldKind::List(&FieldKind::Uint);
        let accepted_kind = PersistedFieldKind::List(Box::new(PersistedFieldKind::Uint));
        let value = Value::List(vec![Value::Uint(1), Value::Uint(2)]);
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
            value: &FieldKind::Uint,
        };
        let accepted_kind = PersistedFieldKind::Map {
            key: Box::new(PersistedFieldKind::Text { max_len: None }),
            value: Box::new(PersistedFieldKind::Uint),
        };
        let value = Value::Map(vec![(Value::Text("alpha".to_string()), Value::Uint(1))]);
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
                Some(&FieldKind::Uint),
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
                Some(Box::new(PersistedFieldKind::Uint)),
                FieldStorageDecode::ByKind,
            )],
        };
        let value = Value::Enum(
            ValueEnum::new("Loaded", Some("tests::State")).with_payload(Value::Uint(9)),
        );

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
