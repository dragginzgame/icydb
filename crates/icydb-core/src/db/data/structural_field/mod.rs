//! Module: data::structural_field
//! Responsibility: canonical persisted-field structural decode helpers.
//! Does not own: row layout planning, typed entity reconstruction, or query semantics.
//! Boundary: runtime paths use this module when they need one persisted field decoded without `E`.

mod cbor;
mod composite;
mod leaf;
mod scalar;
mod storage_key;
mod value_storage;

use crate::{model::field::FieldKind, value::Value};
use thiserror::Error as ThisError;

use composite::{decode_composite_field_by_kind_bytes, validate_composite_field_by_kind_bytes};
use leaf::decode_leaf_field_by_kind_bytes;
use scalar::decode_scalar_fast_path_bytes;

pub(in crate::db) use storage_key::{
    decode_relation_target_storage_keys_bytes, decode_storage_key_field_bytes,
};
pub(in crate::db) use value_storage::{
    decode_structural_value_storage_bytes, validate_structural_value_storage_bytes,
};

///
/// FieldDecodeError
///
/// FieldDecodeError captures one persisted-field structural decode
/// failure.
/// It keeps structural decode diagnostics local to the field boundary so row
/// and relation callers can map them into taxonomy-correct higher-level errors.
///

#[derive(Clone, Debug, ThisError)]
#[error("{message}")]
pub(in crate::db) struct FieldDecodeError {
    message: String,
}

impl FieldDecodeError {
    // Build one structural field-decode failure message.
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Decode one encoded persisted field payload strictly by semantic field kind.
pub(in crate::db) fn decode_structural_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Value, FieldDecodeError> {
    // Keep byte-backed `ByKind` leaves off the generic `ValueWire` bridge
    // whenever their persisted shape is fixed or already owned by the leaf
    // type.
    if let Some(value) = decode_scalar_fast_path_bytes(raw_bytes, kind)? {
        return Ok(value);
    }

    // Keep the root entrypoint as a thin lane router: scalar fast path above,
    // then non-recursive leaves, then the recursive composite authority.
    if let Some(value) = decode_leaf_field_by_kind_bytes(raw_bytes, kind)? {
        return Ok(value);
    }

    decode_composite_field_by_kind_bytes(raw_bytes, kind)
}

/// Validate one encoded persisted field payload strictly by semantic field
/// kind without eagerly building the final runtime `Value`.
pub(in crate::db) fn validate_structural_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<(), FieldDecodeError> {
    // Keep the validate-only entrypoint aligned with the existing decode lane
    // ordering so row-open validation and later materialization still share one
    // field-contract authority.
    if decode_scalar_fast_path_bytes(raw_bytes, kind)?.is_some() {
        return Ok(());
    }

    if decode_leaf_field_by_kind_bytes(raw_bytes, kind)?.is_some() {
        return Ok(());
    }

    validate_composite_field_by_kind_bytes(raw_bytes, kind)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        decode_relation_target_storage_keys_bytes, decode_structural_field_by_kind_bytes,
        decode_structural_value_storage_bytes,
    };
    use crate::{
        model::field::{FieldKind, RelationStrength},
        serialize::serialize,
        types::{Account, Decimal, EntityTag, Int128, Nat128, Principal, Subaccount, Ulid},
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

        let decoded =
            decode_structural_field_by_kind_bytes(&bytes, FieldKind::List(&FieldKind::Text))
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

        let decoded = decode_structural_field_by_kind_bytes(
            &bytes,
            FieldKind::Map {
                key: &FieldKind::Text,
                value: &FieldKind::Uint,
            },
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
    fn structural_field_decode_value_storage_handles_enum_payload() {
        let value = Value::Enum(
            ValueEnum::new("Active", Some("Status")).with_payload(Value::Map(vec![(
                Value::Text("count".into()),
                Value::Uint(7),
            )])),
        );
        let bytes = serde_cbor::to_vec(&value).expect("value bytes should encode");

        let decoded = decode_structural_value_storage_bytes(&bytes)
            .expect("value enum payload should decode");

        assert_eq!(decoded, value);
    }

    #[test]
    fn structural_field_decode_typed_wrappers_preserves_payloads() {
        let account = Account::from_parts(Principal::dummy(7), Some(Subaccount::from([7_u8; 32])));
        let decimal = Decimal::new(1234, 2);

        let account_bytes = serde_cbor::to_vec(&account).expect("account bytes should encode");
        let decimal_bytes = serde_cbor::to_vec(&decimal).expect("decimal bytes should encode");

        let decoded_account =
            decode_structural_field_by_kind_bytes(&account_bytes, FieldKind::Account)
                .expect("account payload should decode");
        let decoded_decimal =
            decode_structural_field_by_kind_bytes(&decimal_bytes, FieldKind::Decimal { scale: 2 })
                .expect("decimal payload should decode");

        assert_eq!(decoded_account, Value::Account(account));
        assert_eq!(decoded_decimal, Value::Decimal(decimal));
    }

    #[test]
    fn structural_field_decode_value_storage_roundtrips_nested_bytes_like_variants() {
        let nested = Value::from_map(vec![
            (
                Value::Text("blob".to_string()),
                Value::Blob(vec![0x10, 0x20, 0x30]),
            ),
            (
                Value::Text("i128".to_string()),
                Value::Int128(Int128::from(-123i128)),
            ),
            (
                Value::Text("u128".to_string()),
                Value::Uint128(Nat128::from(456u128)),
            ),
            (
                Value::Text("list".to_string()),
                Value::List(vec![
                    Value::Blob(vec![0xAA, 0xBB]),
                    Value::Int128(Int128::from(7i128)),
                    Value::Uint128(Nat128::from(8u128)),
                ]),
            ),
            (
                Value::Text("enum".to_string()),
                Value::Enum(
                    ValueEnum::new("Loaded", Some("tests::StructuredPayload"))
                        .with_payload(Value::Blob(vec![0xCC, 0xDD])),
                ),
            ),
        ])
        .expect("nested value payload should normalize");
        let bytes = serialize(&nested).expect("nested value payload should serialize");

        let decoded = decode_structural_value_storage_bytes(&bytes)
            .expect("nested value payload should decode through value storage");

        assert_eq!(decoded, nested);
    }
}
