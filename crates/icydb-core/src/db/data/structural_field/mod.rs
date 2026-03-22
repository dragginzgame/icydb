//! Module: data::structural_field
//! Responsibility: canonical persisted-field structural decode helpers.
//! Does not own: row layout planning, typed entity reconstruction, or query semantics.
//! Boundary: runtime paths use this module when they need one persisted field decoded without `E`.

mod cbor;
mod kind;
mod leaf;
mod scalar;
mod storage_key;
mod value_storage;

use crate::{model::field::FieldKind, value::Value};
use thiserror::Error as ThisError;

use kind::{decode_enum_bytes, decode_list_bytes, decode_map_bytes};
use leaf::{
    decode_account_value_bytes, decode_date_value_bytes, decode_decimal_value_bytes,
    decode_duration_value_bytes, decode_int_big_value_bytes, decode_principal_value_bytes,
    decode_subaccount_value_bytes, decode_timestamp_value_bytes, decode_uint_big_value_bytes,
    decode_unit_value_bytes,
};
use scalar::decode_scalar_fast_path_bytes;

pub(in crate::db) use storage_key::{
    decode_relation_target_storage_keys_bytes, decode_storage_key_field_bytes,
};
pub(in crate::db) use value_storage::decode_structural_value_storage_bytes;

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
    storage_decode: crate::model::field::FieldStorageDecode,
) -> Result<Value, StructuralFieldDecodeError> {
    match storage_decode {
        crate::model::field::FieldStorageDecode::ByKind => {
            decode_structural_field_by_kind_bytes(raw_bytes, kind)
        }
        crate::model::field::FieldStorageDecode::Value => {
            decode_structural_value_storage_bytes(raw_bytes)
        }
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
