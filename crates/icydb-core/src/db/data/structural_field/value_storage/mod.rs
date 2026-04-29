//! Module: data::structural_field::value_storage
//! Responsibility: owner-local binary `Value` envelope encode and decode.
//! Does not own: top-level `ByKind` dispatch, typed wrapper payload definitions, or storage-key policy.
//! Boundary: `FieldStorageDecode::Value` routes through this module without widening authority over sibling structural lanes.

mod decode;
mod encode;
mod primitives;
mod skip;
mod tags;
mod walk;

use crate::value::Value;

pub(in crate::db) use decode::{
    ValueStorageView, decode_account, decode_decimal, decode_enum, decode_int, decode_int128,
    decode_list_item, decode_map_entry, decode_nat, decode_nat128,
    decode_structural_value_storage_blob_bytes, decode_structural_value_storage_bool_bytes,
    decode_structural_value_storage_bytes, decode_structural_value_storage_date_bytes,
    decode_structural_value_storage_duration_bytes, decode_structural_value_storage_float32_bytes,
    decode_structural_value_storage_float64_bytes, decode_structural_value_storage_i64_bytes,
    decode_structural_value_storage_principal_bytes,
    decode_structural_value_storage_subaccount_bytes,
    decode_structural_value_storage_timestamp_bytes, decode_structural_value_storage_u64_bytes,
    decode_structural_value_storage_ulid_bytes, decode_structural_value_storage_unit_bytes,
    decode_text, structural_value_storage_bytes_are_null, validate_structural_value_storage_bytes,
};
pub(in crate::db) use encode::{
    encode_account, encode_decimal, encode_enum, encode_int, encode_int128, encode_list_item,
    encode_map_entry, encode_nat, encode_nat128, encode_owned_list_item, encode_owned_map_entry,
    encode_structural_value_storage_blob_bytes, encode_structural_value_storage_bool_bytes,
    encode_structural_value_storage_bytes, encode_structural_value_storage_date_bytes,
    encode_structural_value_storage_duration_bytes, encode_structural_value_storage_float32_bytes,
    encode_structural_value_storage_float64_bytes, encode_structural_value_storage_i64_bytes,
    encode_structural_value_storage_null_bytes, encode_structural_value_storage_principal_bytes,
    encode_structural_value_storage_subaccount_bytes,
    encode_structural_value_storage_timestamp_bytes, encode_structural_value_storage_u64_bytes,
    encode_structural_value_storage_ulid_bytes, encode_structural_value_storage_unit_bytes,
    encode_text,
};

// Normalize decoded map entries in place when they satisfy the runtime map
// invariants, but preserve the original decoded order when validation rejects
// the shape. This keeps current semantics without cloning the whole entry list.
pub(super) fn normalize_map_entries_or_preserve(mut entries: Vec<(Value, Value)>) -> Value {
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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::data::structural_field::{
            FieldDecodeError,
            binary::{TAG_INT64, TAG_LIST, TAG_MAP, TAG_NULL, TAG_TEXT},
            value_storage::{
                decode::{
                    ValueStorageSlice, ValueStorageView, decode_list_item, decode_map_entry,
                    decode_structural_value_storage_binary_bytes,
                    validate_structural_value_storage_binary_bytes,
                },
                encode::encode_structural_value_storage_binary_bytes,
                tags::{VALUE_BINARY_TAG_ENUM, VALUE_BINARY_TAG_ULID},
            },
        },
        types::{Account, Decimal, Float32, Float64, Principal, Subaccount, Timestamp, Ulid},
        value::{Value, ValueEnum},
    };

    fn push_len_prefixed_head(out: &mut Vec<u8>, tag: u8, len: u32) {
        out.push(tag);
        out.extend_from_slice(&len.to_be_bytes());
    }

    fn push_text_value(out: &mut Vec<u8>, value: &str) {
        push_len_prefixed_head(
            out,
            TAG_TEXT,
            u32::try_from(value.len()).expect("test text length fits"),
        );
        out.extend_from_slice(value.as_bytes());
    }

    fn push_i64_value(out: &mut Vec<u8>, value: i64) {
        out.push(TAG_INT64);
        out.extend_from_slice(&value.to_be_bytes());
    }

    fn decode_binary_value(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
        let slice = ValueStorageSlice::from_raw(raw_bytes)?;

        decode_structural_value_storage_binary_bytes(slice)
    }

    #[test]
    fn binary_value_storage_roundtrips_nested_variants() {
        let value = Value::Map(vec![
            (
                Value::Text("account".to_string()),
                Value::Account(Account::new(
                    Principal::from_slice(&[1, 2, 3]),
                    Some([7u8; 32]),
                )),
            ),
            (
                Value::Text("enum".to_string()),
                Value::Enum(
                    ValueEnum::new("Spell", Some("Demo/Spell")).with_payload(Value::List(vec![
                        Value::Decimal(Decimal::from_i128_with_scale(12345, 2)),
                        Value::Timestamp(Timestamp::from_millis(1_710_013_530_123)),
                        Value::Ulid(Ulid::from_u128(77)),
                    ])),
                ),
            ),
            (
                Value::Text("floats".to_string()),
                Value::List(vec![
                    Value::Float32(Float32::try_new(3.5).expect("finite f32")),
                    Value::Float64(Float64::try_new(9.25).expect("finite f64")),
                    Value::Subaccount(Subaccount::from_array([9u8; 32])),
                ]),
            ),
        ]);

        let encoded = encode_structural_value_storage_binary_bytes(&value)
            .expect("binary value bytes should encode");
        let decoded = decode_binary_value(&encoded).expect("binary value bytes should decode");

        assert_eq!(decoded, value);
    }

    #[test]
    fn binary_value_storage_uses_local_tags_for_ambiguous_variants() {
        let ulid = Value::Ulid(Ulid::from_u128(99));
        let enum_value = Value::Enum(ValueEnum::loose("Loose"));

        let ulid_bytes = encode_structural_value_storage_binary_bytes(&ulid)
            .expect("ulid value bytes should encode");
        let enum_bytes = encode_structural_value_storage_binary_bytes(&enum_value)
            .expect("enum value bytes should encode");

        assert_eq!(ulid_bytes[0], VALUE_BINARY_TAG_ULID);
        assert_eq!(enum_bytes[0], VALUE_BINARY_TAG_ENUM);
        assert_eq!(enum_bytes[1], TAG_LIST);
    }

    #[test]
    fn binary_value_storage_rejects_truncated_list_root_with_exact_error() {
        let err =
            decode_binary_value(&[TAG_LIST]).expect_err("truncated list root must be rejected");

        assert_eq!(
            err.to_string(),
            "structural binary: truncated length prefix"
        );
    }

    #[test]
    fn binary_value_storage_rejects_truncated_map_root_with_exact_error() {
        let err = decode_binary_value(&[TAG_MAP]).expect_err("truncated map root must be rejected");

        assert_eq!(
            err.to_string(),
            "structural binary: truncated length prefix"
        );
    }

    #[test]
    fn binary_value_storage_rejects_trailing_bytes() {
        let mut encoded =
            encode_structural_value_storage_binary_bytes(&Value::Text("alpha".to_string()))
                .expect("binary value bytes should encode");
        encoded.push(0xFF);

        let err = decode_binary_value(&encoded).expect_err("trailing bytes must be rejected");
        assert!(
            err.to_string().contains("trailing bytes"),
            "expected trailing-byte error, got: {err}",
        );
    }

    #[test]
    fn binary_value_storage_rejects_trailing_bytes_after_list_root() {
        let mut encoded = Vec::new();
        push_len_prefixed_head(&mut encoded, TAG_LIST, 0);
        encoded.push(TAG_NULL);

        let err = decode_binary_value(&encoded).expect_err("trailing list bytes must be rejected");

        assert_eq!(
            err.to_string(),
            "structural binary: trailing bytes after value payload"
        );
    }

    #[test]
    fn binary_value_storage_rejects_trailing_bytes_after_map_root() {
        let mut encoded = Vec::new();
        push_len_prefixed_head(&mut encoded, TAG_MAP, 0);
        encoded.push(TAG_NULL);

        let err = decode_binary_value(&encoded).expect_err("trailing map bytes must be rejected");

        assert_eq!(
            err.to_string(),
            "structural binary: trailing bytes after value payload"
        );
    }

    #[test]
    fn binary_value_storage_rejects_truncated_nested_list_item() {
        let mut encoded = Vec::new();
        push_len_prefixed_head(&mut encoded, TAG_LIST, 1);
        encoded.push(TAG_LIST);

        let err =
            decode_binary_value(&encoded).expect_err("truncated nested list must be rejected");

        assert_eq!(
            err.to_string(),
            "structural binary: truncated length prefix"
        );
    }

    #[test]
    fn binary_value_storage_rejects_truncated_nested_map_value() {
        let mut encoded = Vec::new();
        push_len_prefixed_head(&mut encoded, TAG_MAP, 1);
        push_text_value(&mut encoded, "key");
        push_len_prefixed_head(&mut encoded, TAG_TEXT, 4);
        encoded.push(b'a');

        let err =
            decode_binary_value(&encoded).expect_err("truncated nested map value must be rejected");

        assert_eq!(
            err.to_string(),
            "structural binary: truncated scalar payload"
        );
    }

    #[test]
    fn binary_value_storage_rejects_invalid_nested_local_value_tag() {
        let mut encoded = Vec::new();
        push_len_prefixed_head(&mut encoded, TAG_LIST, 1);
        encoded.push(VALUE_BINARY_TAG_ULID);
        encoded.push(0xFF);

        let err = decode_binary_value(&encoded)
            .expect_err("invalid nested local value tag must be rejected");

        assert_eq!(
            err.to_string(),
            "structural binary: unsupported value tag 0xFF"
        );
    }

    #[test]
    fn binary_value_storage_list_split_preserves_borrowed_item_slices() {
        let mut first = Vec::new();
        push_text_value(&mut first, "first");
        let mut second = Vec::new();
        push_i64_value(&mut second, 2);
        let third = vec![TAG_NULL];

        let mut encoded = Vec::new();
        push_len_prefixed_head(&mut encoded, TAG_LIST, 3);
        encoded.extend_from_slice(&first);
        encoded.extend_from_slice(&second);
        encoded.extend_from_slice(&third);

        let items = decode_list_item(&encoded).expect("list items should split");

        assert_eq!(items.len(), 3);
        assert_eq!(items[0], first.as_slice());
        assert_eq!(items[1], second.as_slice());
        assert_eq!(items[2], third.as_slice());
    }

    #[test]
    fn binary_value_storage_map_split_preserves_borrowed_entry_slices() {
        let mut first_key = Vec::new();
        push_text_value(&mut first_key, "first");
        let mut first_value = Vec::new();
        push_i64_value(&mut first_value, 1);
        let mut second_key = Vec::new();
        push_text_value(&mut second_key, "second");
        let second_value = vec![TAG_NULL];

        let mut encoded = Vec::new();
        push_len_prefixed_head(&mut encoded, TAG_MAP, 2);
        encoded.extend_from_slice(&first_key);
        encoded.extend_from_slice(&first_value);
        encoded.extend_from_slice(&second_key);
        encoded.extend_from_slice(&second_value);

        let entries = decode_map_entry(&encoded).expect("map entries should split");

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], (first_key.as_slice(), first_value.as_slice()));
        assert_eq!(entries[1], (second_key.as_slice(), second_value.as_slice()));
    }

    #[test]
    fn binary_value_storage_view_visits_list_items_without_materializing() {
        let mut first = Vec::new();
        push_text_value(&mut first, "first");
        let mut second = Vec::new();
        push_i64_value(&mut second, 2);
        let third = vec![TAG_NULL];

        let mut encoded = Vec::new();
        push_len_prefixed_head(&mut encoded, TAG_LIST, 3);
        encoded.extend_from_slice(&first);
        encoded.extend_from_slice(&second);
        encoded.extend_from_slice(&third);

        let view = ValueStorageView::from_raw(&encoded).expect("list view should validate");
        let mut items = Vec::new();
        view.visit_list_items(|item| {
            items.push(item);
            Ok(())
        })
        .expect("list view should visit borrowed item slices");

        assert_eq!(
            items,
            vec![first.as_slice(), second.as_slice(), third.as_slice()]
        );
    }

    #[test]
    fn binary_value_storage_view_visits_map_entries_without_materializing() {
        let mut first_key = Vec::new();
        push_text_value(&mut first_key, "first");
        let mut first_value = Vec::new();
        push_i64_value(&mut first_value, 1);
        let mut second_key = Vec::new();
        push_text_value(&mut second_key, "second");
        let second_value = vec![TAG_NULL];

        let mut encoded = Vec::new();
        push_len_prefixed_head(&mut encoded, TAG_MAP, 2);
        encoded.extend_from_slice(&first_key);
        encoded.extend_from_slice(&first_value);
        encoded.extend_from_slice(&second_key);
        encoded.extend_from_slice(&second_value);

        let view = ValueStorageView::from_raw(&encoded).expect("map view should validate");
        let mut entries = Vec::new();
        view.visit_map_entries(|key, value| {
            entries.push((key, value));
            Ok(())
        })
        .expect("map view should visit borrowed entry slices");

        assert_eq!(
            entries,
            vec![
                (first_key.as_slice(), first_value.as_slice()),
                (second_key.as_slice(), second_value.as_slice()),
            ]
        );
    }

    #[test]
    fn binary_value_storage_view_resolves_text_keyed_map_child_without_materializing() {
        let value = Value::Map(vec![
            (
                Value::Text("name".to_string()),
                Value::Text("Ada".to_string()),
            ),
            (Value::Text("rank".to_string()), Value::Int(7)),
        ]);
        let encoded = encode_structural_value_storage_binary_bytes(&value)
            .expect("map value bytes should encode");

        let view = ValueStorageView::from_raw(&encoded).expect("map view should validate");
        let name = view
            .map_text_key("name")
            .expect("text-key lookup should walk map")
            .expect("name entry should exist");
        let rank = view
            .map_text_key("rank")
            .expect("text-key lookup should walk map")
            .expect("rank entry should exist");
        let missing = view
            .map_text_key("missing")
            .expect("text-key lookup should walk map");

        assert_eq!(name.as_text().expect("name should be text"), "Ada");
        assert_eq!(rank.as_i64().expect("rank should be i64"), 7);
        assert!(missing.is_none());
    }

    #[test]
    fn binary_value_storage_list_split_rejects_trailing_bytes() {
        let mut encoded = Vec::new();
        push_len_prefixed_head(&mut encoded, TAG_LIST, 0);
        encoded.push(TAG_NULL);

        let err = decode_list_item(&encoded).expect_err("trailing list bytes must be rejected");

        assert_eq!(
            err.to_string(),
            "structural binary: trailing bytes after value list payload"
        );
    }

    #[test]
    fn binary_value_storage_map_split_rejects_trailing_bytes() {
        let mut encoded = Vec::new();
        push_len_prefixed_head(&mut encoded, TAG_MAP, 0);
        encoded.push(TAG_NULL);

        let err = decode_map_entry(&encoded).expect_err("trailing map bytes must be rejected");

        assert_eq!(
            err.to_string(),
            "structural binary: trailing bytes after value map payload"
        );
    }

    #[test]
    fn binary_value_storage_list_split_rejects_malformed_nested_item() {
        let mut encoded = Vec::new();
        push_len_prefixed_head(&mut encoded, TAG_LIST, 1);
        encoded.push(TAG_LIST);

        let err = decode_list_item(&encoded).expect_err("malformed nested item must be rejected");

        assert_eq!(
            err.to_string(),
            "structural binary: truncated length prefix"
        );
    }

    #[test]
    fn binary_value_storage_preserves_duplicate_map_key_rejection() {
        let mut encoded = Vec::new();
        push_len_prefixed_head(&mut encoded, TAG_MAP, 2);
        push_text_value(&mut encoded, "key");
        push_i64_value(&mut encoded, 1);
        push_text_value(&mut encoded, "key");
        push_i64_value(&mut encoded, 2);

        let err = decode_binary_value(&encoded).expect_err("duplicate map key must be rejected");

        assert_eq!(
            err.to_string(),
            "structural binary: map contains duplicate keys at normalized positions 0 and 1"
        );
    }

    #[test]
    fn binary_value_storage_validate_uses_skip_shape_only() {
        let mut encoded = Vec::new();
        push_len_prefixed_head(&mut encoded, TAG_MAP, 2);
        push_text_value(&mut encoded, "key");
        push_i64_value(&mut encoded, 1);
        push_text_value(&mut encoded, "key");
        push_i64_value(&mut encoded, 2);

        validate_structural_value_storage_binary_bytes(&encoded)
            .expect("structural duplicate-key bytes should validate without value decode");
    }

    #[test]
    fn binary_value_storage_validate_rejects_trailing_bytes() {
        let mut encoded =
            encode_structural_value_storage_binary_bytes(&Value::Text("alpha".to_string()))
                .expect("binary value bytes should encode");
        encoded.push(0xFF);

        let err = validate_structural_value_storage_binary_bytes(&encoded)
            .expect_err("trailing bytes must be rejected");

        assert_eq!(
            err.to_string(),
            "structural binary: trailing bytes after value payload"
        );
    }

    #[test]
    fn binary_value_storage_validate_rejects_truncated_payload() {
        let err = validate_structural_value_storage_binary_bytes(&[])
            .expect_err("empty value bytes must be rejected");

        assert_eq!(
            err.to_string(),
            "structural binary: truncated value payload"
        );
    }

    #[test]
    fn binary_value_storage_validate_accepts_nested_value_storage_tags() {
        let value = Value::Enum(
            ValueEnum::new("Arc", Some("Spell/Arc")).with_payload(Value::Ulid(Ulid::from_u128(5))),
        );
        let encoded = encode_structural_value_storage_binary_bytes(&value)
            .expect("binary value bytes should encode");

        validate_structural_value_storage_binary_bytes(&encoded)
            .expect("binary value bytes should validate");
    }
}
