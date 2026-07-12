use crate::{
    db::data::structural_field::{
        FieldDecodeError,
        binary::{TAG_BYTES, TAG_INT64, TAG_LIST, TAG_MAP, TAG_NULL, TAG_TEXT},
        value_storage::{
            decode::{
                ValueStorageView, decode_structural_value_storage_bytes,
                decode_value_storage_list_item_slices, decode_value_storage_map_entry_slices,
                validate_structural_value_storage_bytes,
            },
            encode_structural_value_storage_bytes,
            tags::VALUE_BINARY_TAG_ULID,
        },
    },
    value::Value,
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

fn push_blob_value(out: &mut Vec<u8>, value: &[u8]) {
    push_len_prefixed_head(
        out,
        TAG_BYTES,
        u32::try_from(value.len()).expect("test blob length fits"),
    );
    out.extend_from_slice(value);
}

fn push_i64_value(out: &mut Vec<u8>, value: i64) {
    out.push(TAG_INT64);
    out.extend_from_slice(&value.to_be_bytes());
}

fn decode_value_storage_value(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    decode_structural_value_storage_bytes(raw_bytes)
}

#[test]
fn binary_value_storage_rejects_truncated_list_root() {
    assert!(decode_value_storage_value(&[TAG_LIST]).is_err());
}

#[test]
fn binary_value_storage_rejects_truncated_map_root() {
    assert!(decode_value_storage_value(&[TAG_MAP]).is_err());
}

#[test]
fn binary_value_storage_rejects_trailing_bytes() {
    let mut encoded = encode_structural_value_storage_bytes(&Value::Text("alpha".to_string()))
        .expect("binary value bytes should encode");
    encoded.push(0xFF);

    assert!(decode_value_storage_value(&encoded).is_err());
}

#[test]
fn binary_value_storage_rejects_trailing_bytes_after_list_root() {
    let mut encoded = Vec::new();
    push_len_prefixed_head(&mut encoded, TAG_LIST, 0);
    encoded.push(TAG_NULL);

    assert!(decode_value_storage_value(&encoded).is_err());
}

#[test]
fn binary_value_storage_rejects_trailing_bytes_after_map_root() {
    let mut encoded = Vec::new();
    push_len_prefixed_head(&mut encoded, TAG_MAP, 0);
    encoded.push(TAG_NULL);

    assert!(decode_value_storage_value(&encoded).is_err());
}

#[test]
fn binary_value_storage_rejects_truncated_nested_list_item() {
    let mut encoded = Vec::new();
    push_len_prefixed_head(&mut encoded, TAG_LIST, 1);
    encoded.push(TAG_LIST);

    assert!(decode_value_storage_value(&encoded).is_err());
}

#[test]
fn binary_value_storage_rejects_unvalidated_declared_list_count_without_large_reserve() {
    let mut encoded = Vec::new();
    push_len_prefixed_head(&mut encoded, TAG_LIST, u32::MAX);

    assert!(decode_value_storage_value(&encoded).is_err());
    assert!(decode_value_storage_list_item_slices(&encoded).is_err());
}

#[test]
fn binary_value_storage_rejects_excessive_nested_list_depth() {
    let mut encoded = vec![TAG_NULL];
    for _ in 0..70 {
        let mut outer = Vec::new();
        push_len_prefixed_head(&mut outer, TAG_LIST, 1);
        outer.extend_from_slice(&encoded);
        encoded = outer;
    }

    assert!(validate_structural_value_storage_bytes(&encoded).is_err());
    assert!(decode_value_storage_value(&encoded).is_err());
}

#[test]
fn binary_value_storage_rejects_truncated_nested_map_value() {
    let mut encoded = Vec::new();
    push_len_prefixed_head(&mut encoded, TAG_MAP, 1);
    push_text_value(&mut encoded, "key");
    push_len_prefixed_head(&mut encoded, TAG_TEXT, 4);
    encoded.push(b'a');

    assert!(decode_value_storage_value(&encoded).is_err());
}

#[test]
fn binary_value_storage_rejects_invalid_nested_local_value_tag() {
    let mut encoded = Vec::new();
    push_len_prefixed_head(&mut encoded, TAG_LIST, 1);
    encoded.push(VALUE_BINARY_TAG_ULID);
    encoded.push(0xFF);

    assert!(decode_value_storage_value(&encoded).is_err());
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

    let items = decode_value_storage_list_item_slices(&encoded).expect("list items should split");

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

    let entries =
        decode_value_storage_map_entry_slices(&encoded).expect("map entries should split");

    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0], (first_key.as_slice(), first_value.as_slice()));
    assert_eq!(entries[1], (second_key.as_slice(), second_value.as_slice()));
}

#[test]
fn binary_value_storage_view_resolves_text_keyed_map_child_without_materializing() {
    let value = Value::Map(vec![
        (
            Value::Text("name".to_string()),
            Value::Text("Ada".to_string()),
        ),
        (Value::Text("rank".to_string()), Value::Int64(7)),
    ]);
    let encoded =
        encode_structural_value_storage_bytes(&value).expect("map value bytes should encode");

    let view = ValueStorageView::from_raw_validated(&encoded).expect("map view should validate");
    let name = view
        .map_text_key_bytes(b"name")
        .expect("text-key lookup should walk map")
        .expect("name entry should exist");
    let rank = view
        .map_text_key_bytes(b"rank")
        .expect("text-key lookup should walk map")
        .expect("rank entry should exist");
    let missing = view
        .map_text_key_bytes(b"missing")
        .expect("text-key lookup should walk map");

    assert_eq!(name.as_text().expect("name should be text"), "Ada");
    assert_eq!(rank.as_i64().expect("rank should be i64"), 7);
    assert!(missing.is_none());
}

#[test]
fn binary_value_storage_view_borrows_blob_payload_without_materializing() {
    let bytes = [0x10, 0x20, 0x30, 0x40];
    let mut encoded = Vec::new();
    push_blob_value(&mut encoded, bytes.as_slice());

    let view = ValueStorageView::from_raw_validated(&encoded).expect("blob view should validate");

    assert!(view.is_blob());
    assert_eq!(view.as_blob().expect("blob payload should borrow"), bytes);
}

#[test]
fn binary_value_storage_list_split_rejects_trailing_bytes() {
    let mut encoded = Vec::new();
    push_len_prefixed_head(&mut encoded, TAG_LIST, 0);
    encoded.push(TAG_NULL);

    assert!(decode_value_storage_list_item_slices(&encoded).is_err());
}

#[test]
fn binary_value_storage_map_split_rejects_trailing_bytes() {
    let mut encoded = Vec::new();
    push_len_prefixed_head(&mut encoded, TAG_MAP, 0);
    encoded.push(TAG_NULL);

    assert!(decode_value_storage_map_entry_slices(&encoded).is_err());
}

#[test]
fn binary_value_storage_list_split_rejects_malformed_nested_item() {
    let mut encoded = Vec::new();
    push_len_prefixed_head(&mut encoded, TAG_LIST, 1);
    encoded.push(TAG_LIST);

    assert!(decode_value_storage_list_item_slices(&encoded).is_err());
}

#[test]
fn binary_value_storage_preserves_duplicate_map_key_rejection() {
    let mut encoded = Vec::new();
    push_len_prefixed_head(&mut encoded, TAG_MAP, 2);
    push_text_value(&mut encoded, "key");
    push_i64_value(&mut encoded, 1);
    push_text_value(&mut encoded, "key");
    push_i64_value(&mut encoded, 2);

    assert!(decode_value_storage_value(&encoded).is_err());
}

#[test]
fn binary_value_storage_validate_uses_skip_shape_only() {
    let mut encoded = Vec::new();
    push_len_prefixed_head(&mut encoded, TAG_MAP, 2);
    push_text_value(&mut encoded, "key");
    push_i64_value(&mut encoded, 1);
    push_text_value(&mut encoded, "key");
    push_i64_value(&mut encoded, 2);

    validate_structural_value_storage_bytes(&encoded)
        .expect("structural duplicate-key bytes should validate without value decode");
}

#[test]
fn binary_value_storage_validate_rejects_trailing_bytes() {
    let mut encoded = encode_structural_value_storage_bytes(&Value::Text("alpha".to_string()))
        .expect("binary value bytes should encode");
    encoded.push(0xFF);

    assert!(validate_structural_value_storage_bytes(&encoded).is_err());
}

#[test]
fn binary_value_storage_validate_rejects_truncated_payload() {
    assert!(validate_structural_value_storage_bytes(&[]).is_err());
}
