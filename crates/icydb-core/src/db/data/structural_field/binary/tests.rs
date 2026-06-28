use super::{
    TAG_FALSE, TAG_INT64, TAG_LIST, TAG_MAP, TAG_NAT64, TAG_NULL, TAG_TEXT, TAG_TRUE,
    TAG_VARIANT_PAYLOAD, TAG_VARIANT_UNIT, parse_binary_head, push_binary_bool, skip_binary_value,
    split_binary_variant_payload, walk_binary_list_items, walk_binary_map_entries,
};

type ListState = Vec<Vec<u8>>;
type MapState = Vec<(Vec<u8>, Vec<u8>)>;

fn encode_null() -> Vec<u8> {
    vec![TAG_NULL]
}

fn encode_bool(value: bool) -> Vec<u8> {
    vec![if value { TAG_TRUE } else { TAG_FALSE }]
}

fn encode_nat64(value: u64) -> Vec<u8> {
    let mut out = vec![TAG_NAT64];
    out.extend_from_slice(&value.to_be_bytes());
    out
}

fn encode_int64(value: i64) -> Vec<u8> {
    let mut out = vec![TAG_INT64];
    out.extend_from_slice(&value.to_be_bytes());
    out
}

fn encode_text(value: &str) -> Vec<u8> {
    let mut out = vec![TAG_TEXT];
    out.extend_from_slice(
        &u32::try_from(value.len())
            .expect("text len fits u32")
            .to_be_bytes(),
    );
    out.extend_from_slice(value.as_bytes());
    out
}

fn encode_list(items: &[Vec<u8>]) -> Vec<u8> {
    let mut out = vec![TAG_LIST];
    out.extend_from_slice(
        &u32::try_from(items.len())
            .expect("item count fits u32")
            .to_be_bytes(),
    );
    for item in items {
        out.extend_from_slice(item);
    }
    out
}

fn encode_map(entries: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    let mut out = vec![TAG_MAP];
    out.extend_from_slice(
        &u32::try_from(entries.len())
            .expect("entry count fits u32")
            .to_be_bytes(),
    );
    for (key, value) in entries {
        out.extend_from_slice(key);
        out.extend_from_slice(value);
    }
    out
}

fn encode_variant_unit(label: &str) -> Vec<u8> {
    let mut out = vec![TAG_VARIANT_UNIT];
    out.extend_from_slice(
        &u32::try_from(label.len())
            .expect("label len fits u32")
            .to_be_bytes(),
    );
    out.extend_from_slice(label.as_bytes());
    out
}

fn encode_variant_payload(label: &str, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![TAG_VARIANT_PAYLOAD];
    out.extend_from_slice(
        &u32::try_from(label.len())
            .expect("label len fits u32")
            .to_be_bytes(),
    );
    out.extend_from_slice(label.as_bytes());
    out.extend_from_slice(payload);
    out
}

#[test]
fn parse_binary_head_reports_tag_len_and_payload_offset() {
    let bytes = encode_text("icy");
    let head = parse_binary_head(&bytes, 0)
        .expect("head parse should succeed")
        .expect("text head should exist");

    assert_eq!(head.0, TAG_TEXT);
    assert_eq!(head.1, 3);
    assert_eq!(head.2, 5);
}

#[test]
fn push_binary_bool_emits_tag_only_bool_form() {
    let mut bytes = Vec::new();
    push_binary_bool(&mut bytes, true);

    assert_eq!(bytes, encode_bool(true));
}

#[test]
fn skip_binary_value_skips_nested_list_payloads() {
    let bytes = encode_list(&[
        encode_text("left"),
        encode_list(&[encode_nat64(7), encode_bool(true)]),
        encode_int64(-5),
    ]);

    assert_eq!(
        skip_binary_value(&bytes, 0).expect("list skip should succeed"),
        bytes.len(),
    );
}

#[test]
fn walk_binary_list_items_yields_raw_item_slices() {
    let left = encode_text("left");
    let right = encode_nat64(9);
    let bytes = encode_list(&[left.clone(), right.clone()]);
    let mut state: ListState = Vec::new();

    walk_binary_list_items(&bytes, &mut |item_bytes| {
        state.push(item_bytes.to_vec());

        Ok(())
    })
    .expect("list walk should succeed");

    assert_eq!(state, vec![left, right]);
}

#[test]
fn walk_binary_map_entries_yields_raw_entry_slices() {
    let left_key = encode_text("left");
    let left_value = encode_nat64(1);
    let right_key = encode_text("right");
    let right_value = encode_nat64(2);
    let bytes = encode_map(&[
        (left_key.clone(), left_value.clone()),
        (right_key.clone(), right_value.clone()),
    ]);
    let mut state: MapState = Vec::new();

    walk_binary_map_entries(&bytes, &mut |key_bytes, value_bytes| {
        state.push((key_bytes.to_vec(), value_bytes.to_vec()));

        Ok(())
    })
    .expect("map walk should succeed");

    assert_eq!(
        state,
        vec![(left_key, left_value), (right_key, right_value)],
    );
}

#[test]
fn split_binary_variant_payload_handles_unit_and_payload_variants() {
    let unit = encode_variant_unit("Loaded");
    let payload_value = encode_nat64(7);
    let payload = encode_variant_payload("Loaded", &payload_value);

    let (unit_label, unit_payload) =
        split_binary_variant_payload(&unit).expect("unit variant split should succeed");
    let (payload_label, payload_payload) =
        split_binary_variant_payload(&payload).expect("payload variant split should succeed");

    assert_eq!(unit_label, b"Loaded");
    assert!(unit_payload.is_none());
    assert_eq!(payload_label, b"Loaded");
    assert_eq!(payload_payload, Some(payload_value.as_slice()));
}

#[test]
fn split_binary_variant_payload_rejects_trailing_bytes() {
    let mut bytes = encode_variant_unit("Loaded");
    bytes.extend_from_slice(&encode_null());

    assert!(split_binary_variant_payload(&bytes).is_err());
}
