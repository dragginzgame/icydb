use super::{
    CompleteBinaryValue, TAG_FALSE, TAG_INT64, TAG_LIST, TAG_MAP, TAG_NAT64, TAG_TEXT, TAG_TRUE,
    parse_binary_head, push_binary_bool, skip_binary_value, walk_binary_list_items,
    walk_binary_map_entries,
};

type ListState = Vec<Vec<u8>>;
type MapState = Vec<(Vec<u8>, Vec<u8>)>;

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
fn complete_binary_value_rejects_empty_truncated_and_trailing_bytes() {
    let bytes = encode_text("icy");
    let value = CompleteBinaryValue::parse(&bytes).expect("complete text should parse");
    assert_eq!(value.tag(), TAG_TEXT);
    assert_eq!(value.len(), 3);
    assert_eq!(value.payload_offset(), 5);
    assert_eq!(
        value.scalar_payload().expect("text payload should decode"),
        b"icy",
    );

    assert!(CompleteBinaryValue::parse(&[]).is_err());
    assert!(CompleteBinaryValue::parse(&bytes[..bytes.len() - 1]).is_err());

    let mut trailing = bytes;
    trailing.push(TAG_TRUE);
    assert!(CompleteBinaryValue::parse(&trailing).is_err());
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
fn big_integer_binary_magnitudes_preserve_bits_signs_and_exact_sizes() {
    use super::{
        TAG_BYTES, decode_binary_int_big_payload, decode_binary_nat_big_payload,
        push_binary_int_big_payload, push_binary_nat_big_payload,
    };
    use num_bigint::{BigInt, BigUint, Sign};

    for bits in [
        0_usize, 1, 7, 8, 9, 31, 32, 33, 63, 64, 65, 255, 256, 257, 1024,
    ] {
        let magnitude = (BigUint::from(1_u32) << bits) - BigUint::from(1_u32);
        let bytes = if bits == 0 {
            Vec::new()
        } else {
            magnitude.to_bytes_le()
        };
        let mut encoded = Vec::new();
        push_binary_nat_big_payload(&mut encoded, magnitude.iter_u32_digits());
        assert_eq!(encoded[0], TAG_BYTES);
        assert_eq!(encoded.len(), 5 + bytes.len());
        assert_eq!(&encoded[5..], bytes);
        assert_eq!(decode_binary_nat_big_payload(&encoded).unwrap(), magnitude);
        for negative in [false, true] {
            encoded.clear();
            push_binary_int_big_payload(&mut encoded, negative, magnitude.iter_u32_digits());
            assert_eq!(encoded.len(), 6 + bytes.len());
            assert_eq!(
                encoded[5],
                if bits == 0 {
                    0
                } else if negative {
                    2
                } else {
                    1
                }
            );
            assert_eq!(&encoded[6..], bytes);
            let sign = if negative { Sign::Minus } else { Sign::Plus };
            assert_eq!(
                decode_binary_int_big_payload(&encoded).unwrap(),
                BigInt::from_biguint(sign, magnitude.clone())
            );
        }
    }
    // Nonuniform bytes check order across both byte and limb boundaries.
    let magnitude = BigUint::from_bytes_le(&[0, 1, 2, 3, 0, 4, 5]);
    let mut encoded = Vec::new();
    push_binary_nat_big_payload(&mut encoded, magnitude.iter_u32_digits());
    assert_eq!(&encoded[5..], &[0, 1, 2, 3, 0, 4, 5]);
    assert_eq!(decode_binary_nat_big_payload(&encoded).unwrap(), magnitude);
}

#[test]
fn big_integer_binary_payloads_reject_noncanonical_and_malformed_bytes() {
    use super::{
        TAG_BYTES, decode_binary_int_big_payload, decode_binary_nat_big_payload, push_binary_bytes,
    };

    for payload in [
        &[][..],
        &[0, 1],
        &[1],
        &[2],
        &[3, 1],
        &[255, 1],
        &[1, 0],
        &[2, 1, 0],
        &[0, 0],
    ] {
        let mut encoded = Vec::new();
        push_binary_bytes(&mut encoded, payload);
        assert!(decode_binary_int_big_payload(&encoded).is_err());
    }
    for payload in [&[0][..], &[1, 0], &[0, 0]] {
        let mut encoded = Vec::new();
        push_binary_bytes(&mut encoded, payload);
        assert!(decode_binary_nat_big_payload(&encoded).is_err());
    }
    // This is a complete canonical value in both maintained byte grammars.
    let mut encoded = Vec::new();
    push_binary_bytes(&mut encoded, &[1, 42]);
    for end in 0..encoded.len() {
        assert!(decode_binary_int_big_payload(&encoded[..end]).is_err());
        assert!(decode_binary_nat_big_payload(&encoded[..end]).is_err());
    }
    encoded.push(7);
    assert!(decode_binary_int_big_payload(&encoded).is_err());
    assert!(decode_binary_nat_big_payload(&encoded).is_err());
    let deceptive = [TAG_BYTES, 255, 255, 255, 255];
    assert!(decode_binary_int_big_payload(&deceptive).is_err());
    assert!(decode_binary_nat_big_payload(&deceptive).is_err());
}
