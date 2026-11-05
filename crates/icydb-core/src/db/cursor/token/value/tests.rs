use super::*;

// Build the input and expected current wire independently of the codec.
// kind 3 alternates list, map-value and enum-payload edges.
fn nested_payload(depth: usize, kind: usize) -> (Value, Vec<u8>) {
    let mut value = Value::Bool(false);
    let mut bytes = vec![VALUE_BOOL, 0];
    for level in 0..depth {
        let edge = if kind == 3 { level % 3 } else { kind };
        let mut parent = match edge {
            0 => {
                value = Value::List(vec![value]);
                vec![VALUE_LIST, 0, 0, 0, 1]
            }
            1 => {
                value = Value::Map(vec![(Value::Bool(true), value)]);
                vec![VALUE_MAP, 0, 0, 0, 1, VALUE_BOOL, 1]
            }
            2 => {
                value = Value::Enum(ValueEnum::test_payload(1, 1, value));
                vec![VALUE_ENUM, 0, 0, 0, 1, 0, 0, 0, 1, 1]
            }
            _ => unreachable!("test edge"),
        };
        parent.extend_from_slice(&bytes);
        bytes = parent;
    }
    (value, bytes)
}

#[test]
fn value_nesting_limit_is_symmetric_and_preserves_wire_bytes() {
    for kind in 0..4 {
        for depth in [
            0,
            MAX_VALUE_NESTING_DEPTH - 1,
            MAX_VALUE_NESTING_DEPTH,
            MAX_VALUE_NESTING_DEPTH + 1,
        ] {
            let (value, bytes) = nested_payload(depth, kind);
            let mut encoded = Vec::new();
            let result = write_value(&mut encoded, &value);
            let mut cursor = ByteReader::new(&bytes);
            let decoded = read_value(&mut cursor);
            if depth <= MAX_VALUE_NESTING_DEPTH {
                result.unwrap();
                assert_eq!(encoded, bytes);
                assert_eq!(decoded.unwrap(), value);
                cursor.finish().unwrap();
            } else {
                assert_eq!(result, Err(TokenWireError::Encode));
                assert_eq!(decoded, Err(TokenWireError::Decode));
                assert!(cursor.remaining() > 0);
            }
        }
    }
}

#[test]
fn map_keys_and_empty_containers_obey_the_same_value_depth() {
    for depth in [MAX_VALUE_NESTING_DEPTH - 1, MAX_VALUE_NESTING_DEPTH] {
        let (key, bytes) = nested_payload(depth, 2);
        let value = Value::Map(vec![(key, Value::Bool(false))]);
        let mut wire = vec![VALUE_MAP, 0, 0, 0, 1];
        wire.extend_from_slice(&bytes);
        wire.extend_from_slice(&[VALUE_BOOL, 0]);
        let mut encoded = Vec::new();
        let result = write_value(&mut encoded, &value);
        let decoded = read_value(&mut ByteReader::new(&wire));
        if depth < MAX_VALUE_NESTING_DEPTH {
            result.unwrap();
            assert_eq!(encoded, wire);
            assert_eq!(decoded.unwrap(), value);
        } else {
            assert_eq!(result, Err(TokenWireError::Encode));
            assert_eq!(decoded, Err(TokenWireError::Decode));
        }
    }
    for mut leaf in [
        Value::List(vec![]),
        Value::Map(vec![]),
        Value::Enum(ValueEnum::test_unit(1, 1)),
    ] {
        for _ in 0..MAX_VALUE_NESTING_DEPTH {
            leaf = Value::List(vec![leaf]);
        }
        let mut encoded = Vec::new();
        write_value(&mut encoded, &leaf).unwrap();
        assert_eq!(read_value(&mut ByteReader::new(&encoded)).unwrap(), leaf);
    }
}

#[test]
fn deep_wire_rejects_early_and_sibling_values_start_at_their_own_root() {
    // No deeply nested Rust value is built or dropped for hostile input.
    let mut hostile = Vec::new();
    for _ in 0..1000 {
        hostile.extend_from_slice(&[VALUE_LIST, 0, 0, 0, 1]);
    }
    hostile.push(VALUE_UNIT);
    let mut cursor = ByteReader::new(&hostile);
    assert_eq!(read_value(&mut cursor), Err(TokenWireError::Decode));
    assert!(cursor.remaining() > 4000);

    let (first, _) = nested_payload(MAX_VALUE_NESTING_DEPTH, 0);
    let (second, _) = nested_payload(MAX_VALUE_NESTING_DEPTH, 2);
    let values = vec![first, second, Value::Bool(true)];
    let mut encoded = Vec::new();
    write_value_slice(&mut encoded, &values).unwrap();
    let mut cursor = ByteReader::new(&encoded);
    assert_eq!(read_value_vec(&mut cursor).unwrap(), values);
    cursor.finish().unwrap();

    // A thousand siblings remain one nesting edge, not a depth counter.
    let wide = Value::List(vec![Value::Bool(true); 1000]);
    let mut encoded = Vec::new();
    write_value(&mut encoded, &wide).unwrap();
    assert_eq!(read_value(&mut ByteReader::new(&encoded)).unwrap(), wide);
}

#[test]
fn grouped_token_propagates_value_depth_rejection() {
    use crate::db::{
        cursor::{
            ContinuationSignature,
            token::{decode_grouped_token, encode_grouped_token},
        },
        direction::Direction,
    };
    let signature = ContinuationSignature::from_bytes([7; 32]);
    for depth in [MAX_VALUE_NESTING_DEPTH, MAX_VALUE_NESTING_DEPTH + 1] {
        let (value, _) = nested_payload(depth, 0);
        let encoded = encode_grouped_token(
            signature,
            std::slice::from_ref(&value),
            Direction::Asc,
            0,
            &[0x55; 32],
        );
        if depth == MAX_VALUE_NESTING_DEPTH {
            let decoded = decode_grouped_token(&encoded.unwrap(), &[0x55; 32]);
            assert_eq!(decoded.unwrap().last_group_key, vec![value]);
        } else {
            assert_eq!(encoded, Err(TokenWireError::Encode));
        }
    }
}

#[test]
fn cursor_value_decode_rejects_days_outside_bounded_calendar() {
    let mut encoded = vec![VALUE_DATE];
    encoded.extend_from_slice(&(Date::MIN.as_days_since_epoch() - 1).to_be_bytes());

    assert!(read_value(&mut ByteReader::new(encoded.as_slice())).is_err());
}

#[test]
fn cursor_value_u256_roundtrips_fixed_width_boundaries() {
    for value in [U256::ZERO, U256::ONE, U256::MAX] {
        let mut encoded = Vec::new();
        write_value(&mut encoded, &Value::U256(value)).expect("U256 should encode");

        assert_eq!(encoded.len(), 33);
        assert_eq!(encoded[0], VALUE_U256);
        assert_eq!(
            read_value(&mut ByteReader::new(encoded.as_slice())).expect("U256 should decode"),
            Value::U256(value),
        );
    }
}

#[test]
fn cursor_value_u256_rejects_truncated_payload() {
    let mut encoded = vec![VALUE_U256];
    encoded.extend_from_slice(&[0; 31]);

    assert!(read_value(&mut ByteReader::new(encoded.as_slice())).is_err());
}

#[test]
fn compact_cursor_values_preserve_sizes_domains_and_nested_boundaries() {
    let mut cases = Vec::new();
    for subaccount in [None, Some(Subaccount::MAX)] {
        cases.push((Value::Account(Account::new(Principal::MAX, subaccount)), 63));
    }
    for mantissa in [i128::MIN, -1200, 0, 1200, i128::MAX] {
        for scale in [0, 2, 28] {
            cases.push((
                Value::Decimal(Decimal::from_i128_with_scale(mantissa, scale)),
                18,
            ));
        }
    }
    for (value, width) in cases {
        let mut encoded = Vec::new();
        write_value(&mut encoded, &value).unwrap();
        assert_eq!(encoded.len(), width);
        let mut cursor = ByteReader::new(&encoded);
        let decoded = read_value(&mut cursor).unwrap();
        cursor.finish().unwrap();
        assert_eq!(decoded, value);
        if let (Value::Decimal(expected), Value::Decimal(actual)) = (&value, decoded) {
            assert_eq!(expected.parts(), actual.parts());
        }
        for len in 0..encoded.len() {
            assert!(read_value(&mut ByteReader::new(&encoded[..len])).is_err());
        }
        let nested = Value::List(vec![value; 1000]);
        encoded.clear();
        write_value(&mut encoded, &nested).unwrap();
        assert_eq!(encoded.len(), 5 + 1000 * width);
        let mut cursor = ByteReader::new(&encoded);
        assert_eq!(read_value(&mut cursor).unwrap(), nested);
        cursor.finish().unwrap();
    }
}

#[test]
fn compact_cursor_values_reject_noncanonical_metadata() {
    for scale in [29, 255] {
        let mut payload = vec![VALUE_DECIMAL];
        payload.extend_from_slice(&0_i128.to_be_bytes());
        payload.push(scale);
        assert!(read_value(&mut ByteReader::new(&payload)).is_err());
    }
    let mut invalid_account = vec![VALUE_ACCOUNT];
    invalid_account.extend_from_slice(&[255; 62]);
    assert!(read_value(&mut ByteReader::new(&invalid_account)).is_err());
}
