//! Fixed-width integer wire contracts.

fn assert_wire_sizes<T>(value: T, one_bytes: usize, batch_bytes: usize)
where
    T: serde::Serialize + serde::de::DeserializeOwned + Clone + PartialEq + std::fmt::Debug,
{
    let encoded = crate::wire::encode_one(&value).expect("the integer should encode");
    assert_eq!(encoded.len(), one_bytes);
    assert_eq!(
        crate::wire::decode_one::<T>(&encoded).expect("the integer should decode"),
        value,
    );
    let values = vec![value; 1_000];
    let encoded = crate::wire::encode_one(&values).expect("the integer batch should encode");
    assert_eq!(encoded.len(), batch_bytes);
    assert_eq!(
        crate::wire::decode_one::<Vec<T>>(&encoded).expect("the integer batch should decode"),
        values,
    );
}

#[test]
fn public_128_bit_integers_use_compact_native_wire_values() {
    for (value, one, batch) in [(0_u128, 1, 1_002), (128, 2, 2_002), (u128::MAX, 19, 19_002)] {
        assert_wire_sizes(value, one, batch);
    }
    for (value, one, batch) in [
        (0_i128, 1, 1_002),
        (-64, 1, 1_002),
        (64, 2, 2_002),
        (-65, 2, 2_002),
        (i128::MIN, 19, 19_002),
        (i128::MAX, 19, 19_002),
    ] {
        assert_wire_sizes(value, one, batch);
    }
}

fn assert_integer_json_roundtrip<T>(value: T, text: &str)
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let json = serde_json::to_string(&value).expect("JSON should encode");
    assert_eq!(
        json,
        serde_json::to_string(text).expect("text should encode")
    );
    assert_eq!(
        serde_json::from_str::<T>(&json).expect("JSON should round-trip"),
        value
    );
}

#[test]
fn u256_native_wire_and_json_boundaries() {
    use crate::U256;

    for (value, one, batch) in [
        (U256::ZERO, 33, 33_002),
        (U256::from(u64::MAX), 33, 33_002),
        (U256::MAX, 33, 33_002),
    ] {
        assert_wire_sizes(value, one, batch);
        assert_integer_json_roundtrip(value, &value.to_string());
    }
}

#[test]
fn u256_binary_ingress_rejects_malformed_and_overwide_bodies() {
    use crate::U256;
    for len in [0, 31, 32, 33] {
        let body = vec![0_u8; len];
        let encoded = crate::wire::encode_one(&body).expect("body should Wire-encode");
        assert_eq!(crate::wire::decode_one::<U256>(&encoded).is_ok(), len == 32);
        for end in 0..encoded.len() {
            assert!(crate::wire::decode_one::<U256>(&encoded[..end]).is_err());
        }
    }
}
