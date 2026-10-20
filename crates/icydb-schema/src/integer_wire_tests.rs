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

fn cbor_bytes<T: serde::Serialize>(value: &T) -> Vec<u8> {
    let mut bytes = Vec::new();
    serde_cbor::to_writer(&mut bytes, value).expect("the value should encode as CBOR");
    bytes
}

fn assert_integer_serde_roundtrip<T>(value: T, text: &str)
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let bytes = cbor_bytes(&value);
    assert_eq!(
        serde_cbor::from_slice::<T>(bytes.as_slice()).expect("CBOR should round-trip"),
        value,
    );
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

fn assert_cbor_integer_size<T>(value: T, expected_size: usize)
where
    T: serde::Serialize + serde::de::DeserializeOwned + Clone + PartialEq + std::fmt::Debug,
{
    assert_eq!(cbor_bytes(&value).len(), expected_size);
    let values = vec![value; 1_000];
    let bytes = cbor_bytes(&values);
    assert_eq!(bytes.len(), 3 + 1_000 * expected_size);
    assert_eq!(
        serde_cbor::from_slice::<Vec<T>>(bytes.as_slice()).expect("the batch should round-trip"),
        values,
    );
}

#[test]
fn u256_native_wire_and_binary_serde_boundaries() {
    use crate::U256;

    for (value, one, batch) in [
        (U256::ZERO, 33, 33_002),
        (U256::from(u64::MAX), 33, 33_002),
        (U256::MAX, 33, 33_002),
    ] {
        assert_wire_sizes(value, one, batch);
        assert_integer_serde_roundtrip(value, &value.to_string());
    }
    assert_cbor_integer_size(U256::ZERO, 34);
    assert_cbor_integer_size(U256::MAX, 34);
}

#[test]
fn u256_binary_ingress_rejects_malformed_and_overwide_bodies() {
    use crate::U256;
    for len in [0, 31, 32, 33] {
        let body = vec![0_u8; len];
        let encoded = cbor_bytes(&body);
        assert_eq!(
            serde_cbor::from_slice::<U256>(encoded.as_slice()).is_ok(),
            len == 32
        );
        for end in 0..encoded.len() {
            assert!(serde_cbor::from_slice::<U256>(&encoded[..end]).is_err());
        }
    }
}
