//! Fixed-width integer wire contracts.

fn assert_candid_sizes<T>(value: T, one_bytes: usize, batch_bytes: usize)
where
    T: candid::CandidType + serde::de::DeserializeOwned + Clone + PartialEq + std::fmt::Debug,
{
    let encoded = candid::encode_one(&value).expect("the integer should encode");
    assert_eq!(encoded.len(), one_bytes);
    assert_eq!(
        candid::decode_one::<T>(&encoded).expect("the integer should decode"),
        value,
    );
    let values = vec![value; 1_000];
    let encoded = candid::encode_one(&values).expect("the integer batch should encode");
    assert_eq!(encoded.len(), batch_bytes);
    assert_eq!(
        candid::decode_one::<Vec<T>>(&encoded).expect("the integer batch should decode"),
        values,
    );
}

#[test]
fn public_128_bit_integers_use_compact_native_candid_values() {
    use candid::CandidType;

    assert_eq!(<u128 as candid::CandidType>::ty(), candid::Nat::ty());
    assert_eq!(<i128 as candid::CandidType>::ty(), candid::Int::ty());
    for (value, one, batch) in [(0_u128, 8, 1_011), (128, 9, 2_011), (u128::MAX, 26, 19_011)] {
        assert_candid_sizes(value, one, batch);
    }
    for (value, one, batch) in [
        (0_i128, 8, 1_011),
        (-64, 8, 1_011),
        (64, 9, 2_011),
        (-65, 9, 2_011),
        (i128::MIN, 26, 19_011),
        (i128::MAX, 26, 19_011),
    ] {
        assert_candid_sizes(value, one, batch);
    }
}

fn cbor_bytes<T: serde::Serialize + ?Sized>(value: &T) -> Vec<u8> {
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(value, &mut bytes).expect("the value should encode as CBOR");
    bytes
}

fn assert_integer_serde_roundtrip<T>(value: T, text: &str)
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let bytes = cbor_bytes(&value);
    assert_eq!(
        ciborium::de::from_reader::<T, _>(bytes.as_slice()).expect("CBOR should round-trip"),
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
        ciborium::de::from_reader::<Vec<T>, _>(bytes.as_slice())
            .expect("the batch should round-trip"),
        values,
    );
}

#[test]
fn u256_native_candid_and_binary_serde_boundaries() {
    use crate::U256;
    use candid::CandidType;
    assert_eq!(U256::ty(), candid::Nat::ty());
    for (value, one, batch) in [
        (U256::ZERO, 8, 1_011),
        (U256::from(u64::MAX), 17, 10_011),
        (U256::MAX, 44, 37_011),
    ] {
        assert_candid_sizes(value, one, batch);
        assert_integer_serde_roundtrip(value, &value.to_string());
    }
    assert_cbor_integer_size(U256::ZERO, 1);
    assert_cbor_integer_size(U256::MAX, 35);
}

#[test]
fn u256_binary_ingress_rejects_malformed_and_overwide_bodies() {
    use crate::U256;
    for body in [vec![], vec![1], vec![2, 1], vec![1, 1, 0]] {
        let encoded = cbor_bytes(serde_bytes::Bytes::new(&body));
        assert!(ciborium::de::from_reader::<U256, _>(encoded.as_slice()).is_err());
    }
    let negative = cbor_bytes(&-1_i64);
    assert!(ciborium::de::from_reader::<U256, _>(negative.as_slice()).is_err());
    for len in [31, 32, 33] {
        let mut body = vec![1];
        body.extend(std::iter::repeat_n(0xff, len));
        let encoded = cbor_bytes(serde_bytes::Bytes::new(&body));
        assert_eq!(
            ciborium::de::from_reader::<U256, _>(encoded.as_slice()).is_ok(),
            len <= 32
        );
        for end in 0..encoded.len() {
            assert!(ciborium::de::from_reader::<U256, _>(&encoded[..end]).is_err());
        }
    }
}
