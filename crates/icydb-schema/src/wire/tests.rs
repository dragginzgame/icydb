//! Consumer policy for recursive schema-depth values inside endpoint wrappers.

use crate::wire::{
    Error, MAX_DEPTH, MAX_NODES, MAX_REPLY_BYTES, decode_one, encode_one, encoded_len,
    encoded_reply_len,
};
use serde::{Deserialize, Serialize, Serializer, ser::Error as _};

#[test]
fn decoding_accepts_deserialize_only_values_and_rejects_overlong_varints() {
    #[derive(Debug, Deserialize, PartialEq)]
    struct ReceiveOnly(u32);

    let bytes = encode_one(128_u32).unwrap();
    assert_eq!(bytes, [0x80, 0x01]);
    assert_eq!(decode_one::<ReceiveOnly>(&bytes), Ok(ReceiveOnly(128)));
    assert_eq!(decode_one::<ReceiveOnly>(&[0x81, 0]), Err(Error::Malformed));
}

#[test]
fn map_decoding_does_not_certify_original_byte_identity() {
    use std::collections::BTreeMap;

    for bytes in [[2, 2, 20, 1, 10], [2, 1, 20, 1, 10]] {
        let value: BTreeMap<u8, u8> = decode_one(&bytes).unwrap();
        assert_eq!(value.get(&1), Some(&10));
        let encoded = encode_one(&value).unwrap();
        assert_ne!(encoded, bytes);
        assert_eq!(decode_one::<BTreeMap<u8, u8>>(&encoded).unwrap(), value);
        assert_eq!(value.len(), if bytes[1] == 2 { 2 } else { 1 });
    }
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
enum Nested {
    Leaf(u128),
    Children(Vec<Self>),
}

#[test]
fn schema_depth_values_fit_inside_endpoint_record_wrappers() {
    let mut value = Nested::Leaf(u128::MAX);
    for _ in 0..crate::MAX_SCHEMA_FIELD_TYPE_DEPTH {
        value = Nested::Children(vec![value]);
    }
    let reply: Result<Vec<(String, Nested)>, u16> = Ok(vec![("value".into(), value)]);
    let bytes = encode_one(&reply).unwrap();
    assert_eq!(encoded_reply_len(&reply).unwrap(), bytes.len());
    assert_eq!(
        decode_one::<Result<Vec<(String, Nested)>, u16>>(&bytes).unwrap(),
        reply
    );
}

#[test]
fn reply_sizing_preserves_exact_byte_limits_and_encoded_lengths() {
    for length in [0, 1, 127, 128, 16_383, 16_384] {
        let value = (
            "result",
            Some(serde_bytes::ByteBuf::from(vec![7; length])),
            None::<u8>,
        );
        assert_eq!(
            encoded_reply_len(&value).unwrap(),
            encode_one(&value).unwrap().len()
        );
        assert_eq!(
            encoded_reply_len(value.clone()).unwrap(),
            encode_one(value).unwrap().len()
        );
    }
    let large = vec![0; MAX_REPLY_BYTES];
    let overhead = encoded_len(serde_bytes::Bytes::new(&large)).unwrap() - large.len();
    for extra in [0, 1] {
        let payload = serde_bytes::Bytes::new(&large[..MAX_REPLY_BYTES - overhead + extra]);
        assert_eq!(encoded_len(payload).unwrap(), MAX_REPLY_BYTES + extra);
        assert_eq!(
            encoded_reply_len(payload),
            encode_one(payload).map(|bytes| bytes.len())
        );
        assert_eq!(
            encoded_reply_len(payload),
            if extra == 0 {
                Ok(MAX_REPLY_BYTES)
            } else {
                Err(Error::Limit)
            }
        );
    }
}

// Tiny wire output with a large inline value footprint isolates heap admission
// from the byte and node ceilings without constructing an oversized reply.
#[derive(Clone, Serialize)]
struct HeapWeightedUnit {
    #[serde(skip)]
    _padding: [u8; 1024],
}

struct RefusedValue;

impl Serialize for RefusedValue {
    fn serialize<S: Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
        Err(S::Error::custom("fixture serializer refusal"))
    }
}

#[test]
fn reply_sizing_preserves_encoding_resource_and_serializer_refusals() {
    let mut deep = Nested::Leaf(7);
    for _ in 0..MAX_DEPTH {
        deep = Nested::Children(vec![deep]);
    }
    assert_eq!(encoded_reply_len(&deep), Err(Error::Limit));
    assert_eq!(
        encoded_reply_len(&deep),
        encode_one(&deep).map(|bytes| bytes.len())
    );
    let many = vec![(); MAX_NODES];
    assert_eq!(encoded_reply_len(&many), Err(Error::Limit));
    assert_eq!(
        encoded_reply_len(&many),
        encode_one(&many).map(|bytes| bytes.len())
    );
    let control = vec![Some(()); 32_768];
    assert!(encoded_reply_len(&control).unwrap() < MAX_REPLY_BYTES);
    // Some visits its concrete value, so the skipped inline footprint is
    // admitted even though the encoded elements are only option tags.
    let heap_heavy = vec![
        Some(HeapWeightedUnit {
            _padding: [0; 1024]
        });
        control.len()
    ];
    assert_eq!(encoded_reply_len(&heap_heavy), Err(Error::Limit));
    assert_eq!(
        encoded_reply_len(&heap_heavy),
        encode_one(&heap_heavy).map(|bytes| bytes.len())
    );
    assert_eq!(encoded_reply_len(RefusedValue), Err(Error::Malformed));
    assert_eq!(
        encoded_reply_len(RefusedValue),
        encode_one(RefusedValue).map(|bytes| bytes.len())
    );
}
