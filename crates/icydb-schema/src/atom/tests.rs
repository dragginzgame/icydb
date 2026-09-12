use super::{Blob, Ulid};
use crate::{MAX_PROPOSAL_LITERAL_BYTES, ScalarLiteral, SchemaContractError};
use candid::CandidType;

#[test]
fn blob_candid_round_trip_accepts_runtime_values_above_the_proposal_literal_bound() {
    let blob = Blob::from(vec![0xAB; MAX_PROPOSAL_LITERAL_BYTES + 1]);
    let encoded = candid::encode_one(&blob).expect("runtime blob should encode");
    let encoded_bytes =
        candid::encode_one(blob.as_bytes()).expect("canonical byte vector should encode");
    let decoded = candid::decode_one::<Blob>(&encoded).expect("runtime blob should decode");

    assert_eq!(encoded, encoded_bytes);
    assert_eq!(Blob::ty(), Vec::<u8>::ty());
    assert_eq!(decoded, blob);
}

#[test]
fn ulid_candid_round_trip_uses_exact_binary_bytes() {
    assert_eq!(Ulid::ty(), Vec::<u8>::ty());
    for value in [Ulid::MIN, Ulid::from_u128(42), Ulid::MAX] {
        let encoded = candid::encode_one(value).expect("ULID should encode");
        let bytes = candid::encode_one(value.to_bytes()).expect("bytes should encode");

        assert_eq!(encoded, bytes);
        assert_eq!(encoded.len(), 26);
        assert_eq!(
            candid::decode_one::<Ulid>(&encoded).expect("ULID should decode"),
            value,
        );
    }
}

#[test]
fn ulid_candid_rejects_non_exact_blob_lengths() {
    for len in [0, 15, 17, 1_024] {
        let encoded = candid::encode_one(vec![0_u8; len]).expect("blob should encode");
        assert!(candid::decode_one::<Ulid>(&encoded).is_err());
    }
}

#[test]
fn ulid_candid_requires_byte_elements() {
    let encoded = candid::encode_one(vec![0_u16; 16]).expect("numeric vector should encode");
    assert!(candid::decode_one::<Ulid>(&encoded).is_err());
}

#[test]
fn ulid_binary_serde_round_trip_uses_a_byte_string() {
    for value in [Ulid::MIN, Ulid::from_u128(42), Ulid::MAX] {
        let mut encoded = Vec::new();
        ciborium::into_writer(&value, &mut encoded).expect("ULID should encode");
        let wire: ciborium::Value =
            ciborium::from_reader(encoded.as_slice()).expect("wire should decode");

        assert_eq!(wire, ciborium::Value::Bytes(value.to_bytes().to_vec()));
        assert_eq!(encoded.len(), 17);
        assert_eq!(
            ciborium::from_reader::<Ulid, _>(encoded.as_slice()).expect("ULID should decode"),
            value,
        );
    }
}

#[test]
fn ulid_binary_serde_enforces_width_for_bytes_and_sequences() {
    for len in [0, 15, 16, 17, 1_024] {
        for wire in [
            ciborium::Value::Bytes(vec![0; len]),
            ciborium::Value::Array(vec![ciborium::Value::Integer(0.into()); len]),
        ] {
            let mut encoded = Vec::new();
            ciborium::into_writer(&wire, &mut encoded).expect("wire should encode");
            let decoded = ciborium::from_reader::<Ulid, _>(encoded.as_slice());
            if len == 16 {
                assert_eq!(decoded.expect("exact width should decode"), Ulid::MIN);
            } else {
                assert!(decoded.is_err());
            }
        }
    }
}

#[test]
fn ulid_human_readable_serde_round_trip_uses_canonical_text() {
    let value = Ulid::from_u128(42);
    let encoded = serde_json::to_string(&value).expect("ULID should encode");
    assert_eq!(encoded, format!("\"{value}\""));
    assert_eq!(
        serde_json::from_str::<Ulid>(&encoded).expect("ULID should decode"),
        value,
    );
}

#[test]
fn ulid_candid_vectors_preserve_binary_values() {
    let values = vec![Ulid::from_u128(42); 1_000];
    let encoded = candid::encode_one(&values).expect("ULIDs should encode");
    assert_eq!(encoded.len(), 17_013);
    assert_eq!(
        candid::decode_one::<Vec<Ulid>>(&encoded).expect("ULIDs should decode"),
        values,
    );
}

#[test]
fn blob_proposal_literals_retain_their_dedicated_bound() {
    let maximum = ScalarLiteral::Blob(Blob::from(vec![0; MAX_PROPOSAL_LITERAL_BYTES]));
    let oversized = ScalarLiteral::Blob(Blob::from(vec![0; MAX_PROPOSAL_LITERAL_BYTES + 1]));

    assert_eq!(maximum.validate(), Ok(()));
    assert_eq!(
        oversized.validate(),
        Err(SchemaContractError::InvalidLiteral),
    );
}
