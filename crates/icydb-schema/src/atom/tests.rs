use super::{Blob, Principal, PrincipalDecodeError, PrincipalError, Ulid};
use crate::{MAX_PROPOSAL_LITERAL_BYTES, ScalarLiteral, SchemaContractError};

#[test]
fn blob_wire_round_trip_accepts_runtime_values_above_the_proposal_literal_bound() {
    let blob = Blob::from(vec![0xAB; MAX_PROPOSAL_LITERAL_BYTES + 1]);
    let encoded = crate::wire::encode_one(&blob).expect("runtime blob should encode");
    let encoded_bytes = crate::wire::encode_one(serde_bytes::Bytes::new(blob.as_bytes()))
        .expect("canonical byte vector should encode");
    let decoded = crate::wire::decode_one::<Blob>(&encoded).expect("runtime blob should decode");

    assert_eq!(encoded, encoded_bytes);

    assert_eq!(decoded, blob);
}

#[test]
fn ulid_wire_round_trip_uses_exact_binary_bytes() {
    for value in [Ulid::MIN, Ulid::from_u128(42), Ulid::MAX] {
        let encoded = crate::wire::encode_one(value).expect("ULID should encode");
        let bytes = crate::wire::encode_one(serde_bytes::Bytes::new(&value.to_bytes()))
            .expect("bytes should encode");

        assert_eq!(encoded, bytes);
        assert_eq!(encoded.len(), 17);
        assert_eq!(
            crate::wire::decode_one::<Ulid>(&encoded).expect("ULID should decode"),
            value,
        );
    }
}

#[test]
fn ulid_wire_rejects_non_exact_blob_lengths() {
    for len in [0, 15, 17, 1_024] {
        let encoded = crate::wire::encode_one(vec![0_u8; len]).expect("blob should encode");
        assert!(crate::wire::decode_one::<Ulid>(&encoded).is_err());
    }
}

#[test]
fn ulid_wire_requires_byte_elements() {
    let encoded = crate::wire::encode_one(vec![256_u16; 16]).expect("numeric vector should encode");
    assert!(crate::wire::decode_one::<Ulid>(&encoded).is_err());
}

#[test]
fn ulid_binary_serde_round_trip_uses_a_byte_string() {
    for value in [Ulid::MIN, Ulid::from_u128(42), Ulid::MAX] {
        let mut encoded = Vec::new();
        serde_cbor::to_writer(&mut encoded, &value).expect("ULID should encode");
        let wire: serde_cbor::Value =
            serde_cbor::from_slice(encoded.as_slice()).expect("wire should decode");

        assert_eq!(wire, serde_cbor::Value::Bytes(value.to_bytes().to_vec()));
        assert_eq!(encoded.len(), 17);
        assert_eq!(
            serde_cbor::from_slice::<Ulid>(encoded.as_slice()).expect("ULID should decode"),
            value,
        );
    }
}

#[test]
fn ulid_binary_serde_enforces_width_for_bytes_and_sequences() {
    for len in [0, 15, 16, 17, 1_024] {
        for wire in [
            serde_cbor::Value::Bytes(vec![0; len]),
            serde_cbor::Value::Array(vec![serde_cbor::Value::Integer(0); len]),
        ] {
            let mut encoded = Vec::new();
            serde_cbor::to_writer(&mut encoded, &wire).expect("wire should encode");
            let decoded = serde_cbor::from_slice::<Ulid>(encoded.as_slice());
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
fn ulid_wire_vectors_preserve_binary_values() {
    let values = vec![Ulid::from_u128(42); 1_000];
    let encoded = crate::wire::encode_one(&values).expect("ULIDs should encode");
    assert_eq!(encoded.len(), 17_002);
    assert_eq!(
        crate::wire::decode_one::<Vec<Ulid>>(&encoded).expect("ULIDs should decode"),
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

#[test]
fn principal_platform_identity_preserves_native_wire_bytes() {
    for bytes in [vec![], vec![4], vec![2, 7], vec![0xAB; 29]] {
        let platform = ic_platform::Principal::from_slice(&bytes);
        let external: ic_platform::principal::ExternalPrincipal = platform.into();
        assert_eq!(external.as_slice(), bytes);
        assert_eq!(ic_platform::Principal::from(external), platform);
        let value = Principal::from(platform);
        let recovered: ic_platform::Principal = value.into();
        assert_eq!(recovered, platform);
        assert_eq!(value.as_slice(), bytes);
        assert_eq!(Principal::from_text(&platform.to_text()), Ok(value));

        let encoded = crate::wire::encode_one(value).expect("principal should encode");
        let mut expected = vec![u8::try_from(bytes.len()).expect("principal length fits one byte")];
        expected.extend_from_slice(&bytes);
        assert_eq!(encoded, expected);
        assert_eq!(
            crate::wire::decode_one::<Principal>(&encoded).expect("principal should decode"),
            value,
        );

        // The atom's binary Serde representation remains a raw byte string,
        // including a leading 0x02, for borrowed and reader-backed decoding.
        let encoded = serde_cbor::to_vec(&value).expect("principal should serialize");
        let expected = serde_cbor::to_vec(&serde_bytes::Bytes::new(&bytes))
            .expect("principal bytes should serialize");
        assert_eq!(encoded, expected);
        assert_eq!(
            serde_cbor::from_slice::<Principal>(&encoded).expect("borrowed bytes should decode"),
            value,
        );
        assert_eq!(
            serde_cbor::from_reader::<Principal, _>(encoded.as_slice())
                .expect("reader-backed bytes should decode"),
            value,
        );
    }
}

#[test]
fn principal_platform_adoption_retains_text_and_byte_rejection() {
    let platform = ic_platform::Principal::from_slice(&[0xAB; 29]);
    let text = platform.to_text();
    assert_eq!(
        Principal::from_text(&text.to_uppercase()),
        Ok(platform.into())
    );
    assert_eq!(
        Principal::from_text(&text.replace('-', "")),
        Err(PrincipalError::InvalidText),
    );
    assert_eq!(
        Principal::from_text(&"a".repeat(64)),
        Err(PrincipalError::InvalidText),
    );
    assert_eq!(
        Principal::from_text("invalid principal text"),
        Err(PrincipalError::InvalidText),
    );
    let oversized = [0xAB; 30];
    assert_eq!(
        Principal::try_from_bytes(&oversized),
        Err(PrincipalDecodeError::TooLarge { len: 30 }),
    );
    let encoded = crate::wire::encode_one(serde_bytes::Bytes::new(&oversized))
        .expect("oversized bytes should encode independently of Principal");
    assert!(crate::wire::decode_one::<Principal>(&encoded).is_err());
}
