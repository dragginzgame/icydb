use super::*;
use crate::Account;

#[test]
fn hex_text_preserves_every_byte_with_optional_prefix() {
    for byte in 0..=u8::MAX {
        let expected = Subaccount::from_array([byte; 32]);
        for text in [
            format!("{byte:02x}").repeat(32),
            format!("{byte:02X}").repeat(32),
        ] {
            assert_eq!(Subaccount::from_hex(&text), Ok(expected));
            assert_eq!(Subaccount::from_hex(&format!("0x{text}")), Ok(expected));
        }
    }
}

#[test]
fn hex_text_rejects_width_unicode_and_nonhex_boundaries() {
    for text in [
        String::new(),
        "0x".into(),
        "0".repeat(63),
        "0".repeat(65),
        "g".repeat(64),
        "+1".repeat(32),
        "-1".repeat(32),
        format!("aé{}", "0".repeat(61)),
        format!(" {}", "0".repeat(64)),
        format!("0X{}", "0".repeat(64)),
    ] {
        assert_eq!(
            Subaccount::from_hex(&text),
            Err(SchemaContractError::InvalidLiteral)
        );
    }
}

#[test]
fn binary_serde_and_wire_preserve_exact_subaccount_bytes() {
    for byte in [0, 23, 24, 128, 255] {
        let value = Subaccount::from_array([byte; 32]);
        let json = serde_json::to_string(&value).unwrap();
        assert_eq!(json, serde_json::to_string(&value.to_array()).unwrap());
        assert_eq!(serde_json::from_str::<Subaccount>(&json).unwrap(), value);

        let wire = crate::wire::encode_one(value).unwrap();
        assert_eq!(
            wire,
            crate::wire::encode_one(serde_bytes::Bytes::new(&value.to_array())).unwrap()
        );
        assert_eq!(wire.len(), 33);
        assert_eq!(crate::wire::decode_one::<Subaccount>(&wire).unwrap(), value);

        let batch = vec![value; 1_000];
        let encoded = crate::wire::encode_one(&batch).unwrap();
        assert_eq!(encoded.len(), 33_002);
        assert_eq!(
            crate::wire::decode_one::<Vec<Subaccount>>(&encoded).unwrap(),
            batch
        );
    }
}

#[test]
fn binary_subaccount_ingress_enforces_width_and_byte_elements() {
    for len in [0, 31, 33, 1_024] {
        let wire = crate::wire::encode_one(vec![0_u8; len]).unwrap();
        assert!(crate::wire::decode_one::<Subaccount>(&wire).is_err());
        let bytes = vec![0_u8; len];
        let encoded = crate::wire::encode_one(serde_bytes::Bytes::new(&bytes)).unwrap();
        assert!(crate::wire::decode_one::<Subaccount>(&encoded).is_err());
    }
    // Postcard carries values without source integer-type tags: use a value
    // outside the byte domain rather than zero, which also encodes as u8.
    let wire = crate::wire::encode_one(vec![256_u16; 32]).unwrap();
    assert!(crate::wire::decode_one::<Subaccount>(&wire).is_err());

    // Keep sequence width and element admission covered through Serde's
    // sequence adapter. Wire itself carries no source-type tags.
    let array = serde::de::value::SeqDeserializer::<_, serde::de::value::Error>::new(
        vec![255_u8; 32].into_iter(),
    );
    assert_eq!(Subaccount::deserialize(array).unwrap(), Subaccount::MAX);
    for array in [vec![0_u16; 31], vec![0; 33], vec![256; 32]] {
        let array =
            serde::de::value::SeqDeserializer::<_, serde::de::value::Error>::new(array.into_iter());
        assert!(Subaccount::deserialize(array).is_err());
    }

    let encoded = crate::wire::encode_one(Subaccount::MAX).unwrap();
    for end in 0..encoded.len() {
        assert!(crate::wire::decode_one::<Subaccount>(&encoded[..end]).is_err());
    }
}

#[test]
fn accounts_roundtrip_optional_subaccounts_on_wire_and_json() {
    for subaccount in [None, Some(Subaccount::MIN), Some(Subaccount::MAX)] {
        let account = Account::from_owner_and_subaccount(Principal::anonymous(), subaccount);
        let json = serde_json::to_string(&account).unwrap();
        assert_eq!(serde_json::from_str::<Account>(&json).unwrap(), account);
        let wire = crate::wire::encode_one(account).unwrap();
        // Native account projection is the ordered owner/optional-bytes pair.
        assert_eq!(
            wire,
            crate::wire::encode_one((account.owner(), account.subaccount())).unwrap()
        );
        assert_eq!(crate::wire::decode_one::<Account>(&wire).unwrap(), account);
    }
}
