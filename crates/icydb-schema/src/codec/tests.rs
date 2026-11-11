use super::*;

#[test]
fn artifacts_refuse_noncanonical_wire_and_truncation() {
    let fragment = SchemaFragment::try_new(Vec::new(), Vec::new()).unwrap();
    let canonical = encode_schema_fragment(&fragment).unwrap();
    assert_eq!(&canonical[..5], b"ICWF\x01");
    let mut alternate = canonical.clone();
    alternate.splice(5..6, [0x80, 0]);
    assert_eq!(
        decode_schema_fragment(&alternate),
        Err(SchemaContractError::NonCanonical)
    );
    for end in 0..canonical.len() {
        assert!(decode_schema_fragment(&canonical[..end]).is_err());
    }
}

#[test]
fn artifact_decimal_admission_is_bounded_before_numeric_parsing() {
    // ScalarLiteral's Decimal variant carries text, matching the shared
    // atom's Serde form, but storage artifacts admit only its finite domain.
    let wide = crate::wire::encode_one((4_u32, "0".repeat(1_024))).unwrap();
    assert!(crate::wire::decode_one::<crate::ScalarLiteral>(&wide).is_err());
    let invalid = crate::ScalarLiteral::Decimal(crate::Decimal::new_unchecked(
        1,
        crate::decimal::MAX_SUPPORTED_SCALE + 1,
    ));
    assert_eq!(invalid.validate(), Err(SchemaContractError::InvalidLiteral));
}

#[test]
fn artifact_relation_keys_bound_each_name_and_the_complete_list() {
    for count in [crate::MAX_FRAGMENT_FIELDS, crate::MAX_FRAGMENT_FIELDS + 1] {
        let bytes = crate::wire::encode_one((0_u32, vec!["id"; count])).unwrap();
        assert_eq!(
            crate::wire::decode_one::<crate::RelationSourceFragment>(&bytes).is_ok(),
            count == crate::MAX_FRAGMENT_FIELDS
        );
    }
    let bytes = crate::wire::encode_one((0_u32, vec!["x".repeat(crate::MAX_SOURCE_KEY_BYTES + 1)]))
        .unwrap();
    assert!(crate::wire::decode_one::<crate::RelationSourceFragment>(&bytes).is_err());
}

#[test]
fn artifacts_admit_only_bounded_collections_and_valid_scalar_tags() {
    let excessive = vec![(); crate::MAX_FRAGMENT_ENTITIES + 1];
    let bytes = crate::wire::encode_one((FRAGMENT_HEADER, (&excessive, Vec::<u8>::new()))).unwrap();
    assert_eq!(
        decode_schema_fragment(&bytes),
        Err(SchemaContractError::Decode)
    );
    let invalid = crate::wire::encode_one(u32::MAX).unwrap();
    assert!(
        ic_wire::native::decode::<crate::ScalarLiteral>(&invalid, crate::wire::limits(64)).is_err()
    );
    let wide = crate::wire::encode_one("x".repeat(crate::MAX_SOURCE_KEY_BYTES + 1)).unwrap();
    assert!(crate::wire::decode_one::<crate::SchemaName>(&wide).is_err());
}
