//! Validation counts the canonical encoder's bytes without retaining a key.

use super::*;

fn kinds(fixture: &TargetedRuleFixture) -> Vec<AcceptedFieldKind> {
    use AcceptedFieldKind as K;
    vec![
        K::Account,
        K::Blob { max_len: None },
        K::Blob {
            max_len: Some(u32::MAX),
        },
        K::Bool,
        K::Date,
        K::Decimal { scale: 0 },
        K::Decimal { scale: 18 },
        K::Duration,
        K::Enum {
            type_id: fixture.choice_type,
        },
        K::Float32,
        K::Float64,
        K::Int8,
        K::Int16,
        K::Int32,
        K::Int64,
        K::Int128,
        K::Principal,
        K::Subaccount,
        K::Text { max_len: None },
        K::Text {
            max_len: Some(u32::MAX),
        },
        K::Timestamp,
        K::Nat8,
        K::Nat16,
        K::Nat32,
        K::Nat64,
        K::Nat128,
        K::Ulid,
        K::Unit,
        K::Relation {
            target_path: "tests::Target".into(),
            target_entity_name: "Target".into(),
            target_entity_tag: crate::types::EntityTag::new(7),
            target_store_path: "tests::Store".into(),
            key_kind: Box::new(K::Nat64),
        },
        K::List(Box::new(K::Text { max_len: Some(64) })),
        K::Set(Box::new(K::Nat64)),
        K::Map {
            key: Box::new(K::Text { max_len: None }),
            value: Box::new(K::List(Box::new(K::Nat64))),
        },
        K::Composite {
            type_id: fixture.degree_type,
        },
        K::U256,
    ]
}

fn assert_length(expression: &AcceptedCheckExprV1) {
    assert_eq!(
        expression.canonical_key_len(),
        expression.canonical_key().len()
    );
}

#[test]
fn counted_keys_match_materialized_frames_for_every_kind_codec_and_operand_family() {
    let fixture = targeted_rule_fixture();
    // Encoding coverage includes raw, not necessarily admitted operands. The
    // separate validation controls enforce kind/capability/payload bounds.
    for kind in kinds(&fixture) {
        for storage in [FieldStorageDecode::ByKind, FieldStorageDecode::CatalogValue] {
            for payload in [
                Vec::new(),
                "é🦀".as_bytes().to_vec(),
                vec![255; MAX_CHECK_EXPR_V1_LITERAL_BYTES],
            ] {
                let literal =
                    AcceptedCheckValueExprV1::Literal(AcceptedCheckLiteralV1::from_accepted_parts(
                        kind.clone(),
                        storage,
                        kind.leaf_codec_for_storage(storage),
                        payload,
                    ));
                for left in [
                    AcceptedCheckValueExprV1::Field(FieldId::new(2)),
                    literal,
                    AcceptedCheckValueExprV1::CharLength(FieldId::new(3)),
                    AcceptedCheckValueExprV1::OctetLength(FieldId::new(5)),
                    AcceptedCheckValueExprV1::Cardinality(FieldId::new(4)),
                ] {
                    assert_length(&AcceptedCheckExprV1::IsNull(left.clone()));
                    assert_length(&AcceptedCheckExprV1::IsNotNull(left.clone()));
                    for op in [
                        AcceptedCheckCompareOpV1::Eq,
                        AcceptedCheckCompareOpV1::Ne,
                        AcceptedCheckCompareOpV1::Lt,
                        AcceptedCheckCompareOpV1::Lte,
                        AcceptedCheckCompareOpV1::Gt,
                        AcceptedCheckCompareOpV1::Gte,
                    ] {
                        let comparison = AcceptedCheckExprV1::Compare {
                            left: left.clone(),
                            op,
                            right: AcceptedCheckValueExprV1::Field(FieldId::new(2)),
                        };
                        assert_length(&comparison);
                        assert_length(&AcceptedCheckExprV1::Not(Box::new(
                            AcceptedCheckExprV1::And(vec![
                                AcceptedCheckExprV1::False,
                                AcceptedCheckExprV1::Or(vec![
                                    AcceptedCheckExprV1::True,
                                    comparison,
                                ]),
                            ]),
                        )));
                    }
                }
            }
        }
    }
}

fn map_kind(leaves: usize) -> AcceptedFieldKind {
    if leaves == 1 {
        return AcceptedFieldKind::Nat64;
    }
    AcceptedFieldKind::Map {
        key: Box::new(map_kind(leaves / 2)),
        value: Box::new(map_kind(leaves - leaves / 2)),
    }
}

#[test]
fn local_and_catalog_validation_keep_exact_encoded_byte_boundaries() {
    let snapshot = snapshot();
    let catalog = value_catalog();
    // A balanced map kind writes 2*leaves-1 tags. IsNull/literal framing
    // contributes 12 bytes before payload. These shape-validation fixtures
    // deliberately defer payload decoding, as this boundary always has.
    let leaves = (MAX_CHECK_EXPR_V1_BYTES - 12) / 2;
    for (leaves, payload, expected_bytes) in [
        (leaves - 1, 2, MAX_CHECK_EXPR_V1_BYTES - 1),
        (leaves, 1, MAX_CHECK_EXPR_V1_BYTES),
        (leaves, 2, MAX_CHECK_EXPR_V1_BYTES + 1),
    ] {
        let expression = AcceptedCheckExprV1::IsNull(AcceptedCheckValueExprV1::Literal(
            AcceptedCheckLiteralV1::from_accepted_parts(
                map_kind(leaves),
                FieldStorageDecode::CatalogValue,
                LeafCodec::Structural,
                vec![255; payload],
            ),
        ));
        assert_eq!(expression.canonical_key().len(), expected_bytes);
        assert_eq!(expression.canonical_key_len(), expected_bytes);
        let expected = if expected_bytes > MAX_CHECK_EXPR_V1_BYTES {
            Err(AcceptedCheckExprV1Error::EncodedBytesExceeded)
        } else {
            Ok(())
        };
        assert_eq!(
            expression.validate(&snapshot, catalog.composite_catalog()),
            expected
        );
        assert_eq!(
            expression.validate_snapshot_local(snapshot.fields()),
            expected
        );
    }
}
