//! Canonical key bytes remain stable when Boolean children share a destination.

use super::*;

#[test]
fn nested_boolean_keys_preserve_exact_length_frames() {
    // NOT(AND(FALSE, OR(TRUE, NOT(FALSE)))); the OR child is exactly 28 bytes.
    let expression = AcceptedCheckExprV1::Not(Box::new(AcceptedCheckExprV1::And(vec![
        AcceptedCheckExprV1::False,
        AcceptedCheckExprV1::Or(vec![
            AcceptedCheckExprV1::True,
            AcceptedCheckExprV1::Not(Box::new(AcceptedCheckExprV1::False)),
        ]),
    ])));
    let expected = [
        &[2, 3][..],
        &2_u64.to_be_bytes(),
        &1_u64.to_be_bytes(),
        &[1],
        &28_u64.to_be_bytes(),
        &[4],
        &2_u64.to_be_bytes(),
        &1_u64.to_be_bytes(),
        &[0],
        &2_u64.to_be_bytes(),
        &[2, 1],
    ]
    .concat();
    expression
        .validate(&snapshot(), value_catalog().composite_catalog())
        .unwrap();
    assert_eq!(expression.canonical_key(), expected);
}

fn take_length(bytes: &mut &[u8]) -> usize {
    let (length, rest) = bytes.split_at(size_of::<u64>());
    *bytes = rest;
    usize::try_from(u64::from_be_bytes(length.try_into().unwrap())).unwrap()
}

#[test]
fn boolean_key_frames_preserve_leaf_bytes_across_width_and_payload_boundaries() {
    let snapshot = snapshot();
    let catalog = value_catalog();
    for (and, width, payload) in [
        (false, 2, 64),
        (true, 2, 64),
        (false, MAX_CHECK_EXPR_V1_CHILDREN, 16),
        (true, MAX_CHECK_EXPR_V1_CHILDREN, 16),
    ] {
        let children = (0..width)
            .map(|ordinal| {
                let text = format!("{ordinal:02}{}", "x".repeat(payload - 2));
                bind_check_expr_v1(
                    CheckExprV1Input::Compare {
                        left: CheckValueExprV1Input::Field("nickname".into()),
                        op: [
                            AcceptedCheckCompareOpV1::Eq,
                            AcceptedCheckCompareOpV1::Ne,
                            AcceptedCheckCompareOpV1::Lt,
                            AcceptedCheckCompareOpV1::Lte,
                            AcceptedCheckCompareOpV1::Gt,
                            AcceptedCheckCompareOpV1::Gte,
                        ][ordinal % 6],
                        right: CheckValueExprV1Input::Literal(InputValue::text(text)),
                    },
                    &snapshot,
                    catalog.enum_catalog(),
                    catalog.composite_catalog(),
                )
                .unwrap()
            })
            .collect();
        let expression = if and {
            AcceptedCheckExprV1::canonicalized_and(children)
        } else {
            AcceptedCheckExprV1::canonicalized_or(children)
        }
        .unwrap();
        expression
            .validate(&snapshot, catalog.composite_catalog())
            .unwrap();
        let (AcceptedCheckExprV1::And(children) | AcceptedCheckExprV1::Or(children)) = &expression
        else {
            panic!("distinct clauses must retain a Boolean node");
        };
        let key = expression.canonical_key();
        assert_eq!(key[0], if and { 3 } else { 4 });
        let mut remaining = &key[1..];
        assert_eq!(take_length(&mut remaining), width);
        for child in children {
            let length = take_length(&mut remaining);
            let (framed, rest) = remaining.split_at(length);
            assert_eq!(framed, child.canonical_key());
            remaining = rest;
        }
        assert!(remaining.is_empty());
    }
}
