//! Canonical byte order must survive reuse of differently sized scratch keys.

use super::*;

fn comparison(op: AcceptedCheckCompareOpV1, text: String) -> AcceptedCheckExprV1 {
    let catalog = value_catalog();
    bind_check_expr_v1(
        CheckExprV1Input::Compare {
            left: CheckValueExprV1Input::Field("nickname".into()),
            op,
            right: CheckValueExprV1Input::Literal(InputValue::text(text)),
        },
        &snapshot(),
        catalog.enum_catalog(),
        catalog.composite_catalog(),
    )
    .unwrap()
}

fn canonicalize(and: bool, children: Vec<AcceptedCheckExprV1>) -> AcceptedCheckExprV1 {
    if and {
        AcceptedCheckExprV1::canonicalized_and(children)
    } else {
        AcceptedCheckExprV1::canonicalized_or(children)
    }
    .unwrap()
}

// The reference keeps the original fresh owned keys and uses Rust's standard
// sorter, independently of the maintained compact sort and scratch lifetimes.
fn reference(and: bool, mut children: Vec<AcceptedCheckExprV1>) -> AcceptedCheckExprV1 {
    children.sort_unstable_by_key(AcceptedCheckExprV1::canonical_key);
    children.dedup();
    if children.len() == 1 {
        children.pop().unwrap()
    } else if and {
        AcceptedCheckExprV1::And(children)
    } else {
        AcceptedCheckExprV1::Or(children)
    }
}

#[test]
fn canonical_order_matches_owned_key_reference_across_size_changes_and_duplicates() {
    let pool = [
        AcceptedCheckExprV1::False,
        AcceptedCheckExprV1::Not(Box::new(AcceptedCheckExprV1::True)),
        comparison(AcceptedCheckCompareOpV1::Eq, String::new()),
        comparison(AcceptedCheckCompareOpV1::Ne, "é🦀".repeat(32)),
        comparison(AcceptedCheckCompareOpV1::Lt, "a".into()),
        comparison(AcceptedCheckCompareOpV1::Lte, "long".repeat(16)),
        comparison(AcceptedCheckCompareOpV1::Gt, "\u{feff}".into()),
        comparison(AcceptedCheckCompareOpV1::Gte, "z".repeat(64)),
    ];
    let snapshot = snapshot();
    let catalog = value_catalog();
    for and in [false, true] {
        for first in &pool {
            for second in &pool {
                for third in &pool {
                    let children = vec![first.clone(), second.clone(), third.clone()];
                    let expected = reference(and, children.clone());
                    let actual = canonicalize(and, children);
                    assert_eq!(actual, expected);
                    actual
                        .validate(&snapshot, catalog.composite_catalog())
                        .unwrap();
                    actual.validate_snapshot_local(snapshot.fields()).unwrap();
                    // Same-family flattening and duplicate elimination retain
                    // exactly the same canonical result, including singleton output.
                    assert_eq!(canonicalize(and, vec![actual.clone(), actual]), expected);
                }
            }
        }
    }
}

#[test]
fn canonical_order_handles_maximum_width_and_nested_opposite_booleans() {
    for and in [false, true] {
        let mut children = (0..MAX_CHECK_EXPR_V1_CHILDREN)
            .map(|ordinal| comparison(AcceptedCheckCompareOpV1::Eq, format!("{ordinal:02}")))
            .collect::<Vec<_>>();
        children[0] = canonicalize(!and, vec![children[0].clone(), children[1].clone()]);
        children.reverse();
        let expected = reference(and, children.clone());
        let actual = canonicalize(and, children);
        assert_eq!(actual, expected);
        actual
            .validate(&snapshot(), value_catalog().composite_catalog())
            .unwrap();
    }
}

#[test]
fn validation_refuses_equal_and_reversed_adjacent_keys_with_the_same_typed_error() {
    for and in [false, true] {
        let lower = comparison(AcceptedCheckCompareOpV1::Eq, String::new());
        let upper = comparison(AcceptedCheckCompareOpV1::Eq, "🦀".repeat(64));
        let valid = canonicalize(and, vec![upper, lower]);
        let (AcceptedCheckExprV1::And(children) | AcceptedCheckExprV1::Or(children)) = valid else {
            panic!("distinct children must retain their Boolean node");
        };
        for invalid in [
            vec![children[1].clone(), children[0].clone()],
            vec![children[0].clone(); 2],
        ] {
            let expression = if and {
                AcceptedCheckExprV1::And(invalid)
            } else {
                AcceptedCheckExprV1::Or(invalid)
            };
            let snapshot = snapshot();
            assert_eq!(
                expression.validate(&snapshot, value_catalog().composite_catalog()),
                Err(AcceptedCheckExprV1Error::NonCanonical)
            );
            assert_eq!(
                expression.validate_snapshot_local(snapshot.fields()),
                Err(AcceptedCheckExprV1Error::NonCanonical)
            );
        }
    }
}
