use crate::{
    db::predicate::{
        CoercionId, CoercionSpec, CompareOp, ComparePredicate, Predicate, normalize,
        normalize::{
            normalize_accepted_compare_fields_coercion, normalize_compare_value_for_accepted_kind,
            normalize_value_for_accepted_kind,
        },
    },
    db::schema::AcceptedFieldKind,
    value::Value,
};

#[test]
fn normalize_compact_membership_conjunction_is_idempotent() {
    let predicate = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "stage",
            CompareOp::In,
            Value::List(vec![
                Value::Text("Review".to_string()),
                Value::Text("Draft".to_string()),
            ]),
            CoercionId::Strict,
        )),
        Predicate::eq(
            "collection_id".to_string(),
            Value::Text("01KV5N439P0000000000000000".to_string()),
        ),
        Predicate::Not(Box::new(Predicate::Not(Box::new(Predicate::eq(
            "rank".to_string(),
            Value::Nat64(7),
        ))))),
    ]);

    let normalized = normalize(predicate);
    assert_eq!(normalize(normalized.clone()), normalized);
}

#[test]
fn normalize_moves_nested_text_operands_through_identity_rewrites() {
    let field = "description".to_string();
    let value = "payload".repeat(128);
    let field_backing = field.as_ptr();
    let value_backing = value.as_ptr();
    let predicate = Predicate::Or(vec![
        Predicate::False,
        Predicate::And(vec![
            Predicate::True,
            Predicate::Not(Box::new(Predicate::Not(Box::new(
                Predicate::TextContains {
                    field,
                    value: Value::Text(value),
                },
            )))),
        ]),
    ]);

    let Predicate::TextContains {
        field,
        value: Value::Text(value),
    } = normalize(predicate)
    else {
        panic!("identity rewrites preserve the text predicate");
    };
    assert_eq!(field, "description");
    assert_eq!(value, "payload".repeat(128));
    assert_eq!(field.as_ptr(), field_backing);
    assert_eq!(value.as_ptr(), value_backing);
}

#[test]
fn normalize_nested_negation_preserves_constants_and_canonical_children() {
    let leaf = Predicate::eq("rank".to_string(), Value::Nat64(7));
    for predicate in [
        Predicate::True,
        Predicate::False,
        leaf.clone(),
        Predicate::And(vec![Predicate::True, leaf.clone(), leaf.clone()]),
        Predicate::Or(vec![Predicate::False, leaf.clone(), leaf]),
    ] {
        let expected = normalize(predicate.clone());
        for depth in [1, 2, 3, 4, 32] {
            let mut nested = predicate.clone();
            for _ in 0..depth {
                nested = Predicate::Not(Box::new(nested));
            }
            let normalized = normalize(nested);
            let expected = if depth % 2 == 0 {
                expected.clone()
            } else {
                Predicate::Not(Box::new(expected.clone()))
            };
            assert_eq!(normalized, expected);
            assert_eq!(normalize(normalized.clone()), normalized);
        }
    }
}

#[test]
fn normalize_and_dedups_identical_children_and_collapses_to_singleton() {
    let duplicated = Predicate::And(vec![
        Predicate::eq("rank".to_string(), Value::Nat64(7)),
        Predicate::eq("rank".to_string(), Value::Nat64(7)),
    ]);

    let normalized = normalize(duplicated);

    assert_eq!(
        normalized,
        Predicate::Compare(ComparePredicate::eq("rank".to_string(), Value::Nat64(7))),
        "identical AND children should collapse to one predicate",
    );
}

#[test]
fn normalize_or_dedups_identical_children_and_collapses_to_singleton() {
    let duplicated = Predicate::Or(vec![
        Predicate::eq("rank".to_string(), Value::Nat64(7)),
        Predicate::eq("rank".to_string(), Value::Nat64(7)),
    ]);

    let normalized = normalize(duplicated);

    assert_eq!(
        normalized,
        Predicate::Compare(ComparePredicate::eq("rank".to_string(), Value::Nat64(7))),
        "identical OR children should collapse to one predicate",
    );
}

#[test]
fn normalize_and_orders_cheaper_predicates_before_text_contains() {
    let mixed = Predicate::And(vec![
        Predicate::TextContains {
            field: "name".to_string(),
            value: Value::Text("ada".to_string()),
        },
        Predicate::eq("rank".to_string(), Value::Nat64(7)),
    ]);

    let normalized = normalize(mixed);
    let Predicate::And(children) = normalized else {
        panic!("normalized mixed predicate should remain AND with two children");
    };
    assert_eq!(
        children.len(),
        2,
        "mixed AND should keep exactly two children"
    );
    assert!(
        matches!(children[0], Predicate::Compare(_)),
        "cheap compare predicate should be evaluated before text-contains predicate",
    );
    assert!(
        matches!(children[1], Predicate::TextContains { .. }),
        "text-contains predicate should be placed after cheap compare predicate",
    );
}

#[test]
fn normalize_and_orders_scalar_compares_before_membership() {
    let mixed = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "stage",
            CompareOp::In,
            Value::List(vec![
                Value::Text("Draft".to_string()),
                Value::Text("Review".to_string()),
            ]),
            CoercionId::Strict,
        )),
        Predicate::eq(
            "collection_id".to_string(),
            Value::Text("01KV5N439P0000000000000000".to_string()),
        ),
    ]);

    let normalized = normalize(mixed);
    let Predicate::And(children) = normalized else {
        panic!("normalized mixed predicate should remain AND with two children");
    };
    assert_eq!(
        children.len(),
        2,
        "mixed AND should keep exactly two children"
    );
    assert!(
        matches!(
            children[0],
            Predicate::Compare(ComparePredicate {
                op: CompareOp::Eq,
                ..
            })
        ),
        "scalar compare predicate should be evaluated before membership predicate",
    );
    assert!(
        matches!(
            children[1],
            Predicate::Compare(ComparePredicate {
                op: CompareOp::In,
                ..
            })
        ),
        "membership predicate should be placed after scalar compare predicate",
    );
}

#[test]
fn normalize_and_conflicting_eq_literals_collapses_to_false() {
    let predicate = Predicate::And(vec![
        Predicate::eq("rank".to_string(), Value::Nat64(1)),
        Predicate::eq("rank".to_string(), Value::Nat64(2)),
    ]);

    let normalized = normalize(predicate);

    assert_eq!(
        normalized,
        Predicate::False,
        "conflicting equalities in conjunction must collapse to false",
    );
}

#[test]
fn normalize_and_tightens_lower_bounds() {
    let predicate = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::gt("rank".to_string(), Value::Nat64(3))),
        Predicate::Compare(ComparePredicate::gte("rank".to_string(), Value::Nat64(5))),
    ]);

    let normalized = normalize(predicate);

    assert_eq!(
        normalized,
        Predicate::Compare(ComparePredicate::gte("rank".to_string(), Value::Nat64(5))),
        "conjunction should keep the stricter lower bound",
    );
}

#[test]
fn normalize_and_tightens_upper_bounds() {
    let predicate = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::lt("rank".to_string(), Value::Nat64(9))),
        Predicate::Compare(ComparePredicate::lte("rank".to_string(), Value::Nat64(7))),
    ]);

    let normalized = normalize(predicate);

    assert_eq!(
        normalized,
        Predicate::Compare(ComparePredicate::lte("rank".to_string(), Value::Nat64(7))),
        "conjunction should keep the stricter upper bound",
    );
}

#[test]
fn normalize_and_eq_with_satisfied_bound_collapses_to_eq() {
    let predicate = Predicate::And(vec![
        Predicate::eq("rank".to_string(), Value::Nat64(7)),
        Predicate::Compare(ComparePredicate::gt("rank".to_string(), Value::Nat64(5))),
    ]);

    let normalized = normalize(predicate);

    assert_eq!(
        normalized,
        Predicate::Compare(ComparePredicate::eq("rank".to_string(), Value::Nat64(7))),
        "equality should subsume compatible lower-bound constraints",
    );
}

#[test]
fn normalize_and_eq_with_conflicting_bound_collapses_to_false() {
    let predicate = Predicate::And(vec![
        Predicate::eq("rank".to_string(), Value::Nat64(3)),
        Predicate::Compare(ComparePredicate::gt("rank".to_string(), Value::Nat64(5))),
    ]);

    let normalized = normalize(predicate);

    assert_eq!(
        normalized,
        Predicate::False,
        "equality conflicting with a bound must collapse to false",
    );
}

#[test]
fn normalize_and_equal_lower_and_upper_collapse_to_eq() {
    let predicate = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::Gte,
            Value::Nat64(11),
            crate::db::predicate::CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::Lte,
            Value::Nat64(11),
            crate::db::predicate::CoercionId::Strict,
        )),
    ]);

    let normalized = normalize(predicate);

    assert_eq!(
        normalized,
        Predicate::Compare(ComparePredicate::eq("rank".to_string(), Value::Nat64(11))),
        "matching inclusive lower/upper bounds should collapse to equality",
    );
}

#[test]
fn normalize_and_crossed_bounds_collapse_to_false() {
    let predicate = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::gt("rank".to_string(), Value::Nat64(9))),
        Predicate::Compare(ComparePredicate::lt("rank".to_string(), Value::Nat64(5))),
    ]);

    let normalized = normalize(predicate);

    assert_eq!(
        normalized,
        Predicate::False,
        "crossed lower/upper bounds must collapse to false",
    );
}

#[test]
fn normalize_or_same_field_eq_collapses_to_in() {
    let predicate = Predicate::Or(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "tag",
            CompareOp::Eq,
            Value::Text("beta".to_string()),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "tag",
            CompareOp::Eq,
            Value::Text("alpha".to_string()),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "tag",
            CompareOp::Eq,
            Value::Text("beta".to_string()),
            CoercionId::Strict,
        )),
    ]);

    let normalized = normalize(predicate);
    let Predicate::Compare(compare) = normalized else {
        panic!("same-field strict OR-equality should collapse to one IN compare");
    };

    assert_eq!(compare.field, "tag".to_string());
    assert_eq!(compare.op, CompareOp::In);
    assert_eq!(compare.coercion.id, CoercionId::Strict);
    let Value::List(mut values) = compare.value else {
        panic!("collapsed OR-equality compare should carry list literal");
    };
    values.sort_by(Value::canonical_cmp);
    assert_eq!(
        values,
        vec![
            Value::Text("alpha".to_string()),
            Value::Text("beta".to_string()),
        ],
        "same-field strict OR-equality should collapse to deduplicated IN-list members",
    );
}

#[test]
fn normalize_or_mixed_eq_coercions_do_not_collapse_to_in() {
    let predicate = Predicate::Or(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "tag",
            CompareOp::Eq,
            Value::Text("alpha".to_string()),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "tag",
            CompareOp::Eq,
            Value::Text("beta".to_string()),
            CoercionId::TextCasefold,
        )),
    ]);

    let normalized = normalize(predicate);
    let Predicate::Or(children) = normalized else {
        panic!("mixed coercion OR-equality should remain OR in canonical form");
    };

    assert_eq!(children.len(), 2);
}

#[test]
fn normalize_or_list_equality_literals_do_not_collapse_to_in() {
    let predicate = Predicate::Or(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "tags",
            CompareOp::Eq,
            Value::List(vec![Value::Text("a".to_string())]),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "tags",
            CompareOp::Eq,
            Value::List(vec![Value::Text("b".to_string())]),
            CoercionId::Strict,
        )),
    ]);

    let normalized = normalize(predicate);
    let Predicate::Or(children) = normalized else {
        panic!("list-literal OR-equality should remain OR in canonical form");
    };

    assert_eq!(children.len(), 2);
}

#[test]
fn accepted_numeric_membership_normalization_is_canonical() {
    let value = Value::List(vec![
        Value::Int64(3),
        Value::Nat64(1),
        Value::Int64(3),
        Value::Nat64(2),
    ]);
    let coercion = CoercionSpec::new(CoercionId::Strict);

    let normalized = normalize_compare_value_for_accepted_kind(
        "rank",
        CompareOp::In,
        &value,
        &AcceptedFieldKind::Nat64,
        &coercion,
    )
    .expect("accepted membership normalization should succeed");

    assert_eq!(
        normalized,
        Value::List(vec![Value::Nat64(1), Value::Nat64(2), Value::Nat64(3)]),
    );
}

#[test]
fn accepted_recursive_set_normalization_is_canonical() {
    let normalized = normalize_value_for_accepted_kind(
        "tags",
        &Value::List(vec![
            Value::Text("beta".to_string()),
            Value::Text("alpha".to_string()),
            Value::Text("beta".to_string()),
        ]),
        &AcceptedFieldKind::Set(Box::new(AcceptedFieldKind::Text { max_len: None })),
        &CoercionSpec::new(CoercionId::Strict),
        CompareOp::Eq,
    )
    .expect("accepted set normalization should succeed");

    assert_eq!(
        normalized,
        Value::List(vec![
            Value::Text("alpha".to_string()),
            Value::Text("beta".to_string()),
        ]),
    );
}

#[test]
fn accepted_field_comparison_coercion_uses_accepted_semantics() {
    assert_eq!(
        normalize_accepted_compare_fields_coercion(
            CompareOp::Eq,
            &AcceptedFieldKind::Int64,
            &AcceptedFieldKind::Nat64,
            CoercionId::Strict,
        ),
        CoercionId::NumericWiden,
    );
    assert_eq!(
        normalize_accepted_compare_fields_coercion(
            CompareOp::Lt,
            &AcceptedFieldKind::Text { max_len: None },
            &AcceptedFieldKind::Text { max_len: Some(32) },
            CoercionId::TextCasefold,
        ),
        CoercionId::Strict,
    );
}
