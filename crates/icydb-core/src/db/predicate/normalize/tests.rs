use crate::{
    db::predicate::{
        CoercionId, CoercionSpec, CompareOp, ComparePredicate, Predicate, normalize,
        normalize::{
            normalize_accepted_compare_fields_coercion, normalize_compare_value_for_accepted_kind,
            normalize_compare_value_for_kind, normalize_value_for_accepted_kind,
            normalize_value_for_kind,
        },
    },
    db::schema::AcceptedFieldKind,
    model::field::FieldKind,
    value::Value,
};

#[test]
fn normalize_owned_matches_borrowed_normalize_for_compact_membership_conjunction() {
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

    assert_eq!(
        super::normalize_owned(predicate.clone()),
        normalize(&predicate),
        "owned normalization must preserve borrowed normalization semantics",
    );
}

#[test]
fn normalize_and_dedups_identical_children_and_collapses_to_singleton() {
    let duplicated = Predicate::And(vec![
        Predicate::eq("rank".to_string(), Value::Nat64(7)),
        Predicate::eq("rank".to_string(), Value::Nat64(7)),
    ]);

    let normalized = normalize(&duplicated);

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

    let normalized = normalize(&duplicated);

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

    let normalized = normalize(&mixed);
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

    let normalized = normalize(&mixed);
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

    let normalized = normalize(&predicate);

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

    let normalized = normalize(&predicate);

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

    let normalized = normalize(&predicate);

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

    let normalized = normalize(&predicate);

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

    let normalized = normalize(&predicate);

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

    let normalized = normalize(&predicate);

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

    let normalized = normalize(&predicate);

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

    let normalized = normalize(&predicate);
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

    let normalized = normalize(&predicate);
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

    let normalized = normalize(&predicate);
    let Predicate::Or(children) = normalized else {
        panic!("list-literal OR-equality should remain OR in canonical form");
    };

    assert_eq!(children.len(), 2);
}

#[test]
fn normalize_value_for_set_kind_canonicalizes_members() {
    let normalized = normalize_value_for_kind(
        "tags",
        &Value::List(vec![
            Value::Text("beta".to_string()),
            Value::Text("alpha".to_string()),
            Value::Text("beta".to_string()),
        ]),
        &FieldKind::Set(&FieldKind::Text { max_len: None }),
        &CoercionSpec::new(CoercionId::Strict),
        CompareOp::Eq,
    )
    .expect("set literal normalization should succeed");

    assert_eq!(
        normalized,
        Value::List(vec![
            Value::Text("alpha".to_string()),
            Value::Text("beta".to_string()),
        ]),
        "set literal normalization should sort and deduplicate members",
    );
}

#[test]
fn normalize_compare_value_for_in_kind_canonicalizes_members() {
    let normalized = normalize_compare_value_for_kind(
        "rank",
        CompareOp::In,
        &Value::List(vec![
            Value::Nat64(3),
            Value::Nat64(1),
            Value::Nat64(3),
            Value::Nat64(2),
        ]),
        &FieldKind::Nat64,
        &CoercionSpec::new(CoercionId::Strict),
    )
    .expect("IN literal normalization should succeed");

    assert_eq!(
        normalized,
        Value::List(vec![Value::Nat64(1), Value::Nat64(2), Value::Nat64(3)]),
        "IN literal normalization should sort and deduplicate members",
    );
}

#[test]
fn normalize_compare_value_for_not_in_kind_canonicalizes_members() {
    let normalized = normalize_compare_value_for_kind(
        "rank",
        CompareOp::NotIn,
        &Value::List(vec![
            Value::Nat64(3),
            Value::Nat64(1),
            Value::Nat64(3),
            Value::Nat64(2),
        ]),
        &FieldKind::Nat64,
        &CoercionSpec::new(CoercionId::Strict),
    )
    .expect("NOT IN literal normalization should succeed");

    assert_eq!(
        normalized,
        Value::List(vec![Value::Nat64(1), Value::Nat64(2), Value::Nat64(3)]),
        "NOT IN literal normalization should sort and deduplicate members",
    );
}

#[test]
fn accepted_numeric_membership_normalization_matches_model_only_shape() {
    let value = Value::List(vec![
        Value::Int64(3),
        Value::Nat64(1),
        Value::Int64(3),
        Value::Nat64(2),
    ]);
    let coercion = CoercionSpec::new(CoercionId::Strict);

    let accepted = normalize_compare_value_for_accepted_kind(
        "rank",
        CompareOp::In,
        &value,
        &AcceptedFieldKind::Nat64,
        &coercion,
    )
    .expect("accepted membership normalization should succeed");
    let model = normalize_compare_value_for_kind(
        "rank",
        CompareOp::In,
        &value,
        &FieldKind::Nat64,
        &coercion,
    )
    .expect("model-only membership normalization should succeed");

    assert_eq!(accepted, model);
    assert_eq!(
        accepted,
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
