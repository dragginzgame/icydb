use super::*;

#[test]
fn fingerprint_and_signature_are_stable_for_reordered_and_non_canonical_map_predicates() {
    let map_a = Value::Map(vec![
        (Value::Text("z".to_string()), Value::Int(9)),
        (Value::Text("a".to_string()), Value::Int(1)),
    ]);
    let map_b = Value::Map(vec![
        (Value::Text("a".to_string()), Value::Int(1)),
        (Value::Text("z".to_string()), Value::Int(9)),
    ]);

    let predicate_a = Predicate::And(vec![
        Predicate::eq("other".to_string(), Value::Text("x".to_string())),
        Predicate::Compare(ComparePredicate::eq("meta".to_string(), map_a)),
    ]);
    let predicate_b = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::eq("meta".to_string(), map_b)),
        Predicate::eq("other".to_string(), Value::Text("x".to_string())),
    ]);

    let mut plan_a: AccessPlannedQuery = full_scan_query();
    plan_a.scalar_plan_mut().predicate = Some(predicate_a);

    let mut plan_b: AccessPlannedQuery = full_scan_query();
    plan_b.scalar_plan_mut().predicate = Some(predicate_b);

    assert_eq!(plan_a.fingerprint(), plan_b.fingerprint());
    assert_eq!(
        plan_a.continuation_signature("tests::Entity"),
        plan_b.continuation_signature("tests::Entity")
    );
}

#[test]
fn fingerprint_and_signature_treat_equivalent_decimal_predicate_literals_as_identical() {
    let predicate_a = Predicate::Compare(ComparePredicate::eq(
        "rank".to_string(),
        Value::Decimal(Decimal::new(10, 1)),
    ));
    let predicate_b = Predicate::Compare(ComparePredicate::eq(
        "rank".to_string(),
        Value::Decimal(Decimal::new(100, 2)),
    ));

    let mut plan_a: AccessPlannedQuery = full_scan_query();
    plan_a.scalar_plan_mut().predicate = Some(predicate_a);

    let mut plan_b: AccessPlannedQuery = full_scan_query();
    plan_b.scalar_plan_mut().predicate = Some(predicate_b);

    assert_eq!(plan_a.fingerprint(), plan_b.fingerprint());
    assert_eq!(
        plan_a.continuation_signature("tests::Entity"),
        plan_b.continuation_signature("tests::Entity")
    );
}

#[test]
fn fingerprint_and_signature_treat_equivalent_in_list_predicates_as_identical() {
    let predicate_a = Predicate::Compare(ComparePredicate::in_(
        "rank".to_string(),
        vec![Value::Uint(3), Value::Uint(1), Value::Uint(2)],
    ));
    let predicate_b = Predicate::Compare(ComparePredicate::in_(
        "rank".to_string(),
        vec![Value::Uint(1), Value::Uint(2), Value::Uint(3)],
    ));

    let mut plan_a: AccessPlannedQuery = full_scan_query();
    plan_a.scalar_plan_mut().predicate = Some(predicate_a);

    let mut plan_b: AccessPlannedQuery = full_scan_query();
    plan_b.scalar_plan_mut().predicate = Some(predicate_b);

    assert_eq!(plan_a.fingerprint(), plan_b.fingerprint());
    assert_eq!(
        plan_a.continuation_signature("tests::Entity"),
        plan_b.continuation_signature("tests::Entity")
    );
}

#[test]
fn fingerprint_and_signature_treat_same_field_or_eq_and_in_as_identical() {
    let predicate_or_eq = Predicate::Or(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::Eq,
            Value::Uint(3),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::Eq,
            Value::Uint(1),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::Eq,
            Value::Uint(3),
            CoercionId::Strict,
        )),
    ]);
    let predicate_in = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::In,
        Value::List(vec![Value::Uint(1), Value::Uint(3)]),
        CoercionId::Strict,
    ));

    let mut plan_or_eq: AccessPlannedQuery = full_scan_query();
    plan_or_eq.scalar_plan_mut().predicate = Some(predicate_or_eq);

    let mut plan_in: AccessPlannedQuery = full_scan_query();
    plan_in.scalar_plan_mut().predicate = Some(predicate_in);

    assert_eq!(plan_or_eq.fingerprint(), plan_in.fingerprint());
    assert_eq!(
        plan_or_eq.continuation_signature("tests::Entity"),
        plan_in.continuation_signature("tests::Entity")
    );
}

#[test]
fn fingerprint_and_signature_treat_equivalent_in_list_duplicate_literals_as_identical() {
    let predicate_a = Predicate::Compare(ComparePredicate::in_(
        "rank".to_string(),
        vec![
            Value::Uint(3),
            Value::Uint(1),
            Value::Uint(3),
            Value::Uint(2),
        ],
    ));
    let predicate_b = Predicate::Compare(ComparePredicate::in_(
        "rank".to_string(),
        vec![Value::Uint(1), Value::Uint(2), Value::Uint(3)],
    ));

    let mut plan_a: AccessPlannedQuery = full_scan_query();
    plan_a.scalar_plan_mut().predicate = Some(predicate_a);

    let mut plan_b: AccessPlannedQuery = full_scan_query();
    plan_b.scalar_plan_mut().predicate = Some(predicate_b);

    assert_eq!(plan_a.fingerprint(), plan_b.fingerprint());
    assert_eq!(
        plan_a.continuation_signature("tests::Entity"),
        plan_b.continuation_signature("tests::Entity")
    );
}

#[test]
fn fingerprint_and_signature_treat_implicit_and_explicit_strict_coercion_as_identical() {
    let predicate_a = Predicate::Compare(ComparePredicate::eq("rank".to_string(), Value::Int(7)));
    let predicate_b = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Eq,
        Value::Int(7),
        CoercionId::Strict,
    ));

    let mut plan_a: AccessPlannedQuery = full_scan_query();
    plan_a.scalar_plan_mut().predicate = Some(predicate_a);

    let mut plan_b: AccessPlannedQuery = full_scan_query();
    plan_b.scalar_plan_mut().predicate = Some(predicate_b);

    assert_eq!(plan_a.fingerprint(), plan_b.fingerprint());
    assert_eq!(
        plan_a.continuation_signature("tests::Entity"),
        plan_b.continuation_signature("tests::Entity")
    );
}

#[test]
fn fingerprint_and_signature_distinguish_different_coercion_ids() {
    let predicate_strict = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Eq,
        Value::Int(7),
        CoercionId::Strict,
    ));
    let predicate_numeric_widen = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Eq,
        Value::Int(7),
        CoercionId::NumericWiden,
    ));

    let mut strict_plan: AccessPlannedQuery = full_scan_query();
    strict_plan.scalar_plan_mut().predicate = Some(predicate_strict);

    let mut numeric_widen_plan: AccessPlannedQuery = full_scan_query();
    numeric_widen_plan.scalar_plan_mut().predicate = Some(predicate_numeric_widen);

    assert_ne!(strict_plan.fingerprint(), numeric_widen_plan.fingerprint());
    assert_ne!(
        strict_plan.continuation_signature("tests::Entity"),
        numeric_widen_plan.continuation_signature("tests::Entity")
    );
}

#[test]
fn fingerprint_and_signature_treat_numeric_widen_equivalent_literal_subtypes_as_identical() {
    let predicate_int = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Eq,
        Value::Int(1),
        CoercionId::NumericWiden,
    ));
    let predicate_decimal = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Eq,
        Value::Decimal(Decimal::new(10, 1)),
        CoercionId::NumericWiden,
    ));

    let mut int_plan: AccessPlannedQuery = full_scan_query();
    int_plan.scalar_plan_mut().predicate = Some(predicate_int);

    let mut decimal_plan: AccessPlannedQuery = full_scan_query();
    decimal_plan.scalar_plan_mut().predicate = Some(predicate_decimal);

    assert_eq!(int_plan.fingerprint(), decimal_plan.fingerprint());
    assert_eq!(
        int_plan.continuation_signature("tests::Entity"),
        decimal_plan.continuation_signature("tests::Entity")
    );
}

#[test]
fn fingerprint_and_signature_treat_text_casefold_case_only_literals_as_identical() {
    let predicate_lower = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::Eq,
        Value::Text("ada".to_string()),
        CoercionId::TextCasefold,
    ));
    let predicate_upper = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::Eq,
        Value::Text("ADA".to_string()),
        CoercionId::TextCasefold,
    ));

    let mut lower_plan: AccessPlannedQuery = full_scan_query();
    lower_plan.scalar_plan_mut().predicate = Some(predicate_lower);

    let mut upper_plan: AccessPlannedQuery = full_scan_query();
    upper_plan.scalar_plan_mut().predicate = Some(predicate_upper);

    assert_eq!(lower_plan.fingerprint(), upper_plan.fingerprint());
    assert_eq!(
        lower_plan.continuation_signature("tests::Entity"),
        upper_plan.continuation_signature("tests::Entity")
    );
}

#[test]
fn fingerprint_and_signature_keep_strict_text_case_variants_distinct() {
    let predicate_lower = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::Eq,
        Value::Text("ada".to_string()),
        CoercionId::Strict,
    ));
    let predicate_upper = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::Eq,
        Value::Text("ADA".to_string()),
        CoercionId::Strict,
    ));

    let mut lower_plan: AccessPlannedQuery = full_scan_query();
    lower_plan.scalar_plan_mut().predicate = Some(predicate_lower);

    let mut upper_plan: AccessPlannedQuery = full_scan_query();
    upper_plan.scalar_plan_mut().predicate = Some(predicate_upper);

    assert_ne!(lower_plan.fingerprint(), upper_plan.fingerprint());
    assert_ne!(
        lower_plan.continuation_signature("tests::Entity"),
        upper_plan.continuation_signature("tests::Entity")
    );
}

#[test]
fn fingerprint_and_signature_treat_text_casefold_in_list_case_variants_as_identical() {
    let predicate_mixed = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::In,
        Value::List(vec![
            Value::Text("ADA".to_string()),
            Value::Text("ada".to_string()),
            Value::Text("Bob".to_string()),
        ]),
        CoercionId::TextCasefold,
    ));
    let predicate_canonical = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::In,
        Value::List(vec![
            Value::Text("ada".to_string()),
            Value::Text("bob".to_string()),
        ]),
        CoercionId::TextCasefold,
    ));

    let mut mixed_plan: AccessPlannedQuery = full_scan_query();
    mixed_plan.scalar_plan_mut().predicate = Some(predicate_mixed);

    let mut canonical_plan: AccessPlannedQuery = full_scan_query();
    canonical_plan.scalar_plan_mut().predicate = Some(predicate_canonical);

    assert_eq!(mixed_plan.fingerprint(), canonical_plan.fingerprint());
    assert_eq!(
        mixed_plan.continuation_signature("tests::Entity"),
        canonical_plan.continuation_signature("tests::Entity")
    );
}

#[test]
fn fingerprint_and_signature_distinguish_strict_from_text_casefold_coercion() {
    let predicate_strict = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::Eq,
        Value::Text("ada".to_string()),
        CoercionId::Strict,
    ));
    let predicate_casefold = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::Eq,
        Value::Text("ada".to_string()),
        CoercionId::TextCasefold,
    ));

    let mut strict_plan: AccessPlannedQuery = full_scan_query();
    strict_plan.scalar_plan_mut().predicate = Some(predicate_strict);

    let mut casefold_plan: AccessPlannedQuery = full_scan_query();
    casefold_plan.scalar_plan_mut().predicate = Some(predicate_casefold);

    assert_ne!(strict_plan.fingerprint(), casefold_plan.fingerprint());
    assert_ne!(
        strict_plan.continuation_signature("tests::Entity"),
        casefold_plan.continuation_signature("tests::Entity")
    );
}

#[test]
fn fingerprint_and_signature_distinguish_strict_from_collection_element_coercion() {
    let predicate_strict = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Eq,
        Value::Int(7),
        CoercionId::Strict,
    ));
    let predicate_collection_element = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Eq,
        Value::Int(7),
        CoercionId::CollectionElement,
    ));

    let mut strict_plan: AccessPlannedQuery = full_scan_query();
    strict_plan.scalar_plan_mut().predicate = Some(predicate_strict);

    let mut collection_plan: AccessPlannedQuery = full_scan_query();
    collection_plan.scalar_plan_mut().predicate = Some(predicate_collection_element);

    assert_ne!(strict_plan.fingerprint(), collection_plan.fingerprint());
    assert_ne!(
        strict_plan.continuation_signature("tests::Entity"),
        collection_plan.continuation_signature("tests::Entity")
    );
}
