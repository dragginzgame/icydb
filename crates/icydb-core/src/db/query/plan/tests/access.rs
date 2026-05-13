use super::*;

type ExpressionStartsWithRangeCase<'a> = (
    &'a str,
    fn() -> &'static EntityModel,
    &'a str,
    &'a str,
    &'a str,
);

// Assert one text-casefold `STARTS_WITH` route against the shared expression
// range-lowering contract.
fn assert_expression_starts_with_range_case(
    label: &str,
    model_factory: fn() -> &'static EntityModel,
    raw_value: &str,
    expected_index_name: &str,
    expected_lower: &str,
) {
    let model = model_factory();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = compare_text_casefold(
        "email",
        CompareOp::StartsWith,
        Value::Text(raw_value.to_string()),
    );

    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");
    let (index, prefix, lower, upper) =
        find_index_range(&plan).expect("text-casefold starts-with should plan index range");

    assert_eq!(index.name(), expected_index_name, "{label}: wrong index");
    assert!(
        prefix.is_empty(),
        "{label}: starts-with expression ranges should not carry equality prefix values",
    );
    assert_eq!(
        lower,
        &Bound::Included(Value::Text(expected_lower.to_string())),
        "{label}: wrong lower bound",
    );
    assert_eq!(upper, &Bound::Unbounded, "{label}: wrong upper bound");
}

// Assert one strict text `STARTS_WITH` route against the shared single-field
// range-lowering contract.
fn assert_strict_text_starts_with_range_case(
    label: &str,
    prefix: &str,
    expected_lower: Bound<Value>,
    expected_upper: Bound<Value>,
) {
    let model = model_with_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = compare_strict(
        "tag",
        CompareOp::StartsWith,
        Value::Text(prefix.to_string()),
    );

    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");
    assert_single_field_text_index_range(&plan, expected_lower, expected_upper);

    assert!(
        compile_runtime_predicate_for_test(model, &predicate).uses_scalar_program(),
        "{label}: starts-with should compile onto the scalar executor for scalar text fields",
    );
}

// Assert one strict text `STARTS_WITH` predicate and its equivalent range form
// canonicalize to the same access plan.
fn assert_starts_with_equivalent_range_case(
    label: &str,
    starts_with: Predicate,
    equivalent_range: Predicate,
) {
    let model = model_with_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let starts_with_plan = plan_access_for_test(model, schema, Some(&starts_with))
        .expect("starts_with plan should build");
    let equivalent_range_plan = plan_access_for_test(model, schema, Some(&equivalent_range))
        .expect("equivalent range plan should build");

    assert_eq!(
        starts_with_plan, equivalent_range_plan,
        "{label}: equivalent predicates should canonicalize to identical access plans",
    );
}

// Assert one composite `(a = ?, b range)` predicate lowers to the expected
// shared range-index access shape.
fn assert_composite_index_range_case(
    label: &str,
    predicate: Predicate,
    expected_prefix: &[Value],
    expected_lower: Bound<Value>,
    expected_upper: Bound<Value>,
) {
    let model = model_with_range_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");
    let (index, prefix, lower, upper) =
        find_index_range(&plan).expect("plan should include index range");

    assert_eq!(
        index.name(),
        RANGE_INDEX_MODEL.name(),
        "{label}: wrong index"
    );
    assert_eq!(prefix, expected_prefix, "{label}: wrong equality prefix");
    assert_eq!(lower, &expected_lower, "{label}: wrong lower bound");
    assert_eq!(upper, &expected_upper, "{label}: wrong upper bound");
}

// Assert one invalid composite range predicate stays fail-closed and does not
// emit an index-range access path.
fn assert_rejected_composite_index_range_case(label: &str, predicate: Predicate) {
    let model = model_with_range_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");

    assert!(
        find_index_range(&plan).is_none(),
        "{label}: invalid range shape must not emit an index-range access path",
    );
}

// Assert two equivalent canonicalization inputs lower to the same expected
// access plan after sorting, deduplication, and bounded rewrite passes.
fn assert_equivalent_access_plan_case(
    label: &str,
    model_factory: fn() -> &'static EntityModel,
    left: Predicate,
    right: Predicate,
    expected_plan: AccessPlan<Value>,
) {
    let model = model_factory();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let left_plan =
        plan_access_for_test(model, schema, Some(&left)).expect("left plan should build");
    let right_plan =
        plan_access_for_test(model, schema, Some(&right)).expect("right plan should build");

    assert_eq!(
        left_plan, right_plan,
        "{label}: equivalent predicates should canonicalize to identical access plans",
    );
    assert_eq!(
        left_plan, expected_plan,
        "{label}: canonicalized access plan drifted from the expected normalized shape",
    );
}

// Assert one canonicalization input lowers to the expected normalized access
// plan or fail-closed full-scan shape.
fn assert_access_plan_case(
    label: &str,
    model_factory: fn() -> &'static EntityModel,
    predicate: Predicate,
    expected_plan: AccessPlan<Value>,
) {
    let model = model_factory();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan, expected_plan,
        "{label}: canonicalized access plan drifted from the expected normalized shape",
    );
}

#[test]
fn plan_access_full_scan_without_predicate() {
    let model = model_with_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let plan = plan_access_for_test(model, schema, None).expect("plan should build");

    assert_eq!(plan, AccessPlan::full_scan());
}

#[test]
fn plan_access_primary_key_is_null_lowers_to_empty_by_keys() {
    let model = model_with_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = Predicate::IsNull {
        field: "id".to_string(),
    };

    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::path(AccessPath::ByKeys(Vec::new())),
        "primary_key IS NULL is unsatisfiable and should lower to explicit empty access shape",
    );
}

#[test]
fn plan_access_secondary_is_null_retains_full_scan_fallback() {
    let model = model_with_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = Predicate::IsNull {
        field: "tag".to_string(),
    };

    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::full_scan(),
        "non-primary IS NULL remains full-scan until nullable/index-aware pushdown is available",
    );
}

#[test]
fn plan_access_secondary_is_null_can_compile_scalar_while_still_full_scanning() {
    let model = model_with_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = Predicate::IsNull {
        field: "tag".to_string(),
    };

    let runtime = compile_runtime_predicate_for_test(model, &predicate);
    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");

    assert!(
        runtime.uses_scalar_program(),
        "runtime should keep secondary-field IS NULL on the scalar slot executor",
    );
    assert_eq!(
        plan,
        AccessPlan::full_scan(),
        "access planning remains a full scan until nullable/index-aware pushdown exists",
    );
}

#[test]
fn plan_access_primary_key_is_null_or_secondary_eq_collapses_to_secondary_branch() {
    let model = model_with_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = Predicate::Or(vec![
        Predicate::IsNull {
            field: "id".to_string(),
        },
        compare_strict("tag", CompareOp::Eq, Value::Text("alpha".to_string())),
    ]);

    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::path(AccessPath::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                INDEX_MODEL
            ),
            values: vec![Value::Text("alpha".to_string())],
        }),
        "primary_key IS NULL is an empty OR-identity and should not widen the surviving branch",
    );
}

#[test]
fn plan_access_primary_key_is_null_or_primary_key_is_null_stays_empty() {
    let model = model_with_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = Predicate::Or(vec![
        Predicate::IsNull {
            field: "id".to_string(),
        },
        Predicate::IsNull {
            field: "id".to_string(),
        },
    ]);

    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::path(AccessPath::ByKeys(Vec::new())),
        "OR over only impossible primary_key IS NULL branches should remain explicit empty access",
    );
}

#[test]
fn plan_access_uses_primary_key_lookup() {
    let model = model_with_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let key = Ulid::generate();
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "id",
        CompareOp::Eq,
        Value::Ulid(key),
        CoercionId::Strict,
    ));

    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");

    assert_eq!(plan, AccessPlan::path(AccessPath::ByKey(Value::Ulid(key))));
}

#[test]
fn plan_access_primary_key_half_open_bounds_lower_to_key_range() {
    let model = model_with_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let lower = Ulid::from_u128(9_811);
    let upper = Ulid::from_u128(9_813);
    let predicate = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::Gte,
            Value::Ulid(lower),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::Lt,
            Value::Ulid(upper),
            CoercionId::Strict,
        )),
    ]);

    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::path(AccessPath::KeyRange {
            start: Value::Ulid(lower),
            end: Value::Ulid(upper),
        }),
        "strict primary-key half-open bounds should lower to one explicit key-range access path",
    );
}

#[test]
fn plan_access_uses_index_prefix_for_exact_match() {
    let model = model_with_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "tag",
        CompareOp::Eq,
        Value::Text("alpha".to_string()),
        CoercionId::Strict,
    ));

    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::path(AccessPath::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                INDEX_MODEL
            ),
            values: vec![Value::Text("alpha".to_string())],
        })
    );
}

#[test]
fn plan_access_filtered_index_requires_query_implication_for_predicate() {
    let model = model_with_filtered_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);

    let missing_implication =
        compare_strict("tag", CompareOp::Eq, Value::Text("alpha".to_string()));
    let missing_plan =
        plan_access_for_test(model, schema, Some(&missing_implication)).expect("plan should build");
    assert_eq!(
        missing_plan,
        AccessPlan::full_scan(),
        "filtered index must be rejected when the query does not imply its predicate",
    );

    let implied_predicate = Predicate::And(vec![
        compare_strict("tag", CompareOp::Eq, Value::Text("alpha".to_string())),
        compare_strict("active", CompareOp::Eq, Value::Bool(true)),
    ]);
    let implied_plan =
        plan_access_for_test(model, schema, Some(&implied_predicate)).expect("plan should build");
    assert_eq!(
        implied_plan,
        AccessPlan::path(AccessPath::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                FILTERED_INDEX_MODEL
            ),
            values: vec![Value::Text("alpha".to_string())],
        }),
        "filtered index should be eligible once query predicate implies index predicate",
    );
}

#[test]
fn plan_access_filtered_numeric_index_requires_lower_bound_implication() {
    let model = model_with_filtered_numeric_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);

    let weaker = compare_strict("score", CompareOp::Gte, Value::Nat(5));
    let weaker_plan =
        plan_access_for_test(model, schema, Some(&weaker)).expect("plan should build");
    assert_eq!(
        weaker_plan,
        AccessPlan::full_scan(),
        "query lower bound below index predicate bound must not use filtered index",
    );

    let stronger = compare_strict("score", CompareOp::Gte, Value::Nat(20));
    let stronger_plan =
        plan_access_for_test(model, schema, Some(&stronger)).expect("plan should build");
    let (index, prefix, lower, upper) =
        find_index_range(&stronger_plan).expect("stronger lower bound should use index range");
    assert_eq!(index.name(), FILTERED_NUMERIC_INDEX_MODEL.name());
    assert!(
        prefix.is_empty(),
        "single-field range index should have empty prefix"
    );
    assert_eq!(lower, &Bound::Included(Value::Nat(20)));
    assert_eq!(upper, &Bound::Unbounded);
}

#[test]
fn plan_access_filtered_expression_index_requires_predicate_implication() {
    let model = model_with_filtered_expression_casefold_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);

    let missing_implication = compare_text_casefold(
        "email",
        CompareOp::Eq,
        Value::Text("Alice@Example.Com".to_string()),
    );
    let missing_plan =
        plan_access_for_test(model, schema, Some(&missing_implication)).expect("plan should build");
    assert_eq!(
        missing_plan,
        AccessPlan::full_scan(),
        "filtered expression index must be rejected when query does not imply predicate",
    );

    let implied_predicate = Predicate::And(vec![
        compare_text_casefold(
            "email",
            CompareOp::Eq,
            Value::Text("Alice@Example.Com".to_string()),
        ),
        compare_strict("active", CompareOp::Eq, Value::Bool(true)),
    ]);
    let implied_plan =
        plan_access_for_test(model, schema, Some(&implied_predicate)).expect("plan should build");
    assert_eq!(
        implied_plan,
        AccessPlan::path(AccessPath::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                FILTERED_EXPRESSION_CASEFOLD_INDEX_MODEL
            ),
            values: vec![Value::Text("alice@example.com".to_string())],
        }),
        "query implication should unlock filtered expression-index prefix planning",
    );
}

#[test]
fn plan_access_filtered_expression_prefix_requires_predicate_implication() {
    let model = model_with_filtered_expression_casefold_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);

    let missing_implication = compare_text_casefold(
        "email",
        CompareOp::StartsWith,
        Value::Text("Alice".to_string()),
    );
    let missing_plan =
        plan_access_for_test(model, schema, Some(&missing_implication)).expect("plan should build");
    assert_eq!(
        missing_plan,
        AccessPlan::full_scan(),
        "filtered expression prefix route must be rejected when query does not imply predicate",
    );

    let implied_predicate = Predicate::And(vec![
        compare_text_casefold(
            "email",
            CompareOp::StartsWith,
            Value::Text("Alice".to_string()),
        ),
        compare_strict("active", CompareOp::Eq, Value::Bool(true)),
    ]);
    let implied_plan =
        plan_access_for_test(model, schema, Some(&implied_predicate)).expect("plan should build");
    assert_eq!(
        implied_plan,
        AccessPlan::path(AccessPath::IndexRange {
            spec: SemanticIndexRangeSpec::new(
                FILTERED_EXPRESSION_CASEFOLD_INDEX_MODEL,
                vec![0usize],
                Vec::new(),
                Bound::Included(Value::Text("alice".to_string())),
                Bound::Unbounded,
            ),
        }),
        "query implication should unlock filtered expression-index prefix range planning",
    );
}

#[test]
fn plan_access_uses_index_multi_lookup_for_secondary_in() {
    let model = model_with_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = compare_strict(
        "tag",
        CompareOp::In,
        Value::List(vec![
            Value::Text("alpha".to_string()),
            Value::Text("beta".to_string()),
        ]),
    );

    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::path(AccessPath::IndexMultiLookup {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                INDEX_MODEL
            ),
            values: vec![
                Value::Text("alpha".to_string()),
                Value::Text("beta".to_string()),
            ],
        }),
    );
}

#[test]
fn plan_access_text_casefold_eq_uses_expression_index_prefix() {
    let model = model_with_expression_casefold_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = compare_text_casefold(
        "email",
        CompareOp::Eq,
        Value::Text("Alice@Example.Com".to_string()),
    );

    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::path(AccessPath::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                EXPRESSION_CASEFOLD_INDEX_MODEL
            ),
            values: vec![Value::Text("alice@example.com".to_string())],
        }),
        "text-casefold equality should lower through expression index prefix matching",
    );
}

#[test]
fn plan_access_text_casefold_eq_uses_upper_expression_index_prefix() {
    let model = model_with_expression_upper_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = compare_text_casefold(
        "email",
        CompareOp::Eq,
        Value::Text("Alice@Example.Com".to_string()),
    );

    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::path(AccessPath::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                EXPRESSION_UPPER_INDEX_MODEL
            ),
            values: vec![Value::Text("ALICE@EXAMPLE.COM".to_string())],
        }),
        "text-casefold equality should lower through upper-expression index prefix matching",
    );
}

#[test]
fn plan_access_text_casefold_eq_rejects_unsupported_expression_lookup_kind() {
    let model = model_with_expression_unsupported_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = compare_text_casefold(
        "email",
        CompareOp::Eq,
        Value::Text("Alice@Example.Com".to_string()),
    );

    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::full_scan(),
        "unsupported expression lookup kinds must fail closed for text-casefold equality",
    );
}

#[test]
fn plan_access_text_casefold_in_uses_upper_expression_index_multi_lookup() {
    let model = model_with_expression_upper_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = compare_text_casefold(
        "email",
        CompareOp::In,
        Value::List(vec![
            Value::Text("alice@example.com".to_string()),
            Value::Text("BOB@example.com".to_string()),
        ]),
    );

    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::path(AccessPath::IndexMultiLookup {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                EXPRESSION_UPPER_INDEX_MODEL
            ),
            values: vec![
                Value::Text("ALICE@EXAMPLE.COM".to_string()),
                Value::Text("BOB@EXAMPLE.COM".to_string()),
            ],
        }),
        "text-casefold IN should lower through upper-expression index lookup values",
    );
}

#[test]
fn plan_access_text_casefold_in_rejects_unsupported_expression_lookup_kind() {
    let model = model_with_expression_unsupported_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = compare_text_casefold(
        "email",
        CompareOp::In,
        Value::List(vec![
            Value::Text("alice@example.com".to_string()),
            Value::Text("BOB@example.com".to_string()),
        ]),
    );

    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::full_scan(),
        "unsupported expression lookup kinds must fail closed for text-casefold IN",
    );
}

#[test]
fn plan_access_text_casefold_in_uses_expression_index_multi_lookup() {
    let model = model_with_expression_casefold_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = compare_text_casefold(
        "email",
        CompareOp::In,
        Value::List(vec![
            Value::Text("bob@example.com".to_string()),
            Value::Text("ALICE@example.com".to_string()),
            Value::Text("Bob@Example.Com".to_string()),
        ]),
    );

    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::path(AccessPath::IndexMultiLookup {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                EXPRESSION_CASEFOLD_INDEX_MODEL
            ),
            values: vec![
                Value::Text("alice@example.com".to_string()),
                Value::Text("bob@example.com".to_string()),
            ],
        }),
        "text-casefold IN should lower through canonical expression-index lookup values",
    );
}

#[test]
fn plan_access_gt_rejects_expression_index() {
    let model = model_with_expression_casefold_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = compare_strict("email", CompareOp::Gt, Value::Text("a@x.io".to_string()));

    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::full_scan(),
        "range compare planning must stay fail-closed for expression-key indexes",
    );
}

#[test]
fn plan_access_text_casefold_starts_with_expression_range_matrix() {
    let cases: &[ExpressionStartsWithRangeCase<'_>] = &[
        (
            "casefold expression",
            model_with_expression_casefold_index,
            "ALICE",
            EXPRESSION_CASEFOLD_INDEX_MODEL.name(),
            "alice",
        ),
        (
            "casefold expression single-char prefix",
            model_with_expression_casefold_index,
            "A",
            EXPRESSION_CASEFOLD_INDEX_MODEL.name(),
            "a",
        ),
        (
            "upper expression",
            model_with_expression_upper_index,
            "ALICE",
            EXPRESSION_UPPER_INDEX_MODEL.name(),
            "ALICE",
        ),
    ];

    for (label, model_factory, raw_value, expected_index_name, expected_lower) in
        cases.iter().copied()
    {
        assert_expression_starts_with_range_case(
            label,
            model_factory,
            raw_value,
            expected_index_name,
            expected_lower,
        );
    }
}

#[test]
fn plan_access_text_casefold_starts_with_rejects_unsupported_expression_lookup_kind() {
    let model = model_with_expression_unsupported_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = compare_text_casefold(
        "email",
        CompareOp::StartsWith,
        Value::Text("ALICE".to_string()),
    );

    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::full_scan(),
        "unsupported expression lookup kinds must fail closed for text-casefold starts-with",
    );
}

#[test]
fn plan_access_text_casefold_starts_with_empty_prefix_falls_back_to_full_scan() {
    let model = model_with_expression_casefold_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate =
        compare_text_casefold("email", CompareOp::StartsWith, Value::Text(String::new()));

    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::full_scan(),
        "empty text-casefold starts-with prefixes must fail closed",
    );
}

#[test]
fn plan_access_text_contains_can_compile_scalar_while_still_full_scanning() {
    let model = model_with_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = Predicate::TextContains {
        field: "tag".to_string(),
        value: Value::Text("alp".to_string()),
    };

    let runtime = compile_runtime_predicate_for_test(model, &predicate);
    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");

    assert!(
        runtime.uses_scalar_program(),
        "runtime should compile text-contains onto the scalar executor when the field is scalar",
    );
    assert_eq!(
        plan,
        AccessPlan::full_scan(),
        "text-contains remains an access-planning full scan even when runtime executes it scalar-native",
    );
}

#[test]
fn plan_access_stability_text_casefold_starts_with_case_variants_share_access_plan() {
    let model = model_with_expression_casefold_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let upper = compare_text_casefold(
        "email",
        CompareOp::StartsWith,
        Value::Text("ALICE".to_string()),
    );
    let lower = compare_text_casefold(
        "email",
        CompareOp::StartsWith,
        Value::Text("alice".to_string()),
    );

    let upper_plan = plan_access_for_test(model, schema, Some(&upper)).expect("plan should build");
    let lower_plan = plan_access_for_test(model, schema, Some(&lower)).expect("plan should build");

    assert_eq!(
        upper_plan, lower_plan,
        "text-casefold starts-with planning should canonicalize prefix case consistently",
    );
}

#[test]
fn plan_access_starts_with_rejects_expression_index() {
    let model = model_with_expression_casefold_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = compare_strict(
        "email",
        CompareOp::StartsWith,
        Value::Text("alice".to_string()),
    );

    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::full_scan(),
        "starts-with planning must stay fail-closed for expression-key indexes",
    );
}

// Keep this canonicalization matrix in one test so the paired raw/canonical
// predicates and expected access paths stay reviewable in one place.
#[expect(
    clippy::too_many_lines,
    reason = "the canonical IN/OR planning matrix is intentionally table-driven in one test"
)]
#[test]
fn plan_access_canonical_in_and_or_matrix() {
    let cases = vec![
        (
            "secondary IN permutation and duplicates",
            model_with_index as fn() -> &'static EntityModel,
            compare_strict(
                "tag",
                CompareOp::In,
                Value::List(vec![
                    Value::Text("beta".to_string()),
                    Value::Text("alpha".to_string()),
                    Value::Text("beta".to_string()),
                ]),
            ),
            compare_strict(
                "tag",
                CompareOp::In,
                Value::List(vec![
                    Value::Text("alpha".to_string()),
                    Value::Text("beta".to_string()),
                ]),
            ),
            AccessPlan::path(AccessPath::IndexMultiLookup {
                index:
                    crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                        INDEX_MODEL,
                    ),
                values: vec![
                    Value::Text("alpha".to_string()),
                    Value::Text("beta".to_string()),
                ],
            }),
        ),
        (
            "primary-key IN permutation and duplicates",
            model_with_index as fn() -> &'static EntityModel,
            compare_strict(
                "id",
                CompareOp::In,
                Value::List(vec![
                    Value::Ulid(Ulid::from_u128(3)),
                    Value::Ulid(Ulid::from_u128(1)),
                    Value::Ulid(Ulid::from_u128(3)),
                ]),
            ),
            compare_strict(
                "id",
                CompareOp::In,
                Value::List(vec![
                    Value::Ulid(Ulid::from_u128(1)),
                    Value::Ulid(Ulid::from_u128(3)),
                ]),
            ),
            AccessPlan::path(AccessPath::ByKeys(vec![
                Value::Ulid(Ulid::from_u128(1)),
                Value::Ulid(Ulid::from_u128(3)),
            ])),
        ),
        (
            "secondary OR equality canonicalizes to bounded IN",
            model_with_index as fn() -> &'static EntityModel,
            Predicate::Or(vec![
                compare_strict("tag", CompareOp::Eq, Value::Text("beta".to_string())),
                compare_strict("tag", CompareOp::Eq, Value::Text("alpha".to_string())),
                compare_strict("tag", CompareOp::Eq, Value::Text("beta".to_string())),
            ]),
            compare_strict(
                "tag",
                CompareOp::In,
                Value::List(vec![
                    Value::Text("alpha".to_string()),
                    Value::Text("beta".to_string()),
                ]),
            ),
            AccessPlan::path(AccessPath::IndexMultiLookup {
                index:
                    crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                        INDEX_MODEL,
                    ),
                values: vec![
                    Value::Text("alpha".to_string()),
                    Value::Text("beta".to_string()),
                ],
            }),
        ),
        (
            "text-casefold OR equality canonicalizes to expression IN",
            model_with_expression_casefold_index as fn() -> &'static EntityModel,
            Predicate::Or(vec![
                compare_text_casefold(
                    "email",
                    CompareOp::Eq,
                    Value::Text("alice@example.com".to_string()),
                ),
                compare_text_casefold(
                    "email",
                    CompareOp::Eq,
                    Value::Text("BOB@example.com".to_string()),
                ),
                compare_text_casefold(
                    "email",
                    CompareOp::Eq,
                    Value::Text("Bob@Example.Com".to_string()),
                ),
            ]),
            compare_text_casefold(
                "email",
                CompareOp::In,
                Value::List(vec![
                    Value::Text("bob@example.com".to_string()),
                    Value::Text("ALICE@example.com".to_string()),
                    Value::Text("Bob@Example.Com".to_string()),
                ]),
            ),
            AccessPlan::path(AccessPath::IndexMultiLookup {
                index:
                    crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                        EXPRESSION_CASEFOLD_INDEX_MODEL,
                    ),
                values: vec![
                    Value::Text("alice@example.com".to_string()),
                    Value::Text("bob@example.com".to_string()),
                ],
            }),
        ),
        (
            "primary-key OR equality canonicalizes to by-keys",
            model_with_index as fn() -> &'static EntityModel,
            Predicate::Or(vec![
                compare_strict("id", CompareOp::Eq, Value::Ulid(Ulid::from_u128(3))),
                compare_strict("id", CompareOp::Eq, Value::Ulid(Ulid::from_u128(1))),
                compare_strict("id", CompareOp::Eq, Value::Ulid(Ulid::from_u128(3))),
            ]),
            compare_strict(
                "id",
                CompareOp::In,
                Value::List(vec![
                    Value::Ulid(Ulid::from_u128(1)),
                    Value::Ulid(Ulid::from_u128(3)),
                ]),
            ),
            AccessPlan::path(AccessPath::ByKeys(vec![
                Value::Ulid(Ulid::from_u128(1)),
                Value::Ulid(Ulid::from_u128(3)),
            ])),
        ),
    ];

    for (label, model_factory, left, right, expected_plan) in cases {
        assert_equivalent_access_plan_case(label, model_factory, left, right, expected_plan);
    }
}

#[test]
fn plan_access_in_normalization_matrix() {
    let cases = vec![
        (
            "secondary IN singleton collapses to prefix",
            model_with_index as fn() -> &'static EntityModel,
            compare_strict(
                "tag",
                CompareOp::In,
                Value::List(vec![Value::Text("alpha".to_string())]),
            ),
            AccessPlan::path(AccessPath::IndexPrefix {
                index:
                    crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                        INDEX_MODEL,
                    ),
                values: vec![Value::Text("alpha".to_string())],
            }),
        ),
        (
            "secondary OR with non-strict branch stays fail-closed",
            model_with_index as fn() -> &'static EntityModel,
            Predicate::Or(vec![
                compare_strict("tag", CompareOp::Eq, Value::Text("alpha".to_string())),
                Predicate::Compare(ComparePredicate::with_coercion(
                    "tag",
                    CompareOp::Eq,
                    Value::Text("beta".to_string()),
                    CoercionId::TextCasefold,
                )),
            ]),
            AccessPlan::full_scan(),
        ),
        (
            "secondary IN empty lowers to empty by-keys",
            model_with_index as fn() -> &'static EntityModel,
            compare_strict("tag", CompareOp::In, Value::List(Vec::new())),
            AccessPlan::path(AccessPath::ByKeys(Vec::new())),
        ),
        (
            "secondary IN empty inside AND stays empty by-keys",
            model_with_index as fn() -> &'static EntityModel,
            Predicate::And(vec![
                compare_strict("tag", CompareOp::In, Value::List(Vec::new())),
                compare_strict("tag", CompareOp::Eq, Value::Text("alpha".to_string())),
            ]),
            AccessPlan::path(AccessPath::ByKeys(Vec::new())),
        ),
        (
            "secondary IN mixed literal types stays fail-closed",
            model_with_index as fn() -> &'static EntityModel,
            compare_strict(
                "tag",
                CompareOp::In,
                Value::List(vec![Value::Text("alpha".to_string()), Value::Nat(7)]),
            ),
            AccessPlan::full_scan(),
        ),
    ];

    for (label, model_factory, predicate, expected_plan) in cases {
        assert_access_plan_case(label, model_factory, predicate, expected_plan);
    }
}

#[test]
fn plan_access_secondary_in_empty_remains_distinct_from_false_before_constant_folding() {
    let model = model_with_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let secondary_in_empty = compare_strict("tag", CompareOp::In, Value::List(Vec::new()));

    let plan_from_empty_in =
        plan_access_for_test(model, schema, Some(&secondary_in_empty)).expect("plan should build");
    let plan_from_false =
        plan_access_for_test(model, schema, Some(&Predicate::False)).expect("plan should build");

    assert_eq!(
        plan_from_empty_in,
        AccessPlan::path(AccessPath::ByKeys(Vec::new())),
        "strict secondary IN [] should lower to explicit empty by-keys shape at access planning",
    );
    assert_eq!(
        plan_from_false,
        AccessPlan::full_scan(),
        "constant FALSE folding remains a higher-level planning concern outside direct access planning",
    );
    assert_ne!(
        plan_from_empty_in, plan_from_false,
        "strict secondary IN [] and FALSE should remain distinct at direct access-planning boundary",
    );
}

#[test]
fn plan_access_text_between_equivalent_bounds_lowers_to_index_range() {
    let model = model_with_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = Predicate::And(vec![
        compare_strict("tag", CompareOp::Gte, Value::Text("alpha".to_string())),
        compare_strict("tag", CompareOp::Lte, Value::Text("omega".to_string())),
    ]);

    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");
    assert_single_field_text_index_range(
        &plan,
        Bound::Included(Value::Text("alpha".to_string())),
        Bound::Included(Value::Text("omega".to_string())),
    );
}

#[test]
fn plan_access_text_between_equal_bounds_still_canonicalizes_to_eq() {
    let model = model_with_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let between_equal_bounds = Predicate::And(vec![
        compare_strict("tag", CompareOp::Gte, Value::Text("alpha".to_string())),
        compare_strict("tag", CompareOp::Lte, Value::Text("alpha".to_string())),
    ]);
    let strict_eq = compare_strict("tag", CompareOp::Eq, Value::Text("alpha".to_string()));

    let between_plan = plan_access_for_test(model, schema, Some(&between_equal_bounds))
        .expect("plan should build");
    let eq_plan = plan_access_for_test(model, schema, Some(&strict_eq)).expect("plan should build");

    assert_eq!(
        between_plan, eq_plan,
        "equal text bounds collapse to strict equality before range lowering, so the equality index path remains valid",
    );
}

#[test]
fn plan_access_starts_with_empty_prefix_falls_back_to_full_scan() {
    let model = model_with_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = compare_strict("tag", CompareOp::StartsWith, Value::Text(String::new()));

    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");

    assert_eq!(plan, AccessPlan::full_scan());
}

#[test]
fn plan_access_text_starts_with_range_matrix() {
    let high_unicode_prefix = format!("foo{}", char::from_u32(0xD7FF).expect("valid scalar"));
    let high_unicode_upper = format!("foo{}", char::from_u32(0xE000).expect("valid scalar"));
    let max_unicode_prefix = char::from_u32(0x10_FFFF).expect("valid scalar").to_string();

    let cases = vec![
        (
            "ascii prefix",
            "foo".to_string(),
            Bound::Included(Value::Text("foo".to_string())),
            Bound::Excluded(Value::Text("fop".to_string())),
        ),
        (
            "high unicode prefix",
            high_unicode_prefix.clone(),
            Bound::Included(Value::Text(high_unicode_prefix)),
            Bound::Excluded(Value::Text(high_unicode_upper)),
        ),
        (
            "max unicode prefix",
            max_unicode_prefix.clone(),
            Bound::Included(Value::Text(max_unicode_prefix)),
            Bound::Unbounded,
        ),
    ];

    for (label, prefix, expected_lower, expected_upper) in cases {
        assert_strict_text_starts_with_range_case(label, &prefix, expected_lower, expected_upper);
    }
}

#[test]
fn plan_access_stability_starts_with_equivalent_range_matrix() {
    let max_unicode_prefix = char::from_u32(0x10_FFFF).expect("valid scalar").to_string();
    let cases = vec![
        (
            "bounded ascii prefix",
            compare_strict("tag", CompareOp::StartsWith, Value::Text("foo".to_string())),
            Predicate::And(vec![
                compare_strict("tag", CompareOp::Gte, Value::Text("foo".to_string())),
                compare_strict("tag", CompareOp::Lt, Value::Text("fop".to_string())),
            ]),
        ),
        (
            "max unicode prefix",
            compare_strict(
                "tag",
                CompareOp::StartsWith,
                Value::Text(max_unicode_prefix.clone()),
            ),
            compare_strict("tag", CompareOp::Gte, Value::Text(max_unicode_prefix)),
        ),
    ];

    for (label, starts_with, equivalent_range) in cases {
        assert_starts_with_equivalent_range_case(label, starts_with, equivalent_range);
    }
}

#[test]
fn plan_access_text_gt_lowers_to_index_range() {
    let model = model_with_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = compare_strict("tag", CompareOp::Gt, Value::Text("alpha".to_string()));

    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");
    assert_single_field_text_index_range(
        &plan,
        Bound::Excluded(Value::Text("alpha".to_string())),
        Bound::Unbounded,
    );
}

#[test]
fn plan_access_text_lte_lowers_to_index_range() {
    let model = model_with_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = compare_strict("tag", CompareOp::Lte, Value::Text("omega".to_string()));

    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");
    assert_single_field_text_index_range(
        &plan,
        Bound::Unbounded,
        Bound::Included(Value::Text("omega".to_string())),
    );
}

#[test]
fn plan_access_ignores_non_strict_predicates() {
    let model = model_with_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "tag",
        CompareOp::Eq,
        Value::Text("alpha".to_string()),
        CoercionId::TextCasefold,
    ));

    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");

    assert_eq!(plan, AccessPlan::full_scan());
}

#[test]
fn plan_access_emits_only_one_composite_index_range_for_and_eq_plus_gt() {
    let model = model_with_range_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = Predicate::And(vec![
        compare_strict("a", CompareOp::Eq, Value::Nat(1)),
        compare_strict("b", CompareOp::Gt, Value::Nat(5)),
    ]);

    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");
    let AccessPlan::Path(path) = &plan else {
        panic!("composite eq+range predicate should emit a single access path");
    };
    let AccessPath::IndexRange { spec } = path.as_ref() else {
        panic!("composite eq+range predicate should emit IndexRange");
    };
    let index = spec.index();
    let prefix = spec.prefix_values();
    let lower = spec.lower();
    let upper = spec.upper();

    assert_eq!(index.name(), RANGE_INDEX_MODEL.name());
    assert_eq!(prefix, [Value::Nat(1)].as_slice());
    assert_eq!(lower, &Bound::Excluded(Value::Nat(5)));
    assert_eq!(upper, &Bound::Unbounded);

    let mut index_range_count = 0usize;
    let mut index_prefix_count = 0usize;
    let mut single_field_index_range_count = 0usize;
    visit_access_paths(&plan, &mut |access| match access {
        AccessPath::IndexRange { spec } => {
            index_range_count = index_range_count.saturating_add(1);
            if spec.prefix_values().is_empty() {
                single_field_index_range_count = single_field_index_range_count.saturating_add(1);
            }
        }
        AccessPath::IndexPrefix { .. } => {
            index_prefix_count = index_prefix_count.saturating_add(1);
        }
        _ => {}
    });

    assert_eq!(
        index_range_count, 1,
        "exactly one IndexRange should be emitted"
    );
    assert_eq!(
        index_prefix_count, 0,
        "composite IndexRange should not carry IndexPrefix siblings"
    );
    assert_eq!(
        single_field_index_range_count, 0,
        "composite IndexRange should not carry single-field IndexRange siblings"
    );
}

#[test]
fn plan_access_composite_index_range_matrix() {
    let edge_upper = u64::from(u32::MAX);
    let cases = vec![
        (
            "prefix plus half-open range",
            Predicate::And(vec![
                compare_strict("a", CompareOp::Eq, Value::Nat(7)),
                compare_strict("b", CompareOp::Gte, Value::Nat(100)),
                compare_strict("b", CompareOp::Lt, Value::Nat(200)),
            ]),
            vec![Value::Nat(7)],
            Bound::Included(Value::Nat(100)),
            Bound::Excluded(Value::Nat(200)),
        ),
        (
            "prefix plus closed range",
            Predicate::And(vec![
                compare_strict("a", CompareOp::Eq, Value::Nat(7)),
                compare_strict("b", CompareOp::Gte, Value::Nat(100)),
                compare_strict("b", CompareOp::Lte, Value::Nat(200)),
            ]),
            vec![Value::Nat(7)],
            Bound::Included(Value::Nat(100)),
            Bound::Included(Value::Nat(200)),
        ),
        (
            "edge half-open range",
            Predicate::And(vec![
                compare_strict("a", CompareOp::Eq, Value::Nat(7)),
                compare_strict("b", CompareOp::Gte, Value::Nat(0)),
                compare_strict("b", CompareOp::Lt, Value::Nat(edge_upper)),
            ]),
            vec![Value::Nat(7)],
            Bound::Included(Value::Nat(0)),
            Bound::Excluded(Value::Nat(edge_upper)),
        ),
        (
            "edge closed range",
            Predicate::And(vec![
                compare_strict("a", CompareOp::Eq, Value::Nat(7)),
                compare_strict("b", CompareOp::Gte, Value::Nat(0)),
                compare_strict("b", CompareOp::Lte, Value::Nat(edge_upper)),
            ]),
            vec![Value::Nat(7)],
            Bound::Included(Value::Nat(0)),
            Bound::Included(Value::Nat(edge_upper)),
        ),
    ];

    for (label, predicate, expected_prefix, expected_lower, expected_upper) in cases {
        assert_composite_index_range_case(
            label,
            predicate,
            &expected_prefix,
            expected_lower,
            expected_upper,
        );
    }
}

#[test]
fn plan_access_rejects_invalid_composite_range_shapes_matrix() {
    let cases = vec![
        (
            "trailing equality after range",
            Predicate::And(vec![
                compare_strict("a", CompareOp::Eq, Value::Nat(7)),
                compare_strict("b", CompareOp::Gte, Value::Nat(100)),
                compare_strict("c", CompareOp::Eq, Value::Nat(3)),
            ]),
        ),
        (
            "missing prefix component",
            Predicate::And(vec![
                compare_strict("a", CompareOp::Eq, Value::Nat(7)),
                compare_strict("c", CompareOp::Gte, Value::Nat(100)),
            ]),
        ),
        (
            "range before prefix equality",
            Predicate::And(vec![
                compare_strict("a", CompareOp::Gte, Value::Nat(7)),
                compare_strict("b", CompareOp::Eq, Value::Nat(3)),
            ]),
        ),
        (
            "empty exclusive interval",
            Predicate::And(vec![
                compare_strict("a", CompareOp::Eq, Value::Nat(7)),
                compare_strict("b", CompareOp::Gt, Value::Nat(100)),
                compare_strict("b", CompareOp::Lt, Value::Nat(100)),
            ]),
        ),
        (
            "non-strict numeric widen",
            Predicate::And(vec![
                compare_strict("a", CompareOp::Eq, Value::Nat(7)),
                compare_numeric_widen("b", CompareOp::Gte, Value::Int(100)),
                compare_numeric_widen("b", CompareOp::Lte, Value::Nat(200)),
            ]),
        ),
    ];

    for (label, predicate) in cases {
        assert_rejected_composite_index_range_case(label, predicate);
    }
}

#[test]
fn plan_access_merges_duplicate_lower_bounds_to_stricter_value() {
    let model = model_with_range_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = Predicate::And(vec![
        compare_strict("a", CompareOp::Eq, Value::Nat(7)),
        compare_strict("b", CompareOp::Gte, Value::Nat(50)),
        compare_strict("b", CompareOp::Gt, Value::Nat(80)),
        compare_strict("b", CompareOp::Lte, Value::Nat(200)),
    ]);

    let plan = plan_access_for_test(model, schema, Some(&predicate)).expect("plan should build");
    let (_, _, lower, upper) = find_index_range(&plan).expect("plan should include index range");
    assert_eq!(lower, &Bound::Excluded(Value::Nat(80)));
    assert_eq!(upper, &Bound::Included(Value::Nat(200)));
}

#[test]
fn plan_access_stability_equivalent_predicates_share_identical_access_plan() {
    let model = model_with_range_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate_a = Predicate::And(vec![
        compare_strict("a", CompareOp::Eq, Value::Nat(7)),
        compare_strict("b", CompareOp::Gte, Value::Nat(100)),
        compare_strict("b", CompareOp::Lte, Value::Nat(100)),
    ]);
    let predicate_b = Predicate::And(vec![
        compare_strict("b", CompareOp::Eq, Value::Nat(100)),
        compare_strict("a", CompareOp::Eq, Value::Nat(7)),
    ]);

    let plan_a =
        plan_access_for_test(model, schema, Some(&predicate_a)).expect("plan should build");
    let plan_b =
        plan_access_for_test(model, schema, Some(&predicate_b)).expect("plan should build");

    assert_eq!(
        plan_a, plan_b,
        "equivalent canonical predicate shapes must lower to identical access plans",
    );
}

#[test]
fn plan_access_stability_contradictory_and_predicate_matches_constant_false_shape() {
    let model = model_with_range_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let contradictory = Predicate::And(vec![
        compare_strict("a", CompareOp::Eq, Value::Nat(7)),
        compare_strict("b", CompareOp::Gt, Value::Nat(100)),
        compare_strict("b", CompareOp::Lt, Value::Nat(100)),
    ]);

    let plan_from_contradiction =
        plan_access_for_test(model, schema, Some(&contradictory)).expect("plan should build");
    let plan_from_false =
        plan_access_for_test(model, schema, Some(&Predicate::False)).expect("plan should build");

    assert_eq!(
        plan_from_contradiction, plan_from_false,
        "contradictory conjunctions should canonicalize to the same access shape as false",
    );
}
