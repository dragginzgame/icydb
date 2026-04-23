use super::support::*;
use crate::db::query::plan::expr::CaseWhenArm;

#[test]
fn plan_hash_snapshot_is_stable_across_explain_surfaces() {
    // Phase 1: build one deterministic scalar query shape and capture baseline hash surfaces.
    let query = Query::<PlanSingleton>::new(MissingRowPolicy::Ignore).by_id(Unit);

    let baseline_hash = query
        .plan_hash_hex()
        .expect("baseline plan hash should build");
    let planned_hash = query
        .planned()
        .expect("planned query should build for hash parity")
        .plan_hash_hex();
    let compiled_hash = query
        .plan()
        .expect("compiled query should build for hash parity")
        .plan_hash_hex();

    // Phase 2: force logical + execution explain surfaces for the same query shape.
    let _logical_explain = query
        .explain()
        .expect("logical explain should build for plan-hash parity lock");
    let _execution_text = query
        .explain_execution_text()
        .expect("execution text explain should build for plan-hash parity lock");
    let _execution_json = query
        .explain_execution_json()
        .expect("execution json explain should build for plan-hash parity lock");
    let _execution_verbose = query
        .explain_execution_verbose()
        .expect("execution verbose explain should build for plan-hash parity lock");

    // Phase 3: re-read hash after explain rendering and lock deterministic parity.
    let hash_after_explain = query
        .plan_hash_hex()
        .expect("plan hash should still build after explain rendering");
    assert_eq!(
        baseline_hash, planned_hash,
        "planned-query plan hash must match query plan-hash surface",
    );
    assert_eq!(
        baseline_hash, compiled_hash,
        "compiled-query plan hash must match query plan-hash surface",
    );
    assert_eq!(
        baseline_hash, hash_after_explain,
        "explain rendering surfaces must not change semantic plan-hash identity",
    );
    assert_eq!(
        baseline_hash, "d9bb3fd16ea72a87a4ced9d14ab26a9af25ed756cbb5a873270dba36842dd28b",
        "plan-hash snapshot drifted; update only for intentional semantic identity changes",
    );
}

#[test]
fn canonical_equivalent_scalar_filter_shapes_share_query_plan_hash_surfaces() {
    // Phase 1: build two equivalent scalar filter spellings that now normalize
    // onto one canonical boolean filter identity.
    let left = Query::<PlanNumericEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("rank").gte(2_i32))
        .filter(FieldRef::new("rank").lt(10_i32));
    let right = Query::<PlanNumericEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("rank").lt(10_i32))
        .filter(FieldRef::new("rank").gte(2_i32));

    let left_hash = left
        .plan_hash_hex()
        .expect("left canonical-equivalent query should build a plan hash");
    let right_hash = right
        .plan_hash_hex()
        .expect("right canonical-equivalent query should build a plan hash");

    // Phase 2: require the public query, planned-query, compiled-query, and
    // explain surfaces to follow the same canonical scalar filter identity.
    assert_eq!(
        left_hash, right_hash,
        "canonical-equivalent scalar filter spellings must share the outward query plan hash",
    );
    assert_eq!(
        left.planned()
            .expect("left planned query should build for canonical parity")
            .plan_hash_hex(),
        right
            .planned()
            .expect("right planned query should build for canonical parity")
            .plan_hash_hex(),
        "planned-query hash surface must follow the same canonical scalar filter identity",
    );
    assert_eq!(
        left.plan()
            .expect("left compiled query should build for canonical parity")
            .plan_hash_hex(),
        right
            .plan()
            .expect("right compiled query should build for canonical parity")
            .plan_hash_hex(),
        "compiled-query hash surface must follow the same canonical scalar filter identity",
    );
    assert_eq!(
        left.explain()
            .expect("left explain should build for canonical parity"),
        right
            .explain()
            .expect("right explain should build for canonical parity"),
        "logical explain output must follow the same canonical scalar filter identity",
    );
}

#[test]
fn canonical_equivalent_grouped_having_shapes_share_query_plan_hash_surfaces() {
    // Phase 1: build two grouped queries whose HAVING clauses are semantically
    // identical but arrive through different append order.
    let left = Query::<PlanNumericEntity>::new(MissingRowPolicy::Ignore)
        .group_by("rank")
        .expect("left grouped query should resolve group field")
        .aggregate(crate::db::count())
        .having_group(
            "rank",
            CompareOp::Gte,
            crate::value::InputValue::from(Value::Int(2)),
        )
        .expect("left grouped query should accept grouped field HAVING")
        .having_aggregate(
            0,
            CompareOp::Gt,
            crate::value::InputValue::from(Value::Uint(0)),
        )
        .expect("left grouped query should accept grouped aggregate HAVING");
    let right = Query::<PlanNumericEntity>::new(MissingRowPolicy::Ignore)
        .group_by("rank")
        .expect("right grouped query should resolve group field")
        .aggregate(crate::db::count())
        .having_aggregate(
            0,
            CompareOp::Gt,
            crate::value::InputValue::from(Value::Uint(0)),
        )
        .expect("right grouped query should accept grouped aggregate HAVING")
        .having_group(
            "rank",
            CompareOp::Gte,
            crate::value::InputValue::from(Value::Int(2)),
        )
        .expect("right grouped query should accept grouped field HAVING");

    let left_hash = left
        .plan_hash_hex()
        .expect("left grouped query should build a plan hash");
    let right_hash = right
        .plan_hash_hex()
        .expect("right grouped query should build a plan hash");

    // Phase 2: require grouped hash/explain surfaces to follow the same
    // canonical HAVING identity rather than append order.
    assert_eq!(
        left_hash, right_hash,
        "canonical-equivalent grouped HAVING order must share the outward query plan hash",
    );
    assert_eq!(
        left.planned()
            .expect("left grouped planned query should build for canonical parity")
            .plan_hash_hex(),
        right
            .planned()
            .expect("right grouped planned query should build for canonical parity")
            .plan_hash_hex(),
        "planned grouped-query hash surface must follow canonical HAVING identity",
    );
    assert_eq!(
        left.plan()
            .expect("left grouped compiled query should build for canonical parity")
            .plan_hash_hex(),
        right
            .plan()
            .expect("right grouped compiled query should build for canonical parity")
            .plan_hash_hex(),
        "compiled grouped-query hash surface must follow canonical HAVING identity",
    );
    assert_eq!(
        left.explain()
            .expect("left grouped explain should build for canonical parity"),
        right
            .explain()
            .expect("right grouped explain should build for canonical parity"),
        "grouped logical explain must follow the same canonical HAVING identity",
    );
}

#[test]
fn grouped_null_and_false_having_families_keep_distinct_plan_hash_surfaces() {
    let omitted_else_null_family = Query::<PlanNumericEntity>::new(MissingRowPolicy::Ignore)
        .group_by("rank")
        .expect("grouped omitted-ELSE test query should resolve group field")
        .aggregate(crate::db::count())
        .having_expr(Expr::Case {
            when_then_arms: vec![CaseWhenArm::new(
                Expr::Binary {
                    op: crate::db::query::plan::expr::BinaryOp::Gt,
                    left: Box::new(Expr::Aggregate(crate::db::count())),
                    right: Box::new(Expr::Literal(Value::Uint(1))),
                },
                Expr::Literal(Value::Bool(true)),
            )],
            else_expr: Box::new(Expr::Literal(Value::Null)),
        })
        .expect("grouped omitted-ELSE test query should admit the explicit ELSE NULL family");
    let explicit_false_family = Query::<PlanNumericEntity>::new(MissingRowPolicy::Ignore)
        .group_by("rank")
        .expect("grouped explicit-false test query should resolve group field")
        .aggregate(crate::db::count())
        .having_expr(Expr::Binary {
            op: crate::db::query::plan::expr::BinaryOp::Or,
            left: Box::new(Expr::FunctionCall {
                function: crate::db::query::plan::expr::Function::Coalesce,
                args: vec![
                    Expr::Binary {
                        op: crate::db::query::plan::expr::BinaryOp::Gt,
                        left: Box::new(Expr::Aggregate(crate::db::count())),
                        right: Box::new(Expr::Literal(Value::Uint(1))),
                    },
                    Expr::Literal(Value::Bool(false)),
                ],
            }),
            right: Box::new(Expr::Literal(Value::Bool(false))),
        })
        .expect("grouped explicit-false test query should admit the shipped false family");

    assert_ne!(
        omitted_else_null_family
            .plan_hash_hex()
            .expect("grouped omitted-ELSE null-family hash should build"),
        explicit_false_family
            .plan_hash_hex()
            .expect("grouped explicit-false family hash should build"),
        "grouped omitted-ELSE null-family and explicit-false family must stay distinct on outward plan-hash surfaces",
    );
    assert_ne!(
        omitted_else_null_family
            .planned()
            .expect("grouped omitted-ELSE null-family planned query should build")
            .plan_hash_hex(),
        explicit_false_family
            .planned()
            .expect("grouped explicit-false family planned query should build")
            .plan_hash_hex(),
        "planned grouped-query hash surface must keep the grouped null and false families distinct",
    );
    assert_ne!(
        omitted_else_null_family
            .plan()
            .expect("grouped omitted-ELSE null-family compiled query should build")
            .plan_hash_hex(),
        explicit_false_family
            .plan()
            .expect("grouped explicit-false family compiled query should build")
            .plan_hash_hex(),
        "compiled grouped-query hash surface must keep the grouped null and false families distinct",
    );
}

#[test]
fn scalar_queries_with_distinct_projection_shapes_keep_distinct_plan_hash_surfaces() {
    let left = Query::<PlanNumericEntity>::new(MissingRowPolicy::Ignore)
        .select_fields(["rank"])
        .order_term(crate::db::asc("rank"))
        .limit(2);
    let right = Query::<PlanNumericEntity>::new(MissingRowPolicy::Ignore)
        .select_fields(["id"])
        .order_term(crate::db::asc("rank"))
        .limit(2);

    assert_ne!(
        left.plan_hash_hex()
            .expect("left scalar projection hash should build"),
        right
            .plan_hash_hex()
            .expect("right scalar projection hash should build"),
        "distinct scalar projection shapes must remain distinct on outward plan-hash surfaces",
    );
    assert_ne!(
        left.planned()
            .expect("left planned projection should build")
            .plan_hash_hex(),
        right
            .planned()
            .expect("right planned projection should build")
            .plan_hash_hex(),
        "planned-query hash surface must keep distinct scalar projection shapes separate",
    );
    assert_ne!(
        left.plan()
            .expect("left compiled projection should build")
            .plan_hash_hex(),
        right
            .plan()
            .expect("right compiled projection should build")
            .plan_hash_hex(),
        "compiled-query hash surface must keep distinct scalar projection shapes separate",
    );
}

#[test]
fn explain_execution_verbose_reports_top_n_seek_hints() {
    let verbose = Query::<PlanNumericEntity>::new(MissingRowPolicy::Ignore)
        .order_term(crate::db::desc("id"))
        .offset(2)
        .limit(3)
        .explain_execution_verbose()
        .expect("top-n verbose explain should build");

    let diagnostics = verbose_diagnostics_map(&verbose);
    assert_eq!(
        diagnostics.get("diag.r.top_n_seek"),
        Some(&"fetch(6)".to_string()),
        "verbose execution explain should freeze top-n seek fetch diagnostics",
    );
    assert_eq!(
        diagnostics.get("diag.d.has_top_n_seek"),
        Some(&"true".to_string()),
        "descriptor diagnostics should report TopNSeek node presence",
    );
}

#[test]
fn expression_casefold_eq_access_and_execution_route_stay_in_parity() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "email",
        CompareOp::Eq,
        Value::Text("ALICE@EXAMPLE.COM".to_string()),
        CoercionId::TextCasefold,
    ));

    let explain = Query::<PlanExpressionCasefoldEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(predicate.clone())
        .explain()
        .expect("expression eq explain should build");
    let ExplainAccessPath::IndexPrefix {
        name,
        fields,
        prefix_len,
        values,
    } = explain.access()
    else {
        panic!("expression eq should lower to index-prefix access");
    };
    assert_eq!(name, &PLAN_EXPRESSION_CASEFOLD_INDEX_MODELS[0].name());
    assert_eq!(fields.as_slice(), ["email"]);
    assert_eq!(*prefix_len, 1);
    assert_eq!(
        values.as_slice(),
        [Value::Text("alice@example.com".to_string())]
    );

    let verbose = Query::<PlanExpressionCasefoldEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(predicate.clone())
        .explain_execution_verbose()
        .expect("expression eq verbose explain should build");
    let diagnostics = verbose_diagnostics_map(&verbose);
    assert_expression_access_choice_selected(&diagnostics, "IndexPrefix(email_expr)");

    let execution = Query::<PlanExpressionCasefoldEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(predicate)
        .explain_execution()
        .expect("expression eq execution explain should build");
    assert!(
        explain_execution_contains_node_type(&execution, ExplainExecutionNodeType::IndexPrefixScan),
        "execution route must preserve expression eq index-prefix route selection",
    );
}

#[test]
fn expression_casefold_in_access_and_execution_route_stay_in_parity() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "email",
        CompareOp::In,
        Value::List(vec![
            Value::Text("BOB@EXAMPLE.COM".to_string()),
            Value::Text("alice@example.com".to_string()),
            Value::Text("bob@example.com".to_string()),
        ]),
        CoercionId::TextCasefold,
    ));

    let explain = Query::<PlanExpressionCasefoldEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(predicate.clone())
        .explain()
        .expect("expression IN explain should build");
    let ExplainAccessPath::IndexMultiLookup {
        name,
        fields,
        values,
    } = explain.access()
    else {
        panic!("expression IN should lower to index-multi-lookup access");
    };
    assert_eq!(name, &PLAN_EXPRESSION_CASEFOLD_INDEX_MODELS[0].name());
    assert_eq!(fields.as_slice(), ["email"]);
    assert_eq!(
        values.as_slice(),
        [
            Value::Text("alice@example.com".to_string()),
            Value::Text("bob@example.com".to_string())
        ],
    );

    let verbose = Query::<PlanExpressionCasefoldEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(predicate.clone())
        .explain_execution_verbose()
        .expect("expression IN verbose explain should build");
    let diagnostics = verbose_diagnostics_map(&verbose);
    assert_expression_access_choice_selected(&diagnostics, "IndexMultiLookup(email_expr)");

    let execution = Query::<PlanExpressionCasefoldEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(predicate)
        .explain_execution()
        .expect("expression IN execution explain should build");
    assert!(
        explain_execution_contains_node_type(
            &execution,
            ExplainExecutionNodeType::IndexMultiLookup
        ),
        "execution route must preserve expression IN index-multi-lookup route selection",
    );
}

#[test]
fn expression_casefold_starts_with_access_and_execution_route_stay_in_parity() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "email",
        CompareOp::StartsWith,
        Value::Text("ALI".to_string()),
        CoercionId::TextCasefold,
    ));

    let explain = Query::<PlanExpressionCasefoldEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(predicate.clone())
        .explain()
        .expect("expression starts-with explain should build");
    let ExplainAccessPath::IndexRange {
        name,
        fields,
        prefix_len,
        prefix,
        lower,
        upper,
    } = explain.access()
    else {
        panic!("expression starts-with should lower to index-range access");
    };
    assert_eq!(name, &PLAN_EXPRESSION_CASEFOLD_INDEX_MODELS[0].name());
    assert_eq!(fields.as_slice(), ["email"]);
    assert_eq!(*prefix_len, 0);
    assert!(
        prefix.is_empty(),
        "expression starts-with range should not carry equality prefix values",
    );
    assert!(matches!(
        lower,
        std::ops::Bound::Included(Value::Text(value)) if value == "ali"
    ));
    assert!(matches!(upper, std::ops::Bound::Unbounded));

    let verbose = Query::<PlanExpressionCasefoldEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(predicate.clone())
        .explain_execution_verbose()
        .expect("expression starts-with verbose explain should build");
    let diagnostics = verbose_diagnostics_map(&verbose);
    assert_expression_access_choice_selected(&diagnostics, "IndexRange(email_expr)");
    assert_eq!(
        diagnostics.get("diag.r.predicate_stage"),
        Some(&"index_prefilter(strict_all_or_none)".to_string()),
        "text-casefold expression starts-with should keep the shared strict prefilter stage",
    );
    assert_eq!(
        diagnostics.get("diag.d.has_index_predicate_prefilter"),
        Some(&"true".to_string()),
        "text-casefold expression starts-with should compile the shared strict index prefilter",
    );
    assert_eq!(
        diagnostics.get("diag.d.has_residual_filter"),
        Some(&"false".to_string()),
        "text-casefold expression starts-with should no longer require a residual predicate filter",
    );

    let execution = Query::<PlanExpressionCasefoldEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(predicate)
        .explain_execution()
        .expect("expression starts-with execution explain should build");
    assert!(
        explain_execution_contains_node_type(&execution, ExplainExecutionNodeType::IndexRangeScan),
        "execution route must preserve expression starts-with index-range route selection",
    );
}

#[test]
fn expression_casefold_starts_with_single_char_prefix_keeps_index_range_route() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "email",
        CompareOp::StartsWith,
        Value::Text("A".to_string()),
        CoercionId::TextCasefold,
    ));

    let explain = Query::<PlanExpressionCasefoldEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(predicate.clone())
        .explain()
        .expect("single-char expression starts-with explain should build");
    let ExplainAccessPath::IndexRange {
        name, lower, upper, ..
    } = explain.access()
    else {
        panic!("single-char expression starts-with should lower to index-range access");
    };
    assert_eq!(name, &PLAN_EXPRESSION_CASEFOLD_INDEX_MODELS[0].name());
    assert!(matches!(
        lower,
        std::ops::Bound::Included(Value::Text(value)) if value == "a"
    ));
    assert!(matches!(upper, std::ops::Bound::Unbounded));

    let execution = Query::<PlanExpressionCasefoldEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(predicate)
        .explain_execution()
        .expect("single-char expression starts-with execution explain should build");
    assert!(
        explain_execution_contains_node_type(&execution, ExplainExecutionNodeType::IndexRangeScan),
        "single-char expression starts-with must keep index-range route selection",
    );
}

#[test]
fn explain_execution_text_and_json_surfaces_are_stable() {
    let id = Ulid::from_u128(9_101);
    let query = Query::<PlanSimpleEntity>::new(MissingRowPolicy::Ignore).by_id(id);
    let descriptor = query
        .explain_execution()
        .expect("execution descriptor explain should build");

    let text = query
        .explain_execution_text()
        .expect("execution text explain should build");
    assert!(
        text.contains("ByKeyLookup"),
        "execution text surface should expose access-root node type"
    );
    assert_eq!(
        text,
        descriptor.render_text_tree(),
        "execution text surface should be canonical descriptor text rendering",
    );

    let json = query
        .explain_execution_json()
        .expect("execution json explain should build");
    assert!(
        json.contains("\"node_type\":\"ByKeyLookup\""),
        "execution json surface should expose canonical root node type"
    );
    assert_eq!(
        json,
        descriptor.render_json_canonical(),
        "execution json surface should be canonical descriptor json rendering",
    );
}

#[test]
fn secondary_in_explain_uses_index_multi_lookup_access_shape() {
    let explain = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::In,
            Value::List(vec![Value::Uint(7), Value::Uint(8), Value::Uint(9)]),
            CoercionId::Strict,
        )))
        .explain()
        .expect("secondary IN explain should build");

    assert!(
        matches!(explain.access(), ExplainAccessPath::IndexMultiLookup { .. }),
        "secondary IN predicates should lower to the dedicated index-multi-lookup access shape",
    );
}

#[test]
fn secondary_or_eq_explain_uses_index_multi_lookup_access_shape() {
    let explain = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::Or(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "group",
                CompareOp::Eq,
                Value::Uint(8),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "group",
                CompareOp::Eq,
                Value::Uint(7),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "group",
                CompareOp::Eq,
                Value::Uint(8),
                CoercionId::Strict,
            )),
        ]))
        .explain()
        .expect("secondary OR equality explain should build");

    assert!(
        matches!(explain.access(), ExplainAccessPath::IndexMultiLookup { .. }),
        "same-field strict OR equality should lower to index-multi-lookup access shape",
    );
}
