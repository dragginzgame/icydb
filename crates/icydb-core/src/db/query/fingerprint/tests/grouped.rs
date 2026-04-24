use super::*;

#[test]
fn explain_fingerprint_grouped_strategy_only_change_does_not_invalidate() {
    let mut hash_strategy = grouped_explain_with_fixed_shape();
    let mut ordered_strategy = hash_strategy.clone();

    let ExplainGrouping::Grouped {
        strategy: hash_value,
        ..
    } = &mut hash_strategy.grouping
    else {
        panic!("grouped explain fixture must produce grouped explain shape");
    };
    *hash_value = "hash_group";
    let ExplainGrouping::Grouped {
        strategy: ordered_value,
        ..
    } = &mut ordered_strategy.grouping
    else {
        panic!("grouped explain fixture must produce grouped explain shape");
    };
    *ordered_value = "ordered_group";

    assert_eq!(
        hash_strategy.fingerprint(),
        ordered_strategy.fingerprint(),
        "execution strategy hints are explain/runtime metadata and must not affect semantic fingerprint identity",
    );
}

#[test]
fn grouped_fingerprint_identity_projection_remains_stable() {
    let plan = grouped_query_with_fixed_shape();
    let identity_projection = plan.projection_spec_for_identity();

    assert_eq!(
        plan.fingerprint().as_hex(),
        encode_hex_lower(&fingerprint_with_projection(&plan, &identity_projection)),
        "grouped fingerprint identity must stay stable across plan-owned and explain-owned grouped projection seams",
    );
}

#[test]
fn grouped_continuation_signature_distinguishes_widened_having_expression_shape() {
    let left = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
        .into_grouped_with_having_expr(
            GroupSpec {
                group_fields: vec![FieldSlot::from_parts_for_test(1, "rank")],
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Count,
                    target_field: None,
                    input_expr: None,
                    filter_expr: None,
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            },
            Some(Expr::Binary {
                op: crate::db::query::plan::expr::BinaryOp::Gt,
                left: Box::new(Expr::Binary {
                    op: crate::db::query::plan::expr::BinaryOp::Add,
                    left: Box::new(Expr::Aggregate(crate::db::count())),
                    right: Box::new(Expr::Literal(Value::Uint(1))),
                }),
                right: Box::new(Expr::Literal(Value::Uint(5))),
            }),
        );
    let right = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
        .into_grouped_with_having_expr(
            GroupSpec {
                group_fields: vec![FieldSlot::from_parts_for_test(1, "rank")],
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Count,
                    target_field: None,
                    input_expr: None,
                    filter_expr: None,
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            },
            Some(Expr::Binary {
                op: crate::db::query::plan::expr::BinaryOp::Gt,
                left: Box::new(Expr::Binary {
                    op: crate::db::query::plan::expr::BinaryOp::Add,
                    left: Box::new(Expr::Aggregate(crate::db::count())),
                    right: Box::new(Expr::Literal(Value::Uint(2))),
                }),
                right: Box::new(Expr::Literal(Value::Uint(5))),
            }),
        );

    assert_ne!(
        left.fingerprint(),
        right.fingerprint(),
        "semantic fingerprint must now distinguish grouped HAVING shape changes once grouped plan-hash identity includes grouped semantic shape",
    );
    assert_ne!(
        left.continuation_signature("tests::Entity"),
        right.continuation_signature("tests::Entity"),
        "grouped continuation signature must distinguish widened HAVING expression trees with different arithmetic leaves",
    );
}
