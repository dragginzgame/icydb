use super::support::*;

#[test]
fn grouped_load_limit_without_order_is_allowed() {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .limit(1)
        .plan()
        .expect("grouped pagination should use canonical grouped-key order");
}

#[test]
fn grouped_load_distinct_is_rejected_without_adjacency_eligibility() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .distinct()
        .plan()
        .expect_err("grouped distinct should be rejected until adjacency eligibility exists");

    assert!(query_error_is_group_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::DistinctAdjacencyEligibilityRequired
        )
    }));
}

#[test]
fn grouped_load_order_prefix_mismatch_is_rejected() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("id")
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .limit(1)
        .plan()
        .expect_err("grouped order should be rejected when group keys are not the order prefix");

    assert!(query_error_is_group_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::OrderPrefixNotAlignedWithGroupKeys
        )
    }));
}

#[test]
fn grouped_load_order_prefix_alignment_is_allowed() {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .limit(1)
        .plan()
        .expect("grouped order should be accepted when grouped keys lead ORDER BY and LIMIT is explicit");
}

#[test]
fn grouped_load_order_without_limit_is_rejected() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .plan()
        .expect_err("grouped order should reject missing LIMIT");

    assert!(query_error_is_group_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::OrderRequiresLimit
        )
    }));
}

#[test]
fn grouped_load_distinct_count_terminal_is_allowed() {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count().distinct())
        .plan()
        .expect("grouped distinct count terminal should plan in grouped v1");
}

#[test]
fn grouped_load_distinct_count_field_terminal_is_allowed() {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(count_by("name").distinct())
        .plan()
        .expect("grouped distinct count(field) terminal should now plan");
}

#[test]
fn grouped_aggregate_builder_count_shape_matches_helper_terminal() {
    let helper_explain = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .plan()
        .expect("helper grouped count should plan")
        .into_inner()
        .explain();
    let builder_explain = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(count())
        .plan()
        .expect("builder grouped count should plan")
        .into_inner()
        .explain();

    assert_eq!(
        helper_explain, builder_explain,
        "aggregate(count()) should preserve grouped count logical shape",
    );
}

#[test]
fn grouped_global_distinct_count_field_without_group_by_is_allowed() {
    let plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(crate::db::count_by("name").distinct())
        .plan()
        .expect("global grouped count(distinct field) should plan");

    let Some(grouped) = plan.into_inner().grouped_plan().cloned() else {
        panic!("global grouped distinct field aggregate must compile to grouped logical plan");
    };
    assert!(
        grouped.group.group_fields.is_empty(),
        "global grouped distinct aggregate should use zero group keys"
    );
    assert_eq!(
        grouped.group.aggregates.len(),
        1,
        "global grouped distinct aggregate should declare exactly one terminal"
    );
    assert_eq!(
        grouped.group.aggregates[0].target_field(),
        Some("name"),
        "global grouped distinct count should preserve target field"
    );
    assert!(
        grouped.group.aggregates[0].distinct(),
        "global grouped distinct count should preserve DISTINCT modifier"
    );
}

#[test]
fn grouped_aggregate_builder_global_distinct_count_shape_matches_helper_terminal() {
    let helper_explain = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(crate::db::count_by("name").distinct())
        .plan()
        .expect("helper global count(distinct field) should plan")
        .into_inner()
        .explain();
    let builder_explain = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(count_by("name").distinct())
        .plan()
        .expect("builder global count(distinct field) should plan")
        .into_inner()
        .explain();

    assert_eq!(
        helper_explain, builder_explain,
        "aggregate(count_by(field).distinct()) should preserve global distinct-count logical shape",
    );
}

#[test]
fn grouped_aggregate_builder_global_distinct_sum_shape_matches_helper_terminal() {
    let helper_explain = Query::<PlanNumericEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(crate::db::sum("rank").distinct())
        .plan()
        .expect("helper global sum(distinct field) should plan")
        .into_inner()
        .explain();
    let builder_explain = Query::<PlanNumericEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(sum("rank").distinct())
        .plan()
        .expect("builder global sum(distinct field) should plan")
        .into_inner()
        .explain();

    assert_eq!(
        helper_explain, builder_explain,
        "aggregate(sum(field).distinct()) should preserve global distinct-sum logical shape",
    );
}

#[test]
fn grouped_aggregate_builder_fingerprint_parity_preserves_grouping_and_order_shape() {
    let helper_plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .limit(1)
        .plan()
        .expect("helper grouped count plan should build")
        .into_inner();
    let builder_plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(count())
        .limit(1)
        .plan()
        .expect("builder grouped count plan should build")
        .into_inner();

    assert_eq!(
        helper_plan.fingerprint(),
        builder_plan.fingerprint(),
        "helper and builder grouped count plans must have identical fingerprints",
    );
    assert_eq!(
        helper_plan.continuation_signature("intent::tests::PlanEntity"),
        builder_plan.continuation_signature("intent::tests::PlanEntity"),
        "helper and builder grouped count plans must have identical continuation signatures",
    );
}

#[test]
fn grouped_aggregate_builder_terminal_matrix_matches_helper_fingerprints() {
    for terminal in ["exists", "first", "last", "min", "max"] {
        let helper_plan = match terminal {
            "exists" => Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
                .group_by("name")
                .expect("group field should resolve")
                .aggregate(crate::db::exists())
                .limit(1)
                .plan()
                .expect("helper grouped exists plan should build")
                .into_inner(),
            "first" => Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
                .group_by("name")
                .expect("group field should resolve")
                .aggregate(crate::db::first())
                .limit(1)
                .plan()
                .expect("helper grouped first plan should build")
                .into_inner(),
            "last" => Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
                .group_by("name")
                .expect("group field should resolve")
                .aggregate(crate::db::last())
                .limit(1)
                .plan()
                .expect("helper grouped last plan should build")
                .into_inner(),
            "min" => Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
                .group_by("name")
                .expect("group field should resolve")
                .aggregate(crate::db::min())
                .limit(1)
                .plan()
                .expect("helper grouped min plan should build")
                .into_inner(),
            "max" => Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
                .group_by("name")
                .expect("group field should resolve")
                .aggregate(crate::db::max())
                .limit(1)
                .plan()
                .expect("helper grouped max plan should build")
                .into_inner(),
            _ => unreachable!("terminal matrix is fixed"),
        };
        let builder_plan = match terminal {
            "exists" => Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
                .group_by("name")
                .expect("group field should resolve")
                .aggregate(exists())
                .limit(1)
                .plan()
                .expect("builder grouped exists plan should build")
                .into_inner(),
            "first" => Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
                .group_by("name")
                .expect("group field should resolve")
                .aggregate(first())
                .limit(1)
                .plan()
                .expect("builder grouped first plan should build")
                .into_inner(),
            "last" => Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
                .group_by("name")
                .expect("group field should resolve")
                .aggregate(last())
                .limit(1)
                .plan()
                .expect("builder grouped last plan should build")
                .into_inner(),
            "min" => Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
                .group_by("name")
                .expect("group field should resolve")
                .aggregate(min())
                .limit(1)
                .plan()
                .expect("builder grouped min plan should build")
                .into_inner(),
            "max" => Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
                .group_by("name")
                .expect("group field should resolve")
                .aggregate(max())
                .limit(1)
                .plan()
                .expect("builder grouped max plan should build")
                .into_inner(),
            _ => unreachable!("terminal matrix is fixed"),
        };

        assert_eq!(
            helper_plan.fingerprint(),
            builder_plan.fingerprint(),
            "terminal `{terminal}` helper/builder fingerprints must match",
        );
        assert_eq!(
            helper_plan.continuation_signature("intent::tests::PlanEntity"),
            builder_plan.continuation_signature("intent::tests::PlanEntity"),
            "terminal `{terminal}` helper/builder continuation signatures must match",
        );
    }
}

#[test]
fn grouped_aggregate_builder_fingerprint_parity_preserves_distinct_flag_shape() {
    let helper_plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(crate::db::count_by("name").distinct())
        .limit(1)
        .plan()
        .expect("helper grouped global distinct count plan should build")
        .into_inner();
    let builder_plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(count_by("name").distinct())
        .limit(1)
        .plan()
        .expect("builder grouped global distinct count plan should build")
        .into_inner();

    assert_eq!(
        helper_plan.fingerprint(),
        builder_plan.fingerprint(),
        "helper and builder global distinct-count plans must have identical fingerprints",
    );
    assert_eq!(
        helper_plan.continuation_signature("intent::tests::PlanEntity"),
        builder_plan.continuation_signature("intent::tests::PlanEntity"),
        "helper and builder global distinct-count plans must have identical continuation signatures",
    );
}

#[test]
fn grouped_aggregate_builder_fingerprint_parity_preserves_distinct_sum_shape() {
    let helper_plan = Query::<PlanNumericEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(crate::db::sum("rank").distinct())
        .limit(1)
        .plan()
        .expect("helper grouped global distinct sum plan should build")
        .into_inner();
    let builder_plan = Query::<PlanNumericEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(sum("rank").distinct())
        .limit(1)
        .plan()
        .expect("builder grouped global distinct sum plan should build")
        .into_inner();

    assert_eq!(
        helper_plan.explain().grouping,
        builder_plan.explain().grouping,
        "helper and builder global distinct-sum plans must have identical grouped projection shapes",
    );
    assert_eq!(
        helper_plan.fingerprint(),
        builder_plan.fingerprint(),
        "helper and builder global distinct-sum plans must have identical fingerprints",
    );
    assert_eq!(
        helper_plan.continuation_signature("intent::tests::PlanNumericEntity"),
        builder_plan.continuation_signature("intent::tests::PlanNumericEntity"),
        "helper and builder global distinct-sum plans must have identical continuation signatures",
    );
}

#[test]
fn grouped_aggregate_builder_fingerprint_parity_preserves_projection_order_shape() {
    let helper_plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .aggregate(crate::db::max())
        .limit(1)
        .plan()
        .expect("helper grouped multi-aggregate plan should build")
        .into_inner();
    let builder_plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(count())
        .aggregate(max())
        .limit(1)
        .plan()
        .expect("builder grouped multi-aggregate plan should build")
        .into_inner();

    assert_eq!(
        helper_plan.explain().grouping,
        builder_plan.explain().grouping,
        "helper and builder grouped multi-aggregate projection shapes must match",
    );
    assert_eq!(
        helper_plan.fingerprint(),
        builder_plan.fingerprint(),
        "helper and builder grouped multi-aggregate plans must have identical fingerprints",
    );
}

#[test]
fn grouped_aggregate_builder_continuation_token_bytes_match_helper_shape() {
    let helper_plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .limit(1)
        .plan()
        .expect("helper grouped continuation plan should build")
        .into_inner();
    let builder_plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(count())
        .limit(1)
        .plan()
        .expect("builder grouped continuation plan should build")
        .into_inner();
    let helper_signature = helper_plan.continuation_signature("intent::tests::PlanEntity");
    let builder_signature = builder_plan.continuation_signature("intent::tests::PlanEntity");
    assert_eq!(
        helper_signature, builder_signature,
        "helper and builder grouped continuation signatures must match",
    );

    let helper_token = GroupedContinuationToken::new_with_direction(
        helper_signature,
        vec![Value::Text("alpha".to_string())],
        Direction::Asc,
        0,
    )
    .encode()
    .expect("helper grouped continuation token should encode");
    let builder_token = GroupedContinuationToken::new_with_direction(
        builder_signature,
        vec![Value::Text("alpha".to_string())],
        Direction::Asc,
        0,
    )
    .encode()
    .expect("builder grouped continuation token should encode");
    assert_eq!(
        helper_token, builder_token,
        "helper and builder grouped continuation token bytes must match for equivalent shapes",
    );
}

#[test]
fn grouped_global_distinct_mixed_terminal_shape_without_group_by_is_rejected() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(crate::db::count_by("name").distinct())
        .aggregate(crate::db::count())
        .plan()
        .expect_err(
            "global grouped distinct without group keys should reject mixed aggregate shape",
        );

    assert!(query_error_is_group_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::GlobalDistinctAggregateShapeUnsupported
        )
    }));
}

#[test]
fn grouped_aggregate_builder_rejects_distinct_for_unsupported_kind() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(exists().distinct())
        .plan()
        .expect_err("grouped distinct exists should remain rejected");

    assert!(query_error_is_group_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::DistinctAggregateKindUnsupported { index, kind }
                if *index == 0 && kind == "Exists"
        )
    }));
}

#[test]
fn grouped_aggregate_builder_max_field_terminal_is_allowed() {
    let plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(max_by("name"))
        .plan()
        .expect("grouped max(field) should now plan");

    let projection = plan.projection_spec();
    let fields = projection.fields().collect::<Vec<_>>();
    assert_eq!(
        fields.len(),
        2,
        "grouped max(field) projection should include key + aggregate"
    );

    match fields[1] {
        ProjectionField::Scalar {
            expr: Expr::Aggregate(aggregate),
            alias: None,
        } => {
            assert_eq!(aggregate.kind(), AggregateKind::Max);
            assert_eq!(aggregate.target_field(), Some("name"));
            assert!(!aggregate.is_distinct());
        }
        other @ ProjectionField::Scalar { .. } => {
            panic!("grouped max(field) projection should lower to aggregate expr: {other:?}")
        }
    }
}

#[test]
fn grouped_aggregate_builder_rejects_global_distinct_sum_on_non_numeric_target() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(sum("name").distinct())
        .plan()
        .expect_err("global sum(distinct non-numeric field) should fail");

    assert!(query_error_is_group_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::GlobalDistinctSumTargetNotNumeric { index, field }
                if *index == 0 && field == "name"
        )
    }));
}

#[test]
fn grouped_having_requires_group_by() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .having_group("name", CompareOp::Eq, Value::Text("alpha".to_string()))
        .expect_err("having should fail when group_by is missing");

    assert!(matches!(
        err,
        QueryError::Intent(IntentError::HavingRequiresGroupBy)
    ));
}

#[test]
fn grouped_having_with_distinct_is_rejected() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .having_aggregate(0, CompareOp::Gt, Value::Uint(0))
        .expect("having aggregate clause should append on grouped query")
        .distinct()
        .plan()
        .expect_err("grouped having with distinct should be rejected in this release");

    assert!(query_error_is_group_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::DistinctHavingUnsupported
        )
    }));
}

#[test]
fn grouped_having_with_distinct_is_rejected_for_ordered_eligible_shape() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .having_aggregate(0, CompareOp::Gt, Value::Uint(0))
        .expect("having aggregate clause should append on grouped query")
        .distinct()
        .plan()
        .expect_err(
            "grouped having with distinct should be rejected even when grouped order prefix is aligned",
        );

    assert!(query_error_is_group_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::DistinctHavingUnsupported
        )
    }));
}

#[cfg(feature = "sql")]
#[test]
fn compiled_query_projection_spec_lowers_grouped_shape_in_declaration_order() {
    let compiled = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group by should resolve")
        .aggregate(count())
        .plan()
        .expect("grouped plan should build");
    let projection = compiled.projection_spec();
    let fields = projection.fields().collect::<Vec<_>>();
    assert_eq!(
        fields.len(),
        2,
        "grouped projection should include key + aggregate"
    );

    match fields[0] {
        ProjectionField::Scalar {
            expr: Expr::Field(field),
            alias: None,
        } => assert_eq!(field.as_str(), "name"),
        other @ ProjectionField::Scalar { .. } => {
            panic!("first grouped projection field should be grouped key expr: {other:?}")
        }
    }
    match fields[1] {
        ProjectionField::Scalar {
            expr: Expr::Aggregate(aggregate),
            alias: None,
        } => {
            assert_eq!(aggregate.kind(), AggregateKind::Count);
            assert_eq!(aggregate.target_field(), None);
            assert!(!aggregate.is_distinct());
        }
        other @ ProjectionField::Scalar { .. } => {
            panic!("second grouped projection field should be grouped aggregate expr: {other:?}")
        }
    }
}

#[cfg(feature = "sql")]
#[test]
fn compiled_query_projection_spec_preserves_global_distinct_aggregate_semantics() {
    let compiled = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(count_by("name").distinct())
        .plan()
        .expect("global distinct grouped plan should build");
    let projection = compiled.projection_spec();
    let fields = projection.fields().collect::<Vec<_>>();
    assert_eq!(
        fields.len(),
        1,
        "global distinct grouped projection should only include one aggregate"
    );

    match fields[0] {
        ProjectionField::Scalar {
            expr: Expr::Aggregate(aggregate),
            alias: None,
        } => {
            assert_eq!(aggregate.kind(), AggregateKind::Count);
            assert_eq!(aggregate.target_field(), Some("name"));
            assert!(aggregate.is_distinct());
        }
        other @ ProjectionField::Scalar { .. } => {
            panic!("global distinct projection should lower to aggregate expr: {other:?}")
        }
    }
}
