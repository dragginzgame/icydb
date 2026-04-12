use super::support::*;

type GroupedIntentCaseBuilder = fn() -> AccessPlannedQuery;
type GroupedIntentPlanContractCase<'a> = (
    &'a str,
    GroupedIntentCaseBuilder,
    GroupedIntentCaseBuilder,
    Option<&'static str>,
    bool,
    bool,
);
type GroupedIntentErrorCase<'a> = (&'a str, fn() -> QueryError, fn(&QueryError) -> bool);
type GroupedIntentPlanResultBuilder =
    fn() -> Result<crate::db::CompiledQuery<PlanEntity>, QueryError>;
type GroupedIntentPlanResultCase<'a> = (
    &'a str,
    GroupedIntentPlanResultBuilder,
    fn(Result<crate::db::CompiledQuery<PlanEntity>, QueryError>) -> bool,
);

fn assert_grouped_query_error_case(
    label: &str,
    build_error: fn() -> QueryError,
    predicate: fn(&QueryError) -> bool,
) {
    let err = build_error();
    assert!(
        predicate(&err),
        "{label}: grouped query builder error contract drifted",
    );
}

fn assert_grouped_plan_result_case(
    label: &str,
    build_result: GroupedIntentPlanResultBuilder,
    predicate: fn(Result<crate::db::CompiledQuery<PlanEntity>, QueryError>) -> bool,
) {
    assert!(
        predicate(build_result()),
        "{label}: grouped intent plan result contract drifted",
    );
}

fn grouped_load_limit_without_order_result()
-> Result<crate::db::CompiledQuery<PlanEntity>, QueryError> {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .limit(1)
        .plan()
}

fn grouped_load_distinct_without_adjacency_result()
-> Result<crate::db::CompiledQuery<PlanEntity>, QueryError> {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .distinct()
        .plan()
}

fn grouped_load_order_prefix_mismatch_result()
-> Result<crate::db::CompiledQuery<PlanEntity>, QueryError> {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("id")
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .limit(1)
        .plan()
}

fn grouped_load_order_prefix_alignment_result()
-> Result<crate::db::CompiledQuery<PlanEntity>, QueryError> {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .limit(1)
        .plan()
}

fn grouped_load_order_without_limit_result()
-> Result<crate::db::CompiledQuery<PlanEntity>, QueryError> {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .plan()
}

fn grouped_load_distinct_count_terminal_result()
-> Result<crate::db::CompiledQuery<PlanEntity>, QueryError> {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count().distinct())
        .plan()
}

fn grouped_load_distinct_count_field_terminal_result()
-> Result<crate::db::CompiledQuery<PlanEntity>, QueryError> {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(count_by("name").distinct())
        .plan()
}

fn grouped_plan_result_is_ok(
    result: Result<crate::db::CompiledQuery<PlanEntity>, QueryError>,
) -> bool {
    result.is_ok()
}

fn grouped_plan_result_has_distinct_adjacency_error(
    result: Result<crate::db::CompiledQuery<PlanEntity>, QueryError>,
) -> bool {
    let Err(err) = result else {
        return false;
    };

    query_error_is_group_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::DistinctAdjacencyEligibilityRequired
        )
    })
}

fn grouped_plan_result_has_order_prefix_mismatch(
    result: Result<crate::db::CompiledQuery<PlanEntity>, QueryError>,
) -> bool {
    let Err(err) = result else {
        return false;
    };

    query_error_is_group_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::OrderPrefixNotAlignedWithGroupKeys
        )
    })
}

fn grouped_plan_result_has_order_requires_limit(
    result: Result<crate::db::CompiledQuery<PlanEntity>, QueryError>,
) -> bool {
    let Err(err) = result else {
        return false;
    };

    query_error_is_group_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::OrderRequiresLimit
        )
    })
}

fn grouped_global_distinct_mixed_terminal_shape_error() -> QueryError {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(crate::db::count_by("name").distinct())
        .aggregate(crate::db::count())
        .plan()
        .expect_err(
            "global grouped distinct without group keys should reject mixed aggregate shape",
        )
}

fn grouped_distinct_unsupported_kind_error() -> QueryError {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(exists().distinct())
        .plan()
        .expect_err("grouped distinct exists should remain rejected")
}

fn grouped_global_distinct_non_numeric_sum_error() -> QueryError {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(sum("name").distinct())
        .plan()
        .expect_err("global sum(distinct non-numeric field) should fail")
}

fn grouped_having_with_distinct_error() -> QueryError {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .having_aggregate(0, CompareOp::Gt, Value::Uint(0))
        .expect("having aggregate clause should append on grouped query")
        .distinct()
        .plan()
        .expect_err("grouped having with distinct should be rejected in this release")
}

fn grouped_ordered_having_with_distinct_error() -> QueryError {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
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
        )
}

fn is_global_distinct_shape_unsupported_query_error(err: &QueryError) -> bool {
    query_error_is_group_plan_error(err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::GlobalDistinctAggregateShapeUnsupported
        )
    })
}

fn is_distinct_unsupported_exists_query_error(err: &QueryError) -> bool {
    query_error_is_group_plan_error(err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::DistinctAggregateKindUnsupported { index, kind }
                if *index == 0 && kind == "Exists"
        )
    })
}

fn is_global_distinct_sum_target_not_numeric_query_error(err: &QueryError) -> bool {
    query_error_is_group_plan_error(err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::GlobalDistinctSumTargetNotNumeric { index, field }
                if *index == 0 && field == "name"
        )
    })
}

fn is_distinct_having_unsupported_query_error(err: &QueryError) -> bool {
    query_error_is_group_plan_error(err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::DistinctHavingUnsupported
        )
    })
}

fn helper_grouped_count_plan() -> AccessPlannedQuery {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .plan()
        .expect("helper grouped count should plan")
        .into_inner()
}

fn builder_grouped_count_plan() -> AccessPlannedQuery {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(count())
        .plan()
        .expect("builder grouped count should plan")
        .into_inner()
}

fn helper_global_distinct_count_plan() -> AccessPlannedQuery {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(crate::db::count_by("name").distinct())
        .plan()
        .expect("helper global count(distinct field) should plan")
        .into_inner()
}

fn builder_global_distinct_count_plan() -> AccessPlannedQuery {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(count_by("name").distinct())
        .plan()
        .expect("builder global count(distinct field) should plan")
        .into_inner()
}

fn helper_global_distinct_sum_plan() -> AccessPlannedQuery {
    Query::<PlanNumericEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(crate::db::sum("rank").distinct())
        .plan()
        .expect("helper global sum(distinct field) should plan")
        .into_inner()
}

fn builder_global_distinct_sum_plan() -> AccessPlannedQuery {
    Query::<PlanNumericEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(sum("rank").distinct())
        .plan()
        .expect("builder global sum(distinct field) should plan")
        .into_inner()
}

fn helper_grouped_count_ordered_limited_plan() -> AccessPlannedQuery {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .limit(1)
        .plan()
        .expect("helper grouped count plan should build")
        .into_inner()
}

fn builder_grouped_count_ordered_limited_plan() -> AccessPlannedQuery {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(count())
        .limit(1)
        .plan()
        .expect("builder grouped count plan should build")
        .into_inner()
}

fn helper_global_distinct_count_limited_plan() -> AccessPlannedQuery {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(crate::db::count_by("name").distinct())
        .limit(1)
        .plan()
        .expect("helper grouped global distinct count plan should build")
        .into_inner()
}

fn builder_global_distinct_count_limited_plan() -> AccessPlannedQuery {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(count_by("name").distinct())
        .limit(1)
        .plan()
        .expect("builder grouped global distinct count plan should build")
        .into_inner()
}

fn helper_global_distinct_sum_limited_plan() -> AccessPlannedQuery {
    Query::<PlanNumericEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(crate::db::sum("rank").distinct())
        .limit(1)
        .plan()
        .expect("helper grouped global distinct sum plan should build")
        .into_inner()
}

fn builder_global_distinct_sum_limited_plan() -> AccessPlannedQuery {
    Query::<PlanNumericEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(sum("rank").distinct())
        .limit(1)
        .plan()
        .expect("builder grouped global distinct sum plan should build")
        .into_inner()
}

fn helper_grouped_multi_aggregate_limited_plan() -> AccessPlannedQuery {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .aggregate(crate::db::max())
        .limit(1)
        .plan()
        .expect("helper grouped multi-aggregate plan should build")
        .into_inner()
}

fn builder_grouped_multi_aggregate_limited_plan() -> AccessPlannedQuery {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(count())
        .aggregate(max())
        .limit(1)
        .plan()
        .expect("builder grouped multi-aggregate plan should build")
        .into_inner()
}

fn assert_grouped_builder_explain_parity(
    label: &str,
    build_helper: fn() -> AccessPlannedQuery,
    build_builder: fn() -> AccessPlannedQuery,
) {
    let helper_plan = build_helper();
    let builder_plan = build_builder();

    assert_eq!(
        helper_plan.explain(),
        builder_plan.explain(),
        "{label}: helper and builder grouped explain shapes must stay identical",
    );
}

fn assert_grouped_builder_plan_contract(
    label: &str,
    build_helper: fn() -> AccessPlannedQuery,
    build_builder: fn() -> AccessPlannedQuery,
    entity_path: Option<&'static str>,
    compare_grouping: bool,
    compare_token_bytes: bool,
) {
    let helper_plan = build_helper();
    let builder_plan = build_builder();

    if compare_grouping {
        assert_eq!(
            helper_plan.explain().grouping,
            builder_plan.explain().grouping,
            "{label}: helper and builder grouped projection shapes must match",
        );
    }

    assert_eq!(
        helper_plan.fingerprint(),
        builder_plan.fingerprint(),
        "{label}: helper and builder grouped fingerprints must match",
    );

    let Some(entity_path) = entity_path else {
        return;
    };

    let helper_signature = helper_plan.continuation_signature(entity_path);
    let builder_signature = builder_plan.continuation_signature(entity_path);
    assert_eq!(
        helper_signature, builder_signature,
        "{label}: helper and builder grouped continuation signatures must match",
    );

    if compare_token_bytes {
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
            "{label}: helper and builder grouped continuation token bytes must match",
        );
    }
}

#[test]
fn grouped_load_policy_matrix() {
    let cases: &[GroupedIntentPlanResultCase<'_>] = &[
        (
            "grouped limit without order",
            grouped_load_limit_without_order_result,
            grouped_plan_result_is_ok,
        ),
        (
            "grouped distinct without adjacency",
            grouped_load_distinct_without_adjacency_result,
            grouped_plan_result_has_distinct_adjacency_error,
        ),
        (
            "grouped order prefix mismatch",
            grouped_load_order_prefix_mismatch_result,
            grouped_plan_result_has_order_prefix_mismatch,
        ),
        (
            "grouped order prefix alignment",
            grouped_load_order_prefix_alignment_result,
            grouped_plan_result_is_ok,
        ),
        (
            "grouped order without limit",
            grouped_load_order_without_limit_result,
            grouped_plan_result_has_order_requires_limit,
        ),
    ];

    for (label, build_result, predicate) in cases.iter().copied() {
        assert_grouped_plan_result_case(label, build_result, predicate);
    }
}

#[test]
fn grouped_distinct_terminal_acceptance_matrix() {
    let cases: &[(&str, GroupedIntentPlanResultBuilder)] = &[
        (
            "grouped distinct count terminal",
            grouped_load_distinct_count_terminal_result,
        ),
        (
            "grouped distinct count(field) terminal",
            grouped_load_distinct_count_field_terminal_result,
        ),
    ];

    for (label, build_result) in cases.iter().copied() {
        assert_grouped_plan_result_case(label, build_result, grouped_plan_result_is_ok);
    }
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
fn grouped_aggregate_builder_explain_parity_matrix() {
    let cases = [
        (
            "grouped count",
            helper_grouped_count_plan as fn() -> AccessPlannedQuery,
            builder_grouped_count_plan as fn() -> AccessPlannedQuery,
        ),
        (
            "global distinct count",
            helper_global_distinct_count_plan as fn() -> AccessPlannedQuery,
            builder_global_distinct_count_plan as fn() -> AccessPlannedQuery,
        ),
        (
            "global distinct sum",
            helper_global_distinct_sum_plan as fn() -> AccessPlannedQuery,
            builder_global_distinct_sum_plan as fn() -> AccessPlannedQuery,
        ),
    ];

    for (label, build_helper, build_builder) in cases {
        assert_grouped_builder_explain_parity(label, build_helper, build_builder);
    }
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
fn grouped_aggregate_builder_plan_contract_matrix() {
    let cases: &[GroupedIntentPlanContractCase<'_>] = &[
        (
            "grouped count with ordering and limit",
            helper_grouped_count_ordered_limited_plan,
            builder_grouped_count_ordered_limited_plan,
            Some("intent::tests::PlanEntity"),
            false,
            true,
        ),
        (
            "global distinct count with limit",
            helper_global_distinct_count_limited_plan,
            builder_global_distinct_count_limited_plan,
            Some("intent::tests::PlanEntity"),
            false,
            false,
        ),
        (
            "global distinct sum with limit",
            helper_global_distinct_sum_limited_plan,
            builder_global_distinct_sum_limited_plan,
            Some("intent::tests::PlanNumericEntity"),
            true,
            false,
        ),
        (
            "grouped multi-aggregate projection order",
            helper_grouped_multi_aggregate_limited_plan,
            builder_grouped_multi_aggregate_limited_plan,
            None,
            true,
            false,
        ),
    ];

    for (label, build_helper, build_builder, entity_path, compare_grouping, compare_token_bytes) in
        cases.iter().copied()
    {
        assert_grouped_builder_plan_contract(
            label,
            build_helper,
            build_builder,
            entity_path,
            compare_grouping,
            compare_token_bytes,
        );
    }
}

#[test]
fn grouped_aggregate_builder_rejection_matrix() {
    let cases: &[GroupedIntentErrorCase<'_>] = &[
        (
            "global distinct mixed terminal shape",
            grouped_global_distinct_mixed_terminal_shape_error,
            is_global_distinct_shape_unsupported_query_error,
        ),
        (
            "distinct unsupported kind",
            grouped_distinct_unsupported_kind_error,
            is_distinct_unsupported_exists_query_error,
        ),
        (
            "global distinct sum non-numeric target",
            grouped_global_distinct_non_numeric_sum_error,
            is_global_distinct_sum_target_not_numeric_query_error,
        ),
        (
            "having with distinct",
            grouped_having_with_distinct_error,
            is_distinct_having_unsupported_query_error,
        ),
        (
            "ordered eligible having with distinct",
            grouped_ordered_having_with_distinct_error,
            is_distinct_having_unsupported_query_error,
        ),
    ];

    for (label, build_error, predicate) in cases.iter().copied() {
        assert_grouped_query_error_case(label, build_error, predicate);
    }
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
fn grouped_having_requires_group_by() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .having_group("name", CompareOp::Eq, Value::Text("alpha".to_string()))
        .expect_err("having should fail when group_by is missing");

    assert!(matches!(
        err,
        QueryError::Intent(IntentError::HavingRequiresGroupBy)
    ));
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
