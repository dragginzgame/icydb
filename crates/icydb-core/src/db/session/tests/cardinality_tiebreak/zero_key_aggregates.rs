//! Empty global grouped input retains its implicit result and budget boundary.

use super::*;
use crate::{
    db::{
        RequestExecutionRoot, count, count_by,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        query::{
            builder::AggregateExpr,
            plan::{
                AggregateKind, GroupAggregateSpec,
                expr::{BinaryOp, Expr},
            },
            preparation::with_preparation_work,
        },
    },
    types::Decimal,
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticFactTag, ErrorCode,
};

// Retain the maintained expression-input route; bare field targets without
// keys belong to the separately admitted global-DISTINCT aggregate shape.
fn numeric_aggregate(kind: AggregateKind, field: &str) -> AggregateExpr {
    AggregateExpr::from_expression_input(
        kind,
        Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Field(field.into())),
            right: Box::new(Expr::Literal(Value::Nat64(0))),
        },
    )
}

#[test]
fn zero_key_grouped_empty_result_obeys_having_offset_and_limit() {
    let session = initialize();
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    for filtered in [false, true] {
        for (having_count, offset, expected_rows) in [(0, 0, 1), (1, 0, 0), (0, 1, 0), (0, 2, 0)] {
            let query = with_preparation_work(|work| {
                let aggregate = if filtered {
                    count().with_filter_expr(Expr::Literal(Value::Bool(false)))
                } else {
                    count()
                };
                StructuralQuery::new(MissingRowPolicy::Ignore)
                    .filter_for_schema(
                        catalog.accepted_schema_info(),
                        &FieldRef::new("id").eq(InputValue::nat64(999)),
                        work,
                    )?
                    .group_aggregates(vec![GroupAggregateSpec::from_aggregate_expr(
                        aggregate.clone(),
                    )])
                    .grouped_limits(1, 16 * 1024)
                    .limit(1)
                    .offset(offset)
                    .having_expr_preserving_shape(
                        Expr::Binary {
                            op: BinaryOp::Eq,
                            left: Box::new(Expr::Aggregate(aggregate)),
                            right: Box::new(Expr::Literal(Value::Nat64(having_count))),
                        },
                        work,
                    )
            })
            .unwrap();
            let page = session
                .execute_structural_grouped_from_query(&query, &catalog, None, None)
                .unwrap();
            assert_eq!(page.rows.len(), expected_rows);
            if expected_rows == 1 {
                assert!(page.rows[0].group_key().is_empty());
                assert_eq!(page.rows[0].aggregate_values(), &[OutputValue::nat64(0)]);
            }
            assert!(page.next_cursor.is_none());
        }
    }
}

#[test]
fn zero_key_grouped_count_preserves_nonempty_results_and_read_admission() {
    let session = initialize();
    for populated in [false, true] {
        if populated {
            seed_rows(&session);
        }
        let unbounded = DynamicQuery::new(ENTITY_NAME)
            .aggregate(count())
            .grouped_limits(1, 16 * 1024);
        let error = session
            .execute_public_dynamic_grouped_query(&unbounded)
            .unwrap_err();
        assert_eq!(
            error.diagnostic().error_code(),
            ErrorCode::QUERY_READ_UNBOUNDED_FULL_SCAN_REJECTED
        );
        let trusted = session
            .execute_trusted_dynamic_grouped_query(&unbounded)
            .unwrap();
        assert_eq!(trusted.rows.len(), 1);
        assert_eq!(
            trusted.rows[0].aggregate_values(),
            &[OutputValue::nat64(if populated { 12 } else { 0 })]
        );
        for capacity in [0, 4 * 1024 * 1024] {
            session.clear_shared_query_cache_for_tests(capacity);
            for (aggregate, expected_count) in [
                (count(), if populated { 12 } else { 0 }),
                (
                    numeric_aggregate(AggregateKind::Count, "id"),
                    if populated { 12 } else { 0 },
                ),
                (
                    count().with_filter_expr(Expr::Literal(Value::Bool(false))),
                    0,
                ),
            ] {
                let all_rows = DynamicQuery::new(ENTITY_NAME)
                    .filter(FieldRef::new("common").eq("everyone"))
                    .aggregate(aggregate)
                    .grouped_limits(1, 16 * 1024);
                for _ in 0..2 {
                    for page in [
                        session
                            .execute_public_dynamic_grouped_query(&all_rows)
                            .unwrap(),
                        session
                            .execute_trusted_dynamic_grouped_query(&all_rows)
                            .unwrap(),
                    ] {
                        assert_eq!(page.rows.len(), 1);
                        assert!(page.rows[0].group_key().is_empty());
                        assert_eq!(
                            page.rows[0].aggregate_values(),
                            &[OutputValue::nat64(expected_count)]
                        );
                        assert!(page.next_cursor.is_none());
                    }
                }
            }
        }
    }
}

#[test]
fn zero_key_grouped_empty_input_matches_scalar_and_distinct_results() {
    let session = initialize();
    for populated in [false, true] {
        if populated {
            seed_rows(&session);
        }
        for capacity in [0, 4 * 1024 * 1024] {
            session.clear_shared_query_cache_for_tests(capacity);
            for (aggregate, expected) in [
                (count(), OutputValue::nat64(0)),
                (
                    numeric_aggregate(AggregateKind::Count, "id"),
                    OutputValue::nat64(0),
                ),
                (
                    numeric_aggregate(AggregateKind::Sum, "id"),
                    OutputValue::null(),
                ),
                (
                    numeric_aggregate(AggregateKind::Avg, "id"),
                    OutputValue::null(),
                ),
                (
                    numeric_aggregate(AggregateKind::Min, "id"),
                    OutputValue::null(),
                ),
                (
                    numeric_aggregate(AggregateKind::Max, "id"),
                    OutputValue::null(),
                ),
                (count_by("id").distinct(), OutputValue::nat64(0)),
            ] {
                for filter in [
                    FieldRef::new("id").eq(InputValue::nat64(999)),
                    FieldRef::new("common").eq("missing"),
                ] {
                    let query = DynamicQuery::new(ENTITY_NAME)
                        .filter(filter)
                        .aggregate(aggregate.clone())
                        .grouped_limits(1, 16 * 1024)
                        .limit(1);
                    for _ in 0..2 {
                        for page in [
                            session
                                .execute_public_dynamic_grouped_query(&query)
                                .unwrap(),
                            session
                                .execute_trusted_dynamic_grouped_query(&query)
                                .unwrap(),
                        ] {
                            assert_eq!(page.rows.len(), 1);
                            assert!(page.rows[0].group_key().is_empty());
                            assert_eq!(
                                page.rows[0].aggregate_values(),
                                std::slice::from_ref(&expected)
                            );
                            assert!(page.next_cursor.is_none());
                        }
                    }
                }
            }
            assert_eq!(
                projection_rows(&session, "SELECT COUNT(*) FROM PlannerRow WHERE id = 999"),
                vec![vec![OutputValue::nat64(0)]]
            );
            let keyed = DynamicQuery::new(ENTITY_NAME)
                .filter(FieldRef::new("id").eq(InputValue::nat64(999)))
                .group_by("rare")
                .aggregate(count())
                .grouped_limits(1, 16 * 1024);
            assert!(
                session
                    .execute_public_dynamic_grouped_query(&keyed)
                    .unwrap()
                    .rows
                    .is_empty()
            );
        }
    }
}

#[test]
fn zero_key_grouped_bundles_preserve_nonempty_null_and_filtered_inputs() {
    let session = super::sql_not_null::initialize_nullable_rows();
    for capacity in [0, 4 * 1024 * 1024] {
        session.clear_shared_query_cache_for_tests(capacity);
        for (id, rows, present) in [(2, 1, true), (3, 1, false), (99, 0, false)] {
            let expected = vec![
                OutputValue::nat64(rows),
                OutputValue::nat64(u64::from(present)),
                if present {
                    OutputValue::decimal(Decimal::from(2_u64))
                } else {
                    OutputValue::null()
                },
                if present {
                    OutputValue::decimal(Decimal::from(2_u64))
                } else {
                    OutputValue::null()
                },
                if present {
                    OutputValue::decimal(Decimal::from(2_u64))
                } else {
                    OutputValue::null()
                },
                if present {
                    OutputValue::decimal(Decimal::from(2_u64))
                } else {
                    OutputValue::null()
                },
            ];
            let mut query = DynamicQuery::new(ENTITY_NAME)
                .filter(FieldRef::new("id").eq(InputValue::nat64(id)))
                .grouped_limits(1, 16 * 1024);
            for aggregate in [
                count(),
                numeric_aggregate(AggregateKind::Count, "qty"),
                numeric_aggregate(AggregateKind::Sum, "qty"),
                numeric_aggregate(AggregateKind::Avg, "qty"),
                numeric_aggregate(AggregateKind::Min, "qty"),
                numeric_aggregate(AggregateKind::Max, "qty"),
            ] {
                query = query.aggregate(aggregate);
            }
            for _ in 0..2 {
                for page in [
                    session
                        .execute_public_dynamic_grouped_query(&query)
                        .unwrap(),
                    session
                        .execute_trusted_dynamic_grouped_query(&query)
                        .unwrap(),
                ] {
                    assert_eq!(page.rows.len(), 1);
                    assert_eq!(page.rows[0].aggregate_values(), expected);
                    assert!(page.next_cursor.is_none());
                }
            }
            let filtered = DynamicQuery::new(ENTITY_NAME)
                .filter(FieldRef::new("id").eq(InputValue::nat64(id)))
                .aggregate(count().with_filter_expr(Expr::Literal(Value::Bool(false))))
                .aggregate(
                    numeric_aggregate(AggregateKind::Sum, "qty")
                        .with_filter_expr(Expr::Literal(Value::Bool(false))),
                )
                .grouped_limits(1, 16 * 1024);
            let page = session
                .execute_public_dynamic_grouped_query(&filtered)
                .unwrap();
            assert_eq!(page.rows.len(), 1);
            assert_eq!(
                page.rows[0].aggregate_values(),
                &[OutputValue::nat64(0), OutputValue::null()]
            );
        }
    }
}

#[test]
fn zero_key_grouped_empty_result_charges_group_state_and_retains_typed_rejection() {
    initialize();
    for aggregate in [
        count(),
        numeric_aggregate(AggregateKind::Sum, "id"),
        count_by("id").distinct(),
    ] {
        for filter in [
            FieldRef::new("id").eq(InputValue::nat64(999)),
            FieldRef::new("common").eq("missing"),
        ] {
            for resource in [
                Resource::GroupDistinctEntries,
                Resource::GroupDistinctStateBytes,
            ] {
                let root = RequestExecutionRoot::new_for_tests(
                    HardExecutionBudget::uniform_for_tests(
                        16_000_000,
                        HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                    )
                    .with_limit_for_tests(resource, 0),
                );
                let error = new_request_session(&root)
                    .execute_trusted_dynamic_grouped_query(
                        &DynamicQuery::new(ENTITY_NAME)
                            .filter(filter.clone())
                            .aggregate(aggregate.clone())
                            .grouped_limits(1, 16 * 1024),
                    )
                    .unwrap_err();
                assert_eq!(
                    error.diagnostic().error_code(),
                    ErrorCode::RUNTIME_BOUNDARY_EXECUTION_BUDGET_EXCEEDED
                );
                assert!(
                    error
                        .diagnostic_facts()
                        .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                );
            }
        }
    }
}
