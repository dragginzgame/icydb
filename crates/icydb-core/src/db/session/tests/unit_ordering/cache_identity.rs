//! Warm query plans must preserve current intent and typed expression identity.

use super::*;
use crate::db::{
    MissingRowPolicy,
    query::{
        builder::{AggregateExpr, count},
        intent::StructuralQuery,
        plan::{
            AggregateKind, GroupAggregateSpec, OrderSpec, QueryMode,
            expr::{BinaryOp, Expr, ProjectionSelection},
        },
        preparation::with_preparation_work,
    },
};
use icydb_diagnostic_code::DiagnosticExecutionLane;

#[test]
fn memoized_query_keys_survive_weighted_cache_eviction() {
    let session = initialize();
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let prepare = |query: &StructuralQuery| {
        session
            .cached_shared_query_plan_for_accepted_authority_with_catalog_and_reuse(
                catalog.accepted_entity_authority(),
                &catalog,
                query,
                DiagnosticExecutionLane::TrustedRead,
            )
            .unwrap()
    };
    let original = query();
    let (held_plan, reuse) = prepare(&original);
    assert!(!reuse.is_hit());
    let capacity = session.shared_query_cache_usage_for_tests().1;
    let changed = original.clone().limit(2);

    // Keep the query memo and a detached plan alive while a one-entry byte
    // allowance forces the cache to release each preceding key/artifact.
    session.clear_shared_query_cache_for_tests(capacity);
    for current in [&original, &changed, &original] {
        assert!(!prepare(current).1.is_hit());
        assert!(prepare(current).1.is_hit());
        let (entries, bytes) = session.shared_query_cache_usage_for_tests();
        assert_eq!(entries, 1);
        assert!(bytes <= capacity);
    }
    assert!(matches!(
        held_plan.logical_plan().scalar_plan().mode,
        QueryMode::Load(spec) if spec.limit() == Some(1),
    ));
}

#[test]
fn projection_cache_preserves_aliases_across_warm_calls() {
    for suffix in ["", ", COUNT(*)"] {
        for sequence in [[0, 1, 0], [1, 0, 1]] {
            // Isolate both SQL-command and shared-plan caches for each call order.
            std::thread::spawn(move || {
                let session = initialize();
                seed_singleton(&session);
                let grouped = if suffix.is_empty() {
                    ""
                } else {
                    " GROUP BY label"
                };
                let execute = |alias| {
                    let result = session
                        .execute_trusted_sql_query(&format!(
                            "SELECT label AS {alias}{suffix} FROM Singleton{grouped}"
                        ))
                        .unwrap();
                    match result {
                        SqlStatementResult::Projection {
                            columns,
                            fixed_scales,
                            rows,
                            row_count,
                        } => {
                            assert_eq!(rows, vec![vec![OutputValue::text("singleton".into())]]);
                            (columns, fixed_scales, row_count)
                        }
                        SqlStatementResult::Grouped {
                            columns,
                            fixed_scales,
                            rows,
                            row_count,
                            next_cursor,
                        } => {
                            assert_eq!(rows.len(), 1);
                            assert_eq!(
                                rows[0].group_key(),
                                &[OutputValue::text("singleton".into())]
                            );
                            assert_eq!(rows[0].aggregate_values(), &[OutputValue::nat64(1)]);
                            assert!(next_cursor.is_none());
                            (columns, fixed_scales, row_count)
                        }
                        _ => panic!("projected result"),
                    }
                };
                let aliases = ["first_label", "second_label"];
                session.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
                for index in sequence {
                    let (columns, fixed_scales, row_count) = execute(aliases[index]);
                    assert_eq!(columns[0], aliases[index]);
                    assert_eq!(fixed_scales, vec![None; columns.len()]);
                    assert_eq!(row_count, 1);
                }
                assert!(session.shared_query_cache_usage_for_tests().0 > 0);
            })
            .join()
            .unwrap();
        }
    }
}

#[test]
fn order_cache_preserves_aggregate_type_rejections_after_warm_calls() {
    let session = initialize();
    seed_singleton(&session);
    let aggregate =
        |input| AggregateExpr::from_expression_input(AggregateKind::Sum, Expr::Literal(input));
    let decimal = Value::Decimal(crate::types::Decimal::from(1_u64));
    let wide = Value::U256(U256::from(1_u64));
    let execute = |order_input: Value| {
        session.execute_trusted_dynamic_grouped_query(
            &DynamicQuery::new(ENTITY_NAME)
                .group_by("label")
                .aggregate(aggregate(decimal.clone()))
                .order_by(asc(aggregate(order_input)))
                .grouped_limits(1, 4096)
                .limit(1),
        )
    };
    session.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
    let cold_error = execute(wide.clone()).unwrap_err();
    session.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
    let expected = execute(decimal.clone()).unwrap();
    // An undeclared aggregate order rejects during execution and can retain its
    // prepared plan. Neither that plan nor the valid one may mask the other.
    for sequence in [[false, true, false], [true, false, true]] {
        session.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
        for valid in sequence {
            if valid {
                assert_eq!(execute(decimal.clone()).unwrap(), expected);
            } else {
                assert_eq!(
                    execute(wide.clone()).unwrap_err().diagnostic(),
                    cold_error.diagnostic(),
                );
            }
        }
        assert!(session.shared_query_cache_usage_for_tests().0 > 0);
    }
}

#[test]
fn aggregate_cache_keeps_filter_rejections_after_valid_warm_calls() {
    let session = initialize();
    seed_singleton(&session);
    let execute = |right: Value| {
        session.execute_trusted_dynamic_grouped_query(
            &DynamicQuery::new(ENTITY_NAME)
                .group_by("label")
                .aggregate(count().with_filter_expr(Expr::Binary {
                    op: BinaryOp::Eq,
                    left: Box::new(Expr::Literal(Value::U256(U256::from(1_u64)))),
                    right: Box::new(Expr::Literal(right)),
                }))
                .grouped_limits(1, 4096)
                .limit(1),
        )
    };
    let invalid = Value::Decimal(crate::types::Decimal::from(1_u64));
    let valid = Value::U256(U256::from(1_u64));
    session.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
    let cold_error = execute(invalid.clone()).unwrap_err();
    // This mixed-domain comparison rejects during evaluation, so its prepared
    // plan may remain cached. It must not replace the valid filter's plan.
    let expected = execute(valid.clone()).unwrap();
    assert_eq!(expected.row_count, 1);
    let retained = session.shared_query_cache_usage_for_tests();
    for _ in 0..2 {
        assert_eq!(
            execute(invalid.clone()).unwrap_err().diagnostic(),
            cold_error.diagnostic()
        );
        assert_eq!(session.shared_query_cache_usage_for_tests(), retained);
        assert_eq!(execute(valid.clone()).unwrap(), expected);
    }
}

#[test]
fn aggregate_cache_preserves_typed_input_results_across_warm_calls() {
    let session = initialize();
    seed_singleton(&session);
    let decimal = crate::types::Decimal::from(1_u64);
    let wide = U256::from(1_u64);
    let cases = [
        (Value::Decimal(decimal), OutputValue::decimal(decimal)),
        (Value::U256(wide), OutputValue::u256(wide)),
    ];
    let execute = |input: Value| {
        session
            .execute_trusted_dynamic_grouped_query(
                &DynamicQuery::new(ENTITY_NAME)
                    .group_by("label")
                    .aggregate(AggregateExpr::from_expression_input(
                        AggregateKind::Sum,
                        Expr::Literal(input),
                    ))
                    .grouped_limits(1, 4096)
                    .limit(1),
            )
            .unwrap()
    };
    // Each domain works cold; shared-cache reuse must preserve the same type.
    for (input, expected) in &cases {
        session.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
        let result = execute(input.clone());
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].aggregate_values(),
            std::slice::from_ref(expected)
        );
    }
    for sequence in [[0, 1, 0], [1, 0, 1]] {
        session.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
        for index in sequence {
            let (input, expected) = &cases[index];
            let result = execute(input.clone());
            assert_eq!(result.rows.len(), 1);
            assert_eq!(
                result.rows[0].aggregate_values(),
                std::slice::from_ref(expected)
            );
        }
    }
    assert!(session.shared_query_cache_usage_for_tests().0 > 0);
}

fn query() -> StructuralQuery {
    StructuralQuery::new(MissingRowPolicy::Ignore)
        .order_spec(OrderSpec {
            fields: vec![asc("id").lower()],
        })
        .limit(1)
}

// Compare maintained semantics, not whether a particular memo cell is empty.
fn assert_current_identity(
    base: impl Fn() -> StructuralQuery,
    change: impl Fn(StructuralQuery) -> StructuralQuery,
) {
    let original = base();
    let old_key = original.structural_cache_key_with_normalized_predicate_fingerprint(None);
    let changed = change(original.clone());
    let fresh_key = change(base()).structural_cache_key_with_normalized_predicate_fingerprint(None);
    assert_ne!(fresh_key, old_key, "fixture must change semantic identity");
    assert_eq!(
        changed.structural_cache_key_with_normalized_predicate_fingerprint(None),
        fresh_key,
    );
    assert_eq!(
        original.structural_cache_key_with_normalized_predicate_fingerprint(None),
        old_key,
    );
}

#[test]
fn warmed_scalar_builder_identity_matches_fresh_intent() {
    assert_current_identity(query, |query| query.limit(0));
    assert_current_identity(query, |query| query.offset(1));
    assert_current_identity(query, StructuralQuery::distinct);
    assert_current_identity(query, StructuralQuery::delete);
    assert_current_identity(|| query().delete(), StructuralQuery::into_load_selection);
    assert_current_identity(query, |query| {
        query.order_spec(OrderSpec {
            fields: vec![crate::db::desc("id").lower()],
        })
    });
    assert_current_identity(query, |query| query.select_fields(["label"]));
    assert_current_identity(query, |query| {
        query.projection_selection(ProjectionSelection::Fields(vec!["label".into()]))
    });
    with_preparation_work(|work| {
        assert_current_identity(query, |query| {
            query
                .filter_expr(Expr::Literal(Value::Bool(false)), work)
                .unwrap()
        });
        assert_current_identity(query, |query| {
            query
                .filter_expr_with_normalized_predicate(
                    Expr::Literal(Value::Bool(false)),
                    crate::db::predicate::Predicate::False,
                    work,
                )
                .unwrap()
        });
    });
}

#[test]
fn warmed_grouped_builder_identity_matches_fresh_intent() {
    let session = initialize();
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    with_preparation_work(|work| {
        let grouped = || {
            query()
                .group_fields_with_schema(
                    &["label".to_string()],
                    catalog.accepted_schema_info(),
                    work,
                )
                .unwrap()
        };
        assert_current_identity(query, |query| {
            query
                .group_fields_with_schema(
                    &["label".to_string()],
                    catalog.accepted_schema_info(),
                    work,
                )
                .unwrap()
        });
        assert_current_identity(grouped, |query| {
            query
                .group_fields_with_schema(&["id".to_string()], catalog.accepted_schema_info(), work)
                .unwrap()
        });
        assert_current_identity(grouped, |query| {
            query.group_aggregates(vec![GroupAggregateSpec::from_aggregate_expr(count())])
        });
        assert_current_identity(grouped, |query| query.grouped_limits(2, 1024));
        assert_current_identity(grouped, |query| {
            query
                .having_expr_preserving_shape(Expr::Literal(Value::Bool(false)), work)
                .unwrap()
        });
    });
}

#[test]
fn warm_query_limit_changes_cannot_reuse_the_previous_plan() {
    let session = initialize();
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    for lane in [
        DiagnosticExecutionLane::PublicRead,
        DiagnosticExecutionLane::TrustedRead,
        DiagnosticExecutionLane::Diagnostic,
    ] {
        session.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
        let prepare = |query: &StructuralQuery| {
            session
                .cached_shared_query_plan_for_accepted_authority_with_catalog_and_reuse(
                    catalog.accepted_entity_authority(),
                    &catalog,
                    query,
                    lane,
                )
                .unwrap()
        };
        let original = query();
        assert!(!prepare(&original).1.is_hit());
        assert!(prepare(&original).1.is_hit());
        let changed = original.clone().limit(0);
        let (changed_plan, reuse) = prepare(&changed);
        assert!(!reuse.is_hit());
        assert!(matches!(
            &changed_plan.logical_plan().scalar_plan().mode,
            QueryMode::Load(spec) if spec.limit() == Some(0),
        ));
        assert!(prepare(&query().limit(0)).1.is_hit());
        assert!(prepare(&original).1.is_hit());
        assert!(prepare(&changed.limit(1)).1.is_hit());
    }
}
