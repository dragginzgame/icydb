//! Accepted exact-key selections preserve partially extracted filter expressions.

use super::*;
use crate::{
    db::{
        RequestExecutionRoot,
        executor::{
            SharedPreparedExecutionPlan, StructuralAggregateRequest, StructuralAggregateTerminal,
            StructuralAggregateTerminalKind, StructuralProjectionRequest,
            execute_structural_aggregate_rows_for_canister, execute_structural_projection_rows,
        },
        predicate::Predicate,
        query::{
            admission::{QueryAdmissionPolicy, QueryAdmissionSummary},
            builder::count,
            plan::expr::{BinaryOp, Expr, ProjectionField, ProjectionSpec},
            preparation::with_preparation_work,
        },
    },
    value::Value,
};

fn initialize_rows() -> DbSession<TestCanister> {
    scalar_page_limits::initialize_payload_schema(
        vec![
            field(1, "id", 0, AcceptedFieldKind::Nat64),
            field(2, "category", 1, AcceptedFieldKind::Nat64),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(3),
                "operand".into(),
                SchemaFieldSlot::new(2),
                AcceptedFieldKind::Nat64,
                vec![],
                true,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                AcceptedFieldKind::Nat64.leaf_codec_for_storage(FieldStorageDecode::ByKind),
            ),
            field(4, "marker", 3, AcceptedFieldKind::Nat64),
        ],
        vec![PersistedIndexSnapshot::new(
            SchemaIndexId::new(1).unwrap(),
            1,
            "category_idx".into(),
            STORE_PATH.into(),
            false,
            PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                FieldId::new(2),
                SchemaFieldSlot::new(1),
                vec!["category".into()],
                AcceptedFieldKind::Nat64,
                false,
            )]),
            None,
        )],
    );
    let session = new_request_session(&RequestExecutionRoot::__new_runtime_root());
    for (id, category, operand) in [
        (1, 3, Some(7)),
        (2, 3, Some(2)),
        (3, 3, None),
        (4, 9, Some(9)),
    ] {
        session
            .execute_trusted_dynamic_insert_batch(
                ENTITY_NAME,
                vec![DynamicStructuralPatch::new(vec![
                    ("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))),
                    (
                        "category".into(),
                        DynamicWriteCell::Value(InputValue::nat64(category)),
                    ),
                    (
                        "operand".into(),
                        operand.map_or(DynamicWriteCell::Null, |v| {
                            DynamicWriteCell::Value(InputValue::nat64(v))
                        }),
                    ),
                    (
                        "marker".into(),
                        DynamicWriteCell::Value(InputValue::nat64(0)),
                    ),
                ])],
            )
            .unwrap();
    }
    session
}

fn expression() -> Expr {
    Expr::Binary {
        op: BinaryOp::Gt,
        left: Box::new(Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Field("operand".into())),
            right: Box::new(Expr::Literal(Value::Nat64(1))),
        }),
        right: Box::new(Expr::Literal(Value::Nat64(5))),
    }
}

fn query(predicate: Predicate) -> StructuralQuery {
    query_in_append_order(predicate, true)
}

fn query_in_append_order(predicate: Predicate, expression_first: bool) -> StructuralQuery {
    with_preparation_work(|work| {
        let query = StructuralQuery::new(MissingRowPolicy::Ignore);
        let query = if expression_first {
            query
                .filter_expr(expression(), work)
                .map(|query| query.filter_normalized_predicate(predicate))
        } else {
            query
                .filter_normalized_predicate(predicate)
                .filter_expr(expression(), work)
        };
        query.map(|query| query.select_fields(["id"]))
    })
    .unwrap()
}

fn prepare(
    session: &DbSession<TestCanister>,
    query: &StructuralQuery,
    lane: DiagnosticExecutionLane,
) -> SharedPreparedExecutionPlan {
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    session
        .cached_shared_query_plan_for_accepted_authority_with_catalog(
            catalog.accepted_entity_authority(),
            &catalog,
            query,
            lane,
        )
        .unwrap()
}

fn keys(ids: &[u64]) -> Predicate {
    Predicate::in_("id".into(), ids.iter().copied().map(Value::Nat64).collect())
}

fn rows(
    session: &DbSession<TestCanister>,
    query: &StructuralQuery,
    lane: DiagnosticExecutionLane,
) -> Vec<Vec<Value>> {
    let plan = prepare(session, query, lane);
    assert!(
        plan.logical_plan()
            .residual_filter_expr()
            .unwrap()
            .is_some()
    );
    if lane == DiagnosticExecutionLane::PublicRead {
        let policy = QueryAdmissionPolicy::default_bounded_read();
        assert!(
            policy
                .evaluate(
                    QueryAdmissionSummary::from_plan(policy.lane(), plan.logical_plan()).unwrap()
                )
                .rejection()
                .is_none()
        );
    }
    execute_structural_projection_rows(&session.db, StructuralProjectionRequest::new(plan, lane))
        .unwrap()
        .into_value_rows()
}

#[test]
fn residual_filter_preservation_accepted_reads_keep_partial_exact_key_scopes() {
    let session = initialize_rows();
    let cases = [
        (Predicate::eq("id".into(), Value::Nat64(1)), vec![1]),
        (Predicate::eq("id".into(), Value::Nat64(2)), vec![]),
        (Predicate::eq("id".into(), Value::Nat64(3)), vec![]),
        (Predicate::eq("id".into(), Value::Nat64(4)), vec![4]),
        (Predicate::eq("id".into(), Value::Nat64(99)), vec![]),
        (keys(&[4, 2, 1, 3, 2]), vec![1, 4]),
        (keys(&[2, 3]), vec![]),
        (keys(&[1]), vec![1]),
        (keys(&[2]), vec![]),
        (keys(&[3]), vec![]),
        (keys(&[4]), vec![4]),
        (keys(&[2, 2]), vec![]),
    ];
    for capacity in [0, 4 * 1024 * 1024] {
        // Reset only between cache policies; every scope shares one cache.
        session.clear_shared_query_cache_for_tests(capacity);
        for expression_first in [true, false] {
            for reversed in [false, true] {
                for step in 0..cases.len() {
                    let index = if reversed {
                        cases.len() - 1 - step
                    } else {
                        step
                    };
                    let (predicate, expected) = &cases[index];
                    let expected = expected
                        .iter()
                        .map(|id| vec![Value::Nat64(*id)])
                        .collect::<Vec<_>>();
                    for lane in [
                        DiagnosticExecutionLane::PublicRead,
                        DiagnosticExecutionLane::TrustedRead,
                    ] {
                        // Fresh syntax and request budgets still use the shared cache.
                        let query = query_in_append_order(predicate.clone(), expression_first);
                        let reader =
                            new_request_session(&RequestExecutionRoot::__new_runtime_root());
                        assert_eq!(rows(&reader, &query, lane), expected);
                    }
                }
            }
        }
    }
}

#[test]
fn residual_filter_preservation_accepted_windows_follow_expression_filtering() {
    let session = initialize_rows();
    for descending in [false, true] {
        let order = if descending {
            crate::db::desc("id").lower()
        } else {
            asc("id").lower()
        };
        for (limit, offset, expected) in [
            (0, 0, vec![]),
            (1, 0, vec![if descending { 4 } else { 1 }]),
            (1, 1, vec![if descending { 1 } else { 4 }]),
            (9, 9, vec![]),
        ] {
            let query = query(keys(&[4, 2, 1, 3, 2]))
                .order_spec(OrderSpec {
                    fields: vec![order.clone()],
                })
                .limit(limit)
                .offset(offset);
            let expected = expected
                .into_iter()
                .map(|id| vec![Value::Nat64(id)])
                .collect::<Vec<_>>();
            for _ in 0..2 {
                assert_eq!(
                    rows(&session, &query, DiagnosticExecutionLane::TrustedRead),
                    expected
                );
            }
        }
    }
}

#[test]
fn residual_filter_preservation_structural_count_keeps_partial_and_empty_scopes() {
    let session = initialize_rows();
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    for capacity in [0, 4 * 1024 * 1024] {
        session.clear_shared_query_cache_for_tests(capacity);
        for expression_first in [true, false] {
            for (ids, expected) in [
                (&[1, 2, 3, 4][..], 2),
                (&[2, 3][..], 0),
                (&[4, 2, 1, 3, 2][..], 2),
                (&[99][..], 0),
                (&[1, 2, 3, 4][..], 2),
            ] {
                let reader = new_request_session(&RequestExecutionRoot::__new_runtime_root());
                let query = query_in_append_order(keys(ids), expression_first);
                let plan = prepare(&reader, &query, DiagnosticExecutionLane::TrustedRead);
                let request = StructuralAggregateRequest::new(
                    vec![StructuralAggregateTerminal::new(
                        StructuralAggregateTerminalKind::CountRows,
                        None,
                        None,
                        None,
                        false,
                    )],
                    ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
                        expr: Expr::Aggregate(count()),
                        alias: None,
                    }]),
                    None,
                    catalog.accepted_schema_info().clone(),
                );
                assert_eq!(
                    execute_structural_aggregate_rows_for_canister(&reader.db, plan, request)
                        .unwrap(),
                    vec![vec![Value::Nat64(expected)]]
                );
            }
        }
    }
    for _ in 0..2 {
        assert_eq!(
            projection_rows(
                &session,
                "SELECT COUNT(*) FROM PlannerRow WHERE category = 3 AND operand + 1 > 5"
            ),
            vec![vec![OutputValue::nat64(1)]]
        );
    }
}

#[test]
fn mixed_filter_cache_identity_reuses_only_the_current_scope_across_requests() {
    let session = initialize_rows();
    session.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
    for expression_first in [true, false] {
        for (id, revisited, expected) in
            [(1, false, vec![1]), (2, false, vec![]), (1, true, vec![1])]
        {
            let reader = new_request_session(&RequestExecutionRoot::__new_runtime_root());
            let catalog = reader
                .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
                .unwrap();
            let query = query_in_append_order(
                Predicate::eq("id".into(), Value::Nat64(id)),
                expression_first,
            );
            let (plan, reuse) = reader
                .cached_shared_query_plan_for_accepted_authority_with_catalog_and_reuse(
                    catalog.accepted_entity_authority(),
                    &catalog,
                    &query,
                    DiagnosticExecutionLane::TrustedRead,
                )
                .unwrap();
            assert_eq!(reuse.is_hit(), revisited || !expression_first);
            let actual = execute_structural_projection_rows(
                &reader.db,
                StructuralProjectionRequest::new(plan, DiagnosticExecutionLane::TrustedRead),
            )
            .unwrap()
            .into_value_rows();
            assert_eq!(
                actual,
                expected
                    .into_iter()
                    .map(|id| vec![Value::Nat64(id)])
                    .collect::<Vec<_>>()
            );
        }
    }
}

#[test]
fn mixed_filter_cache_identity_mutation_selections_keep_changed_key_scopes() {
    let session = initialize_rows();
    for capacity in [0, 4 * 1024 * 1024] {
        session.clear_shared_query_cache_for_tests(capacity);
        for expression_first in [true, false] {
            for (predicate, expected) in [
                (Predicate::eq("id".into(), Value::Nat64(1)), vec![1]),
                (Predicate::eq("id".into(), Value::Nat64(2)), vec![]),
                (Predicate::eq("id".into(), Value::Nat64(3)), vec![]),
                (Predicate::eq("id".into(), Value::Nat64(4)), vec![4]),
                (keys(&[1, 2, 3, 4]), vec![1, 4]),
                (keys(&[2, 3]), vec![]),
                (keys(&[1]), vec![1]),
                (keys(&[2]), vec![]),
                (keys(&[3]), vec![]),
                (keys(&[4]), vec![4]),
                (Predicate::eq("id".into(), Value::Nat64(1)), vec![1]),
            ] {
                let selection = query_in_append_order(predicate.clone(), expression_first)
                    .delete()
                    .into_load_selection();
                let reader = new_request_session(&RequestExecutionRoot::__new_runtime_root());
                assert_eq!(
                    rows(&reader, &selection, DiagnosticExecutionLane::Mutation),
                    expected
                        .into_iter()
                        .map(|id| vec![Value::Nat64(id)])
                        .collect::<Vec<_>>(),
                    "capacity={capacity}, expression_first={expression_first}, predicate={predicate:?}"
                );
            }
        }
    }
}

fn count_rows(session: &DbSession<TestCanister>, query: &StructuralQuery) -> Vec<Vec<Value>> {
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let plan = prepare(session, query, DiagnosticExecutionLane::TrustedRead);
    let request = StructuralAggregateRequest::new(
        vec![StructuralAggregateTerminal::new(
            StructuralAggregateTerminalKind::CountRows,
            None,
            None,
            None,
            false,
        )],
        ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
            expr: Expr::Aggregate(count()),
            alias: None,
        }]),
        None,
        catalog.accepted_schema_info().clone(),
    );
    execute_structural_aggregate_rows_for_canister(&session.db, plan, request).unwrap()
}

#[test]
fn complete_residual_runtime_scan_enforces_both_filters_and_windows() {
    let session = initialize_rows();
    session
        .execute_trusted_sql_exact_update("UPDATE PlannerRow SET marker = 1 WHERE id = 4", 1)
        .unwrap();
    for capacity in [0, 4 * 1024 * 1024] {
        session.clear_shared_query_cache_for_tests(capacity);
        for expression_first in [true, false] {
            for _ in 0..2 {
                for (marker, expected) in [(0, vec![1]), (1, vec![4]), (99, vec![])] {
                    let predicate = Predicate::eq("marker".into(), Value::Nat64(marker));
                    let query = query_in_append_order(predicate, expression_first);
                    for lane in [
                        DiagnosticExecutionLane::TrustedRead,
                        DiagnosticExecutionLane::Mutation,
                    ] {
                        let reader =
                            new_request_session(&RequestExecutionRoot::__new_runtime_root());
                        let selection = if lane == DiagnosticExecutionLane::Mutation {
                            query.clone().delete().into_load_selection()
                        } else {
                            query.clone()
                        };
                        assert_eq!(
                            rows(&reader, &selection, lane),
                            expected
                                .iter()
                                .map(|id| vec![Value::Nat64(*id)])
                                .collect::<Vec<_>>()
                        );
                        let plan = prepare(&reader, &selection, lane);
                        // Predicate-only capability consumers must not omit the expression.
                        assert!(
                            plan.logical_plan()
                                .effective_runtime_compiled_predicate()
                                .is_none()
                        );
                    }
                    for (limit, offset, window) in
                        [(0, 0, vec![]), (1, 0, expected.clone()), (1, 1, vec![])]
                    {
                        let reader =
                            new_request_session(&RequestExecutionRoot::__new_runtime_root());
                        let windowed = query
                            .clone()
                            .order_spec(OrderSpec {
                                fields: vec![asc("id").lower()],
                            })
                            .limit(limit)
                            .offset(offset);
                        assert_eq!(
                            rows(&reader, &windowed, DiagnosticExecutionLane::TrustedRead),
                            window
                                .into_iter()
                                .map(|id| vec![Value::Nat64(id)])
                                .collect::<Vec<_>>()
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn complete_residual_runtime_counts_keep_singleton_and_scan_scopes() {
    let session = initialize_rows();
    session
        .execute_trusted_sql_exact_update("UPDATE PlannerRow SET marker = 1 WHERE id = 4", 1)
        .unwrap();
    for capacity in [0, 4 * 1024 * 1024] {
        session.clear_shared_query_cache_for_tests(capacity);
        for expression_first in [true, false] {
            for _ in 0..2 {
                for (predicate, expected) in [
                    (keys(&[2]), 0),
                    (keys(&[3]), 0),
                    (keys(&[1]), 1),
                    (keys(&[4]), 1),
                    (keys(&[99]), 0),
                    (keys(&[2, 2]), 0),
                    (keys(&[1, 2, 3, 4]), 2),
                    (Predicate::eq("marker".into(), Value::Nat64(0)), 1),
                    (Predicate::eq("marker".into(), Value::Nat64(1)), 1),
                    (Predicate::eq("marker".into(), Value::Nat64(99)), 0),
                ] {
                    let reader = new_request_session(&RequestExecutionRoot::__new_runtime_root());
                    assert_eq!(
                        count_rows(&reader, &query_in_append_order(predicate, expression_first)),
                        vec![vec![Value::Nat64(expected)]]
                    );
                }
            }
        }
    }
}

#[test]
fn residual_filter_preservation_mutation_lane_and_sql_writes_keep_unknown_rows() {
    let session = initialize_rows();
    let selection = query(keys(&[1, 2, 3, 4])).delete().into_load_selection();
    assert_eq!(
        rows(&session, &selection, DiagnosticExecutionLane::Mutation),
        vec![vec![Value::Nat64(1)], vec![Value::Nat64(4)]]
    );
    let updated = session
        .execute_trusted_sql_exact_update(
            "UPDATE PlannerRow SET marker = 1 WHERE id IN (1,2,3,4) AND operand + 1 > 5",
            4,
        )
        .unwrap();
    assert!(matches!(
        updated,
        SqlStatementResult::Count { row_count: 2 }
    ));
    assert_eq!(
        projection_rows(
            &session,
            "SELECT id FROM PlannerRow WHERE marker = 1 ORDER BY id"
        ),
        vec![vec![OutputValue::nat64(1)], vec![OutputValue::nat64(4)]]
    );
    let deleted = session
        .execute_trusted_sql_mutation(
            "DELETE FROM PlannerRow WHERE id IN (1,2,3,4) AND operand + 1 > 5",
        )
        .unwrap();
    assert!(matches!(
        deleted,
        SqlStatementResult::Count { row_count: 2 }
    ));
    assert_eq!(
        projection_rows(&session, "SELECT id FROM PlannerRow ORDER BY id"),
        vec![vec![OutputValue::nat64(2)], vec![OutputValue::nat64(3)]]
    );
}
