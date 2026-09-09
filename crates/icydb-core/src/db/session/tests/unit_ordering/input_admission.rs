use super::*;
use crate::db::query::admission::input::{MAX_QUERY_INPUT_BYTES, MAX_QUERY_INPUT_DEPTH};
use icydb_diagnostic_code::QueryReadAdmissionCode;

#[test]
fn grouped_key_clause_reserves_final_representation_before_resolution() {
    use crate::db::{
        RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        query::{
            plan::{FieldSlot, GroupField, resolve_group_fields_with_schema},
            preparation::PreparationWork,
        },
    };
    use icydb_diagnostic_code::{
        DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane,
    };

    let session = initialize();
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let fields = ["label".to_string(), "label".to_string(), "id".to_string()];
    let bytes = (fields.len() * size_of::<FieldSlot>()) as u64;
    let budget = |limit| {
        RequestExecutionRoot::new_for_tests(
            HardExecutionBudget::uniform_for_tests(
                16_000_000,
                HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
            )
            .with_limit_for_tests(Resource::TemporaryBytes, limit),
        )
    };
    let root = budget(bytes);
    let resolved =
        PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
            resolve_group_fields_with_schema(catalog.accepted_schema_info(), &fields, work)
        })
        .unwrap();
    assert_eq!(root.observed(Resource::TemporaryBytes), bytes);
    assert_eq!(resolved.as_direct().unwrap().len(), 2);
    assert_eq!(
        resolved
            .iter()
            .map(|field| field.field())
            .collect::<Vec<_>>(),
        ["label", "id"]
    );

    // A later dotted key selects path-aware backing before any symbol lookup.
    // Exhaustion wins even though the path itself would fail resolution.
    let fields = ["label".to_string(), "missing.path".to_string()];
    let bytes = (fields.len() * size_of::<GroupField>()) as u64;
    let root = budget(bytes - 1);
    let error = PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
        resolve_group_fields_with_schema(catalog.accepted_schema_info(), &fields, work)
    })
    .unwrap_err();
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::TemporaryBytes.raw(),
    )));
    assert_eq!(root.observed(Resource::TemporaryBytes), bytes);
    let root = budget(bytes);
    let error = PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
        resolve_group_fields_with_schema(catalog.accepted_schema_info(), &fields, work)
    })
    .unwrap_err();
    assert_query_field(&error, QueryFieldRole::GroupBy, "missing.path");
}

#[test]
fn grouped_clause_backing_rejects_before_cold_warm_and_trusted_execution() {
    use crate::db::{
        RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        query::plan::{FieldSlot, GroupAggregateSpec},
    };
    use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

    let setup = initialize();
    seed_singleton(&setup);
    let query = DynamicQuery::new(ENTITY_NAME)
        .group_by("label")
        .aggregate(sum("amount"))
        .grouped_limits(1, 16 * 1024)
        .limit(1);
    // Key payload accounting is separate; these are the two clause containers.
    for first_bytes in [
        size_of::<FieldSlot>() as u64,
        (size_of::<FieldSlot>() + size_of::<GroupAggregateSpec>()) as u64,
    ] {
        for warm in [false, true] {
            setup.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
            if warm {
                setup
                    .execute_trusted_dynamic_grouped_query(&query)
                    .expect("warm grouping");
            }
            let before = setup.shared_query_cache_usage_for_tests();
            let root = RequestExecutionRoot::new_for_tests(
                HardExecutionBudget::uniform_for_tests(
                    16_000_000,
                    HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                )
                .with_limit_for_tests(Resource::TemporaryBytes, first_bytes - 1),
            );
            let mut previous = 0;
            for (attempt, public) in [true, false, true].into_iter().enumerate() {
                let session = new_request_session_with_root(&root);
                let result = if public {
                    session.execute_public_dynamic_grouped_query(&query)
                } else {
                    session.execute_trusted_dynamic_grouped_query(&query)
                };
                let error = result.expect_err("grouped clause backing exhausts request");
                assert!(error.diagnostic_facts().contains(&(
                    DiagnosticFactTag::BudgetResource,
                    Resource::TemporaryBytes.raw(),
                )));
                let observed = root.observed(Resource::TemporaryBytes);
                assert!(observed > previous);
                if attempt == 0 {
                    assert_eq!(observed, first_bytes);
                }
                previous = observed;
                assert_eq!(root.observed(Resource::RowsVisited), 0);
                assert_eq!(root.observed(Resource::QueryExecutions), 0);
                assert_eq!(setup.shared_query_cache_usage_for_tests(), before);
            }
            assert_eq!(
                setup
                    .execute_trusted_dynamic_grouped_query(&query)
                    .unwrap()
                    .row_count,
                1
            );
        }
    }
}

#[test]
fn grouped_clause_materialization_preserves_duplicate_keys_and_aggregate_order() {
    let session = initialize();
    seed_singleton(&session);
    let query = DynamicQuery::new(ENTITY_NAME)
        .group_by("label")
        .group_by("label")
        .aggregate(sum("amount"))
        .aggregate(crate::db::count())
        .grouped_limits(1, 16 * 1024)
        .limit(1);
    for _ in 0..2 {
        let typed = session
            .execute_trusted_dynamic_grouped_query(&query)
            .unwrap_or_else(|error| {
                panic!("grouped parity: {error:?}, {:?}", error.diagnostic_facts())
            });
        assert_eq!(typed.row_count, 1);
        assert_eq!(typed.rows[0].group_key().len(), 1);
        let sql = session
            .execute_trusted_sql_query(
                "SELECT label, SUM(amount), COUNT(*) FROM Singleton GROUP BY label, label",
            )
            .unwrap();
        let SqlStatementResult::Grouped { rows, .. } = sql else {
            panic!("grouped SQL result");
        };
        assert_eq!(typed.rows, rows);
    }
}

#[test]
fn scalar_clause_backing_rejects_before_execution_and_cache_reuse() {
    use crate::db::{
        RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        query::{
            expr::OrderTerm,
            plan::{OrderTerm as PlannedOrderTerm, expr::FieldId},
        },
    };
    use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

    let setup = initialize();
    seed_singleton(&setup);
    let order_bytes = size_of::<PlannedOrderTerm>() as u64;
    let projected_bytes = order_bytes + "id".len() as u64 + 3 * size_of::<FieldId>() as u64;
    for (query, first_bytes, first_steps) in [
        (
            DynamicQuery::new(ENTITY_NAME)
                .order_by(OrderTerm::asc("label"))
                .limit(1),
            order_bytes,
            0,
        ),
        (DynamicQuery::new(ENTITY_NAME).limit(1), order_bytes, 0),
        (
            DynamicQuery::new(ENTITY_NAME)
                .select(["label", "id", "label"])
                .limit(1),
            projected_bytes,
            3,
        ),
    ] {
        for warm in [false, true] {
            setup.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
            if warm {
                setup
                    .execute_trusted_live_page(&query, None)
                    .expect("warm scalar plan");
            }
            let before = setup.shared_query_cache_usage_for_tests();
            let root = RequestExecutionRoot::new_for_tests(
                HardExecutionBudget::uniform_for_tests(
                    16_000_000,
                    HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                )
                .with_limit_for_tests(Resource::TemporaryBytes, first_bytes - 1),
            );
            let mut previous = 0;
            for (attempt, public) in [true, false, true].into_iter().enumerate() {
                let session = new_request_session_with_root(&root);
                let result = if public {
                    session.execute_public_live_page(&query, None)
                } else {
                    session.execute_trusted_live_page(&query, None)
                };
                let error = result.expect_err("clause backing consumes current request budget");
                assert!(error.diagnostic_facts().contains(&(
                    DiagnosticFactTag::BudgetResource,
                    Resource::TemporaryBytes.raw()
                )));
                assert!(root.observed(Resource::TemporaryBytes) > previous);
                previous = root.observed(Resource::TemporaryBytes);
                if attempt == 0 {
                    assert_eq!(previous, first_bytes);
                    // Only the accepted implicit primary-key name precedes
                    // projection backing. Rejected clause children are unvisited.
                    assert_eq!(
                        root.observed(Resource::PredicateExpressionSteps),
                        first_steps
                    );
                }
                assert_eq!(root.observed(Resource::RowsVisited), 0);
                assert_eq!(root.observed(Resource::QueryExecutions), 0);
                assert_eq!(setup.shared_query_cache_usage_for_tests(), before);
            }
            assert_eq!(
                setup.execute_trusted_live_page(&query, None).unwrap().len(),
                1
            );
        }
    }
}

#[test]
fn typed_order_and_aggregate_copy_exhaustion_precedes_cold_and_warm_execution() {
    use crate::db::{
        RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        query::{builder::sum, expr::OrderTerm},
    };
    use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

    let setup = initialize();
    seed_singleton(&setup);
    let cases = [
        (
            DynamicQuery::new(ENTITY_NAME)
                .order_by(OrderTerm::asc("label"))
                .limit(1),
            false,
        ),
        (
            DynamicQuery::new(ENTITY_NAME)
                .group_by("label")
                .aggregate(sum("amount"))
                .grouped_limits(1, 4096)
                .limit(1),
            true,
        ),
    ];
    for (query, grouped) in cases {
        for warm in [false, true] {
            setup.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
            if warm {
                if grouped {
                    setup
                        .execute_trusted_dynamic_grouped_query(&query)
                        .expect("warm grouped plan");
                } else {
                    setup
                        .execute_trusted_live_page(&query, None)
                        .expect("warm ordered plan");
                }
            }
            let cache_before = setup.shared_query_cache_usage_for_tests();
            for resource in [Resource::TemporaryBytes, Resource::PredicateExpressionSteps] {
                let root = RequestExecutionRoot::new_for_tests(
                    HardExecutionBudget::uniform_for_tests(
                        16_000_000,
                        HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                    )
                    .with_limit_for_tests(resource, 0),
                );
                let mut previous_charge = 0;
                for public in [true, false, true] {
                    let session = new_request_session_with_root(&root);
                    let result = match (grouped, public) {
                        (true, true) => session
                            .execute_public_dynamic_grouped_query(&query)
                            .map(drop),
                        (true, false) => session
                            .execute_trusted_dynamic_grouped_query(&query)
                            .map(drop),
                        (false, true) => session.execute_public_live_page(&query, None).map(drop),
                        (false, false) => session.execute_trusted_live_page(&query, None).map(drop),
                    };
                    let error =
                        result.expect_err("operand copying consumes current request budget");
                    assert!(
                        error
                            .diagnostic_facts()
                            .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                    );
                    assert!(root.observed(resource) > previous_charge);
                    previous_charge = root.observed(resource);
                    assert_eq!(root.observed(Resource::RowsVisited), 0);
                    assert_eq!(root.observed(Resource::QueryExecutions), 0);
                    assert_eq!(setup.shared_query_cache_usage_for_tests(), cache_before);
                }
            }
            // The reusable request survives failed copies and still executes
            // under a fresh authority, without retaining a partial operand.
            if grouped {
                assert_eq!(
                    setup
                        .execute_trusted_dynamic_grouped_query(&query)
                        .unwrap()
                        .row_count,
                    1
                );
            } else {
                assert_eq!(
                    setup.execute_trusted_live_page(&query, None).unwrap().len(),
                    1
                );
            }
        }
    }
}

#[test]
fn typed_scalar_conversion_exhaustion_precedes_cold_and_warm_execution() {
    use crate::db::{
        RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    };
    use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

    let setup = initialize();
    seed_singleton(&setup);
    let query = DynamicQuery::new(ENTITY_NAME)
        .filter(FilterExpr::eq("amount", "2"))
        .limit(1);
    for warm in [false, true] {
        setup.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
        if warm {
            setup
                .execute_trusted_live_page(&query, None)
                .expect("warm numeric filter plan");
        }
        let cache_before = setup.shared_query_cache_usage_for_tests();
        let root = RequestExecutionRoot::new_for_tests(
            HardExecutionBudget::uniform_for_tests(
                16_000_000,
                HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
            )
            .with_limit_for_tests(Resource::PredicateExpressionSteps, 1),
        );
        // The filter visit fits; the first parse-byte charge rejects. Later
        // retries fail on their filter visit against that same exhausted root.
        for (attempt, public) in [true, false, true].into_iter().enumerate() {
            let session = new_request_session_with_root(&root);
            let error = if public {
                session.execute_public_live_page(&query, None)
            } else {
                session.execute_trusted_live_page(&query, None)
            }
            .expect_err("numeric conversion consumes the current request");
            assert!(error.diagnostic_facts().contains(&(
                DiagnosticFactTag::BudgetResource,
                Resource::PredicateExpressionSteps.raw(),
            )));
            assert_eq!(
                root.observed(Resource::PredicateExpressionSteps),
                attempt as u64 + 2
            );
            assert_eq!(root.observed(Resource::RowsVisited), 0);
            assert_eq!(root.observed(Resource::QueryExecutions), 0);
            assert_eq!(setup.shared_query_cache_usage_for_tests(), cache_before);
        }
    }
}

#[test]
fn typed_field_copy_exhaustion_precedes_cold_and_warm_execution() {
    use crate::db::{
        RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    };
    use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

    let setup = initialize();
    seed_singleton(&setup);
    let query = DynamicQuery::new(ENTITY_NAME)
        .filter(FilterExpr::is_not_null("label"))
        .limit(1);
    for warm in [false, true] {
        setup.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
        if warm {
            setup
                .execute_trusted_live_page(&query, None)
                .expect("warm filter plan");
        }
        let cache_before = setup.shared_query_cache_usage_for_tests();
        let root = RequestExecutionRoot::new_for_tests(
            HardExecutionBudget::uniform_for_tests(
                16_000_000,
                HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
            )
            .with_limit_for_tests(Resource::TemporaryBytes, 0),
        );
        for (attempt, public) in [true, false, true].into_iter().enumerate() {
            let session = new_request_session_with_root(&root);
            let error = if public {
                session.execute_public_live_page(&query, None)
            } else {
                session.execute_trusted_live_page(&query, None)
            }
            .expect_err("field construction consumes the retained request");
            assert!(error.diagnostic_facts().contains(&(
                DiagnosticFactTag::BudgetResource,
                Resource::TemporaryBytes.raw()
            )));
            assert_eq!(
                root.observed(Resource::TemporaryBytes),
                (attempt as u64 + 1) * "label".len() as u64
            );
            assert_eq!(root.observed(Resource::RowsVisited), 0);
            assert_eq!(root.observed(Resource::QueryExecutions), 0);
            assert_eq!(setup.shared_query_cache_usage_for_tests(), cache_before);
        }
    }
}

#[test]
fn canonical_ordering_exhaustion_precedes_cold_and_warm_typed_execution() {
    use crate::db::{
        RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    };
    use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

    let setup = initialize();
    seed_singleton(&setup);
    let query = DynamicQuery::new(ENTITY_NAME)
        .filter(FilterExpr::and(vec![
            FilterExpr::eq("label", "singleton"),
            FilterExpr::eq("label", "singleton"),
        ]))
        .limit(1);
    for warm in [false, true] {
        setup.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
        if warm {
            setup
                .execute_trusted_live_page(&query, None)
                .expect("warm canonical plan");
        }
        let cache_before = setup.shared_query_cache_usage_for_tests();
        let root = RequestExecutionRoot::new_for_tests(
            HardExecutionBudget::uniform_for_tests(
                16_000_000,
                HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
            )
            .with_limit_for_tests(Resource::SortComparisons, 0),
        );
        for (attempt, public) in [true, false, true].into_iter().enumerate() {
            let session = new_request_session_with_root(&root);
            let error = if public {
                session.execute_public_live_page(&query, None)
            } else {
                session.execute_trusted_live_page(&query, None)
            }
            .expect_err("canonical sorting consumes the retained request");
            assert!(error.diagnostic_facts().contains(&(
                DiagnosticFactTag::BudgetResource,
                Resource::SortComparisons.raw()
            )));
            assert_eq!(root.observed(Resource::SortComparisons), attempt as u64 + 1);
            assert_eq!(root.observed(Resource::RowsVisited), 0);
            assert_eq!(root.observed(Resource::QueryExecutions), 0);
            assert_eq!(setup.shared_query_cache_usage_for_tests(), cache_before);
        }
    }
}

#[test]
fn sql_canonical_exhaustion_is_not_an_aggregate_lane_miss_or_cached_command() {
    use crate::db::{
        RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    };
    use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

    let _setup = initialize();
    let root = RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::SortComparisons, 0),
    );
    let session = new_request_session_with_root(&root);
    let cache_before = session.sql_compiled_cache_len_for_tests();
    let mut observed = 0;
    for sql in [
        "SELECT id FROM Singleton WHERE label = 'a' AND label = 'b'",
        "SELECT COUNT(*) + 1 FROM Singleton WHERE label = 'a' AND label = 'b'",
        "EXPLAIN SELECT COUNT(*) + 1 FROM Singleton WHERE label = 'a' AND label = 'b'",
    ] {
        for _ in 0..2 {
            let error = session
                .compile_sql_query_for_tests(sql)
                .expect_err("canonical request rejection");
            assert!(
                error.diagnostic_facts().contains(&(
                    DiagnosticFactTag::BudgetResource,
                    Resource::SortComparisons.raw()
                )),
                "{:?}",
                error.diagnostic()
            );
            assert!(root.observed(Resource::SortComparisons) > observed);
            observed = root.observed(Resource::SortComparisons);
            assert_eq!(root.observed(Resource::RowsVisited), 0);
            assert_eq!(root.observed(Resource::QueryExecutions), 0);
            assert_eq!(session.sql_compiled_cache_len_for_tests(), cache_before);
        }
    }
}

#[test]
fn warm_sql_command_skips_canonical_sorting_but_a_new_binding_must_prepare() {
    use crate::db::{
        RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    };
    use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

    let setup = initialize();
    let sql = "SELECT id FROM Singleton WHERE label = 'warm' AND label = 'warm'";
    setup
        .compile_sql_query_for_tests(sql)
        .expect("cache concrete command");
    let root = RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::SortComparisons, 0),
    );
    let session = new_request_session_with_root(&root);
    let cache_before = session.sql_compiled_cache_len_for_tests();
    for _ in 0..2 {
        session
            .compile_sql_query_for_tests(sql)
            .expect("warm command avoids semantic compilation");
        assert_eq!(root.observed(Resource::SortComparisons), 0);
    }
    let dispatch = sql_statement_dispatch("SELECT id FROM Singleton WHERE label = ? AND label = ?")
        .expect("retained syntax");
    for text in ["a", "b", "a"] {
        let error = session
            .execute_trusted_sql_query_with_entity_name(
                &dispatch,
                &[InputValue::text(text.into()), InputValue::text(text.into())],
            )
            .expect_err("bound invocations must prepare their current values");
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::SortComparisons.raw()
        )));
        assert_eq!(session.sql_compiled_cache_len_for_tests(), cache_before);
    }
    assert_eq!(root.observed(Resource::SortComparisons), 3);
    assert_eq!(root.observed(Resource::RowsVisited), 0);
    assert_eq!(root.observed(Resource::QueryExecutions), 0);
}

#[test]
fn typed_preparation_exhaustion_is_cumulative_before_cold_and_warm_execution() {
    use crate::db::{
        RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    };
    use icydb_diagnostic_code::{
        DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionBudgetScope,
        DiagnosticExecutionLane,
    };

    let setup = initialize();
    seed_singleton(&setup);
    let query = DynamicQuery::new(ENTITY_NAME)
        .filter(FilterExpr::eq("label", "singleton"))
        .limit(1);
    for warm in [false, true] {
        setup.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
        if warm {
            setup
                .execute_trusted_live_page(&query, None)
                .expect("warm valid plan");
            assert!(setup.shared_query_cache_usage_for_tests().0 > 0);
        }
        let cache_before = setup.shared_query_cache_usage_for_tests();
        for resource in [
            Resource::PredicateExpressionSteps,
            Resource::NestedValueSteps,
            Resource::TemporaryBytes,
        ] {
            let budget = HardExecutionBudget::uniform_for_tests(
                16_000_000,
                HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
            )
            .with_limit_for_tests(resource, 0);
            let root = RequestExecutionRoot::new_for_tests(budget);
            let mut observed = 0;
            for public in [true, false, true] {
                let session = new_request_session_with_root(&root);
                let error = if public {
                    session.execute_public_live_page(&query, None)
                } else {
                    session.execute_trusted_live_page(&query, None)
                }
                .expect_err("preparation must use the retained request without an active executor");
                let facts = error.diagnostic_facts();
                assert!(
                    facts.contains(&(DiagnosticFactTag::BudgetResource, resource.raw())),
                    "{resource:?}: {:?} {facts:?}",
                    error.diagnostic()
                );
                assert!(facts.contains(&(
                    DiagnosticFactTag::ExecutionBudgetScope,
                    DiagnosticExecutionBudgetScope::Request.raw()
                )));
                let lane = if public {
                    DiagnosticExecutionLane::PublicRead
                } else {
                    DiagnosticExecutionLane::TrustedRead
                };
                assert!(facts.contains(&(DiagnosticFactTag::ExecutionLane, lane.raw())));
                assert!(facts.contains(&(DiagnosticFactTag::QueryShapeFingerprintPrefix, 0)));
                assert!(
                    root.observed(resource) > observed,
                    "retries cannot reset counters"
                );
                observed = root.observed(resource);
                assert_eq!(root.observed(Resource::RowsVisited), 0);
                assert_eq!(root.observed(Resource::QueryExecutions), 0);
                assert_eq!(setup.shared_query_cache_usage_for_tests(), cache_before);
            }
        }
    }
}

#[test]
fn retained_sql_rejects_large_effective_bindings_without_poisoning_reuse() {
    let session = initialize();
    seed_singleton(&session);
    let mut condition = "CASE WHEN label = ? THEN 1 ELSE 0 END".to_string();
    for _ in 0..5 {
        condition = format!("CASE WHEN ({condition}) BETWEEN 1 AND 1 THEN 1 ELSE 0 END");
    }
    let sql = format!("SELECT id FROM Singleton WHERE ({condition}) = 1 ORDER BY id LIMIT 1");
    let dispatch = sql_statement_dispatch(&sql).expect("parse reusable SQL");
    let parse_count = crate::db::sql::parser::sql_parse_count_for_tests();
    for label in ["singleton", "missing", "singleton"] {
        let (result, _) = new_request_session()
            .execute_trusted_sql_query_with_entity_name(
                &dispatch,
                &[InputValue::text(label.into())],
            )
            .expect("small current binding");
        let SqlStatementResult::Projection { rows, .. } = result else {
            panic!("projection");
        };
        assert_eq!(rows.len(), usize::from(label == "singleton"));
        let error = new_request_session()
            .execute_trusted_sql_query_with_entity_name(
                &dispatch,
                &[InputValue::text("x".repeat(64 * 1024))],
            )
            .expect_err("effective operand copies exceed shared input budget");
        assert_eq!(
            error.diagnostic(),
            QueryError::from(QueryReadAdmissionCode::InputBytesExceeded).diagnostic()
        );
        assert_eq!(
            crate::db::sql::parser::sql_parse_count_for_tests(),
            parse_count
        );
    }
}

#[test]
fn query_input_admission_precedes_lowering_on_public_trusted_and_warm_reads() {
    let session = initialize();
    seed_singleton(&session);
    let valid = DynamicQuery::new(ENTITY_NAME)
        .filter(FilterExpr::eq("label", "singleton"))
        .limit(1);
    assert_eq!(
        session
            .execute_trusted_live_page(&valid, None)
            .expect("cold valid input")
            .row_count,
        1
    );

    let mut deep = FilterExpr::Constant(true);
    for _ in 0..MAX_QUERY_INPUT_DEPTH {
        deep = FilterExpr::Not(Box::new(deep));
    }
    let rejected = DynamicQuery::new(ENTITY_NAME)
        .filter(FilterExpr::and(vec![FilterExpr::Constant(false), deep]))
        .limit(1);
    let expected = QueryError::from(QueryReadAdmissionCode::InputDepthExceeded).diagnostic();
    assert_eq!(
        session
            .execute_public_live_page(&rejected, None)
            .expect_err("public input rejection")
            .diagnostic(),
        expected
    );
    assert_eq!(
        session
            .execute_trusted_live_page(&rejected, None)
            .expect_err("trusted input rejection")
            .diagnostic(),
        expected
    );

    let oversized = DynamicQuery::new(ENTITY_NAME)
        .filter(FilterExpr::eq("label", "x".repeat(MAX_QUERY_INPUT_BYTES)))
        .limit(1);
    assert_eq!(
        session
            .execute_trusted_live_page(&oversized, None)
            .expect_err("large rebound operand")
            .diagnostic(),
        QueryError::from(QueryReadAdmissionCode::InputBytesExceeded).diagnostic()
    );
    assert_eq!(
        session
            .execute_trusted_live_page(&valid, None)
            .expect("warm valid input after rejection")
            .row_count,
        1
    );
}
