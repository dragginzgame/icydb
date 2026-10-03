//! SQL aggregate modifier composition through maintained scalar/grouped reducers.

use super::{OutputValue, SqlStatementResult, initialize, seed_rows};
use crate::types::Decimal;

#[test]
fn nullable_global_distinct_public_exact_reads_preserve_null_and_empty_results() {
    use super::{DynamicQuery, ENTITY_NAME, FieldRef, InputValue};
    use crate::db::{avg, count_by, sum};

    let session = super::sql_not_null::initialize_nullable_rows();
    for _ in 0..2 {
        for (id, present) in [(2, true), (3, false), (99, false)] {
            for (aggregate, expected) in [
                (count_by("qty"), OutputValue::nat64(u64::from(present))),
                (
                    sum("qty"),
                    if present {
                        OutputValue::decimal(Decimal::from(2_u64))
                    } else {
                        OutputValue::null()
                    },
                ),
                (
                    avg("qty"),
                    if present {
                        OutputValue::decimal(Decimal::from(2_u64))
                    } else {
                        OutputValue::null()
                    },
                ),
            ] {
                let query = DynamicQuery::new(ENTITY_NAME)
                    .filter(FieldRef::new("id").eq(InputValue::nat64(id)))
                    .aggregate(aggregate.distinct())
                    .grouped_limits(1, 16 * 1024);
                let public = session
                    .execute_public_dynamic_grouped_query(&query)
                    .unwrap();
                let trusted = session
                    .execute_trusted_dynamic_grouped_query(&query)
                    .unwrap();
                for page in [public, trusted] {
                    assert_eq!(page.rows.len(), 1);
                    assert_eq!(
                        page.rows[0].aggregate_values(),
                        std::slice::from_ref(&expected)
                    );
                    assert!(page.next_cursor.is_none());
                }
            }
        }
    }
}

#[test]
fn nullable_global_distinct_skips_only_distinct_state_charges() {
    use super::{DynamicQuery, ENTITY_NAME, FieldRef, InputValue, new_request_session};
    use crate::db::{
        RequestExecutionRoot, count_by,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    };
    use icydb_diagnostic_code::{
        DiagnosticExecutionBudgetResource as Resource, DiagnosticFactTag, ErrorCode,
    };

    super::sql_not_null::initialize_nullable_rows();
    for resource in [
        Resource::GroupDistinctEntries,
        Resource::GroupDistinctStateBytes,
        Resource::RowsVisited,
        Resource::StoredBytesRead,
    ] {
        for null in [false, true] {
            let root = RequestExecutionRoot::new_for_tests(
                HardExecutionBudget::uniform_for_tests(
                    16_000_000,
                    HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                )
                .with_limit_for_tests(resource, 0),
            );
            let session = new_request_session(&root);
            let query = DynamicQuery::new(ENTITY_NAME)
                .filter(FieldRef::new("id").eq(InputValue::nat64(if null { 3 } else { 2 })))
                .aggregate(count_by("qty").distinct())
                .grouped_limits(1, 16 * 1024);
            let result = session.execute_trusted_dynamic_grouped_query(&query);
            if null
                && matches!(
                    resource,
                    Resource::GroupDistinctEntries | Resource::GroupDistinctStateBytes
                )
            {
                let page = result.unwrap();
                assert_eq!(page.rows[0].aggregate_values(), &[OutputValue::nat64(0)]);
                assert_eq!(root.observed(resource), 0);
            } else {
                let error = result.unwrap_err();
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

#[test]
fn nullable_global_distinct_matches_grouped_and_scalar_on_cold_and_warm_calls() {
    use super::{DynamicQuery, ENTITY_NAME, FieldRef, InputValue};
    use crate::db::{avg, count_by, sum};

    let session = super::sql_not_null::initialize_nullable_rows();
    for values in [
        "7, 'active', NULL, 2, NULL, FALSE, FALSE",
        "8, NULL, NULL, NULL, NULL, NULL, FALSE",
    ] {
        session
            .execute_trusted_sql_mutation(&format!(
                "INSERT INTO PlannerRow (id, status, peer, qty, other, flag, marked) VALUES ({values})"
            ))
            .unwrap();
    }
    for capacity in [0, 4 * 1024 * 1024] {
        session.clear_shared_query_cache_for_tests(capacity);
        for _ in 0..2 {
            for (minimum_id, count, total, average) in [
                (0, 4, Some(6), Some("1.5")),
                (7, 1, Some(2), Some("2")),
                (8, 0, None, None),
                (99, 0, None, None),
            ] {
                for (aggregate, sql_terminal, expected) in [
                    (
                        count_by("qty"),
                        "COUNT(DISTINCT qty)",
                        OutputValue::nat64(count),
                    ),
                    (
                        count_by("status"),
                        "COUNT(DISTINCT status)",
                        OutputValue::nat64(count),
                    ),
                    (
                        sum("qty"),
                        "SUM(DISTINCT qty)",
                        total.map_or_else(OutputValue::null, |total: u64| {
                            OutputValue::decimal(Decimal::from(total))
                        }),
                    ),
                    (
                        avg("qty"),
                        "AVG(DISTINCT qty)",
                        average.map_or_else(OutputValue::null, |average| {
                            OutputValue::decimal(average.parse().unwrap())
                        }),
                    ),
                ] {
                    let query = DynamicQuery::new(ENTITY_NAME)
                        .filter(FieldRef::new("id").gte(InputValue::nat64(minimum_id)))
                        .aggregate(aggregate.distinct())
                        .grouped_limits(1, 16 * 1024);
                    let global = session
                        .execute_trusted_dynamic_grouped_query(&query)
                        .unwrap();
                    assert_eq!(global.rows.len(), 1);
                    assert!(global.rows[0].group_key().is_empty());
                    assert_eq!(
                        global.rows[0].aggregate_values(),
                        std::slice::from_ref(&expected)
                    );
                    assert!(global.next_cursor.is_none());
                    let grouped = session
                        .execute_trusted_dynamic_grouped_query(&query.group_by("marked"))
                        .unwrap();
                    if minimum_id == 99 {
                        assert!(grouped.rows.is_empty());
                    } else {
                        assert_eq!(grouped.rows.len(), 1);
                        assert_eq!(
                            grouped.rows[0].aggregate_values(),
                            std::slice::from_ref(&expected)
                        );
                    }
                    let sql =
                        format!("SELECT {sql_terminal} FROM PlannerRow WHERE id >= {minimum_id}");
                    let SqlStatementResult::Projection { rows, .. } =
                        session.execute_trusted_sql_query(&sql).unwrap()
                    else {
                        panic!("scalar aggregate projection");
                    };
                    assert_eq!(rows, vec![vec![expected]]);
                }
            }
        }
    }
}

#[test]
fn global_distinct_filter_composes_before_deduplication_on_cold_and_warm_calls() {
    let session = initialize();
    seed_rows(&session);
    let projection = "COUNT(DISTINCT common) FILTER (WHERE id >= 6), \
        COUNT(DISTINCT MOD(id, 3)) FILTER (WHERE id >= 6), \
        SUM(DISTINCT MOD(id, 3)) FILTER (WHERE id >= 6), \
        AVG(DISTINCT MOD(id, 3)) FILTER (WHERE id >= 6), \
        COUNT(DISTINCT NULLIF(MOD(id, 3), 0)) FILTER (WHERE id >= 6)";
    for (predicate, expected) in [
        (
            "TRUE",
            vec![
                OutputValue::nat64(1),
                OutputValue::nat64(3),
                OutputValue::decimal(Decimal::from(3_u64)),
                OutputValue::decimal(Decimal::from(1_u64)),
                OutputValue::nat64(2),
            ],
        ),
        (
            "id < 6",
            vec![
                OutputValue::nat64(0),
                OutputValue::nat64(0),
                OutputValue::null(),
                OutputValue::null(),
                OutputValue::nat64(0),
            ],
        ),
        (
            "id > 99",
            vec![
                OutputValue::nat64(0),
                OutputValue::nat64(0),
                OutputValue::null(),
                OutputValue::null(),
                OutputValue::nat64(0),
            ],
        ),
    ] {
        let sql = format!("SELECT {projection} FROM PlannerRow WHERE {predicate}");
        for _ in 0..2 {
            let SqlStatementResult::Projection { rows, .. } = session
                .execute_trusted_sql_query(&sql)
                .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
            else {
                panic!("global aggregates must return one projection row");
            };
            assert_eq!(rows, vec![expected.clone()]);
        }
    }
}

#[test]
fn grouped_distinct_filter_keeps_each_terminal_and_empty_group_independent() {
    let session = initialize();
    seed_rows(&session);
    let sql = "SELECT rare, COUNT(DISTINCT common) FILTER (WHERE id < 1), \
        SUM(DISTINCT MOD(id, 3)) FILTER (WHERE id < 1), \
        COUNT(DISTINCT NULLIF(MOD(id, 3), 0)) FILTER (WHERE id < 9), COUNT(*) \
        FROM PlannerRow GROUP BY rare ORDER BY rare LIMIT 2";
    for _ in 0..2 {
        let SqlStatementResult::Grouped { rows, .. } = session
            .execute_trusted_sql_query(sql)
            .expect("grouped composed aggregate query")
        else {
            panic!("grouped aggregates must return grouped rows");
        };
        assert_eq!(rows.len(), 2);
        for (row, group, expected) in [
            (
                &rows[0],
                "group-a",
                vec![
                    OutputValue::nat64(1),
                    OutputValue::decimal(Decimal::from(0_u64)),
                    OutputValue::nat64(2),
                    OutputValue::nat64(6),
                ],
            ),
            (
                &rows[1],
                "group-b",
                vec![
                    OutputValue::nat64(0),
                    OutputValue::null(),
                    OutputValue::nat64(2),
                    OutputValue::nat64(6),
                ],
            ),
        ] {
            assert_eq!(row.group_key(), &[OutputValue::text(group.into())]);
            assert_eq!(row.aggregate_values(), expected);
        }
    }
}
