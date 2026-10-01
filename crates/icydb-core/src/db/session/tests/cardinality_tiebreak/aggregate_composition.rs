//! SQL aggregate modifier composition through maintained scalar/grouped reducers.

use super::{OutputValue, SqlStatementResult, initialize, seed_rows};
use crate::types::Decimal;

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
