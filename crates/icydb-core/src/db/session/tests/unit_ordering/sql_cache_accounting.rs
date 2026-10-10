//! SQL retention must bound all syntax owners without changing statement results.

use super::*;
use crate::db::session::sql::CompiledSqlCommand;

fn projection_rows(sql: &str) -> Vec<Vec<OutputValue>> {
    match new_request_session()
        .execute_trusted_sql_query(sql)
        .unwrap()
    {
        SqlStatementResult::Projection { rows, .. } => rows,
        _ => panic!("expected projection"),
    }
}

#[test]
fn sql_cache_admits_exact_retained_capacity_and_execution_cannot_grow_it() {
    let session = initialize();
    seed_singleton(&session);
    for sql in [
        "SELECT label FROM Singleton WHERE label = 'singleton'",
        "SELECT COUNT(*) FROM Singleton",
        "SELECT COUNT(DISTINCT label) FROM Singleton",
        "SELECT label, SUM(amount) FROM Singleton GROUP BY label",
        "EXPLAIN SELECT label FROM Singleton WHERE label = 'singleton'",
        "EXPLAIN SELECT COUNT(DISTINCT label) FROM Singleton",
    ] {
        session.clear_sql_compiled_cache_for_tests(4 * 1024 * 1024);
        new_request_session()
            .compile_sql_query_for_tests(sql)
            .unwrap();
        let (entries, required) = session.sql_compiled_cache_usage_for_tests();
        assert_eq!(entries, 1);
        assert!(required > 2 * sql.len());
        let mut expected = None;
        for capacity in [required - 1, required, required + 1] {
            session.clear_sql_compiled_cache_for_tests(capacity);
            for _ in 0..2 {
                let result = new_request_session()
                    .execute_trusted_sql_query(sql)
                    .unwrap();
                // The DTO does not implement PartialEq; its complete Debug view
                // covers rows, metadata and diagnostics for this result matrix.
                let actual = format!("{result:?}");
                if let Some(expected) = &expected {
                    assert_eq!(&actual, expected);
                } else {
                    expected = Some(actual);
                }
                assert_eq!(
                    session.sql_compiled_cache_usage_for_tests(),
                    if capacity >= required {
                        (1, required)
                    } else {
                        (0, 0)
                    },
                );
                let (_, bytes) = session.shared_query_cache_usage_for_tests();
                assert!(bytes <= 4 * 1024 * 1024);
            }
        }
    }
}

#[test]
fn oversized_sql_executes_without_retention_or_evicting_a_small_command() {
    let session = initialize();
    seed_singleton(&session);
    let small = "SELECT label FROM Singleton";
    let expected = projection_rows(small);
    let usage = session.sql_compiled_cache_usage_for_tests();
    // Conservative charging of both key handles exceeds the 4 MiB allowance,
    // while the statement remains within the SQL input ceiling.
    let oversized = format!("{}{small}", " ".repeat(2 * 1024 * 1024));
    assert_eq!(projection_rows(&oversized), expected);
    assert_eq!(session.sql_compiled_cache_usage_for_tests(), usage);
    assert!(session.sql_compiled_cache_contains_for_tests(small));
    assert!(!session.sql_compiled_cache_contains_for_tests(&oversized));
}

#[test]
fn literal_payloads_and_all_mutation_commands_are_weighted() {
    let session = initialize();
    seed_singleton(&session);
    let payload = "p".repeat(8192);
    for sql in [
        format!(
            "INSERT INTO Singleton (label, amount) VALUES ('{payload}', U256 '2') RETURNING label"
        ),
        format!(
            "INSERT INTO Singleton (label, amount) SELECT REPLACE(label, 'singleton', '{payload}'), amount FROM Singleton"
        ),
        format!(
            "UPDATE Singleton SET label = '{payload}' WHERE label = 'singleton' RETURNING label"
        ),
        "DELETE FROM Singleton WHERE label = 'singleton' RETURNING label".to_string(),
    ] {
        session.clear_sql_compiled_cache_for_tests(4 * 1024 * 1024);
        let dispatch = sql_statement_dispatch(&sql).unwrap();
        new_request_session()
            .compile_sql_mutation_with_execution_context(&dispatch)
            .unwrap();
        let (entries, required) = session.sql_compiled_cache_usage_for_tests();
        assert_eq!(entries, 1);
        assert!(required > 2 * sql.len());
        if sql.contains(&payload) {
            assert!(required >= 2 * sql.len() + payload.len());
        }
        for capacity in [required - 1, required] {
            session.clear_sql_compiled_cache_for_tests(capacity);
            new_request_session()
                .compile_sql_mutation_with_execution_context(&dispatch)
                .unwrap();
            assert_eq!(
                session.sql_compiled_cache_usage_for_tests(),
                if capacity >= required {
                    (1, required)
                } else {
                    (0, 0)
                }
            );
        }
    }
}

#[test]
fn trusted_mutation_results_do_not_depend_on_cache_retention() {
    let session = initialize();
    for capacity in [0, 4 * 1024 * 1024] {
        session.clear_sql_compiled_cache_for_tests(capacity);
        for _ in 0..2 {
            seed_singleton(&session);
            let result = new_request_session()
                .execute_trusted_sql_mutation(
                    "DELETE FROM Singleton WHERE amount = U256 '2' RETURNING label",
                )
                .unwrap();
            let SqlStatementResult::Projection {
                rows, row_count, ..
            } = result
            else {
                panic!("expected mutation returning projection");
            };
            assert_eq!(row_count, 1);
            assert_eq!(rows, vec![vec![OutputValue::text("singleton".into())]]);
            assert_eq!(
                session.sql_compiled_cache_usage_for_tests().0,
                usize::from(capacity != 0)
            );
        }
    }
}

#[test]
fn sql_byte_eviction_is_fifo_and_bound_queries_do_not_retain_commands() {
    let session = initialize();
    seed_singleton(&session);
    let statements = [
        "SELECT label FROM Singleton WHERE label = 'a'",
        "SELECT label FROM Singleton WHERE label = 'b'",
        "SELECT label FROM Singleton WHERE label = 'c'",
    ];
    new_request_session()
        .compile_sql_query_for_tests(statements[0])
        .unwrap();
    let required = session.sql_compiled_cache_usage_for_tests().1;
    session.clear_sql_compiled_cache_for_tests(2 * required);
    for index in [0, 1, 0, 2] {
        new_request_session()
            .compile_sql_query_for_tests(statements[index])
            .unwrap();
    }
    assert_eq!(
        session.sql_compiled_cache_usage_for_tests(),
        (2, 2 * required)
    );
    assert!(!session.sql_compiled_cache_contains_for_tests(statements[0]));
    for sql in &statements[1..] {
        assert!(session.sql_compiled_cache_contains_for_tests(sql));
    }
    let usage = session.sql_compiled_cache_usage_for_tests();
    let dispatch = sql_statement_dispatch("SELECT label FROM Singleton WHERE label = ?").unwrap();
    for (label, expected) in [("singleton", 1), ("missing", 0)] {
        let (result, _) = new_request_session()
            .execute_trusted_sql_query_with_entity_name(
                &dispatch,
                &[InputValue::text(label.into())],
            )
            .unwrap();
        let SqlStatementResult::Projection { rows, .. } = result else {
            panic!("expected projection");
        };
        assert_eq!(rows.len(), expected);
        assert_eq!(session.sql_compiled_cache_usage_for_tests(), usage);
    }
}

#[test]
fn sql_compilations_do_not_pin_prepared_residents_after_shared_cache_eviction() {
    let session = initialize();
    seed_singleton(&session);
    for sql in [
        "SELECT label FROM Singleton",
        "SELECT COUNT(DISTINCT label) FROM Singleton",
    ] {
        session.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
        let (context, _) = new_request_session()
            .compile_sql_query_for_tests(sql)
            .unwrap();
        let query = match context.command() {
            CompiledSqlCommand::Select { query } => query.as_ref(),
            CompiledSqlCommand::GlobalAggregate { command } => command.query(),
            _ => panic!("expected a prepared read"),
        };
        let catalog = context.accepted_catalog();
        let plan = session
            .cached_shared_query_plan_for_accepted_authority_with_catalog(
                catalog.accepted_entity_authority(),
                catalog,
                query,
                icydb_diagnostic_code::DiagnosticExecutionLane::TrustedRead,
            )
            .unwrap();
        let alive = plan.resident_lifetime_for_tests();
        drop(plan);
        session
            .execute_compiled_sql_query_context(&context)
            .unwrap();
        assert!(alive());
        let sql_usage = session.sql_compiled_cache_usage_for_tests();
        session.clear_shared_query_cache_for_tests(0);
        // Keep both the execution context and resident SQL syntax alive.
        assert!(!alive());
        session
            .execute_compiled_sql_query_context(&context)
            .unwrap();
        assert_eq!(session.sql_compiled_cache_usage_for_tests(), sql_usage);
        assert_eq!(session.shared_query_cache_usage_for_tests(), (0, 0));
    }
}
