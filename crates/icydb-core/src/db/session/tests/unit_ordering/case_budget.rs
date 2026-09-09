//! Compact CASE uses the maintained accepted-schema preparation/execution path.

use super::*;

#[test]
fn case_admission_exhaustion_is_not_cached_and_warm_commands_skip_the_walk() {
    use crate::db::{
        RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    };
    use icydb_diagnostic_code::{DiagnosticExecutionBudgetResource as Resource, DiagnosticFactTag};

    let setup = initialize();
    let warm = "SELECT id FROM Singleton WHERE CASE WHEN label = 'warm' THEN amount >= U256 '1' ELSE amount < U256 '1' END";
    let cold = "SELECT id FROM Singleton WHERE CASE WHEN label = 'cold' THEN amount >= U256 '1' ELSE amount < U256 '1' END";
    setup
        .compile_sql_query_for_tests(warm)
        .expect("prepare warm command");
    let root = RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::NestedValueSteps, 0),
    );
    let session = new_request_session_with_root(&root);
    let resident_count = session.sql_compiled_cache_len_for_tests();
    for _ in 0..2 {
        session
            .compile_sql_query_for_tests(warm)
            .expect("cached command skips CASE admission");
        assert_eq!(root.observed(Resource::NestedValueSteps), 0);
    }
    let mut observed = 0;
    for _ in 0..2 {
        let error = session
            .compile_sql_query_for_tests(cold)
            .expect_err("cold CASE admission must use current request work");
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::NestedValueSteps.raw(),
        )));
        assert!(root.observed(Resource::NestedValueSteps) > observed);
        observed = root.observed(Resource::NestedValueSteps);
        assert_eq!(session.sql_compiled_cache_len_for_tests(), resident_count);
        assert_eq!(root.observed(Resource::RowsVisited), 0);
        assert_eq!(root.observed(Resource::QueryExecutions), 0);
    }
    new_request_session()
        .compile_sql_query_for_tests(cold)
        .expect("failed admission leaves no poisoned command");
}

#[test]
fn compact_nested_case_executes_current_bindings_on_warm_preparation() {
    let session = initialize();
    seed_singleton(&session);
    let mut condition = "label = ?".to_string();
    for level in 0..6 {
        condition = format!(
            "CASE WHEN {condition} THEN amount >= U256 '{level}' ELSE amount < U256 '{level}' END"
        );
    }
    let sql = format!("SELECT label FROM Singleton WHERE {condition} ORDER BY id ASC LIMIT 1");
    let dispatch = sql_statement_dispatch(&sql).expect("nested CASE parses");
    for label in ["singleton", "missing", "singleton"] {
        let expected = sql_rows(&session, &sql.replace('?', &format!("'{label}'")));
        let (result, entity) = new_request_session()
            .execute_trusted_sql_query_with_entity_name(
                &dispatch,
                &[InputValue::text(label.into())],
            )
            .expect("compact CASE bound execution");
        assert_eq!(entity, ENTITY_NAME);
        let SqlStatementResult::Projection { rows: actual, .. } = result else {
            panic!("expected rows");
        };
        assert_eq!(actual, expected);
        // Stored amount is 2; the final three branches invert the initial match.
        assert_eq!(actual.len(), usize::from(label == "missing"));
    }
}
