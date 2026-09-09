//! SQL construction borrows current request work; warm commands skip lowering.

use super::*;
use crate::db::{
    RequestExecutionRoot,
    executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
};
use icydb_diagnostic_code::{DiagnosticExecutionBudgetResource as Resource, DiagnosticFactTag};

#[test]
fn alias_copy_exhaustion_does_not_cache_a_command_and_warm_hits_skip_it() {
    let setup = initialize();
    let warm = "SELECT id AS result FROM Singleton ORDER BY result LIMIT 10";
    let cold = "SELECT id AS result FROM Singleton ORDER BY result LIMIT 11";
    setup
        .compile_sql_query_for_tests(warm)
        .expect("warm alias command");
    // Admit the four full/tail entity scope candidates, but no copied alias operand.
    let scope_bytes = 4 * size_of::<String>() as u64 + 4 * "Singleton".len() as u64;
    let root = RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::TemporaryBytes, scope_bytes),
    );
    let session = new_request_session_with_root(&root);
    let count = session.sql_compiled_cache_len_for_tests();
    session
        .compile_sql_query_for_tests(warm)
        .expect("hit skips alias copy");
    assert_eq!(root.observed(Resource::TemporaryBytes), 0);
    let error = session
        .compile_sql_query_for_tests(cold)
        .expect_err("alias copy must be charged");
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::TemporaryBytes.raw(),
    )));
    assert_eq!(root.observed(Resource::TemporaryBytes), scope_bytes + 2);
    assert_eq!(session.sql_compiled_cache_len_for_tests(), count);
    assert_eq!(root.observed(Resource::RowsVisited), 0);
    new_request_session()
        .compile_sql_query_for_tests(cold)
        .expect("fresh budget");
}

#[test]
fn literal_copy_exhaustion_is_not_cached_and_warm_commands_skip_copying() {
    let setup = initialize();
    let warm = "SELECT id FROM Singleton WHERE label = 'warm'";
    let cold = "SELECT id FROM Singleton WHERE label = 'cold'";
    setup
        .compile_sql_query_for_tests(warm)
        .expect("warm command");
    let root = RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::NestedValueSteps, 0),
    );
    let session = new_request_session_with_root(&root);
    let count = session.sql_compiled_cache_len_for_tests();
    session
        .compile_sql_query_for_tests(warm)
        .expect("hit skips literal copy");
    assert_eq!(root.observed(Resource::NestedValueSteps), 0);
    for _ in 0..2 {
        let error = session
            .compile_sql_query_for_tests(cold)
            .expect_err("literal copy must be charged");
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::NestedValueSteps.raw()
        )));
        assert_eq!(session.sql_compiled_cache_len_for_tests(), count);
        assert_eq!(root.observed(Resource::RowsVisited), 0);
    }
    assert_eq!(root.observed(Resource::NestedValueSteps), 2);
    new_request_session()
        .compile_sql_query_for_tests(cold)
        .expect("fresh request after rejected copy");
}

#[test]
fn sql_construction_exhaustion_preserves_cache_and_current_request_charges() {
    let setup = initialize();
    let warm = "SELECT id FROM Singleton WHERE label = 'warm'";
    let cold = "SELECT id FROM Singleton WHERE label = 'cold'";
    setup
        .compile_sql_query_for_tests(warm)
        .expect("warm command");
    let root = RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::TemporaryBytes, 0),
    );
    let session = new_request_session_with_root(&root);
    let count = session.sql_compiled_cache_len_for_tests();
    session
        .compile_sql_query_for_tests(warm)
        .expect("warm command skips construction");
    assert_eq!(root.observed(Resource::TemporaryBytes), 0);
    let mut observed = 0;
    for _ in 0..2 {
        let error = session
            .compile_sql_query_for_tests(cold)
            .expect_err("field copy must be charged");
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::TemporaryBytes.raw()
        )));
        assert!(root.observed(Resource::TemporaryBytes) > observed);
        observed = root.observed(Resource::TemporaryBytes);
        assert_eq!(session.sql_compiled_cache_len_for_tests(), count);
        assert_eq!(root.observed(Resource::RowsVisited), 0);
        assert_eq!(root.observed(Resource::QueryExecutions), 0);
    }
    new_request_session()
        .compile_sql_query_for_tests(cold)
        .expect("fresh authority can compile after failure");
}

#[test]
fn warm_qualified_commands_skip_scope_construction_but_cold_commands_charge_it() {
    let setup = initialize();
    let warm = "SELECT s.id FROM Singleton s WHERE s.label = 'warm'";
    let cold = "SELECT s.id FROM Singleton s WHERE s.label = 'cold'";
    setup
        .compile_sql_query_for_tests(warm)
        .expect("warm command");
    let root = RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::TemporaryBytes, 0),
    );
    let session = new_request_session_with_root(&root);
    let count = session.sql_compiled_cache_len_for_tests();
    session
        .compile_sql_query_for_tests(warm)
        .expect("cached command");
    assert_eq!(root.observed(Resource::TemporaryBytes), 0);
    for retry in 1..=2 {
        let error = session
            .compile_sql_query_for_tests(cold)
            .expect_err("scope backing");
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::TemporaryBytes.raw(),
        )));
        assert_eq!(
            root.observed(Resource::TemporaryBytes),
            retry * 6 * size_of::<String>() as u64
        );
        assert_eq!(session.sql_compiled_cache_len_for_tests(), count);
        assert_eq!(root.observed(Resource::RowsVisited), 0);
    }
}

#[test]
fn bound_qualified_preparation_charges_scope_without_retaining_a_command() {
    let _setup = initialize();
    let dispatch = sql_statement_dispatch("SELECT s.id FROM Singleton s WHERE s.label = ?")
        .expect("retained syntax");
    let root = RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::TemporaryBytes, 0),
    );
    let session = new_request_session_with_root(&root);
    let count = session.sql_compiled_cache_len_for_tests();
    let error = session
        .execute_trusted_sql_query_with_entity_name(
            &dispatch,
            &[InputValue::text("value".to_string())],
        )
        .expect_err("bound scope backing");
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::TemporaryBytes.raw(),
    )));
    assert_eq!(
        root.observed(Resource::TemporaryBytes),
        6 * size_of::<String>() as u64
    );
    assert_eq!(root.observed(Resource::RowsVisited), 0);
    assert_eq!(session.sql_compiled_cache_len_for_tests(), count);
}
