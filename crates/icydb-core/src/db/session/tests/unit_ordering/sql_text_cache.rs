//! Exact-text hits reuse admitted syntax while current authority stays mandatory.

use super::*;
use crate::db::sql::parser::sql_parse_count_for_tests;

#[test]
fn sql_text_cache_skips_warm_parsing_and_preserves_entity_and_results() {
    let session = initialize();
    seed_singleton(&session);
    for sql in [
        "SELECT label FROM sInGlEtOn",
        "SELECT COUNT(*) FROM Singleton",
        "SELECT label, COUNT(*) FROM Singleton GROUP BY label",
        "DESCRIBE Singleton",
        "SHOW ENTITIES",
        "SHOW STORES",
        "SHOW MEMORY",
        "EXPLAIN SELECT label FROM Singleton",
    ] {
        let before = sql_parse_count_for_tests();
        let first = new_request_session()
            .execute_trusted_sql_query_text_with_entity_name(sql)
            .unwrap();
        assert_eq!(sql_parse_count_for_tests(), before + 1);
        let usage = session.sql_compiled_cache_usage_for_tests();
        for _ in 0..2 {
            let current = new_request_session()
                .execute_trusted_sql_query_text_with_entity_name(sql)
                .unwrap();
            assert_eq!(format!("{current:?}"), format!("{first:?}"));
            assert_eq!(sql_parse_count_for_tests(), before + 1);
            assert_eq!(session.sql_compiled_cache_usage_for_tests(), usage);
        }
        let dispatch = sql_statement_dispatch(sql).unwrap();
        let retained = new_request_session()
            .execute_trusted_sql_query_with_entity_name(&dispatch, &[])
            .unwrap();
        assert_eq!(format!("{retained:?}"), format!("{first:?}"));
        assert_eq!(sql_parse_count_for_tests(), before + 2);
    }
}

#[test]
fn sql_text_cache_eviction_and_schema_changes_return_to_the_parser() {
    let session = initialize();
    seed_singleton(&session);
    let sql = "SELECT label FROM Singleton";
    session.execute_trusted_sql_query(sql).unwrap();
    session.clear_sql_compiled_cache_for_tests(0);
    let before = sql_parse_count_for_tests();
    for count in 1..=2 {
        new_request_session()
            .execute_trusted_sql_query(sql)
            .unwrap();
        assert_eq!(sql_parse_count_for_tests(), before + count);
    }
    session.clear_sql_compiled_cache_for_tests(4 * 1024 * 1024);
    new_request_session()
        .execute_trusted_sql_query(sql)
        .unwrap();
    publish_schema_with_label(
        &session,
        AcceptedSchemaRevision::INITIAL,
        AcceptedSchemaRevision::new(2),
        "renamed",
    );
    let before = sql_parse_count_for_tests();
    new_request_session()
        .execute_trusted_sql_query(sql)
        .expect_err("stale syntax must compile against current fields");
    assert_eq!(sql_parse_count_for_tests(), before + 1);
    let current = "SELECT renamed FROM Singleton";
    new_request_session()
        .execute_trusted_sql_query(current)
        .unwrap();
    let before = sql_parse_count_for_tests();
    new_request_session()
        .execute_trusted_sql_query(current)
        .unwrap();
    assert_eq!(sql_parse_count_for_tests(), before);
}

#[test]
fn sql_text_cache_does_not_admit_mutations_bindings_or_invalid_input() {
    let session = initialize();
    seed_singleton(&session);
    let mutation = "DELETE FROM Singleton WHERE label = 'singleton'";
    let dispatch = sql_statement_dispatch(mutation).unwrap();
    session
        .compile_sql_mutation_with_execution_context(&dispatch)
        .unwrap();
    let usage = session.sql_compiled_cache_usage_for_tests();
    for sql in [
        mutation,
        "SELECT missing FROM Singleton",
        "SELECT label FROM Missing",
        "SELECT label FROM Singleton WHERE label = ?",
        "SELECT FROM",
    ] {
        for _ in 0..2 {
            let before = sql_parse_count_for_tests();
            new_request_session()
                .execute_trusted_sql_query(sql)
                .expect_err("inadmissible query must reject on every call");
            assert_eq!(sql_parse_count_for_tests(), before + 1);
            assert_eq!(session.sql_compiled_cache_usage_for_tests(), usage);
        }
    }
    let oversized = " ".repeat(crate::db::sql_shared::MAX_SQL_INPUT_BYTES + 1);
    let Err(expected) = sql_statement_dispatch(&oversized) else {
        panic!("oversized syntax must reject");
    };
    let before = sql_parse_count_for_tests();
    let actual = new_request_session()
        .execute_trusted_sql_query(&oversized)
        .unwrap_err();
    assert_eq!(actual.diagnostic(), expected.diagnostic());
    assert_eq!(sql_parse_count_for_tests(), before);
    assert_eq!(session.sql_compiled_cache_usage_for_tests(), usage);
}
