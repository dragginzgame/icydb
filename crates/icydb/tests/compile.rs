#[test]
fn public_facade_compile_contract() {
    let t = trybuild::TestCases::new();
    t.pass("tests/pass/**/*.rs");
    t.compile_fail("tests/fail-endpoints/duplicate.rs");
    t.compile_fail("tests/fail-endpoints/duplicate_block.rs");
    t.compile_fail("tests/fail-endpoints/handwritten_duplicate.rs");
    t.compile_fail("tests/fail-endpoints/invalid_option.rs");
    t.compile_fail("tests/fail-endpoints/nested.rs");
    t.compile_fail("tests/fail-endpoints/unknown.rs");
    t.compile_fail("tests/fail-endpoints/unsupported_attribute.rs");
    t.compile_fail("tests/fail-endpoints/without_start.rs");

    #[cfg(not(feature = "sql"))]
    t.compile_fail("tests/fail-endpoints/missing_sql_capability.rs");

    #[cfg(not(feature = "metrics-extended"))]
    t.compile_fail("tests/fail-endpoints/missing_metrics_extended_capability.rs");

    #[cfg(all(feature = "sql", not(feature = "sql-explain")))]
    t.compile_fail("tests/fail-endpoints/missing_sql_explain_capability.rs");

    #[cfg(feature = "sql")]
    t.compile_fail("tests/fail-endpoints/missing_test_admin_capability.rs");
}

#[cfg(feature = "query")]
#[test]
fn public_query_facade_compile_contract() {
    let t = trybuild::TestCases::new();
    t.pass("tests/pass-query/**/*.rs");
}

#[cfg(feature = "sql")]
#[test]
fn public_trusted_sql_facade_compile_contract() {
    let t = trybuild::TestCases::new();
    t.pass("tests/pass-sql/**/*.rs");
}

#[cfg(feature = "sql")]
#[test]
fn source_declared_primary_key_update_policy_selects_exact_handler() {
    let t = trybuild::TestCases::new();
    t.pass("tests/pass-sql/endpoint_update_primary_key.rs");
}

#[cfg(feature = "sql")]
#[test]
fn source_declared_bounded_update_policy_selects_exact_handler() {
    let t = trybuild::TestCases::new();
    t.pass("tests/pass-sql/endpoint_update_bounded.rs");
}
