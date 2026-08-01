#[test]
fn public_facade_compile_contract() {
    let t = trybuild::TestCases::new();
    t.pass("tests/pass/**/*.rs");
    t.compile_fail("tests/fail-endpoints/duplicate.rs");
    t.compile_fail("tests/fail-endpoints/nested.rs");
    t.compile_fail("tests/fail-endpoints/unknown.rs");
    t.compile_fail("tests/fail-endpoints/without_start.rs");

    #[cfg(not(feature = "sql"))]
    t.compile_fail("tests/fail-endpoints/missing_sql_capability.rs");
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
