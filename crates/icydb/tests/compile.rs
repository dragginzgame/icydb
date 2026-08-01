#[test]
fn public_facade_compile_contract() {
    let t = trybuild::TestCases::new();
    t.pass("tests/pass/**/*.rs");
    t.compile_fail("tests/fail-endpoints/**/*.rs");
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
