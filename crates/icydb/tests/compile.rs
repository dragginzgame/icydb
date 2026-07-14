#[test]
fn public_facade_compile_contract() {
    let t = trybuild::TestCases::new();
    t.pass("tests/pass/**/*.rs");
}

#[cfg(feature = "sql")]
#[test]
fn public_trusted_sql_facade_compile_contract() {
    let t = trybuild::TestCases::new();
    t.pass("tests/pass-sql/**/*.rs");
}
