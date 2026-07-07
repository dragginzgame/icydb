#[test]
fn public_facade_compile_contract() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/**/*.rs");
    t.pass("tests/pass/**/*.rs");
}
