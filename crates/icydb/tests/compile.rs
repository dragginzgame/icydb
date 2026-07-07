#[test]
fn public_facade_compile_contract() {
    let t = trybuild::TestCases::new();
    t.pass("tests/pass/**/*.rs");
}
