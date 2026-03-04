#[test]
fn compile_pass() {
    let t = trybuild::TestCases::new();
    t.pass("tests/pass/**/*.rs");
}
