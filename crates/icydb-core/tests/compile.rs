//! Compile-time acceptance and rejection coverage for the public Core API.

#[test]
fn compile_ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/**/*.rs");
    t.pass("tests/pass/**/*.rs");
}
