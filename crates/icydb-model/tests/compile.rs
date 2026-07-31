#[test]
fn required_generated_trait_diagnostics() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/ui/required_traits/*.rs");
}

#[test]
fn typed_durable_rule_grammar() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/ui/rules/pass/*.rs");
    tests.compile_fail("tests/ui/rules/fail/*.rs");
}
