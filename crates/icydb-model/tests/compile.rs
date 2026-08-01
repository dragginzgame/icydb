#[test]
fn required_generated_trait_diagnostics() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/ui/required_traits/*.rs");
}

#[test]
fn generated_custom_trait_policy() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/ui/generated_custom_traits/pass/*.rs");
    tests.compile_fail("tests/ui/generated_custom_traits/fail/*.rs");
}

#[test]
fn derive_and_helper_trait_policy() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/ui/derive_helper_traits/pass/*.rs");
    tests.compile_fail("tests/ui/derive_helper_traits/fail/*.rs");
}

#[test]
fn typed_durable_rule_grammar() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/ui/rules/pass/*.rs");
    tests.compile_fail("tests/ui/rules/fail/*.rs");
}
