#[test]
fn typed_write_identity_compile_contract() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/fail-typed-write/raw_ulid_identity.rs");
    t.compile_fail("tests/fail-typed-write/wrong_entity_identity.rs");
}

#[test]
fn request_execution_compile_contract() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/fail-request-execution/*.rs");
    t.pass("tests/pass/request_execution.rs");
}
