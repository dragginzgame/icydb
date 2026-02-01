#[test]
fn compile_fail() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/primary_key_relation_fail.rs");
    t.compile_fail("tests/ui/primary_key_cardinality_fail.rs");
}
