#[test]
fn public_facade_compile_contract() {
    {
        let t = trybuild::TestCases::new();
        t.compile_fail("tests/fail-endpoints/duplicate.rs");
        t.compile_fail("tests/fail-endpoints/duplicate_block.rs");
        t.compile_fail("tests/fail-endpoints/invalid_option.rs");
        t.compile_fail("tests/fail-endpoints/nested.rs");
        t.compile_fail("tests/fail-endpoints/unknown.rs");
        t.compile_fail("tests/fail-endpoints/unsupported_attribute.rs");
        t.compile_fail("tests/fail-endpoints/without_start.rs");

        #[cfg(not(feature = "sql"))]
        t.compile_fail("tests/fail-endpoints/missing_sql_capability.rs");

        #[cfg(feature = "sql")]
        t.compile_fail("tests/fail-endpoints/missing_test_admin_capability.rs");

        #[cfg(not(feature = "migration"))]
        {
            t.compile_fail("tests/fail-migration/missing_capability.rs");
            t.compile_fail("tests/fail-endpoints/missing_migration_capability.rs");
        }
    }

    // This collision is diagnosed only while linking. Including one maintained
    // pass case makes trybuild use `cargo build` for this isolated contract.
    let t = trybuild::TestCases::new();
    t.pass("tests/pass/endpoint_declarations.rs");
    t.compile_fail("tests/fail-endpoints/handwritten_duplicate.rs");
}
