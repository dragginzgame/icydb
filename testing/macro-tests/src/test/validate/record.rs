pub use icydb_testing_fixtures::macro_test::validate::record::*;

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::visitor::validate;

    #[test]
    fn base_record_validation_fields_fail_as_expected() {
        let r = Record {
            duration_ms: 100,             // invalid (too low)
            attempts: 0,                  // invalid (too low)
            bytes: vec![0u8; 600].into(), // invalid (too long)
        };

        validate(&r).expect_err("validation should fail for invalid values");
    }

    #[test]
    fn base_record_validation_reports_field_paths() {
        let r = Record {
            duration_ms: 100,
            attempts: 0,
            bytes: vec![0u8; 600].into(),
        };

        let err = validate(&r).expect_err("expected validation error");
        let issues = err.issues();

        for key in ["duration_ms", "attempts", "bytes"] {
            assert!(
                issues.contains_key(key),
                "expected error issues to include field `{key}`"
            );
        }
        assert_eq!(issues.len(), 3);
    }
}
