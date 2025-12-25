use crate::prelude::*;

///
/// Record
///

#[record(fields(
    field(
        ident = "duration_ms",
        value(item(
            prim = "Nat32",
            validator(path = "base::validator::num::Range", args(180000, 604800000))
        ))
    ),
    field(
        ident = "attempts",
        value(item(
            prim = "Nat32",
            validator(path = "base::validator::num::Range", args(1, 20))
        ))
    ),
    field(
        ident = "bytes",
        value(item(
            prim = "Blob",
            validator(path = "base::validator::len::Max", args(500))
        )),
    )
))]
pub struct Record {}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::core::{Error, validate};

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

        let err = validate(&r).expect_err("expected validation issues");
        let err_string = err.to_string();
        let issues = match &err {
            Error::ValidateError(issues) => issues,
            other => panic!("unexpected error: {other:?}"),
        };

        assert_eq!(issues.len(), 3);

        for key in ["duration_ms", "attempts", "bytes"] {
            let messages = issues
                .get(key)
                .unwrap_or_else(|| panic!("missing issues for {key}"));
            assert!(
                !messages.is_empty(),
                "expected validation messages for {key}"
            );
            assert!(
                err_string.contains(key),
                "expected error string to mention {key}"
            );
        }
    }
}
