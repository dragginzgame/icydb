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
    use icydb::core::validate;

    #[test]
    fn base_record_validation_fields_fail_as_expected() {
        let r = Record {
            duration_ms: 100,             // invalid (too low)
            attempts: 0,                  // invalid (too low)
            bytes: vec![0u8; 600].into(), // invalid (too long)
        };

        validate(&r).expect_err("validation should fail for invalid values");
    }
}
